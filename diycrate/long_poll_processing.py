import configparser
import json
import logging
import os
import shutil
import time
from datetime import datetime
from functools import partial
from pathlib import Path

import dateutil
from boxsdk import Client, exception
from dateutil.parser import parse
from send2trash import send2trash

from diycrate.cache_utils import (
    redis_set,
    r_c,
    redis_key,
    redis_get,
    local_or_box_file_m_time_key_func,
)
from diycrate.gui import notify_user_with_gui
from diycrate.item_queue_io import download_queue
from diycrate.log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)

conf_obj = configparser.ConfigParser()
conf_dir = Path("~/.config/diycrate").expanduser().resolve()
if not conf_dir.is_dir():
    conf_dir.mkdir()
cloud_credentials_file_path = conf_dir / "box.ini"
if not cloud_credentials_file_path.is_file():
    cloud_credentials_file_path.write_text("")
conf_obj.read(cloud_credentials_file_path)
BOX_DIR = Path(conf_obj["box"]["directory"]).expanduser().resolve()


def process_long_poll_event(client, event):
    if event["event_type"] == "ITEM_CREATE":
        process_item_create_long_poll(client, event)
    if event["event_type"] == "ITEM_MOVE":
        process_item_move_long_poll(event)
    if event["event_type"] == "ITEM_RENAME":
        process_item_rename_long_poll(client, event)
    elif event["event_type"] == "ITEM_UPLOAD":
        process_item_upload_long_poll(client, event)
    elif event["event_type"] == "ITEM_TRASH":
        process_item_trash_long_poll(event)
    elif event["event_type"] == "ITEM_DOWNLOAD":
        pass


def process_item_create_long_poll(client, event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if int(event["source"]["path_collection"]["total_count"]) > 1:
        path = Path(
            *list(
                folder["name"]
                for folder in event["source"]["path_collection"]["entries"][1:]
            )
        )
    else:
        path = Path()
    path = BOX_DIR / path
    if not path.is_dir():
        os.makedirs(path)
    file_path = path / event["source"]["name"]
    if obj_type == "folder":
        if not file_path.is_dir():
            os.makedirs(file_path)
        box_item = client.folder(folder_id=obj_id).get(
            fields=["id", "modified_at", "etag", "name"]
        )
        redis_set(
            r_c,
            box_item,
            datetime.fromtimestamp(os.path.getmtime(file_path))
            .astimezone(dateutil.tz.tzutc())
            .timestamp(),
            BOX_DIR,
            True,
            path,
        )
        time_data_map = {
            Path(file_path)
            .resolve()
            .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
            .astimezone(dateutil.tz.tzutc())
            .timestamp()
        }
        for mkey, mvalue in time_data_map.items():
            r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
        r_c.set(
            local_or_box_file_m_time_key_func(path / box_item.name, True),
            parse(box_item.modified_at).astimezone(dateutil.tz.tzutc()).timestamp(),
        )

    elif obj_type == "file":
        if not file_path.is_file():
            download_queue.put(
                [
                    client.file(file_id=obj_id).get(),
                    path / event["source"]["name"],
                    client._oauth,
                ]
            )


def process_item_trash_long_poll(event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type == "file":
        process_item_trash_file(event, obj_id)
    elif obj_type == "folder":
        process_item_trash_folder(event, obj_id)


def process_item_trash_file(event, obj_id):
    item_info = r_c.get(redis_key(obj_id))
    if item_info:
        item_info = json.loads(str(item_info, encoding="utf-8", errors="strict"))
    if int(event["source"]["path_collection"]["total_count"]) > 1:
        path = Path(
            *list(
                folder["name"]
                for folder in event["source"]["path_collection"]["entries"][1:]
            )
        )
    else:
        path = Path()
    path = BOX_DIR / path
    if not item_info:
        file_path = path / event["source"]["name"]
    else:
        file_path = path / item_info["file_path"]
    if file_path.exists():
        send2trash(file_path.as_posix())
    if r_c.exists(redis_key(obj_id)):
        r_c.delete(redis_key(obj_id))
        r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
    notify_user_with_gui(
        "Box message: Deleted {}".format(file_path), crate_logger, expire_time=10000
    )


def process_item_trash_folder(event, obj_id):
    item_info = r_c.get(redis_key(obj_id))
    if item_info:
        item_info = json.loads(str(item_info, encoding="utf-8", errors="strict"))
    if int(event["source"]["path_collection"]["total_count"]) > 1:
        path = Path(
            *list(
                folder["name"]
                for folder in event["source"]["path_collection"]["entries"][1:]
            )
        )
    else:
        path = Path()
    path = BOX_DIR / path
    if not item_info:
        file_path = path / event["source"]["name"]
    else:
        file_path = path / item_info["file_path"]
    if file_path.is_dir():
        # still need to get the parent_id
        for box_id in get_sub_ids(obj_id):
            r_c.delete(redis_key(box_id))
        r_c.delete(redis_key(obj_id))
        shutil.rmtree(file_path)
        obj_cache_data = r_c.get(redis_key(obj_id))
        parent_id = obj_cache_data.get("parent_id") if obj_cache_data else None
        if parent_id:
            parent_folder = r_c.get(redis_key(parent_id))
            sub_ids = parent_folder.get("sub_ids", [])
            if sub_ids:
                sub_ids.remove(obj_id)
                r_c.set(redis_key(parent_id), json.dumps(parent_folder))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
        deletion_msg = f"Box message: Deleted {file_path}"
        crate_logger.info(deletion_msg)
        notify_user_with_gui(deletion_msg, crate_logger)


def process_item_upload_long_poll(client, event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type == "file":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = Path(
                *list(
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                )
            )
        else:
            path = Path()
        path = BOX_DIR / path
        if not path.exists():  # just in case this is a file in a new subfolder
            os.makedirs(path)
        crate_logger.debug(
            f"Submitting {path / event['source']['name']=} onto the download queue."
        )
        download_queue.put(
            [
                client.file(file_id=obj_id).get(
                    fields=["modified_at", "etag", "name", "path_collection"]
                ),
                path / event["source"]["name"],
                client._oauth,
            ]
        )
    elif obj_type == "folder":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = Path(
                *list(
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                )
            )
        else:
            path = Path()
        path = BOX_DIR / path
        if not path.exists():  # just in case this is a file in a new subfolder
            os.makedirs(path)
        file_path = path / event["source"]["name"]
        box_message = "new version"
        if not file_path.exists():
            os.makedirs(file_path)
            box_item = client.folder(folder_id=obj_id).get(
                fields=["id", "name", "etag", "modified_at"]
            )
            redis_set(
                r_c,
                box_item,
                datetime.fromtimestamp(os.path.getmtime(file_path))
                .astimezone(dateutil.tz.tzutc())
                .timestamp(),
                BOX_DIR,
                True,
                path,
            )
            box_message = "new folder"
        notify_user_with_gui(
            "Box message: {} {}".format(box_message, file_path), crate_logger
        )


def process_item_rename_long_poll(client, event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type == "file":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = Path(
                *list(
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                )
            )
        else:
            path = Path()
        path = BOX_DIR / path
        file_path = path / event["source"]["name"]
        file_obj = client.file(file_id=obj_id).get()
        src_file_path = (
            None
            if not r_c.exists(redis_key(obj_id))
            else Path(redis_get(r_c, file_obj)["file_path"])
        )
        if (
            src_file_path
            and src_file_path.exists()
            and not src_file_path.samefile(file_path)
        ):
            version_info = redis_get(r_c, obj=file_obj)
            os.rename(src_file_path, file_path)
            version_info["file_path"] = file_path.as_posix()
            version_info["etag"] = file_obj["etag"]
            r_c.set(redis_key(obj_id), json.dumps(version_info))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
            time_data_map = {
                Path(file_path)
                .resolve()
                .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                .astimezone(dateutil.tz.tzutc())
                .timestamp()
            }
            for mkey, mvalue in time_data_map.items():
                r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
            r_c.set(
                local_or_box_file_m_time_key_func(path / file_obj.name, True),
                parse(file_obj.modified_at).astimezone(dateutil.tz.tzutc()).timestamp(),
            )
            r_c.delete(local_or_box_file_m_time_key_func(src_file_path, False))
            r_c.delete(local_or_box_file_m_time_key_func(src_file_path, True))
        else:
            try:
                version_info = redis_get(r_c, obj=file_obj)
            except TypeError:
                crate_logger.error(
                    f"Key likely did not exist in the cache for file_obj id {file_obj.object_id=}",
                    exc_info=True,
                )
            else:
                if not file_path.exists() or file_obj["etag"] != version_info["etag"]:
                    crate_logger.info(
                        f"Downloading the version from box, "
                        f"on a rename operation, "
                        f"and the etag (local {version_info['etag']=}) "
                        f"(box {file_obj['etag']=}) is different"
                    )
                    download_queue.put([file_obj, file_path, client._oauth])
    elif obj_type == "folder":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = Path(
                *list(
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                )
            )
        else:
            path = Path()
        path = BOX_DIR / path
        file_path = path / event["source"]["name"]
        folder_obj = client.folder(folder_id=obj_id).get()
        src_file_path = (
            None
            if not r_c.exists(redis_key(obj_id))
            else Path(redis_get(r_c, folder_obj)["file_path"])
        )
        if src_file_path and src_file_path.exists():
            os.rename(src_file_path, file_path)
            version_info = redis_get(r_c, obj=folder_obj)
            version_info["file_path"] = file_path.as_posix()
            version_info["etag"] = folder_obj["etag"]
            r_c.set(redis_key(obj_id), json.dumps(version_info))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
            time_data_map = {
                Path(file_path)
                .resolve()
                .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                .astimezone(dateutil.tz.tzutc())
                .timestamp()
            }
            for mkey, mvalue in time_data_map.items():
                r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
            r_c.set(
                local_or_box_file_m_time_key_func(path / folder_obj.name, True),
                parse(folder_obj.modified_at)
                .astimezone(dateutil.tz.tzutc())
                .timestamp(),
            )
            r_c.delete(local_or_box_file_m_time_key_func(src_file_path, False))
            r_c.delete(local_or_box_file_m_time_key_func(src_file_path, True))


def process_item_move_long_poll(event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type in ("file", "folder"):
        if r_c.exists(redis_key(obj_id)):
            item_info = json.loads(
                str(r_c.get(redis_key(obj_id)), encoding="utf-8", errors="strict")
            )
            src_file_path = Path(
                json.loads(
                    str(r_c.get(redis_key(obj_id)), encoding="utf-8", errors="strict")
                )["file_path"]
            )
            if int(event["source"]["path_collection"]["total_count"]) > 1:
                path = Path(
                    *list(
                        folder["name"]
                        for folder in event["source"]["path_collection"]["entries"][1:]
                    )
                )
            else:
                path = Path()
            path = BOX_DIR / path
            file_path = path / event["source"]["name"]
            if src_file_path.exists():
                to_set = []
                if obj_type == "folder":
                    for sub_id in get_sub_ids(obj_id):
                        if r_c.exists(redis_key(sub_id)):
                            sub_item_info = json.loads(
                                str(
                                    r_c.get(redis_key(sub_id)),
                                    encoding="utf-8",
                                    errors="strict",
                                )
                            )
                            orig_len = len(src_file_path.split(os.path.sep))
                            tail = os.path.sep.join(
                                sub_item_info["file_path"].split(os.path.sep)[orig_len:]
                            )
                            new_sub_path = file_path / tail
                            sub_item_info["file_path"] = new_sub_path
                            to_set.append(
                                partial(
                                    r_c.set,
                                    redis_key(sub_id),
                                    json.dumps(sub_item_info),
                                )
                            )
                shutil.move(src_file_path, file_path)
                for item in to_set:
                    item()
                item_info["file_path"] = file_path
                item_info["etag"] = event["source"]["etag"]
                r_c.set(redis_key(obj_id), json.dumps(item_info))
                time_data_map = {
                    Path(file_path)
                    .resolve()
                    .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                    .astimezone(dateutil.tz.tzutc())
                    .timestamp()
                }
                for mkey, mvalue in time_data_map.items():
                    r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
                r_c.set(
                    local_or_box_file_m_time_key_func(file_path, True),
                    parse(event["source"]["modified_at"])
                    .astimezone(dateutil.tz.tzutc())
                    .timestamp(),
                )
                r_c.delete(local_or_box_file_m_time_key_func(src_file_path, False))
                r_c.delete(local_or_box_file_m_time_key_func(src_file_path, True))


def get_sub_ids(box_id):
    """
    Retrieve the box item ids that are stored in this sub path
    :param box_id:
    :return:
    """
    ids = []
    item_info = json.loads(
        str(r_c.get(redis_key(box_id)), encoding="utf-8", errors="strict")
    )
    for sub_id in item_info["sub_ids"]:
        sub_item_info = json.loads(
            str(r_c.get(redis_key(sub_id)), encoding="utf-8", errors="strict")
        )
        if os.path.isdir(sub_item_info["file_path"]):
            ids.extend(get_sub_ids(sub_id))
        ids.append(sub_id)
    return ids


def long_poll_event_listener(file_event_handler):
    """
    Receive and process remote cloud item events in real-time
    :return:
    """
    handler = file_event_handler
    client = Client(oauth=handler.oauth)
    long_poll_streamer = client.events()

    while True:
        try:
            stream_position = long_poll_streamer.get_latest_stream_position()
            event_stream = long_poll_streamer.generate_events_with_long_polling(
                stream_position
            )
            for event in event_stream:
                event_message = (
                    f"{str(event)=} happened! {event.event_type=} {event.created_at=}"
                )
                crate_logger.debug(event_message)
                if event.get("message", "").lower() == "reconnect":
                    break
                process_long_poll_event(client, event)
        except (exception.BoxAPIException, AttributeError):
            crate_logger.warning("Box or AttributeError occurred.", exc_info=True)
        except Exception:
            crate_logger.warning("General error occurred.", exc_info=True)
