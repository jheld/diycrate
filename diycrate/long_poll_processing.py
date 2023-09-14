import configparser
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Generator, Union, Mapping

import boxsdk
from boxsdk.object.file import File
from boxsdk.object.folder import Folder
from boxsdk import Client, exception
from boxsdk.object.event import Event
from boxsdk.object.events import UserEventsStreamType, Events
from boxsdk.session.box_response import BoxResponse
from boxsdk.util.api_call_decorator import api_call
from boxsdk.util.lru_cache import LRUCache
from dateutil.parser import parse
from dateutil.tz import tzutc
from requests import Timeout
from send2trash import send2trash

from diycrate.cache_utils import (
    redis_set,
    r_c,
    redis_key,
    redis_get,
    local_or_box_file_m_time_key_func,
    redis_path_for_object_id_key,
)
from diycrate.gui import notify_user_with_gui
from diycrate.item_queue_io import (
    DownloadQueueItem,
    download_pool_executor,
    download_queue_processor,
)
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


def process_long_poll_event(client: Client, event: Union[Event, Mapping]):
    if event["event_type"] == "ITEM_CREATE":
        process_item_create_long_poll(client, event)
    if event["event_type"] == "ITEM_COPY":
        process_item_copy_long_poll(client, event)
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


def process_item_create_long_poll(client: Client, event: Union[Event, Mapping]):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
        return

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
        try:
            box_item = client.folder(folder_id=obj_id).get(
                fields=["id", "modified_at", "etag", "name"]
            )
            redis_set(
                r_c,
                box_item,
                datetime.fromtimestamp(os.path.getmtime(file_path))
                .astimezone(tzutc())
                .timestamp(),
                BOX_DIR,
                True,
                path,
            )
            time_data_map = {
                Path(file_path)
                .resolve()
                .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                .astimezone(tzutc())
                .timestamp()
            }
            for mkey, mvalue in time_data_map.items():
                r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
            r_c.set(
                local_or_box_file_m_time_key_func(path / box_item.name, True),
                parse(box_item.modified_at).astimezone(tzutc()).timestamp(),
            )
            r_c.set(
                redis_path_for_object_id_key(path / box_item.name), box_item.object_id
            )
            r_c.setex(
                f"diy_crate:event_ids:{event.event_id}",
                timedelta(days=32),
                path.as_posix(),
            )
        except boxsdk.exception.BoxAPIException as box_exc:
            if box_exc.status == 404 and box_exc.code == "trashed":
                crate_logger.debug(
                    f"Object {obj_id=} was previously deleted from Box. "
                    f"{path / event['source']['name']=}"
                )
                r_c.delete(redis_key(obj_id))
                r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )
            else:
                raise

    elif obj_type == "file":
        if not file_path.is_file():
            try:
                queue_item = DownloadQueueItem(
                    client.file(file_id=obj_id).get(),
                    path / event["source"]["name"],
                    client._oauth,
                    event,
                )
                download_pool_executor.apply_async(
                    download_queue_processor, kwds=dict(queue_item=queue_item)
                )
            except boxsdk.exception.BoxAPIException as box_exc:
                if box_exc.status == 404 and box_exc.code == "trashed":
                    crate_logger.debug(
                        f"Object {obj_id=} was previously deleted from Box. "
                        f"{path / event['source']['name']=}"
                    )
                    r_c.delete(redis_key(obj_id))
                    r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                    r_c.setex(
                        f"diy_crate:event_ids:{event.event_id}",
                        timedelta(days=32),
                        path.as_posix(),
                    )
                else:
                    raise


def process_item_copy_long_poll(client: Client, event: Union[Event, Mapping]):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
        return

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
        try:
            box_item = client.folder(folder_id=obj_id).get(
                fields=["id", "modified_at", "etag", "name"]
            )
            redis_set(
                r_c,
                box_item,
                datetime.fromtimestamp(os.path.getmtime(file_path))
                .astimezone(tzutc())
                .timestamp(),
                BOX_DIR,
                True,
                path,
            )
            time_data_map = {
                Path(file_path)
                .resolve()
                .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                .astimezone(tzutc())
                .timestamp()
            }
            for mkey, mvalue in time_data_map.items():
                r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
            r_c.set(
                local_or_box_file_m_time_key_func(path / box_item.name, True),
                parse(box_item.modified_at).astimezone(tzutc()).timestamp(),
            )
            r_c.set(
                redis_path_for_object_id_key(path / box_item.name), box_item.object_id
            )
            r_c.setex(
                f"diy_crate:event_ids:{event.event_id}",
                timedelta(days=32),
                path.as_posix(),
            )
        except boxsdk.exception.BoxAPIException as box_exc:
            if box_exc.status == 404 and box_exc.code == "trashed":
                crate_logger.debug(
                    f"Object {obj_id=} was previously deleted from Box. "
                    f"{path / event['source']['name']=}"
                )
                r_c.delete(redis_key(obj_id))
                r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )
            else:
                raise

    elif obj_type == "file":
        if not file_path.is_file():
            try:
                box_item = client.file(file_id=obj_id).get()
                queue_item = DownloadQueueItem(
                    box_item,
                    path / event["source"]["name"],
                    client._oauth,
                    event,
                )
                download_pool_executor.apply_async(
                    download_queue_processor, kwds=dict(queue_item=queue_item)
                )

            except boxsdk.exception.BoxAPIException as box_exc:
                if box_exc.status == 404 and box_exc.code == "trashed":
                    crate_logger.debug(
                        f"Object {obj_id=} was previously deleted from Box. "
                        f"{path / event['source']['name']=}"
                    )
                    r_c.delete(redis_key(obj_id))
                    r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                    r_c.setex(
                        f"diy_crate:event_ids:{event.event_id}",
                        timedelta(days=32),
                        path.as_posix(),
                    )
                else:
                    raise


def process_item_trash_long_poll(event: Union[Event, Mapping]):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
        return

    if obj_type == "file":
        process_item_trash_file(event, obj_id)
    elif obj_type == "folder":
        process_item_trash_folder(event, obj_id)


def process_item_trash_file(event: Union[Event, Mapping], obj_id):
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
    file_path: Path
    if not item_info:
        file_path = path / event["source"]["name"]
    else:
        file_path = path / item_info["file_path"]
    if file_path.exists():
        send2trash(file_path.as_posix())
    if r_c.exists(redis_key(obj_id)):
        r_c.delete(redis_key(obj_id), redis_path_for_object_id_key(file_path))
        r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
    notify_user_with_gui(
        "Box message: Deleted:",
        file_path.as_posix(),
        crate_logger=crate_logger,
        expire_time=10000,
    )
    r_c.setex(
        f"diy_crate:event_ids:{event.event_id}", timedelta(days=32), path.as_posix()
    )


def process_item_trash_folder(event: Union[Event, Mapping], obj_id):
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
        for sub_box_id in get_sub_ids(obj_id):
            cur_sub_box_redis_data = r_c.get(redis_key(sub_box_id))
            if cur_sub_box_redis_data:
                cur_sub_box_item_info = json.loads(
                    str(cur_sub_box_redis_data, encoding="utf-8", errors="strict")
                )
                if cur_sub_box_item_info:
                    r_c.delete(cur_sub_box_item_info["file_path"])
            r_c.delete(redis_key(sub_box_id))
        r_c.delete(redis_key(obj_id))
        shutil.rmtree(file_path)
        obj_cache_data_raw = r_c.get(redis_key(obj_id))
        obj_cache_data = (
            json.loads(str(obj_cache_data_raw))
            if obj_cache_data_raw is not None
            else obj_cache_data_raw
        )
        parent_id = obj_cache_data.get("parent_id") if obj_cache_data else None
        if parent_id:
            parent_folder_raw = r_c.get(redis_key(parent_id))
            parent_folder = (
                json.loads(str(parent_folder_raw))
                if parent_folder_raw is not None
                else parent_folder_raw
            )
            sub_ids = (
                parent_folder.get("sub_ids", [])
                if parent_folder is not None
                else parent_folder
            )
            if sub_ids:
                sub_ids.remove(obj_id)
                r_c.set(redis_key(parent_id), json.dumps(parent_folder))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
        deletion_msg = ["Box message: Deleted:", file_path.as_posix()]
        crate_logger.info(" ".join(deletion_msg))
        notify_user_with_gui(*deletion_msg, crate_logger=crate_logger)
        r_c.setex(
            f"diy_crate:event_ids:{event.event_id}", timedelta(days=32), path.as_posix()
        )


def process_item_upload_long_poll(client: Client, event: Union[Event, Mapping]):
    event_source: Union[File, Folder] = event["source"]
    obj_id = event_source["id"]
    obj_type = event_source["type"]
    if r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
        return

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
        try:
            box_file_for_download: File = event["source"]
            queue_item = DownloadQueueItem(
                box_file_for_download,
                path / event["source"]["name"],
                client._oauth,
                event,
            )

            download_pool_executor.apply_async(
                download_queue_processor, kwds=dict(queue_item=queue_item)
            )

        except boxsdk.exception.BoxAPIException as box_exc:
            if box_exc.status == 404 and box_exc.code == "trashed":
                crate_logger.debug(
                    f"Object {obj_id=} was previously deleted from Box. "
                    f"{path / event['source']['name']=}"
                )
                r_c.delete(redis_key(obj_id))
                r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )
            else:
                raise
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
            try:
                box_item: Folder = client.folder(folder_id=obj_id).get(
                    fields=["id", "name", "etag", "modified_at"]
                )
                redis_set(
                    r_c,
                    box_item,
                    datetime.fromtimestamp(os.path.getmtime(file_path))
                    .astimezone(tzutc())
                    .timestamp(),
                    BOX_DIR,
                    True,
                    path,
                )
            except boxsdk.exception.BoxAPIException as box_exc:
                if box_exc.status == 404 and box_exc.code == "trashed":
                    crate_logger.debug(
                        f"Object {obj_id=} was previously deleted from Box. {file_path=}"
                    )
                    r_c.setex(
                        f"diy_crate:event_ids:{event.event_id}",
                        timedelta(days=32),
                        path.as_posix(),
                    )
                    r_c.delete(redis_key(obj_id))
                    r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                else:
                    raise
            box_message = "new folder"
        notify_user_with_gui(
            f"Box message: {box_message}:",
            file_path.as_posix(),
            crate_logger=crate_logger,
        )
        r_c.setex(
            f"diy_crate:event_ids:{event.event_id}", timedelta(days=32), path.as_posix()
        )


def process_item_rename_long_poll(client: Client, event: Union[Event, Mapping]):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
        return
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
        try:
            file_obj = client.file(file_id=obj_id).get()
            src_file_path = (
                None
                if not r_c.exists(redis_key(obj_id))
                else Path(redis_get(r_c, file_obj)["file_path"])
            )
            if (
                src_file_path
                and src_file_path.exists()
                and (not file_path.exists() or not src_file_path.samefile(file_path))
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
                    .astimezone(tzutc())
                    .timestamp()
                }
                for mkey, mvalue in time_data_map.items():
                    r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
                r_c.set(
                    local_or_box_file_m_time_key_func(path / file_obj.name, True),
                    parse(file_obj.modified_at).astimezone(tzutc()).timestamp(),
                )
                r_c.set(
                    redis_path_for_object_id_key(path / file_obj.name),
                    file_obj.object_id,
                )
                r_c.delete(
                    redis_path_for_object_id_key(src_file_path),
                    local_or_box_file_m_time_key_func(src_file_path, False),
                    local_or_box_file_m_time_key_func(src_file_path, True),
                )
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )

            else:
                try:
                    version_info = redis_get(r_c, obj=file_obj)
                except TypeError:
                    crate_logger.error(
                        f"Key likely did not exist in the cache for file_obj id "
                        f"{file_obj.object_id=}",
                        exc_info=True,
                    )
                else:
                    if (
                        not file_path.exists()
                        or file_obj["etag"] != version_info["etag"]
                    ):
                        crate_logger.info(
                            f"Downloading the version from box, "
                            f"on a rename operation, "
                            f"and the etag (local {version_info['etag']=}) "
                            f"(box {file_obj['etag']=}) is different"
                        )
                        queue_item = DownloadQueueItem(
                            file_obj, file_path, client._oauth, event
                        )
                        download_pool_executor.apply_async(
                            download_queue_processor, kwds=dict(queue_item=queue_item)
                        )

        except boxsdk.exception.BoxAPIException as box_exc:
            if box_exc.status == 404 and box_exc.code == "trashed":
                crate_logger.debug(
                    f"Object {obj_id=} was previously deleted from Box. {file_path=}"
                )
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )
                r_c.delete(redis_key(obj_id))
                r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
            else:
                raise

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
        try:
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
                    .astimezone(tzutc())
                    .timestamp()
                }
                for mkey, mvalue in time_data_map.items():
                    r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
                r_c.set(
                    local_or_box_file_m_time_key_func(path / folder_obj.name, True),
                    parse(folder_obj.modified_at).astimezone(tzutc()).timestamp(),
                )
                r_c.set(
                    redis_path_for_object_id_key(path / folder_obj.name),
                    folder_obj.object_id,
                )
                r_c.delete(local_or_box_file_m_time_key_func(src_file_path, False))
                r_c.delete(local_or_box_file_m_time_key_func(src_file_path, True))
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )
        except boxsdk.exception.BoxAPIException as box_exc:
            if box_exc.status == 404 and box_exc.code == "trashed":
                crate_logger.debug(
                    f"Object {obj_id=} was previously deleted from Box. {file_path=}"
                )
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )
                r_c.delete(redis_key(obj_id))
                r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
            else:
                raise


def process_item_move_long_poll(event: Union[Event, Mapping]):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
        return
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
            if (
                obj_type == "file"
                and not src_file_path.exists()
                and file_path.exists()
                and item_info["etag"] != event["source"]["etag"]
            ):
                item_info["file_path"] = file_path.as_posix()
                item_info["etag"] = event["source"]["etag"]
                crate_logger.info(f"{event['source']['etag']=} updated.")
                r_c.set(redis_key(obj_id), json.dumps(item_info))
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )
                time_data_map = {
                    Path(file_path)
                    .resolve()
                    .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                    .astimezone(tzutc())
                    .timestamp()
                }
                for mkey, mvalue in time_data_map.items():
                    r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
                r_c.set(
                    local_or_box_file_m_time_key_func(file_path, True),
                    parse(event["source"]["modified_at"])
                    .astimezone(tzutc())
                    .timestamp(),
                )
            elif src_file_path.exists():
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
                            orig_len = len(src_file_path.as_posix().split(os.path.sep))
                            tail = os.path.sep.join(
                                sub_item_info["file_path"].split(os.path.sep)[orig_len:]
                            )
                            new_sub_path = file_path / tail
                            sub_item_info["file_path"] = new_sub_path.as_posix()
                            to_set.append(
                                partial(
                                    r_c.set,
                                    redis_key(sub_id),
                                    json.dumps(sub_item_info),
                                )
                            )
                            to_set.append(
                                partial(
                                    r_c.set,
                                    redis_path_for_object_id_key(new_sub_path),
                                    sub_id,
                                )
                            )
                shutil.move(src_file_path, file_path)
                for item in to_set:
                    item()
                item_info["file_path"] = file_path.as_posix()
                item_info["etag"] = event["source"]["etag"]
                r_c.set(redis_key(obj_id), json.dumps(item_info))
                time_data_map = {
                    Path(file_path)
                    .resolve()
                    .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                    .astimezone(tzutc())
                    .timestamp()
                }
                for mkey, mvalue in time_data_map.items():
                    r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
                r_c.set(
                    local_or_box_file_m_time_key_func(file_path, True),
                    parse(event["source"]["modified_at"])
                    .astimezone(tzutc())
                    .timestamp(),
                )
                r_c.set(redis_path_for_object_id_key(file_path), obj_id)
                r_c.delete(redis_path_for_object_id_key(src_file_path))
                r_c.delete(local_or_box_file_m_time_key_func(src_file_path, False))
                r_c.delete(local_or_box_file_m_time_key_func(src_file_path, True))
                r_c.setex(
                    f"diy_crate:event_ids:{event.event_id}",
                    timedelta(days=32),
                    path.as_posix(),
                )


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
    sub_ids = item_info.get("sub_ids")
    sub_ids = sub_ids if sub_ids is not None else []
    for sub_id in sub_ids:
        sub_id_cache_raw = r_c.get(redis_key(sub_id))
        if sub_id_cache_raw is not None:
            sub_item_info = json.loads(
                str(sub_id_cache_raw, encoding="utf-8", errors="strict")
            )
            if os.path.isdir(sub_item_info["file_path"]):
                ids.extend(get_sub_ids(sub_id))
            ids.append(sub_id)
    return ids


BOX_EVENT_IGNORED_TYPES = ["ITEM_PREVIEW", "ITEM_DOWNLOAD"]


class CustomBoxEvents(Events):
    @api_call
    def generate_events_with_long_polling(
        self, stream_position=None, stream_type=UserEventsStreamType.ALL
    ):
        """
        Subscribe to events from the given stream position.

        :param stream_position:
            The location in the stream from which to start getting events.
            0 is the beginning of time. 'now' will
            return no events and just current stream position.
        :type stream_position:
            `unicode`
        :param stream_type:
            (optional) Which type of events to return.
            Defaults to `UserEventsStreamType.ALL`.

            NOTE: Currently, the Box API requires this to be one of the user
            events stream types. The request will fail if an enterprise events
            stream type is passed.
        :type stream_type:
            :enum:`UserEventsStreamType`
        :returns:
            Events corresponding to changes on Box in realtime, as they come in.
        :rtype:
            `generator` of :class:`Event`
        """
        event_ids = LRUCache()
        stream_position = (
            stream_position
            if stream_position is not None
            else self.get_latest_stream_position(stream_type=stream_type)
        )
        while True:
            options = self.get_long_poll_options(stream_type=stream_type)
            while True:
                try:
                    long_poll_response: BoxResponse = self.long_poll(  # noqa
                        options, stream_position
                    )
                except Timeout:
                    break
                else:
                    message = long_poll_response.json()["message"]
                    if message == "new_change":
                        next_stream_position = stream_position
                        for event, next_stream_position in self._get_all_events_since(
                            stream_position, stream_type=stream_type
                        ):
                            try:
                                event_ids.get(event["event_id"])
                            except KeyError:
                                yield event, next_stream_position
                                event_ids.set(event["event_id"])
                        stream_position = next_stream_position
                        break
                    elif message == "reconnect":
                        continue
                    else:
                        break


class CustomBoxClient(Client):
    def events(self):
        """
        Get an events object that can get the latest events from Box
        or set up a long polling event subscription.
        """
        return CustomBoxEvents(self._session)


def long_poll_event_listener(file_event_handler):
    """
    Receive and process remote cloud item events in real-time
    :return:
    """
    handler = file_event_handler
    client = CustomBoxClient(oauth=handler.oauth)
    long_poll_streamer = client.events()

    while True:
        try:
            crate_logger.info("Determining stream position.")
            next_streamed_position = r_c.get("diy_crate.box.next_stream_position")
            if next_streamed_position:
                stream_position = str(
                    next_streamed_position, encoding="utf-8", errors="strict"
                )
                crate_logger.debug(
                    f"Using cached latest stream position: {str(stream_position)=}"
                )

            else:
                crate_logger.info("About to get latest stream position.")
                stream_position = long_poll_streamer.get_latest_stream_position()
                crate_logger.debug(
                    f"Using as latest stream position: {str(stream_position)=}"
                )
                r_c.setex(
                    "diy_crate.box.next_stream_position",
                    timedelta(days=32),
                    stream_position,
                )
            event_stream: Generator[
                Union[Mapping, Event], None, None
            ] = long_poll_streamer.generate_events_with_long_polling(stream_position)
            for event, next_stream_position in event_stream:
                if event.get("message", "").lower() == "reconnect":
                    break
                if event.event_type in BOX_EVENT_IGNORED_TYPES:
                    continue
                if r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
                    continue
                ev_source = event["source"]
                ev_src_name = getattr(ev_source, "name", None)
                ev_src_etag = getattr(ev_source, "etag", None)
                ev_src_modified_at = getattr(ev_source, "modified_at", None)
                ev_src_id = getattr(ev_source, "id", None)
                event_message = (
                    f"{event=} happened! {event.event_type=} "
                    f"{event.created_at=}, {event.event_id=}, "
                    f"{ev_src_name=}, "
                    f"{ev_src_etag=}, "
                    f"{ev_src_modified_at=}, "
                    f"{ev_src_id=}"
                )
                crate_logger.debug(event_message)
                process_long_poll_event(client, event)
                crate_logger.debug(
                    f"Will set diy_crate.box.next_stream_position to {next_stream_position=}"
                )
                r_c.setex(
                    "diy_crate.box.next_stream_position",
                    timedelta(days=32),
                    str(next_stream_position),
                )
        except (exception.BoxAPIException, AttributeError):
            crate_logger.warning("Box or AttributeError occurred.", exc_info=True)
        except Exception:
            crate_logger.warning("General error occurred.", exc_info=True)
