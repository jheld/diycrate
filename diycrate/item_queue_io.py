import configparser
import json
import multiprocessing.pool
import os
import random
import time
import logging
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Callable, Any, List, NamedTuple, Union, Mapping

import boxsdk.exception
import dateutil
from boxsdk import Client, OAuth2
from boxsdk.exception import BoxAPIException
from boxsdk.object.event import Event
from boxsdk.object.file import File
from boxsdk.object.folder import Folder
from boxsdk.object.user import User
from dateutil.parser import parse
from dateutil.tz import tzutc
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError

from .cache_utils import (
    redis_key,
    redis_set,
    redis_get,
    r_c,
    local_or_box_file_m_time_key_func,
    redis_path_for_object_id_key,
)
from .gui import notify_user_with_gui
from .log_utils import setup_logger
from .oauth_utils import setup_remote_oauth

setup_logger()

crate_logger = logging.getLogger(__name__)


class DownloadQueueItem(NamedTuple):
    item: Union[File, Folder]
    path: Union[Path, str]
    oauth: OAuth2
    event: Union[Event, Mapping, None]


class UploadQueueItemReal(NamedTuple):
    timestamp: float
    callable_up: Callable[..., Any]
    oauth: OAuth2
    explicit_file_path: Union[Path, str, None]


UploadQueueItem = Union[Callable[..., Any], UploadQueueItemReal]

uploads_given_up_on: List[Callable[..., Any]] = []


def upload_queue_processor(queue_item: UploadQueueItem):
    """
    Implements a simple re-try mechanism for pending uploads
    :return:
    """
    callable_up = queue_item
    # TODO: pass in the actual item being updated/uploaded,
    #  so we can do more intelligent retry mechanisms
    was_list = isinstance(callable_up, (list, tuple))
    last_modified_time = oauth = explicit_file_path = None
    if was_list:
        original_callable_up = callable_up
        last_modified_time, callable_up, oauth = original_callable_up[:3]
        if len(original_callable_up) > 3:
            explicit_file_path = original_callable_up[3]
        elif isinstance(callable_up, partial):
            explicit_file_path = callable_up.args[0]
    args = callable_up.args if isinstance(callable_up, partial) else None
    num_retries = 15
    perform_upload(
        args,
        callable_up,
        last_modified_time,
        num_retries,
        oauth,
        explicit_file_path,
        was_list,
        retry_limit=num_retries,
    )


def perform_upload(
    args,
    callable_up,
    last_modified_time,
    num_retries,
    oauth,
    explicit_file_path,
    was_list,
    retry_limit=15,
):
    for x in range(retry_limit):
        try:
            crate_logger.debug(
                f"Attempt upload{' ' + explicit_file_path if was_list else ''}"
            )
            ret_val = callable_up()
            post_ret_callable = None
            if isinstance(ret_val, tuple):
                ret_val, post_ret_callable = ret_val
            crate_logger.info(
                f"Completed upload/operation/{' ' + explicit_file_path if was_list else ''}"
            )
            if was_list:
                path_name = explicit_file_path
                item = ret_val  # is the new/updated item
                client = Client(oauth)
                if isinstance(item, File):
                    file_obj: File = client.file(file_id=item.object_id).get(
                        fields=["path_collection", "name", "etag", "modified_at"]
                    )
                    redis_set(r_c, file_obj, last_modified_time, box_dir_path=BOX_DIR)
                    r_c.set(
                        local_or_box_file_m_time_key_func(path_name, False),
                        datetime.fromtimestamp(Path(path_name).stat().st_mtime)
                        .astimezone(dateutil.tz.tzutc())
                        .timestamp(),
                    )
                    r_c.set(
                        local_or_box_file_m_time_key_func(path_name, True),
                        parse(file_obj.modified_at).astimezone(tzutc()).timestamp(),
                    )
                    r_c.set(redis_path_for_object_id_key(path_name), item.object_id)

                    path_builder = BOX_DIR
                    # oauth = setup_remote_oauth(r_c, conf=conf_obj)
                    # client = Client(oauth)
                    for entry in file_obj.path_collection["entries"]:
                        if entry.id == "0":
                            continue
                        path_builder /= entry.name
                        folder_entry = client.folder(entry.id).get(
                            fields=["modified_at"]
                        )
                        r_c.set(
                            local_or_box_file_m_time_key_func(path_builder, True),
                            parse(folder_entry.modified_at)
                            .astimezone(dateutil.tz.tzutc())
                            .timestamp(),
                        )
                        r_c.set(
                            redis_path_for_object_id_key(path_builder),
                            folder_entry.object_id,
                        )
                if isinstance(post_ret_callable, partial):
                    post_ret_callable()
                user_resource: User = client.user()
                user: User = user_resource.get(fields=["space_used", "space_amount"])
                crate_logger.debug(
                    "User space used/amount:  %d%% -> (used: %dGiB, total: %dGiB)",
                    (user.space_used / user.space_amount) * 100,
                    user.space_used / 1024 / 1024 / 1024,
                    user.space_amount / 1024 / 1024 / 1024,
                )
            break
        except BoxAPIException as e:
            crate_logger.info(f"{args}", exc_info=True)
            if e.status == 400 and e.code == "item_name_invalid":
                crate_logger.warning(
                    f"darn, bad name on the file (box does not like), {args}, cannot upload.",
                    exc_info=True,
                )
                break
            if e.status == 409:
                crate_logger.warning(
                    f"Apparently Box says this item already exists..."
                    f"and we were trying to create it. "
                    f"Need to handle this better. message: {e.message}",
                    exc_info=True,
                )
                break
        except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError):
            sleep_time = random.randint(0, 8)
            time.sleep(sleep_time)
            crate_logger.debug(f"{args}", exc_info=True)
            if x >= num_retries - 1:
                crate_logger.warning(
                    f"Upload giving up on: {callable_up}", exc_info=True
                )
                # no immediate plans to do anything with this info, yet.
                uploads_given_up_on.append(callable_up)
        except (TypeError, FileNotFoundError):
            crate_logger.debug("Error occurred", exc_info=True)
            break


def download_queue_processor(queue_item: "DownloadQueueItem"):
    """
    Implements a simple re-try mechanism for pending downloads
    :return:
    """
    download_queue_item = queue_item
    item = download_queue_item.item
    path = download_queue_item.path
    event = download_queue_item.event
    if event and r_c.exists(f"diy_crate:event_ids:{event.event_id}"):
        crate_logger.debug(f"Skipping download due to processed event ID for {path=}")
    if item["type"] == "file":
        info = redis_get(r_c, item) if r_c.exists(redis_key(item.object_id)) else None
        # client = Client(oauth)  # keep it around for easy access
        # hack because we did not use to store the file_path,
        # but do not want to force a download
        if info and "file_path" not in info:
            crate_logger.debug(
                f"updating local cache so the data is accurate, "
                f"but not doing the download {path=}"
            )
            info["file_path"] = path
            r_c.set(redis_key(item.object_id), json.dumps(info))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
        if info and info["etag"] == item["etag"] and os.path.exists(path):
            crate_logger.debug(
                f"ETAG {info['etag']} for {path=} is identical and file exists locally, "
                f"will skip download."
            )
        if info and info["etag"] > item["etag"] and os.path.exists(path):
            crate_logger.debug(
                f"ETAG {info['etag']} for {path=} is greater than the incoming data "
                f"{item['etag']} and file exists locally, "
                f"will skip download."
            )
        # no version, or diff version, or the file does not exist locally
        elif not info or info["etag"] != item["etag"] or not os.path.exists(path):
            if info:
                crate_logger.debug(
                    f"Preprocessing logic before download attempt. "
                    f"{path=} ETAG {info['etag']=} vs {item['etag']=}"
                )
            try:
                crate_logger.debug(f"attempt the download {path=}")
                perform_download(item, path)
            except (ConnectionResetError, ConnectionError):
                crate_logger.debug("Error occurred.", exc_info=True)
                time.sleep(5)
    else:
        crate_logger.debug(f"was not a file, making this a no-op on {path=}")
    if event:
        r_c.setex(
            f"diy_crate:event_ids:{event.event_id}",
            timedelta(days=32),
            (path.as_posix() if isinstance(path, Path) else path),
        )


def perform_download(item: File, path: Union[str, Path], retry_limit=15):
    if isinstance(path, str):
        path = Path(path)
    for i in range(retry_limit):
        if os.path.basename(path).startswith(".~lock"):  # avoid downloading lock files
            break
        try:
            with open(path, "wb") as item_handler:
                crate_logger.debug(
                    f"About to download: {item['name']}, " f"{item['id']}"
                )

                dlwd_key = f"diy_crate.breadcrumb.create_from_box.{path}"
                r_c.setex(dlwd_key, 300, 1)
                item.download_to(item_handler)
                # if item_wd is not None:
                #     wm.update_watch(item_wd, mask=mask | in_create)

            crate_logger.debug(f"Did download: {item['name']}, " f"{item['id']}")

        except BoxAPIException as e:
            crate_logger.info("Error occurred", exc_info=True)
            if e.status == 404:
                crate_logger.debug(
                    f"Apparently item: {item['id']}, {path} has been deleted, "
                    f"right before we tried to download",
                    exc_info=True,
                )
            elif e.status == 429:
                sleep_time = random.randint(2, pow(2, 5))
                crate_logger.info(
                    f"Rate limited during download operation with "
                    f"item: {item['id']}, "
                    f"{path}. Will sleep {sleep_time} before retry."
                )
                time.sleep(sleep_time)
                continue
            break
        else:
            if i:
                crate_logger.info(f"Retry recovered, for path: {path}")
            # path_to_add = os.path.dirname(path)
            # wm.add_watch(path=path_to_add, mask=mask, rec=True, auto_add=True)
            notify_user_with_gui(
                "Downloaded:", path.as_posix() if isinstance(path, Path) else path
            )
            was_versioned = r_c.exists(redis_key(item.object_id))
            redis_set(
                r_c,
                item,
                datetime.fromtimestamp(os.path.getmtime(path))
                .astimezone(dateutil.tz.tzutc())
                .timestamp(),
                box_dir_path=BOX_DIR,
                fresh_download=not was_versioned,
                folder=os.path.dirname(path),
            )
            time_data_map = {
                Path(path)
                .resolve()
                .as_posix(): datetime.fromtimestamp(os.path.getmtime(path))
                .astimezone(dateutil.tz.tzutc())
                .timestamp()
            }
            for mkey, mvalue in time_data_map.items():
                r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
            crate_logger.debug(f"downloaded {item}, modified_at: {item.modified_at}")
            r_c.set(
                local_or_box_file_m_time_key_func(path / item.name, True),
                parse(item.modified_at).astimezone(dateutil.tz.tzutc()).timestamp(),
            )
            r_c.set(redis_path_for_object_id_key(path / item.name), item.object_id)
            path_builder = BOX_DIR
            oauth = setup_remote_oauth(r_c, conf=conf_obj)
            client = Client(oauth)
            try:
                box_file = client.file(item.object_id).get(fields=["path_collection"])
                for entry in box_file.path_collection["entries"]:
                    if entry.id == "0":
                        continue
                    path_builder /= entry.name
                    folder_entry = client.folder(entry.id).get(fields=["modified_at"])
                    r_c.set(
                        local_or_box_file_m_time_key_func(path_builder, True),
                        parse(folder_entry.modified_at)
                        .astimezone(dateutil.tz.tzutc())
                        .timestamp(),
                    )
                    r_c.set(
                        redis_path_for_object_id_key(path_builder),
                        folder_entry.object_id,
                    )
            except boxsdk.exception.BoxAPIException as box_exc:
                if box_exc.status == 404 and box_exc.code == "trashed":
                    crate_logger.debug(
                        f"Object {item.object_id=} was previously deleted from Box. "
                        f"{(path / item.name)=}"
                    )

                    object_id_lookup = r_c.get(
                        redis_path_for_object_id_key(path / item.name)
                    )
                    object_id_lookup = (
                        str(object_id_lookup) if object_id_lookup else object_id_lookup
                    )
                    if object_id_lookup == item.object_id:
                        r_c.delete(redis_path_for_object_id_key(path / item.name))
                    r_c.delete(redis_key(item.object_id))
                    r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                else:
                    raise
            break


# cannot use the concurrent future executor classes due to a daemon issue 3.9+.
download_pool_executor = multiprocessing.pool.ThreadPool()
upload_pool_executor = multiprocessing.pool.ThreadPool()

conf_obj = configparser.ConfigParser()
conf_dir = Path("~/.config/diycrate").expanduser().resolve()
if not conf_dir.is_dir():
    os.mkdir(conf_dir)
cloud_credentials_file_path = conf_dir / "box.ini"
if not cloud_credentials_file_path.is_file():
    cloud_credentials_file_path.write_text("")
conf_obj.read(cloud_credentials_file_path)
BOX_DIR = Path(conf_obj["box"]["directory"]).expanduser().resolve()
