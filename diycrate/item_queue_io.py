import configparser
import json
import os
import queue
import random
import time
import logging
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Callable, Any, List, NamedTuple, Union, Tuple

import dateutil
from boxsdk import Client, OAuth2
from boxsdk.exception import BoxAPIException
from boxsdk.object.file import File
from boxsdk.object.folder import Folder
from dateutil.parser import parse
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError

from .cache_utils import (
    redis_key,
    redis_set,
    redis_get,
    r_c,
    local_or_box_file_m_time_key_func,
)
from .gui import notify_user_with_gui
from .log_utils import setup_logger
from .oauth_utils import setup_remote_oauth

setup_logger()

crate_logger = logging.getLogger(__name__)


def upload_queue_processor():
    """
    Implements a simple re-try mechanism for pending uploads
    :return:
    """
    while True:
        if upload_queue.not_empty:
            callable_up = upload_queue.get()  # blocks
            # TODO: pass in the actual item being updated/uploaded,
            #  so we can do more intelligent retry mechanisms
            was_list = isinstance(callable_up, (list, tuple))
            last_modified_time = oauth = None
            if was_list:
                last_modified_time, callable_up, oauth = callable_up
            args = callable_up.args if isinstance(callable_up, partial) else None
            num_retries = 15
            perform_upload(
                args,
                callable_up,
                last_modified_time,
                num_retries,
                oauth,
                was_list,
                retry_limit=num_retries,
            )
            upload_queue.task_done()


def perform_upload(
    args, callable_up, last_modified_time, num_retries, oauth, was_list, retry_limit=15
):
    for x in range(retry_limit):
        try:
            ret_val = callable_up()
            if was_list:
                path_name = args[0]
                item = ret_val  # is the new/updated item
                if isinstance(item, File):
                    client = Client(oauth)
                    file_obj = client.file(file_id=item.object_id).get()
                    redis_set(r_c, file_obj, last_modified_time, box_dir_path=BOX_DIR)
                    r_c.set(
                        local_or_box_file_m_time_key_func(path_name, False),
                        datetime.fromtimestamp(Path(path_name).stat().st_mtime)
                        .astimezone(dateutil.tz.tzutc())
                        .timestamp(),
                    )

                    path_builder = BOX_DIR
                    oauth = setup_remote_oauth(r_c, conf=conf_obj)
                    client = Client(oauth)
                    for entry in (
                        client.file(item.object_id)
                        .get(fields=["path_collection"])
                        .path_collection["entries"]
                    ):
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


def download_queue_processor():
    """
    Implements a simple re-try mechanism for pending downloads
    :return:
    """
    while True:
        if download_queue.not_empty:
            item, path, oauth = download_queue.get()  # blocks
            if item["type"] == "file":
                info = (
                    redis_get(r_c, item)
                    if r_c.exists(redis_key(item.object_id))
                    else None
                )
                # client = Client(oauth)  # keep it around for easy access
                # hack because we did not use to store the file_path,
                # but do not want to force a download
                if info and "file_path" not in info:
                    info["file_path"] = path
                    r_c.set(redis_key(item.object_id), json.dumps(info))
                    r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                # no version, or diff version, or the file does not exist locally
                if not info or info["etag"] != item["etag"] or not os.path.exists(path):
                    try:
                        perform_download(item, path)
                    except (ConnectionResetError, ConnectionError):
                        crate_logger.debug("Error occurred.", exc_info=True)
                        time.sleep(5)
                download_queue.task_done()
            else:
                download_queue.task_done()


def perform_download(item: File, path, retry_limit=15):
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
            break
        else:
            if i:
                crate_logger.info(f"Retry recovered, for path: {path}")
            # path_to_add = os.path.dirname(path)
            # wm.add_watch(path=path_to_add, mask=mask, rec=True, auto_add=True)
            notify_user_with_gui(f"Downloaded: {path}")
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
            path_builder = BOX_DIR
            oauth = setup_remote_oauth(r_c, conf=conf_obj)
            client = Client(oauth)
            for entry in (
                client.file(item.object_id)
                .get(fields=["path_collection"])
                .path_collection["entries"]
            ):
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
            break


class DownloadQueueItem(NamedTuple):
    item: Union[File, Folder]
    path: Union[Path, str]
    oauth: OAuth2


UploadQueueItem = Union[Callable[..., Any], Tuple[float, Callable[..., Any], OAuth2]]

download_queue: "queue.Queue[DownloadQueueItem]" = queue.Queue()
upload_queue: "queue.Queue[UploadQueueItem]" = queue.Queue()
uploads_given_up_on: List[Callable[..., Any]] = []

conf_obj = configparser.ConfigParser()
conf_dir = Path("~/.config/diycrate").expanduser().resolve()
if not conf_dir.is_dir():
    os.mkdir(conf_dir)
cloud_credentials_file_path = conf_dir / "box.ini"
if not cloud_credentials_file_path.is_file():
    cloud_credentials_file_path.write_text("")
conf_obj.read(cloud_credentials_file_path)
BOX_DIR = Path(conf_obj["box"]["directory"]).expanduser().resolve()
