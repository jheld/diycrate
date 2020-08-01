import configparser
import json
import os
import queue
import random
import time
import logging
from functools import partial

from boxsdk import Client
from boxsdk.exception import BoxAPIException
from boxsdk.object.file import File
from requests import ConnectionError
from urllib3.exceptions import ProtocolError

from .file_operations import wm, mask
from .cache_utils import redis_key, redis_set, redis_get, r_c
from .gui import notify_user_with_gui
from .log_utils import setup_logger

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
            was_list = isinstance(callable_up, list)
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
                item = ret_val  # is the new/updated item
                if isinstance(item, File):
                    client = Client(oauth)
                    file_obj = client.file(file_id=item.object_id).get()
                    redis_set(r_c, file_obj, last_modified_time, box_dir_path=BOX_DIR)
            break
        except BoxAPIException as e:
            crate_logger.debug("{the_args}".format(the_args=args), exc_info=True)
            if e.status == 409:
                crate_logger.warning(
                    "Apparently Box says this item already exists..."
                    "and we were trying to create it. "
                    "Need to handle this better. message: {}".format(e.message),
                    exc_info=True,
                )
                break
        except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError):
            sleep_time = random.randint(0, 8)
            time.sleep(sleep_time)
            crate_logger.debug("{the_args}".format(the_args=args), exc_info=True)
            if x >= num_retries - 1:
                crate_logger.warning(
                    "Upload giving up on: {}".format(callable_up), exc_info=True
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
                    redis_get(r_c, item) if r_c.exists(redis_key(item["id"])) else None
                )
                # client = Client(oauth)  # keep it around for easy access
                # hack because we did not use to store the file_path,
                # but do not want to force a download
                if info and "file_path" not in info:
                    info["file_path"] = path
                    r_c.set(redis_key(item["id"]), json.dumps(info))
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


def perform_download(item, path, retry_limit=15):
    for i in range(retry_limit):
        if os.path.basename(path).startswith(".~lock"):  # avoid downloading lock files
            break
        try:
            with open(path, "wb") as item_handler:
                crate_logger.debug(
                    "About to download: {obj_name}, "
                    "{obj_id}".format(obj_name=item["name"], obj_id=item["id"])
                )

                dlwd_key = "diy_crate.breadcrumb.create_from_box.{path}".format(
                    path=path
                )
                r_c.setex(dlwd_key, 300, 1)
                item.download_to(item_handler)
                # if item_wd is not None:
                #     wm.update_watch(item_wd, mask=mask | in_create)

            crate_logger.debug(
                "Did download: {obj_name}, "
                "{obj_id}".format(obj_name=item["name"], obj_id=item["id"])
            )

        except BoxAPIException as e:
            crate_logger.info("Error occurred", exc_info=True)
            if e.status == 404:
                crate_logger.debug(
                    "Apparently item: {obj_id}, {path} has been deleted, "
                    "right before we tried to download".format(
                        obj_id=item["id"], path=path
                    ),
                    exc_info=True,
                )
            break
        else:
            redis_set(
                r_c,
                item,
                os.path.getmtime(path),
                box_dir_path=BOX_DIR,
                folder=os.path.dirname(path),
            )
            if i:
                crate_logger.info("Retry recovered, for path: {path}".format(path=path))
            path_to_add = os.path.dirname(path)
            wm.add_watch(path=path_to_add, mask=mask, rec=True, auto_add=True)
            notify_user_with_gui("Downloaded: {}".format(path))
        was_versioned = r_c.exists(redis_key(item["id"]))
        redis_set(
            r_c,
            item,
            os.path.getmtime(path),
            box_dir_path=BOX_DIR,
            fresh_download=not was_versioned,
            folder=os.path.dirname(path),
        )
        break


download_queue = queue.Queue()
upload_queue = queue.Queue()
uploads_given_up_on = []

conf_obj = configparser.ConfigParser()
conf_dir = os.path.abspath(os.path.expanduser("~/.config/diycrate"))
if not os.path.isdir(conf_dir):
    os.mkdir(conf_dir)
cloud_credentials_file_path = os.path.join(conf_dir, "box.ini")
if not os.path.isfile(cloud_credentials_file_path):
    open(cloud_credentials_file_path, "w").write("")
conf_obj.read(cloud_credentials_file_path)
BOX_DIR = os.path.expanduser(conf_obj["box"]["directory"])
