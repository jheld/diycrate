import os
import time
import logging
from datetime import timedelta
from functools import partial


import requests

from boxsdk.exception import BoxAPIException
from boxsdk.client import Client

from .file_operations import wm, mask, BOX_DIR
from .item_queue_io import download_queue, upload_queue
from .cache_utils import redis_key, r_c, redis_set
from .iter_utils import SafeIter
from .log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)


def walk_and_notify_and_download_tree(
    path,
    box_folder,
    oauth_obj,
    oauth_meta_info,
    p_id=None,
    bottle_app=None,
    file_event_handler=None,
):
    """
    Walk the path recursively and add watcher and create the path.
    :param path:
    :param box_folder:
    :param oauth_obj:
    :param oauth_meta_info:
    :param p_id:
    :param bottle_app:
    :param file_event_handler:
    :return:
    """
    if os.path.isdir(path):
        wm.add_watch(path, mask, rec=True, auto_add=True)
        local_files = os.listdir(path)
    else:
        raise ValueError(
            "path: {path} is not a path; " "cannot walk it.".format(path=path)
        )
    crate_logger.debug("Walking path: {path}".format(path=path))
    client = oauth_setup_within_directory_walk(bottle_app)
    while True:
        try:
            b_folder = client.folder(folder_id=box_folder["id"]).get()
            break
        except requests.exceptions.ConnectionError:
            crate_logger.warning("Moving on with sleep", exc_info=True)
            time.sleep(5)

    limit = 100
    local_files_walk_pre_process(b_folder, limit, local_files, oauth_obj, path)
    ids_in_folder = []
    offset = 0
    folder_items = b_folder.get_items(limit=limit, offset=offset)

    safe_iter = SafeIter(folder_items)
    for box_item in safe_iter:
        if box_item is None:
            continue
        if box_item["id"] not in ids_in_folder:
            ids_in_folder.append(box_item["id"])
        if box_item["name"] in local_files:
            local_files.remove(box_item["name"])
        if box_item["type"] == "folder":
            local_path = os.path.join(path, box_item["name"])
            fresh_download = False
            if not os.path.isdir(local_path):
                os.mkdir(local_path)
                fresh_download = True
            retry_limit = 15
            kick_off_sub_directory_box_folder_download_walk(
                bottle_app,
                box_folder,
                box_item,
                client,
                file_event_handler,
                fresh_download,
                local_path,
                oauth_meta_info,
                oauth_obj,
                retry_limit,
            )
        else:
            kick_off_download_file_from_box_via_walk(box_item, oauth_obj, path)
    redis_set(
        cache_client=r_c,
        cloud_item=b_folder,
        last_modified_time=os.path.getmtime(path),
        box_dir_path=BOX_DIR,
        fresh_download=not r_c.exists(redis_key(box_folder["id"])),
        folder=os.path.dirname(path),
        sub_ids=ids_in_folder,
        parent_id=p_id,
    )


def oauth_setup_within_directory_walk(bottle_app):
    client = Client(bottle_app.oauth)
    client.auth._access_token = r_c.get("diy_crate.auth.access_token")
    client.auth._refresh_token = r_c.get("diy_crate.auth.refresh_token")
    if client.auth._access_token:
        client.auth._access_token = (
            client.auth._access_token.decode(encoding="utf-8")
            if isinstance(client.auth._access_token, bytes)
            else client.auth._access_token
        )
    if client.auth._refresh_token:
        client.auth._refresh_token = (
            client.auth._refresh_token.decode(encoding="utf-8")
            if isinstance(client.auth._refresh_token, bytes)
            else client.auth._refresh_token
        )
    return client


def kick_off_download_file_from_box_via_walk(box_item, oauth_obj, path):
    try:
        file_obj = box_item
        download_queue.put((file_obj, os.path.join(path, box_item["name"]), oauth_obj))
    except BoxAPIException as e:
        crate_logger.debug("Error occurred", exc_info=True)
        if e.status == 404:
            crate_logger.debug(
                "Box says: {obj_id}, {obj_name}, "
                "is a 404 status.".format(
                    obj_id=box_item["id"], obj_name=box_item["name"]
                )
            )
            if r_c.exists(redis_key(box_item["id"])):
                crate_logger.debug(
                    "Deleting {obj_id}, "
                    "{obj_name}".format(
                        obj_id=box_item["id"], obj_name=box_item["name"]
                    )
                )
                r_c.delete(redis_key(box_item["id"]))
        raise e


def kick_off_sub_directory_box_folder_download_walk(
    bottle_app,
    box_folder,
    box_item,
    client,
    file_event_handler,
    fresh_download,
    local_path,
    oauth_meta_info,
    oauth_obj,
    retry_limit,
):
    for i in range(0, retry_limit):
        try:
            redis_set(
                cache_client=r_c,
                cloud_item=box_item,
                last_modified_time=os.path.getmtime(local_path),
                box_dir_path=BOX_DIR,
                fresh_download=fresh_download,
                folder=os.path.dirname(local_path),
            )
            box_folder_obj = client.folder(folder_id=box_item["id"]).get()
        except BoxAPIException as e:
            crate_logger.debug("Box error occurred.")
            if e.status == 404:
                crate_logger.debug(
                    "Box says: {obj_id}, "
                    "{obj_name}, is a 404 status.".format(
                        obj_id=box_item["id"], obj_name=box_item["name"]
                    )
                )
                crate_logger.debug(
                    "But, this is a folder, we do not handle recursive "
                    "folder deletes correctly yet."
                )
                break
        except (ConnectionError, ConnectionResetError, BrokenPipeError, OSError):
            crate_logger.debug(
                "Attempt {idx}/{limit}".format(idx=i + 1, limit=retry_limit),
                exc_info=True,
            )
        else:
            if i:
                crate_logger.debug("Succeeded on retry.")
            walk_and_notify_and_download_tree(
                local_path,
                box_folder_obj,
                oauth_obj,
                oauth_meta_info,
                p_id=box_folder["id"],
                bottle_app=bottle_app,
                file_event_handler=file_event_handler,
            )
            break


def local_files_walk_pre_process(
    b_folder, limit, local_files, oauth_obj, path, offset=0
):
    for box_item in b_folder.get_items(limit=limit, offset=offset):
        if box_item["name"] in local_files:
            local_files.remove(box_item["name"])
    for (
        local_file
    ) in local_files:  # prioritize the local_files not yet on box's server.
        cur_box_folder = b_folder
        local_path = os.path.join(path, local_file)
        if os.path.isfile(local_path):
            upload_queue.put(
                [
                    os.path.getmtime(local_path),
                    partial(cur_box_folder.upload, local_path, local_file),
                    oauth_obj,
                ]
            )


def re_walk(
    path,
    box_folder,
    oauth_obj,
    oauth_meta_info,
    bottle_app=None,
    file_event_handler=None,
):
    """

    :param path:
    :param box_folder:
    :param oauth_obj:
    :param oauth_meta_info:
    :param bottle_app:
    :param file_event_handler:
    :return:
    """
    while True:
        start = time.time()
        crate_logger.info("Starting walk.")
        walk_and_notify_and_download_tree(
            path,
            box_folder,
            oauth_obj,
            oauth_meta_info,
            bottle_app=bottle_app,
            file_event_handler=file_event_handler,
        )
        end = time.time()
        duration = int(end - start)
        if duration >= 60:
            duration = round(timedelta(seconds=duration) / timedelta(minutes=1), 2)
            unit = "m"
        else:
            unit = "s"
        crate_logger.info(
            "Finished walking! Took {duration}{unit}".format(
                duration=duration, unit=unit
            )
        )
        time.sleep(3600)  # once an hour we walk the tree
