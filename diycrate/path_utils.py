import os
import time
import logging
from datetime import timedelta, datetime
from functools import partial
from os import PathLike
from pathlib import Path
from typing import Union, Optional, Dict, List

import dateutil
from bottle import Bottle
from boxsdk import OAuth2
from boxsdk.exception import BoxAPIException
from boxsdk.client import Client
from boxsdk.object.file import File
from boxsdk.object.folder import Folder
from dateutil.parser import parse

from .file_operations import wm, BOX_DIR, path_time_recurse_func
from .item_queue_io import download_queue, upload_queue
from .cache_utils import (
    redis_key,
    r_c,
    redis_set,
    redis_get,
    local_or_box_file_m_time_key_func,
)
from .iter_utils import SafeIter
from .log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)


def walk_and_notify_and_download_tree(
    path: PathLike,
    box_folder: Folder,
    oauth_obj: OAuth2,
    bottle_app: Bottle,
    p_id: Optional[str] = None,
    local_only: bool = False,
) -> None:
    """
    Walk the path recursively and add watcher and create the path.
    :param path:
    :param box_folder:
    :param oauth_obj:
    :param p_id:
    :param bottle_app:
    :param file_event_handler:
    :param local_only:
    :return:
    """
    start_t = time.monotonic()
    if os.path.isdir(path):
        # wm.add_watch(path, mask, rec=True, auto_add=True)
        local_files = os.listdir(path)
    else:
        raise ValueError(f"path: {path} is not a path; cannot walk it.")
    path = Path(path)
    crate_logger.debug(f"Walking path: {path}")
    bottle_app.oauth = getattr(bottle_app, "oauth", oauth_obj)
    client = oauth_setup_within_directory_walk(bottle_app.oauth)

    time_data_map = path_time_recurse_func(path, wm)
    if Path(path) == BOX_DIR:
        for mkey, mvalue in time_data_map.items():
            r_c.set(local_or_box_file_m_time_key_func(mkey, False), mvalue)
    while True:
        try:
            basic_folder_keys = ["id", "etag", "name", "path_collection", "modified_at"]
            b_folder: Folder = client.folder(folder_id=box_folder.object_id).get(
                fields=basic_folder_keys
            )
            break
        except ConnectionError:
            crate_logger.warning("Trying again after small sleep", exc_info=True)
            time.sleep(5)

    folder_cache_info = (
        redis_get(r_c, b_folder) if r_c.exists(redis_key(b_folder.object_id)) else None
    )
    do_process = True
    if folder_cache_info and not local_only:
        do_process = any_unresolved_modifications(b_folder, path, time_data_map)
    if not do_process:
        return
    limit = 100
    for _ in range(5):
        try:
            if not local_only:
                local_files_walk_pre_process(
                    b_folder, limit, local_files, oauth_obj, path
                )
            break
        except Exception:
            pass

    ids_in_folder = []
    offset = 0
    if not local_only:
        folder_items = b_folder.get_items(
            limit=limit, offset=offset, fields=["name", "id", "etag", "modified_at"]
        )
    else:
        folder_items = iter([])
    safe_iter = SafeIter(folder_items)
    for box_item in safe_iter:
        if box_item is None:
            continue
        if box_item.object_id not in ids_in_folder:
            ids_in_folder.append(box_item.object_id)
        if box_item["name"] in local_files:
            local_files.remove(box_item["name"])
        if box_item["type"] == "folder":
            local_path = path / box_item["name"]
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
                fresh_download,
                local_path,
                oauth_obj,
                retry_limit,
            )
        else:
            kick_off_download_file_from_box_via_walk(box_item, oauth_obj, path)
        r_c.set(
            local_or_box_file_m_time_key_func(path / box_item.name, True),
            parse(box_item.modified_at).astimezone(dateutil.tz.tzutc()).timestamp(),
        )
    if not local_only:
        redis_set(
            cache_client=r_c,
            cloud_item=b_folder,
            last_modified_time=datetime.fromtimestamp(os.path.getmtime(path))
            .astimezone(dateutil.tz.tzutc())
            .timestamp(),
            box_dir_path=BOX_DIR,
            fresh_download=not r_c.exists(redis_key(box_folder.object_id)),
            folder=os.path.dirname(path),
            sub_ids=ids_in_folder,
            parent_id=p_id,
        )
    end_t = time.monotonic()
    crate_logger.debug(
        f"walk_and_notify_and_download_tree on {box_folder}, took: {end_t - start_t:.2f}s"
    )


def any_unresolved_modifications(
    b_folder: Folder, path: Path, time_data_map: Dict[str, float]
) -> bool:
    have_any = True
    b_folder_modified_at = b_folder["modified_at"]
    if b_folder_modified_at:
        path_sorted_by_m_time = sorted(
            (
                (Path(k).as_posix(), v)
                for k, v in time_data_map.items()
                if path in Path(k).parents or Path(k) == path
            ),
            key=lambda x: x[1],
        )
        if path_sorted_by_m_time:
            redis_path_sorted_by_m_time = sorted(
                (
                    (
                        Path(
                            k.decode()[
                                k.decode().find(BOX_DIR.as_posix()) :  # noqa: E203
                            ]  # noqa: E203
                        ).as_posix(),
                        float(r_c.get(k)),
                    )
                    for k in r_c.keys(
                        local_or_box_file_m_time_key_func(path, False) + "*"
                    )
                ),
                key=lambda x: x[1],
            )
            # box folder's modified_at value changes when it, a file,
            # or a sub-directory's files changes,
            # so we only care about this specific folder's key.
            box_redis_path_sorted_by_m_time = sorted(
                (
                    (
                        Path(
                            k.decode()[
                                k.decode().find(BOX_DIR.as_posix()) :  # noqa: E203
                            ]  # noqa: E203
                        ).as_posix(),
                        float(r_c.get(k)),
                    )
                    for k in r_c.keys(local_or_box_file_m_time_key_func(path, True))
                ),
                key=lambda x: x[1],
            )
            if redis_path_sorted_by_m_time and box_redis_path_sorted_by_m_time:
                # box folder's modified_at value changes when it, a file,
                # or a sub-directory's files changes,
                # so we only care about this specific folder's key.
                box_current_path_and_m_time = tuple(
                    [
                        (path.parent / b_folder.name).as_posix(),
                        parse(b_folder_modified_at)
                        .astimezone(dateutil.tz.tzutc())
                        .timestamp(),
                    ]
                )
                differences_full = []
                differences_full.append(
                    {
                        "kind": "local",
                        "value": [
                            list(path_sorted_by_m_time[-1]),
                            list(redis_path_sorted_by_m_time[-1]),
                        ],
                    }
                )
                differences_full.append(
                    {
                        "kind": "box",
                        "value": [
                            list(box_current_path_and_m_time),
                            list(box_redis_path_sorted_by_m_time[-1]),
                        ],
                    }
                )
                differences = [
                    item
                    for item in differences_full
                    if item["value"][0] != item["value"][1]
                ]
                if not differences:
                    crate_logger.debug(
                        f"Skipping walk on folder {b_folder}; "
                        f"neither the box or local history has changed since the last run."
                    )

                    have_any = False
                else:
                    # crate_logger.debug(f"are differences: {differences}")
                    have_any = differences
    elif path == BOX_DIR:
        path_sorted_by_m_time = sorted(
            (
                (Path(k).as_posix(), v)
                for k, v in time_data_map.items()
                if path in Path(k).parents
            ),
            key=lambda x: x[1],
        )
        redis_path_sorted_by_m_time = sorted(
            (
                (
                    Path(
                        k.decode()[k.decode().find(BOX_DIR.as_posix()) :]  # noqa: E203
                    ).as_posix(),
                    float(r_c.get(k)),
                )
                for k in r_c.keys(local_or_box_file_m_time_key_func(path, False) + "*")
            ),
            key=lambda x: x[1],
        )
        box_redis_path_sorted_by_m_time = sorted(
            (
                (
                    Path(
                        k.decode()[
                            k.decode().find(BOX_DIR.as_posix()) :  # noqa: E203
                        ]  # noqa: E203
                    ).as_posix(),
                    float(r_c.get(k)),
                )
                for k in r_c.keys(local_or_box_file_m_time_key_func(path, True) + "*")
            ),
            key=lambda x: x[1],
        )

        b_children = sorted(
            b_folder.get_items(fields=["name", "modified_at", "path_collection"]),
            key=lambda x: datetime.fromisoformat(x.modified_at)
            .astimezone(dateutil.tz.tzutc())
            .timestamp(),
        )
        b_oldest_child: Optional[Union[Folder, File]] = (
            b_children[-1] if b_children else None
        )
        if b_oldest_child:
            crate_logger.debug([item.name for item in b_children])
        b_oldest_child_modified_at = (
            datetime.fromisoformat(b_oldest_child.modified_at)
            .astimezone(dateutil.tz.tzutc())
            .timestamp()
            if b_oldest_child
            else None
        )
        if box_redis_path_sorted_by_m_time:
            # because of the keys pattern, we received back all paths which matched path*,
            # but we really only want path | path/*/
            # that is, our matching path, or just its first level files & sub directories.
            box_redis_path_sorted_by_m_time = [
                item
                for item in box_redis_path_sorted_by_m_time
                if Path(item[0]).parent == path or Path(item[0]) == path
            ]

        if (
            path_sorted_by_m_time
            and redis_path_sorted_by_m_time
            and box_redis_path_sorted_by_m_time
            and b_oldest_child_modified_at
            and b_oldest_child
        ):
            differences_full: List[
                Dict[str, Union[str, List[List[Union[str, float]]]]]
            ] = [
                {
                    "kind": "local",
                    "value": [
                        list(path_sorted_by_m_time[-1]),
                        list(redis_path_sorted_by_m_time[-1]),
                    ],
                }
            ]
            box_current_path_of_choice = (
                path
                / "/".join(
                    entry.name
                    for entry in b_oldest_child.path_collection["entries"]
                    if entry.id != "0"
                )
                / b_oldest_child.name
            ).as_posix()
            box_redis_entry_of_choice = list(box_redis_path_sorted_by_m_time[-1])
            differences_full.append(
                {
                    "kind": "box",
                    "value": [
                        [box_current_path_of_choice, b_oldest_child_modified_at],
                        box_redis_entry_of_choice,
                    ],
                }
            )
            differences = [
                item
                for item in differences_full
                if item["value"][0] != item["value"][1]
            ]
            if not differences:
                crate_logger.debug(
                    f"folder {b_folder} on "
                    f"{path.as_posix()} has not been changed locally, "
                    f"new local vs prev redis local ({path_sorted_by_m_time[-1]}, "
                    f"{redis_path_sorted_by_m_time[-1]}), "
                    f"and also current newest box child against new box child "
                    f"in redis, we will skip!"
                )
                have_any = False
            else:
                crate_logger.debug(
                    f"Difference detected on {path.as_posix()}, {differences}"
                )
                have_any = differences
    return have_any


def oauth_setup_within_directory_walk(oauth):
    client = Client(oauth)
    access_token = r_c.get("diy_crate.auth.access_token")
    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")
    refresh_token = r_c.get("diy_crate.auth.refresh_token")
    if isinstance(refresh_token, bytes):
        refresh_token = refresh_token.decode(encoding="utf-8")
    if access_token:
        client.auth._access_token = access_token
    if refresh_token:
        client.auth._refresh_token = refresh_token
    return client


def kick_off_download_file_from_box_via_walk(box_item, oauth_obj, path):
    try:
        file_obj = box_item
        download_queue.put((file_obj, os.path.join(path, box_item["name"]), oauth_obj))
    except BoxAPIException as e:
        crate_logger.debug("Error occurred", exc_info=True)
        if e.status == 404:
            crate_logger.debug(
                f"Box says: {box_item['id']}, {box_item['name']}, is a 404 status."
            )
            if r_c.exists(redis_key(box_item.object_id)):
                crate_logger.debug(f"Deleting {box_item['id']}, {box_item['name']}")
                r_c.delete(redis_key(box_item.object_id))
        raise e


def kick_off_sub_directory_box_folder_download_walk(
    bottle_app: Bottle,
    box_folder: Folder,
    box_item: Union[File, Folder],
    client: Client,
    fresh_download: bool,
    local_path: PathLike,
    oauth_obj: OAuth2,
    retry_limit: int,
) -> None:
    bottle_app.oauth = getattr(bottle_app, "oauth", oauth_obj)
    start_t = time.monotonic()
    for i in range(0, retry_limit):
        try:
            redis_set(
                cache_client=r_c,
                cloud_item=box_item,
                last_modified_time=datetime.fromtimestamp(os.path.getmtime(local_path))
                .astimezone(dateutil.tz.tzutc())
                .timestamp(),
                box_dir_path=BOX_DIR,
                fresh_download=fresh_download,
                folder=os.path.dirname(local_path),
            )
            box_folder_obj = client.folder(folder_id=box_item.object_id)
        except BoxAPIException as e:
            crate_logger.debug("Box error occurred.")
            if e.status == 404:
                crate_logger.debug(
                    f"Box says: {box_item['id']}, {box_item['name']}, is a 404 status."
                )
                crate_logger.debug(
                    "But, this is a folder, we do not handle recursive "
                    "folder deletes correctly yet."
                )
                break
        except (ConnectionError, ConnectionResetError, BrokenPipeError, OSError):
            crate_logger.debug(f"Attempt {i + 1}/{retry_limit}", exc_info=True)
        else:
            if i:
                crate_logger.debug("Succeeded on retry.")
            walk_and_notify_and_download_tree(
                local_path,
                box_folder_obj,
                oauth_obj,
                bottle_app,
                p_id=box_folder.object_id,
            )
            break
    end_t = time.monotonic()
    crate_logger.debug(
        f"kick_off_sub_directory_box_folder_download_walk "
        f"on {box_item}, took: {end_t - start_t:.2f}s"
    )


def local_files_walk_pre_process(
    b_folder: Folder, limit, local_files, oauth_obj, path, offset=0
):
    folder_items = b_folder.get_items(
        limit=limit, offset=offset, fields=["name", "etag", "id", "modified_at"]
    )
    safe_iter = SafeIter(folder_items)
    start_t = time.monotonic()
    for box_item in safe_iter:
        if box_item is None:
            continue
        if box_item["name"] in local_files:
            local_files.remove(box_item["name"])
    for (
        local_file
    ) in local_files:  # prioritize the local_files not yet on box's server.
        cur_box_folder = b_folder
        local_path = os.path.join(path, local_file)
        if os.path.isfile(local_path):
            crate_logger.info(f"upload, {local_path}, {local_file}")
            m_timestamp = (
                datetime.fromtimestamp(os.path.getmtime(local_path))
                .astimezone(dateutil.tz.tzutc())
                .timestamp()
            )

            upload_queue.put(
                (
                    m_timestamp,
                    partial(cur_box_folder.upload, local_path, local_file),
                    oauth_obj,
                )
            )
    end_t = time.monotonic()
    crate_logger.debug(
        f"local_files_walk_pre_process, {b_folder}, took: {end_t - start_t:.2f}s"
    )


def re_walk(path, box_folder, oauth_obj, bottle_app: Bottle, file_event_handler=None):
    """

    :param path:
    :param box_folder:
    :param oauth_obj:
    :param bottle_app:
    :param file_event_handler:
    :return:
    """
    while True:
        try:
            start = time.time()
            crate_logger.info("Starting walk.")
            # wm.add_watch(path.as_posix(), mask, rec=True, auto_add=True)
            walk_and_notify_and_download_tree(path, box_folder, oauth_obj, bottle_app)
            end = time.time()
            duration: Union[int, float] = int(end - start)
            if duration >= 60:
                duration = round(timedelta(seconds=duration) / timedelta(minutes=1), 2)
                unit = "m"
            else:
                unit = "s"
            crate_logger.info(f"Finished walking! Took {duration}{unit}")
            time.sleep(3600)  # once an hour we walk the tree
        except KeyboardInterrupt:
            raise
        except Exception:
            crate_logger.warning(
                "re_walk encountered error (will reloop).", exc_info=True
            )
