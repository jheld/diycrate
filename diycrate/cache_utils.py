import json
import logging
import os
import time
from os import PathLike
from pathlib import Path
from typing import Union, Optional, List, Dict, Any

import redis
from boxsdk.object.file import File
from boxsdk.object.folder import Folder

from . import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)


def redis_key(key):
    """

    :param key:
    :return:
    """
    return "diy_crate.version.{}".format(key)


def redis_set(
    cache_client: redis.Redis,
    cloud_item: Union[File, Folder],
    last_modified_time: float,
    box_dir_path: PathLike,
    fresh_download: bool = False,
    folder: str = None,
    sub_ids: Optional[List[str]] = None,
    parent_id: Optional[str] = None,
) -> Dict[str, Any]:
    """

    :param cache_client:
    :param cloud_item:
    :param last_modified_time:
    :param box_dir_path:
    :param fresh_download:
    :param folder:
    :param sub_ids:
    :param parent_id:
    :return:
    """
    key = redis_key(cloud_item.object_id)
    if "etag" not in cloud_item:
        cloud_item = cloud_item.get(fields=["etag", "path_collection", "name"])
    if folder:
        path = Path(folder)
    elif int(cloud_item["path_collection"]["total_count"]) > 1:
        path = Path(
            os.path.sep.join(
                [
                    folder["name"]
                    for folder in cloud_item["path_collection"]["entries"][1:]
                ]
            )
        )
    else:
        path = Path()
    path = Path(box_dir_path) / path
    item_info = {
        "fresh_download": fresh_download,
        "time_stamp": last_modified_time,
        "etag": cloud_item["etag"],
        "file_path": (path / cloud_item["name"]).as_posix(),
    }
    if sub_ids is not None:
        item_info["sub_ids"] = sub_ids
    if parent_id:
        item_info["parent_id"] = parent_id
    cache_client.set(key, json.dumps(item_info))
    last_save_time_stamp = int(time.time())
    cache_client.set("diy_crate.last_save_time_stamp", last_save_time_stamp)
    crate_logger.debug(f"Storing/updating info to redis:  {key=} {item_info=}")
    # assert redis_get(obj)
    return {key: item_info, "diy_crate.last_save_time_stamp": last_save_time_stamp}


def redis_get(cache_client, obj):
    """

    :param cache_client:
    :param obj:
    :return:
    """
    key = redis_key(obj.object_id)
    return json.loads(str(cache_client.get(key), encoding="utf-8", errors="strict"))


def id_for_file_path(cache_client, file_path):
    """

    :param cache_client:
    :param file_path:
    :return:
    """
    for key in cache_client.keys("diy_crate.version.*"):
        value = json.loads(
            str(cache_client.get(key), encoding="utf-8", errors="strict")
        )
        if value.get("file_path") == file_path:
            return str(key, encoding="utf-8", errors="strict").split(".")[-1]


r_c = redis.StrictRedis()


def local_or_box_file_m_time_key_func(x: Union[PathLike, str], from_box: bool) -> str:
    return f"diy_crate.{'box' if from_box else 'local'}_mtime.{x}"


def local_or_box_file_m_time(x: PathLike, from_box: bool) -> float:
    return float(r_c.get(local_or_box_file_m_time_key_func(x, from_box)))
