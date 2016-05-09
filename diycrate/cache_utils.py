import json
import os
import time

import redis


def redis_key(key):
    """

    :param key:
    :return:
    """
    return 'diy_crate.version.{}'.format(key)


def redis_set(cache_client, cloud_item, last_modified_time, box_dir_path, fresh_download=False, folder=None, sub_ids=None, parent_id=None):
    """

    :param cache_client:
    :param cloud_item:
    :param last_modified_time:
    :param box_dir_path:
    :param fresh_download:
    :param folder:
    :return:
    """
    key = redis_key(cloud_item['id'])
    if folder:
        path = folder
    elif int(cloud_item['path_collection']['total_count']) > 1:
        path = '{}'.format(os.path.sep).join([folder['name']
                                              for folder in
                                              cloud_item['path_collection']['entries'][1:]])
    else:
        path = ''
    path = os.path.join(box_dir_path, path)
    item_info = {'fresh_download': fresh_download,
                 'time_stamp': last_modified_time,
                 'etag': cloud_item['etag'],
                 'file_path': os.path.join(path, cloud_item['name'])}
    if sub_ids is not None:
        item_info['sub_ids'] = sub_ids
    if parent_id:
        item_info['parent_id'] = parent_id
    cache_client.set(key, json.dumps(item_info))
    cache_client.set('diy_crate.last_save_time_stamp', int(time.time()))
    # assert redis_get(obj)


def redis_get(cache_client, obj):
    """

    :param cache_client:
    :param obj:
    :return:
    """
    key = redis_key(obj['id'])
    return json.loads(str(cache_client.get(key), encoding='utf-8', errors='strict'))


def id_for_file_path(cache_client, file_path):
    """

    :param cache_client:
    :param file_path:
    :return:
    """
    for key in cache_client.keys('diy_crate.version.*'):
        value = json.loads(str(cache_client.get(key), encoding='utf-8', errors='strict'))
        if value.get('file_path') == file_path:
            return str(key, encoding='utf-8', errors='strict').split('.')[-1]

r_c = redis.StrictRedis()
