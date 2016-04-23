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


def redis_set(r_c, obj, last_modified_time, BOX_DIR, fresh_download=False, folder=None):
    """

    :param r_c:
    :param obj:
    :param last_modified_time:
    :param fresh_download:
    :return:
    """
    key = redis_key(obj['id'])
    if folder:
        path = folder
    elif int(obj['path_collection']['total_count']) > 1:
        path = '{}'.format(os.path.sep).join([folder['name']
                                                  for folder in
                                                  obj['path_collection']['entries'][1:]])
    else:
        path = ''
    path = os.path.join(BOX_DIR, path)
    r_c.set(key, json.dumps({'fresh_download': fresh_download,
                             'time_stamp': last_modified_time,
                             'etag': obj['etag'],
                             'file_path': os.path.join(path, obj['name'])}))
    r_c.set('diy_crate.last_save_time_stamp', int(time.time()))
    # assert redis_get(obj)


def redis_get(r_c, obj):
    """

    :param r_c:
    :param obj:
    :return:
    """
    key = redis_key(obj['id'])
    return json.loads(str(r_c.get(key), encoding='utf-8', errors='strict'))


r_c = redis.StrictRedis()