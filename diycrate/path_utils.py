import os
import time
import traceback
import logging
from logging import handlers
from functools import partial

from boxsdk.exception import BoxAPIException

from diycrate.file_operations import wm, mask, BOX_DIR
from diycrate.item_queue_io import download_queue, upload_queue
from diycrate.cache_utils import redis_key, r_c, redis_set


crate_logger = logging.getLogger('diy_crate_logger')
crate_logger.setLevel(logging.DEBUG)

l_handler = handlers.SysLogHandler(address='/dev/log')

crate_logger.addHandler(l_handler)

log_format = 'diycrate' + ' %(levelname)-9s %(name)-15s %(threadName)-14s +%(lineno)-4d %(message)s'
log_format = logging.Formatter(log_format)
l_handler.setFormatter(log_format)


def walk_and_notify_and_download_tree(path, box_folder, client, oauth_obj, p_id=None):
    """
    Walk the path recursively and add watcher and create the path.
    :param path:
    :param box_folder:
    :param client:
    :param oauth_obj:
    :param p_id:
    :return:
    """
    if os.path.isdir(path):
        wm.add_watch(path, mask, rec=True, auto_add=True)
        local_files = os.listdir(path)
    b_folder = client.folder(folder_id=box_folder['id']).get()
    num_entries_in_folder = b_folder['item_collection']['total_count']
    limit = 100
    for offset in range(0, num_entries_in_folder, limit):
        for box_item in b_folder.get_items(limit=limit, offset=offset):
            if box_item['name'] in local_files:
                local_files.remove(box_item['name'])
    for local_file in local_files:  # prioritize the local_files not yet on box's server.
        cur_box_folder = b_folder
        local_path = os.path.join(path, local_file)
        if os.path.isfile(local_path):
            upload_queue.put([os.path.getmtime(local_path), partial(cur_box_folder.upload, local_path, local_file),
                              oauth_obj])
    ids_in_folder = []
    for offset in range(0, num_entries_in_folder, limit):
        for box_item in b_folder.get_items(limit=limit, offset=offset):
            ids_in_folder.append(box_item['id'])
            if box_item['name'] in local_files:
                local_files.remove(box_item['name'])
            if box_item['type'] == 'folder':
                local_path = os.path.join(path, box_item['name'])
                fresh_download = False
                if not os.path.isdir(local_path):
                    os.mkdir(local_path)
                    fresh_download = True
                retry_limit = 15
                for i in range(0, retry_limit):
                    try:
                        redis_set(cache_client=r_c, cloud_item=box_item,
                                  last_modified_time=os.path.getmtime(local_path),
                                  box_dir_path=BOX_DIR, fresh_download=fresh_download,
                                  folder=os.path.dirname(local_path))
                        walk_and_notify_and_download_tree(local_path,
                                                          client.folder(folder_id=box_item['id']).get(),
                                                          client, oauth_obj,
                                                          p_id=box_folder['id'])
                        break
                    except BoxAPIException as e:
                        crate_logger.debug(traceback.format_exc())
                        if e.status == 404:
                            crate_logger.debug('Box says: {obj_id}, '
                                               '{obj_name}, is a 404 status.'.format(obj_id=box_item['id'],
                                                                                     obj_name=box_item[
                                                                                         'name']))
                            crate_logger.debug(
                                'But, this is a folder, we do not handle recursive folder deletes correctly yet.')
                            break
                    except (ConnectionError, ConnectionResetError, BrokenPipeError):
                        crate_logger.debug('Attempt {idx}/{limit}; {the_trace}'.format(the_trace=traceback.format_exc(),
                                                                                       idx=i+1, limit=retry_limit))
            else:
                try:
                    file_obj = box_item
                    download_queue.put((file_obj, os.path.join(path, box_item['name']), oauth_obj))
                except BoxAPIException as e:
                    crate_logger.debug(traceback.format_exc())
                    if e.status == 404:
                        crate_logger.debug('Box says: {obj_id}, {obj_name}, '
                                           'is a 404 status.'.format(obj_id=box_item['id'], obj_name=box_item['name']))
                        if r_c.exists(redis_key(box_item['id'])):
                            crate_logger.debug('Deleting {obj_id}, '
                                               '{obj_name}'.format(obj_id=box_item['id'], obj_name=box_item['name']))
                            r_c.delete(redis_key(box_item['id']))
    redis_set(cache_client=r_c, cloud_item=b_folder, last_modified_time=os.path.getmtime(path),
              box_dir_path=BOX_DIR, fresh_download=not r_c.exists(redis_key(box_folder['id'])),
              folder=os.path.dirname(path),
              sub_ids=ids_in_folder, parent_id=p_id)


def re_walk(path, box_folder, client, oauth_obj):
    """

    :param path:
    :param box_folder:
    :param client:
    :param oauth_obj:
    :return:
    """
    while True:
        walk_and_notify_and_download_tree(path, box_folder, client, oauth_obj)
        time.sleep(3600)  # once an hour we walk the tree
