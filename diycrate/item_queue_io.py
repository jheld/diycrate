import json
import os
import time
import traceback
from functools import partial

from boxsdk import Client
from boxsdk.exception import BoxAPIException
from boxsdk.object.file import File
from requests import ConnectionError
from requests.packages.urllib3.exceptions import ProtocolError

from diycrate.application import upload_queue, oauth, redis_set, uploads_given_up_on, download_queue, redis_get, r_c, \
    wm, mask
from diycrate.cache_utils import redis_key, redis_set, redis_get


def upload_queue_processor():
    """
    Implements a simple re-try mechanism for pending uploads
    :return:
    """
    while True:
        if upload_queue.not_empty:
            callable_up = upload_queue.get()  # blocks
            # TODO: pass in the actual item being updated/uploaded, so we can do more intelligent retry mechanisms
            was_list = isinstance(callable_up, list)
            last_modified_time = None
            if was_list:
                last_modified_time, callable_up = callable_up
            args = callable_up.args if isinstance(callable_up, partial) else None
            num_retries = 15
            for x in range(15):
                try:
                    ret_val = callable_up()
                    if was_list:
                        item = ret_val  # is the new/updated item
                        if isinstance(item, File):
                            client = Client(oauth)
                            file_obj = client.file(file_id=item.object_id).get()
                            redis_set(file_obj, last_modified_time)
                    break
                except BoxAPIException as e:
                    print(args, traceback.format_exc())
                    if e.status == 409:
                        print('Apparently Box says this item already exists...'
                              'and we were trying to create it. Need to handle this better')
                        break
                except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError):
                    time.sleep(3)
                    print(args, traceback.format_exc())
                    if x >= num_retries - 1:
                        print('Upload giving up on: {}'.format(callable_up))
                        # no immediate plans to do anything with this info, yet.
                        uploads_given_up_on.append(callable_up)
                except (TypeError, FileNotFoundError) as e:
                    print(traceback.format_exc())
                    break
            upload_queue.task_done()


def download_queue_processor():
    """
    Implements a simple re-try mechanism for pending downloads
    :return:
    """
    while True:
        if download_queue.not_empty:
            item, path = download_queue.get()  # blocks
            if item['type'] == 'file':
                info = redis_get(item) if r_c.exists(redis_key(item['id'])) else None
                client = Client(oauth)
                # hack because we did not use to store the file_path, but do not want to force a download
                if info and 'file_path' not in info:
                    info['file_path'] = path
                    r_c.set(redis_key(item['id']), json.dumps(info))
                # no version, or diff version, or the file does not exist locally
                if not info or info['etag'] != item['etag'] or not os.path.exists(path):
                    try:
                        for i in range(15):
                            if os.path.basename(path).startswith('.~lock'):  # avoid downloading lock files
                                break
                            with open(path, 'wb') as item_handler:
                                print('About to download: ', item['name'], item['id'])
                                item.download_to(item_handler)
                                path_to_add = os.path.dirname(path)
                                wm.add_watch(path=path_to_add, mask=mask, rec=True, auto_add=True)
                            was_versioned = r_c.exists(redis_key(item['id']))
                            #
                            # version_info[item['id']] = version_info.get(item['id'], {'etag': item['etag'],
                            #                                                          'fresh_download': True,
                            #                                                          'time_stamp': time.time()})
                            # version_info[item['id']]['etag'] = item['etag']
                            # version_info[item['id']]['fresh_download'] = not was_versioned
                            # version_info[item['id']]['time_stamp'] = os.path.getmtime(path)  # duh...since we have it!
                            redis_set(item, os.path.getmtime(path), fresh_download=not was_versioned,
                                      folder=os.path.dirname(path))
                            break
                    except (ConnectionResetError, ConnectionError):
                        print(traceback.format_exc())
                        time.sleep(5)
                download_queue.task_done()
            else:
                download_queue.task_done()