import argparse
import configparser
import os
import json
import queue
import threading
import time
import traceback
import webbrowser
from functools import partial

import bottle
import pyinotify
import redis
from bottle import ServerAdapter
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException
from boxsdk.object.folder import File
from cherrypy import wsgiserver
from cherrypy.wsgiserver import ssl_builtin
from requests.exceptions import ConnectionError
from requests.packages.urllib3.exceptions import ProtocolError

cloud_provider_name = 'Box'

csrf_token = ''

bottle_app = bottle.Bottle()

# The watch manager stores the watches and provides operations on watches
wm = pyinotify.WatchManager()

# keep the lint-ing & introspection from complaining that these attributes don't exist before run-time.
in_delete = getattr(pyinotify, 'IN_DELETE')
in_modify = getattr(pyinotify, 'IN_MODIFY')
in_close_write = getattr(pyinotify, 'IN_CLOSE_WRITE')
in_moved_to = getattr(pyinotify, 'IN_MOVED_TO')
in_moved_from = getattr(pyinotify, 'IN_MOVED_FROM')
mask = in_delete | in_modify | in_close_write | in_moved_to | in_moved_from  # watched events

BOX_DIR = os.path.expanduser('~/box')

download_queue = queue.Queue()

upload_queue = queue.Queue()

uploads_given_up_on = []

r_c = redis.StrictRedis()


def redis_key(key):
    """

    :param key:
    :return:
    """
    return 'diy_crate.version.{}'.format(key)


def redis_set(obj, last_modified_time, fresh_download=False, folder=None):
    """

    :param obj:
    :param last_modified_time:
    :param fresh_download:
    :return:
    """
    key = redis_key(obj['id'])
    if folder:
        path = folder
    elif int(obj['path_collection']['total_count']) > 1:
        path = '{}'.format(os.path.pathsep).join([folder['name']
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


def redis_get(obj):
    """

    :param obj:
    :return:
    """
    key = redis_key(obj['id'])
    return json.loads(str(r_c.get(key), encoding='utf-8', errors='strict'))


def upload_queue_processor():
    """

    :return:
    """
    while True:
        if upload_queue.not_empty:
            callable_up = upload_queue.get()  # blocks
            was_list = isinstance(callable_up, list)
            last_modified_time = None
            if was_list:
                last_modified_time, callable_up = callable_up
            num_retries = 15
            for x in range(15):
                try:
                    ret_val = callable_up()
                    if was_list:
                        item = ret_val  # is the new/updated item
                        if isinstance(item, File):
                            client = Client(oauth)
                            file_obj = client.file(file_id=item.object_id).get()
                            # version_info[item.object_id] = version_info.get(file_obj['id'],
                            #                                                 {'fresh_download': True,
                            #                                                  'time_stamp': 0, 'etag': '0'})
                            # version_info[file_obj['id']]['fresh_download'] = False
                            # version_info[file_obj['id']]['time_stamp'] = last_modified_time
                            # version_info[file_obj['id']]['etag'] = file_obj['etag']
                            redis_set(file_obj, last_modified_time)
                    break
                except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError, BoxAPIException):
                    time.sleep(3)
                    print(traceback.format_exc())
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
                            redis_set(item, os.path.getmtime(path), fresh_download=not was_versioned, folder=os.path.dirname(path))
                            break
                    except (ConnectionResetError, ConnectionError):
                        print(traceback.format_exc())
                        time.sleep(5)
                download_queue.task_done()
            else:
                download_queue.task_done()


download_thread = threading.Thread(target=download_queue_processor)
upload_thread = threading.Thread(target=upload_queue_processor)

trash_directory = os.path.expanduser('~/.local/share/Trash/files')


def download_queue_monitor():
    """

    :return:
    """
    while True:
        time.sleep(10)
        if download_queue.not_empty:
            print('Download queue size:', download_queue.qsize())
        else:
            print('Download queue is empty.')


def upload_queue_monitor():
    """

    :return:
    """
    while True:
        time.sleep(10)
        if upload_queue.not_empty:
            print('Upload queue size:', upload_queue.qsize())
        else:
            print('Upload queue is empty.')


upload_mon_thread = threading.Thread(target=upload_queue_monitor)
upload_mon_thread.daemon = True
upload_mon_thread.start()

download_mon_thread = threading.Thread(target=download_queue_monitor)
download_mon_thread.daemon = True
download_mon_thread.start()


class EventHandler(pyinotify.ProcessEvent):
    """
    EventHandler to manage cloud storage synchronization.
    """

    def my_init(self, **kargs):
        """
        Extends the super to add cloud storage state.
        :return:
        """
        super(EventHandler, self).my_init()
        self.move_events = []
        self.files_from_box = []
        self.folders_from_box = []
        self.operations = []
        self.wait_time = 1
        self.operations_thread = threading.Thread(target=self.operation_coalesce)
        self.operations_thread.daemon = True
        self.operations_thread.start()

    def operation_coalesce(self):
        """
        Coalesce and process the operations in a more timely-fashion.
        :return:
        """
        while True:
            time.sleep(self.wait_time)
            cur_num_operations = len(self.operations)
            operations_to_perform = self.operations[:cur_num_operations]  # keep a local copy for this loop-run
            # operations list could have changed since the previous two instructions
            self.operations = self.operations[cur_num_operations:] if len(self.operations) > cur_num_operations else []
            for operation in operations_to_perform:
                self.process_event(*operation)

    @staticmethod
    def get_folder(client, folder_id):
        """

        :param client:
        :param folder_id:
        :return:
        """
        folder = None
        num_retry = 15
        for x in range(num_retry):
            try:
                folder = client.folder(folder_id=folder_id).get()
                break
            except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError, BoxAPIException):
                print(traceback.format_exc())
                if x >= num_retry - 1:
                    print('Failed for the last time to get the folder: ', folder_id)
        return folder

    @staticmethod
    def folders_to_traverse(event_path):
        """

        :param event_path:
        :return:
        """
        folders_to_traverse = [folder for folder in event_path.replace(BOX_DIR, '').split(os.path.sep) if
                               folder and folder != '/']
        if len(folders_to_traverse) and folders_to_traverse[0][0] == '/':
            folders_to_traverse[0] = folders_to_traverse[0][1:]
        return folders_to_traverse

    @staticmethod
    def traverse_path(client, event, cur_box_folder, folders_to_traverse):
        """

        :param cur_box_folder:
        :param client:
        :param event:
        :param folders_to_traverse:
        :return:
        """
        for folder in folders_to_traverse:
            did_find_folder = False
            num_entries = cur_box_folder['item_collection']['total_count']
            limit = 100
            for offset in range(0, num_entries, limit):
                for entry in cur_box_folder.get_items(offset=offset, limit=limit):
                    if folder == entry['name'] and entry['type'] == 'folder':
                        did_find_folder = True
                        cur_box_folder = EventHandler.get_folder(client, entry['id'])
                        break  # found this piece of the path, keep going
            if not did_find_folder:
                try:
                    cur_box_folder = cur_box_folder.create_subfolder(folder).get()
                    print('Subfolder creation: ', event.pathname)
                except BoxAPIException as e:
                    print(e)
        return cur_box_folder

    def process_event(self, event, operation):
        """
        Wrapper to process the given event on the operation.
        :param event:
        :param operation:
        :return:
        """
        if operation == 'delete':
            print('Doing a delete on, ', event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = client.folder(folder_id='0').get()
            cur_box_folder = box_folder
            # if we're modifying in root box dir, then we've already found the folder
            is_base = BOX_DIR in (event.path, event.path[:-1],)
            cur_box_folder = self.traverse_path(client, event, cur_box_folder, folders_to_traverse)
            last_dir = os.path.split(event.path)[-1]
            if not is_base:
                AssertionError(cur_box_folder['name'] == last_dir,
                               cur_box_folder['name'] + 'not equals ' + last_dir)
            event_was_for_dir = 'IN_ISDIR'.lower() in event.maskname.lower()
            num_entries = cur_box_folder['item_collection']['total_count']
            limit = 100
            for offset in range(0, num_entries, limit):
                for entry in cur_box_folder.get_items(offset=offset, limit=limit):
                    if not event_was_for_dir and entry['type'] == 'file' and entry['name'] == event.name:
                        if entry['id'] not in self.files_from_box:
                            cur_file = client.file(file_id=entry['id']).get()
                            if cur_file.delete():  # does not actually check correctly...unless not "ok" is false
                                # del version_info[cur_file['id']]
                                r_c.delete(redis_key(cur_file['id']))
                        else:
                            self.files_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
                    elif event_was_for_dir and entry['type'] == 'folder' and entry['name'] == event.name:
                        if entry['id'] not in self.folders_from_box:
                            self.get_folder(client, entry['id']).delete()
                            # cur_folder = client.folder(folder_id=entry['id']).get()
                            # upload_queue.put(partial(cur_folder.update_contents, event.pathname))
                        else:
                            self.folders_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
        elif operation == 'move':
            print('Doing a move on, ', event)
            src_event, dest_event = event
            folders_to_traverse = self.folders_to_traverse(dest_event.path)
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = client.folder(folder_id='0').get()
            cur_box_folder = box_folder
            # if we're modifying in root box dir, then we've already found the folder
            cur_box_folder = self.traverse_path(client, dest_event, cur_box_folder, folders_to_traverse)
            src_folders_to_traverse = self.folders_to_traverse(src_event.path)
            src_box_folder = box_folder
            src_box_folder = self.traverse_path(client, src_event, src_box_folder, src_folders_to_traverse)
            is_rename = src_event.path == dest_event.path
            # is_a_directory = 'IN_ISDIR'.lower() in dest_event.maskname.lower()
            did_find_src_file = os.path.isdir(dest_event.pathname)  # true if we are a directory :)
            did_find_src_folder = os.path.isfile(dest_event.pathname)  # true if we are a regular file :)
            is_file = os.path.isfile(dest_event.pathname)
            is_dir = os.path.isdir(dest_event.pathname)
            move_from_remote = False
            src_num_entries = src_box_folder['item_collection']['total_count']
            limit = 100
            for offset in range(0, src_num_entries, limit):
                for entry in src_box_folder.get_items(offset=offset, limit=limit):
                    did_find_src_file = is_file and entry['name'] == src_event.name and entry['type'] == 'file'
                    did_find_src_folder = is_dir and entry['name'] == src_event.name and entry['type'] == 'folder'
                    if did_find_src_file:
                        src_file = client.file(file_id=entry['id']).get()
                        if is_rename:
                            src_file.rename(dest_event.name)
                        else:
                            src_file.move(cur_box_folder)
                            # do not yet support moving and renaming in one go
                            assert src_file['name'] == dest_event.name
                    elif did_find_src_folder:
                        src_folder = client.folder(folder_id=entry['id']).get()
                        if is_rename:
                            src_folder.rename(dest_event.name)
                        else:
                            src_folder.move(cur_box_folder)
                            # do not yet support moving and renaming in one go
                            assert src_folder['name'] == dest_event.name
                    elif entry['name'] == dest_event.name:
                        move_from_remote = True
            if not move_from_remote:  # if it was moved from a different folder on remote, could be false still
                dest_box_folder = box_folder
                dest_folders_to_traverse = self.folders_to_traverse(dest_event.path)
                dest_box_folder = self.traverse_path(client, dest_event, dest_box_folder, dest_folders_to_traverse)
                dest_num_entries = dest_box_folder['item_collection']['total_count']
                limit = 100
                for offset in range(0, dest_num_entries, limit):
                    for entry in cur_box_folder.get_items(offset=offset, limit=limit):
                        if entry['name'] == dest_event.name:
                            move_from_remote = True
                            break
                if not move_from_remote:
                    if is_file and not did_find_src_file:
                        # src file [should] no longer exist[s]. this file did not originate in box, too.
                        last_modified_time = os.path.getmtime(dest_event.pathname)
                        upload_queue.put([last_modified_time,
                                          partial(cur_box_folder.upload, dest_event.pathname, dest_event.name)])
                    elif is_dir and not did_find_src_folder:
                        upload_queue.put(partial(cur_box_folder.create_subfolder, dest_event.name))
                        wm.add_watch(dest_event.pathname, rec=True, mask=mask)

        elif operation == 'create':
            print("Creating:", event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = client.folder(folder_id='0').get()
            cur_box_folder = box_folder
            # if we're modifying in root box dir, then we've already found the folder
            is_base = BOX_DIR in (event.path, event.path[:-1],)
            cur_box_folder = self.traverse_path(client, event, cur_box_folder, folders_to_traverse)
            last_dir = os.path.split(event.path)[-1]
            if not is_base:
                assert cur_box_folder['name'] == last_dir
            did_find_the_file = os.path.isdir(event.pathname)  # true if we are a directory :)
            did_find_the_folder = os.path.isfile(event.pathname)  # true if we are a regular file :)
            is_file = os.path.isfile(event.pathname)
            is_dir = os.path.isdir(event.pathname)
            num_entries = cur_box_folder['item_collection']['total_count']
            limit = 100
            for offset in range(0, num_entries, limit):
                for entry in cur_box_folder.get_items(offset=offset, limit=limit):
                    did_find_the_file = is_file and entry['type'] == 'file' and entry['name'] == event.name
                    did_find_the_folder = is_dir and entry['type'] == 'folder' and entry['name'] == event.name
                    if did_find_the_file:
                        if entry['id'] not in self.files_from_box:
                            # more accurately, was this created offline?
                            AssertionError(False,
                                           'We should not be able to create a '
                                           'file that exists in box; should be a close/modify.')
                            print('Update the file: ', event.pathname)
                            a_file = client.file(file_id=entry['id']).get()
                            # seem it is possible to get more than one create (without having a delete in between)
                            upload_queue.put(partial(a_file.update_contents, event.pathname))
                            # cur_box_folder.upload(event.pathname, event.name)
                        else:
                            self.files_from_box.remove(entry['id'])  # just downloaded it
                        break
                    elif did_find_the_folder:
                        # we are not going to re-create the folder, but we are also not checking if the contents in this
                        # local creation are different from the contents in box.
                        if entry['id'] in self.folders_from_box:
                            self.folders_from_box.remove(entry['id'])  # just downloaded it
                        break
            if is_file and not did_find_the_file:
                print('Upload the file: ', event.pathname)
                last_modified_time = os.path.getctime(event.pathname)
                upload_queue.put([last_modified_time, partial(cur_box_folder.upload, event.pathname, event.name)])
            elif is_dir and not did_find_the_folder:
                print('Upload the folder: ', event.pathname)
                upload_queue.put(partial(cur_box_folder.create_subfolder, event.name))
                wm.add_watch(event.pathname, rec=True, mask=mask)
        elif operation == 'close':
            print("Closing...:", event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = cur_box_folder = None
            for _ in range(5):
                try:
                    box_folder = client.folder(folder_id='0').get()
                    cur_box_folder = box_folder
                    break
                except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError, BoxAPIException):
                    print(traceback.format_exc())
            # if we're modifying in root box dir, then we've already found the folder
            is_base = BOX_DIR in (event.path, event.path[:-1],)
            cur_box_folder = self.traverse_path(client, event, cur_box_folder, folders_to_traverse)
            last_dir = os.path.split(event.path)[-1]
            if not is_base:
                AssertionError(cur_box_folder['name'] == last_dir,
                               cur_box_folder['name'] + 'not equals ' + last_dir)
            did_find_the_file = os.path.isdir(event.pathname)  # true if we are a directory :)
            did_find_the_folder = os.path.isfile(event.pathname)  # true if we are a regular file :)
            is_file = os.path.isfile(event.pathname)
            is_dir = os.path.isdir(event.pathname)
            num_entries = cur_box_folder['item_collection']['total_count']
            limit = 100
            for offset in range(0, num_entries, limit):
                for entry in cur_box_folder.get_items(offset=offset, limit=limit):
                    did_find_the_file = is_file and entry['type'] == 'file' and entry['name'] == event.name
                    did_find_the_folder = is_dir and entry['type'] == 'folder' and entry['name'] == event.name
                    if did_find_the_file:
                        last_modified_time = os.path.getmtime(event.pathname)
                        if entry['id'] not in self.files_from_box:
                            cur_file = client.file(file_id=entry['id']).get()
                            can_update = True
                            was_versioned = r_c.exists(redis_key(cur_file['id']))
                            try:
                                info = redis_get(cur_file)
                                info = info if was_versioned else {'fresh_download': True,
                                                                   'etag': '0', 'time_stamp': 0}
                                item_version = info
                                if cur_file['etag'] == item_version['etag'] and \
                                        ((item_version['fresh_download'] and item_version[
                                            'time_stamp'] >= last_modified_time) or
                                             (not item_version['fresh_download'] and item_version[
                                                 'time_stamp'] >= last_modified_time)):
                                    can_update = False
                                if can_update:
                                    upload_queue.put([last_modified_time,
                                                      partial(cur_file.update_contents, event.pathname)])
                                else:
                                    print('Skipping the update because not versioned: {}, '
                                          'fresh_download: {}, '
                                          'version time_stamp >= '
                                          'new time stamp: {}'.format(not was_versioned,
                                                                      item_version['fresh_download'],
                                                                      item_version['time_stamp'] >= last_modified_time),
                                          event.pathname, cur_file['id'])
                            except TypeError as e:
                                print(traceback.format_exc())
                            except Exception:
                                print(traceback.format_exc())

                        else:
                            self.files_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
                    elif did_find_the_folder:
                        if entry['id'] not in self.folders_from_box:
                            print('Cannot create a subfolder when it already exists: ', event.pathname)
                            # cur_folder = client.folder(folder_id=entry['id']).get()
                            # upload_queue.put(partial(cur_folder.update_contents, event.pathname))
                        else:
                            self.folders_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
            if is_file and not did_find_the_file:
                print('Uploading contents...', event.pathname)
                last_modified_time = os.path.getmtime(event.pathname)
                upload_queue.put([last_modified_time,
                                  partial(cur_box_folder.upload, event.pathname, event.name)])
            if is_dir and not did_find_the_folder:
                print('Creating a sub-folder...', event.pathname)
                upload_queue.put(partial(cur_box_folder.create_subfolder, event.name))
                wm.add_watch(event.pathname, rec=True, mask=mask)
        elif operation == 'real_close':
            print("Real  close...:", event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = cur_box_folder = None
            for _ in range(5):
                try:
                    box_folder = client.folder(folder_id='0').get()
                    cur_box_folder = box_folder
                    break
                except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError, BoxAPIException):
                    print(traceback.format_exc())
            # if we're modifying in root box dir, then we've already found the folder
            is_base = BOX_DIR in (event.path, event.path[:-1],)
            cur_box_folder = self.traverse_path(client, event, cur_box_folder, folders_to_traverse)
            last_dir = os.path.split(event.path)[-1]
            if not is_base:
                AssertionError(cur_box_folder['name'] == last_dir,
                               cur_box_folder['name'] + 'not equals ' + last_dir)
            did_find_the_file = os.path.isdir(event.pathname)  # true if we are a directory :)
            did_find_the_folder = os.path.isfile(event.pathname)  # true if we are a regular file :)
            is_file = os.path.isfile(event.pathname)
            is_dir = os.path.isdir(event.pathname)
            num_entries = cur_box_folder['item_collection']['total_count']
            limit = 100
            for offset in range(0, num_entries, limit):
                for entry in cur_box_folder.get_items(offset=offset, limit=limit):
                    did_find_the_file = is_file and entry['type'] == 'file' and entry['name'] == event.name
                    did_find_the_folder = is_dir and entry['type'] == 'folder' and entry['name'] == event.name
                    if did_find_the_file:
                        break
            # not a box file/folder (though could have been copied from a local box item)
            if is_file and not did_find_the_file:
                last_modified_time = os.path.getmtime(event.pathname)
                upload_queue.put([last_modified_time,
                                  partial(cur_box_folder.upload, event.pathname, event.name)])
            elif is_dir and not did_find_the_folder:
                cur_box_folder.create_subfolder(event.name)
                wm.add_watch(event.pathname, rec=True, mask=mask, auto_add=True)
                # TODO: recursively add this directory to box

    def process_IN_CREATE(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        self.operations.append([event, 'create'])

    def process_IN_DELETE(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        self.operations.append([event, 'delete'])

    def process_IN_MODIFY(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        self.operations.append([event, 'close'])

    def process_IN_MOVED_FROM(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        print("Moved from:", event.pathname)
        self.move_events.append(event)

    def process_IN_MOVED_TO(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        found_from = False
        for move_event in self.move_events:
            if move_event.cookie == event.cookie and 'in_moved_from' in move_event.maskname.lower():
                found_from = True
                if os.path.commonprefix([trash_directory, event.pathname]) == trash_directory:
                    self.operations.append([move_event, 'delete'])
                else:
                    self.operations.append([[move_event, event], 'move'])
                break
        if not found_from:
            self.operations.append([event, 'close'])  # "close"/"modify" seems appropriate
        print("Moved to:", event.pathname)

    def process_IN_CLOSE(self, event):
        print('Had a close on:', event)
        self.operations.append([event, 'real_close'])


handler = EventHandler()

notifier = pyinotify.ThreadedNotifier(wm, handler, read_freq=10)
notifier.coalesce_events()


def store_tokens_callback(access_token, refresh_token):
    """
    Intention is to store the oauth tokens
    :param access_token:
    :param refresh_token:
    :return:
    """
    assert access_token
    assert refresh_token
    r_c.set('diy_crate.auth.access_token', access_token)
    r_c.set('diy_crate.auth.refresh_token', refresh_token)


# def walk_and_notify_tree(path):
#     if os.path.isdir(path):
#         wm.add_watch(path, mask, rec=True)
#     for _, dirs, _ in os.scandir(path):
#         for a_dir in dirs:
#             walk_and_notify_tree(os.path.join(path, a_dir))

def walk_and_notify_and_download_tree(path, box_folder, client):
    """
    Walk the path recursively and add watcher and create the path.
    :param path:
    :param box_folder:
    :param client:
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
            upload_queue.put([os.path.getmtime(local_path), partial(cur_box_folder.upload, local_path, local_file)])
    for offset in range(0, num_entries_in_folder, limit):
        for box_item in b_folder.get_items(limit=limit, offset=offset):
            if box_item['name'] in local_files:
                local_files.remove(box_item['name'])
            if box_item['type'] == 'folder':
                local_path = os.path.join(path, box_item['name'])
                if not os.path.isdir(local_path):
                    os.mkdir(local_path)
                walk_and_notify_and_download_tree(local_path, client.folder(folder_id=box_item['id']).get(), client)
            else:
                try:
                    file_obj = box_item
                    download_queue.put((file_obj, os.path.join(path, box_item['name'])))
                    # open(os.path.join(path, box_item['name']), 'wb').write(client.file(file_id=box_item['id']).get().content())
                except BoxAPIException as e:
                    print(traceback.format_exc())
                    if e.status == 404:
                        print('Box says: {}, {}, is a 404 status.'.format(box_item['id'], box_item['name']))
                        if r_c.exists(redis_key(box_item['id'])):
                            print('Deleting {}, {}'.format(box_item['id'], box_item['name']))
                            r_c.delete(redis_key(box_item['id']))


def re_walk(path, box_folder, client):
    while True:
        walk_and_notify_and_download_tree(path, box_folder, client)
        time.sleep(3600)  # once an hour we walk the tree


def long_poll_event_listener():
    """

    :return:
    """
    client = Client(oauth=oauth)
    while True:
        try:
            stream_position = client.events().get_latest_stream_position()
            for event in client.events().generate_events_with_long_polling(stream_position=stream_position):
                print(event, ' happened!')
                if event.get('message', '').lower() == 'reconnect':
                    break
                if event['event_type'] == 'ITEM_RENAME':
                    obj_id = event['source']['id']
                    obj_type = event['source']['type']
                    if obj_type == 'file':
                        if int(event['source']['path_collection']['total_count']) > 1:
                            path = '{}'.format(os.path.pathsep).join([folder['name']
                                                                      for folder in
                                                                      event['source']['path_collection']['entries'][1:]])
                        else:
                            path = ''
                        path = os.path.join(BOX_DIR, path)
                        file_path = os.path.join(path, event['source']['name'])
                        file_obj = client.file(file_id=obj_id).get()
                        src_file_path = None if not r_c.exists(redis_key(obj_id)) else redis_get(file_obj)['file_path']
                        if src_file_path and os.path.exists(src_file_path):
                            version_info = redis_get(obj=file_obj)
                            src_file_path = version_info['file_path']
                            os.rename(src_file_path, file_path)
                            version_info['file_path'] = file_path
                            version_info['etag'] = file_obj['etag']
                            r_c.set(redis_key(obj_id), json.dumps(version_info))
                        else:
                            download_queue.put([file_obj, file_path])
                elif event['event_type'] == 'ITEM_UPLOAD':
                    obj_id = event['source']['id']
                    obj_type = event['source']['type']
                    if obj_type == 'file':
                        if int(event['source']['path_collection']['total_count']) > 1:
                            path = '{}'.format(os.path.pathsep).join([folder['name']
                                                                      for folder in
                                                                      event['source']['path_collection']['entries'][1:]])
                        else:
                            path = ''
                        path = os.path.join(BOX_DIR, path)
                        if not os.path.exists(path):  # just in case this is a file in a new subfolder
                            os.makedirs(path)
                        download_queue.put([client.file(file_id=obj_id).get(), os.path.join(path, event['source']['name'])])
                        # was_versioned = r_c.exists(redis_key(obj_id))
                        # if not was_versioned:
                        #     if int(event['source']['path_collection']['total_count']) > 1:
                        #         path = '{}'.format(os.path.pathsep).join([folder['name']
                        #                                                   for folder in
                        #                                                   event['source']['path_collection']['entries'][1:]])
                        #     else:
                        #         path = ''
                        #     path = os.path.join(BOX_DIR, path)
                        #     download_queue.put([client.file(file_id=obj_id).get(), os.path.join(path, event['source']['name'])])
                        #     # with open(os.path.join(path, event['source']['name']), 'w') as new_file_handler:
                        #     #     client.file(file_id=obj_id).get().download_to(new_file_handler)
                        #     #
                        #     # r_c.set(redis_key([obj_id]), json.dumps({'etag': event['source']['etag'],
                        #     #                         'fresh_download': True,
                        #     #                         'time_stamp': time.time()}))
                        # else:
                        #     pass
                elif event['event_type'] == 'ITEM_TRASH':
                    obj_id = event['source']['id']
                    obj_type = event['source']['type']
                    if obj_type == 'file':
                        if int(event['source']['path_collection']['total_count']) > 1:
                            path = '{}'.format(os.path.pathsep).join([folder['name']
                                                                      for folder in
                                                                      event['source']['path_collection']['entries'][1:]])
                        else:
                            path = ''
                        path = os.path.join(BOX_DIR, path)
                        file_path = os.path.join(path, event['source']['name'])
                        if os.path.exists(file_path):
                            os.unlink(file_path)
                        if r_c.exists(redis_key(obj_id)):
                            r_c.delete(redis_key(obj_id))
                elif event['event_type'] == 'ITEM_DOWNLOAD':
                    pass
        except Exception:
            print(traceback.format_exc())

long_poll_thread = threading.Thread(target=long_poll_event_listener)
long_poll_thread.daemon = True


walk_thread = None

@bottle_app.route('/')
def oauth_handler():
    """
    RESTful end-point for the oauth handling
    :return:
    """
    assert csrf_token == bottle.request.GET['state']
    access_token, refresh_token = oauth.authenticate(bottle.request.GET['code'])
    start_manual_synching()

    return 'OK'


def start_manual_synching():
    client = Client(oauth)
    wm.add_watch(BOX_DIR, mask, rec=False)
    box_folder = client.folder(folder_id='0').get()
    download_thread.daemon = True
    download_thread.start()
    upload_thread.daemon = True
    upload_thread.start()
    # local trash can
    wm.add_watch(trash_directory, mask=in_moved_to | in_moved_from)
    if not long_poll_thread.is_alive():  # start before doing anything else
        long_poll_thread.start()
    # walk_and_notify_and_download_tree(BOX_DIR, box_folder, client)
    global walk_thread
    walk_thread = threading.Thread(target=re_walk, args=(BOX_DIR, box_folder, client,))
    walk_thread.daemon = True
    walk_thread.start()


# Create our own sub-class of Bottle's ServerAdapter
# so that we can specify SSL. Using just server='cherrypy'
# uses the default cherrypy server, which doesn't use SSL
class SSLCherryPyServer(ServerAdapter):
    """
    Custom server adapter using cherry-py with ssl
    """
    def run(self, server_handler):
        """
        Overrides super to setup Cherry py with ssl and start the server.
        :param server_handler: originating server type
        :type server_handler:
        """
        server = wsgiserver.CherryPyWSGIServer((self.host, self.port), server_handler)
        # Uses the following github page's recommendation for setting up the cert:
        # https://github.com/nickbabcock/bottle-ssl
        server.ssl_adapter = ssl_builtin.BuiltinSSLAdapter('cacert.pem', 'privkey.pem')
        try:
            server.start()
        finally:
            server.stop()


if __name__ == '__main__':
    conf_obj = configparser.ConfigParser()
    conf_dir = os.path.abspath(os.path.expanduser('~/.config/diycrate'))
    if not os.path.isdir(conf_dir):
        os.mkdir(conf_dir)
    cloud_credentials_file_path = os.path.join(conf_dir, 'box.ini')
    if not os.path.isfile(cloud_credentials_file_path):
        open(cloud_credentials_file_path, 'w').write('')
    conf_obj.read(cloud_credentials_file_path)
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--client_id', type=str, help='Client ID provided by {}'.format(cloud_provider_name),
                            default='')
    arg_parser.add_argument('--client_secret', type=str,
                            help='Client Secret provided by {}'.format(cloud_provider_name), default='')
    arg_parser.add_argument('--box_dir', type=str, help='directory for the root of box', default='~/box')
    args = arg_parser.parse_args()
    had_oauth2 = conf_obj.has_section('oauth2')
    if not had_oauth2:
        conf_obj.add_section('oauth2')
    conf_obj['oauth2'] = {
        'client_id': args.client_id or conf_obj['oauth2']['client_id'],
        'client_secret': args.client_secret or conf_obj['oauth2']['client_secret']
    }

    conf_obj.write(open(cloud_credentials_file_path, 'w'))
    oauth = OAuth2(
        client_id=conf_obj['oauth2']['client_id'],
        client_secret=conf_obj['oauth2']['client_secret'],
        store_tokens=store_tokens_callback,
        access_token=r_c.get('diy_crate.auth.access_token'),
        refresh_token=r_c.get('diy_crate.auth.refresh_token')
    )
    had_box = conf_obj.has_section('box')
    if not had_box:
        conf_obj.add_section('box')

    conf_obj['box'] = {
        'directory': args.box_dir or conf_obj['box']['directory']
    }
    conf_obj.write(open(cloud_credentials_file_path, 'w'))
    BOX_DIR = os.path.expanduser(conf_obj['box']['directory'])
    if not os.path.isdir(BOX_DIR):
        os.mkdir(BOX_DIR)
    if not r_c.exists('diy_crate.auth.access_token') and not r_c.exists('diy_crate.auth.refresh_token'):
        auth_url, csrf_token = oauth.get_authorization_url('https://diycrate.com:8080/')
        webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
    else:
        start_manual_synching()
    notifier.start()
    # notifier_thread = threading.Thread(target=notifier.loop)
    # notifier_thread.daemon = True
    # notifier_thread.start()
    if not r_c.exists('diy_crate.auth.access_token') and not r_c.exists('diy_crate.auth.refresh_token'):
        bottle_app.run(server=SSLCherryPyServer)
