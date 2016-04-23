import configparser
import os
import threading
import time
import traceback
from functools import partial

import pyinotify
from boxsdk import Client
from boxsdk.exception import BoxAPIException
from requests import ConnectionError
from requests.packages.urllib3.exceptions import ProtocolError

from diycrate.oauth_utils import setup_oauth, store_tokens_callback
from diycrate.cache_utils import redis_key, redis_get, r_c

trash_directory = os.path.expanduser('~/.local/share/Trash/files')


conf_obj = configparser.ConfigParser()
conf_dir = os.path.abspath(os.path.expanduser('~/.config/diycrate'))
if not os.path.isdir(conf_dir):
    os.mkdir(conf_dir)
cloud_credentials_file_path = os.path.join(conf_dir, 'box.ini')
if not os.path.isfile(cloud_credentials_file_path):
    open(cloud_credentials_file_path, 'w').write('')
conf_obj.read(cloud_credentials_file_path)
BOX_DIR = os.path.expanduser(conf_obj['box']['directory'])


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
        self.upload_queue = kargs['upload_queue']
        self.operations = []
        self.wait_time = 1
        self.oauth = kargs.get('oauth')
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
            client = Client(self.oauth)
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
            client = Client(self.oauth)
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
                        self.upload_queue.put([last_modified_time,
                                          partial(cur_box_folder.upload, dest_event.pathname, dest_event.name),
                                               self.oauth])
                    elif is_dir and not did_find_src_folder:
                        self.upload_queue.put(partial(cur_box_folder.create_subfolder, dest_event.name))
                        wm.add_watch(dest_event.pathname, rec=True, mask=mask)

        elif operation == 'create':
            print("Creating:", event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(self.oauth)
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
                            self.upload_queue.put(partial(a_file.update_contents, event.pathname))
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
                self.upload_queue.put([last_modified_time, partial(cur_box_folder.upload, event.pathname, event.name),
                                       self.oauth])
            elif is_dir and not did_find_the_folder:
                print('Upload the folder: ', event.pathname)
                self.upload_queue.put(partial(cur_box_folder.create_subfolder, event.name))
                wm.add_watch(event.pathname, rec=True, mask=mask)
        elif operation == 'close':
            print("Closing...:", event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(self.oauth)
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
                                info = redis_get(r_c, cur_file) if was_versioned else None
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
                                    self.upload_queue.put([last_modified_time,
                                                      partial(cur_file.update_contents, event.pathname),
                                                           self.oauth])
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
                self.upload_queue.put([last_modified_time,
                                  partial(cur_box_folder.upload, event.pathname, event.name),
                                       self.oauth])
            if is_dir and not did_find_the_folder:
                print('Creating a sub-folder...', event.pathname)
                self.upload_queue.put(partial(cur_box_folder.create_subfolder, event.name))
                wm.add_watch(event.pathname, rec=True, mask=mask)
        elif operation == 'real_close':
            print("Real  close...:", event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(self.oauth)
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
                self.upload_queue.put([last_modified_time,
                                  partial(cur_box_folder.upload, event.pathname, event.name),
                                       self.oauth])
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
        if not event.name.startswith('.~lock'):  # avoid propagating lock files
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
        if not event.name.startswith('.~lock'):  # avoid propagating lock files
            self.operations.append([event, 'close'])

    def process_IN_MOVED_FROM(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        if not event.name.startswith('.~lock'):  # avoid propagating lock files
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
        print("Moved to:", event.pathname)  # allow moving from a ~.lock file...i guess that may be okay

    def process_IN_CLOSE(self, event):
        if not event.name.startswith('.~lock'):  # avoid propagating lock files
            print('Had a close on:', event)
            self.operations.append([event, 'real_close'])


wm = pyinotify.WatchManager()
in_delete = getattr(pyinotify, 'IN_DELETE')
in_modify = getattr(pyinotify, 'IN_MODIFY')
in_close_write = getattr(pyinotify, 'IN_CLOSE_WRITE')
in_moved_to = getattr(pyinotify, 'IN_MOVED_TO')
in_moved_from = getattr(pyinotify, 'IN_MOVED_FROM')
mask = in_delete | in_modify | in_close_write | in_moved_to | in_moved_from  # watched events