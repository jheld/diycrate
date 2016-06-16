import configparser
import os
import threading
import logging
import time
import traceback
from logging import handlers
from functools import partial

import pyinotify
from boxsdk import Client
from boxsdk.exception import BoxAPIException
from boxsdk.object.file import File
from boxsdk.object.folder import Folder
from requests import ConnectionError
from requests.packages.urllib3.exceptions import ProtocolError

from diycrate.cache_utils import redis_key, redis_get, r_c


crate_logger = logging.getLogger('diy_crate_logger')
crate_logger.setLevel(logging.DEBUG)

l_handler = handlers.SysLogHandler(address='/dev/log')

crate_logger.addHandler(l_handler)

log_format = 'diycrate' + ' %(levelname)-9s %(name)-15s %(threadName)-14s +%(lineno)-4d %(message)s'
log_format = logging.Formatter(log_format)
l_handler.setFormatter(log_format)

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
                crate_logger.debug(traceback.format_exc())
                if x >= num_retry - 1:
                    crate_logger.debug('Failed for the last time to get the folder: {}'.format(folder_id))
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
                    crate_logger.debug('Subfolder creation: {}'.format(event.pathname))
                except BoxAPIException as e:
                    crate_logger.debug(e)
        return cur_box_folder

    def process_event(self, event, operation):
        """
        Wrapper to process the given event on the operation.
        :param event:
        :param operation:
        :return:
        """
        if operation == 'delete':
            crate_logger.debug('Doing a delete on {}'.format(event.pathname))
            folders_to_traverse = self.folders_to_traverse(event.path)
            crate_logger.debug(folders_to_traverse)
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
            crate_logger.debug('Doing a move on: {}'.format(event))
            src_event, dest_event = event
            folders_to_traverse = self.folders_to_traverse(dest_event.path)
            crate_logger.debug(folders_to_traverse)
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
                            did_find_cur_file = os.path.isdir(dest_event.pathname)  # should check box instead
                            did_find_cur_folder = os.path.isfile(dest_event.pathname)  # should check box instead
                            cur_num_entries = cur_box_folder['item_collection']['total_count']
                            for cur_offset in range(0, cur_num_entries, limit):
                                for cur_entry in cur_box_folder.get_items(offset=cur_offset, limit=limit):
                                    matching_name = cur_entry['name'] == dest_event.name
                                    did_find_cur_file = is_file and matching_name and isinstance(cur_entry, File)
                                    did_find_cur_folder = is_dir and matching_name and isinstance(cur_entry, Folder)
                                    if did_find_cur_file:
                                        self.upload_queue.put([os.path.getmtime(dest_event.pathname),
                                                               partial(cur_entry.update_contents,
                                                                       dest_event.pathname),
                                                               self.oauth])
                                        self.upload_queue.put(partial(src_file.delete))
                                        break
                                    elif did_find_cur_folder:
                                        crate_logger.debug(
                                            'do not currently support movinga same name folder into parent with'
                                            'folder inside of the same name -- would may need to update the '
                                            'contents')
                                        break
                                if (is_file and did_find_cur_file) or (is_dir and did_find_cur_folder):
                                    break
                            if is_file and not did_find_cur_file:
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
            crate_logger.debug("Creating: {}".format(event.pathname))
            folders_to_traverse = self.folders_to_traverse(event.path)
            crate_logger.debug(folders_to_traverse)
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
                            crate_logger.debug('Update the file: {}'.format(event.pathname))
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
                crate_logger.debug('Upload the file: {}'.format(event.pathname))
                last_modified_time = os.path.getctime(event.pathname)
                self.upload_queue.put([last_modified_time, partial(cur_box_folder.upload, event.pathname, event.name),
                                       self.oauth])
            elif is_dir and not did_find_the_folder:
                crate_logger.debug('Upload the folder: {}'.format(event.pathname))
                self.upload_queue.put(partial(cur_box_folder.create_subfolder, event.name))
                wm.add_watch(event.pathname, rec=True, mask=mask)
        elif operation == 'modify':
            crate_logger.debug("{op}...: {pathname}".format(op=operation, pathname=event.pathname))
            folders_to_traverse = self.folders_to_traverse(event.path)
            crate_logger.debug(folders_to_traverse)
            client = Client(self.oauth)
            cur_box_folder = None
            folder_id = '0'
            retry_limit = 5
            cur_box_folder = get_box_folder(client, cur_box_folder, folder_id, retry_limit)
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
                                    is_new_time_stamp = item_version['time_stamp'] >= last_modified_time
                                    crate_logger.debug('Skipping the update because not versioned: {not_versioned}, '
                                                       'fresh_download: {fresh_download}, '
                                                       'version time_stamp >= '
                                                       'new time stamp: {new_time_stamp}, '
                                                       'event pathname: {path_name}, '
                                                       'cur file id: {obj_id}'.format(not_versioned=not was_versioned,
                                                                                      fresh_download=item_version[
                                                                                          'fresh_download'],
                                                                                      new_time_stamp=is_new_time_stamp,
                                                                                      path_name=event.pathname,
                                                                                      obj_id=cur_file['id']))
                            except TypeError:
                                crate_logger.debug(traceback.format_exc())
                            except Exception:
                                crate_logger.debug(traceback.format_exc())

                        else:
                            self.files_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
                    elif did_find_the_folder:
                        if entry['id'] not in self.folders_from_box:
                            crate_logger.debug('Cannot create a subfolder when it already exists: {}'.format(event.pathname))
                            # cur_folder = client.folder(folder_id=entry['id']).get()
                            # upload_queue.put(partial(cur_folder.update_contents, event.pathname))
                        else:
                            self.folders_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
            if is_file and not did_find_the_file:
                crate_logger.debug('Uploading contents...: {}'.format(event.pathname))
                last_modified_time = os.path.getmtime(event.pathname)
                self.upload_queue.put([last_modified_time,
                                       partial(cur_box_folder.upload, event.pathname, event.name),
                                       self.oauth])
            if is_dir and not did_find_the_folder:
                crate_logger.debug('Creating a sub-folder...: {}'.format(event.pathname))
                self.upload_queue.put(partial(cur_box_folder.create_subfolder, event.name))
                wm.add_watch(event.pathname, rec=True, mask=mask)
        elif operation == 'real_close':
            crate_logger.debug("Real  close...: {}".format(event.pathname))
            folders_to_traverse = self.folders_to_traverse(event.path)
            crate_logger.debug(folders_to_traverse)
            client = Client(self.oauth)
            cur_box_folder = None
            cur_box_folder = get_box_folder(client, cur_box_folder, '0', 5)
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
            self.operations.append([event, 'modify'])

    def process_IN_MOVED_FROM(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        if not event.name.startswith('.~lock'):  # avoid propagating lock files
            crate_logger.debug("Moved from: {}".format(event.pathname))
            self.move_events.append(event)

    def process_IN_MOVED_TO(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        found_from = False
        to_trash = os.path.commonprefix([trash_directory, event.pathname]) == trash_directory
        to_box = os.path.commonprefix([BOX_DIR, event.pathname]) == BOX_DIR
        for move_event in self.move_events:
            was_moved_from = 'in_moved_from' in move_event.maskname.lower()
            if move_event.cookie == event.cookie and was_moved_from and os.path.commonprefix(
                    [BOX_DIR, move_event.pathname]) == BOX_DIR:
                found_from = True
                # only count deletes that come from within the box path -- though this should always be the case
                if to_trash:
                    self.operations.append([move_event, 'delete'])
                else:
                    self.operations.append([[move_event, event], 'move'])
                break
        if not found_from and (not to_trash and to_box):
            self.operations.append([event, 'modify'])  # "close"/"modify" seems appropriate
            # allow moving from a ~.lock file...i guess that may be okay
            crate_logger.debug("Moved to: {}".format(event.pathname))

    def process_IN_CLOSE(self, event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        if not event.name.startswith('.~lock'):  # avoid propagating lock files
            crate_logger.debug('Had a close on: {}'.format(event))
            self.operations.append([event, 'real_close'])


def get_box_folder(client, cur_box_folder, folder_id, retry_limit):
    """

    :param client:
    :param cur_box_folder:
    :param folder_id:
    :param retry_limit:
    :return:
    """
    for i in range(retry_limit):
        try:
            box_folder = client.folder(folder_id=folder_id).get()
            cur_box_folder = box_folder
            break
        except (ConnectionError, BrokenPipeError, ProtocolError, ConnectionResetError, BoxAPIException):
            if i + 1 >= retry_limit:
                crate_logger.warn('Attempt ({retry_count}) out of ({max_count}); Going to give '
                                  'up on the write event because: {trace}'.format(retry_count=i,
                                                                                  max_count=retry_limit,
                                                                                  trace=traceback.format_exc()))
            else:
                crate_logger.warn('Attempt ({retry_count}) '
                                  'out of ({max_count}): {trace}'.format(retry_count=i,
                                                                         max_count=retry_limit,
                                                                         trace=traceback.format_exc()))
    return cur_box_folder


wm = pyinotify.WatchManager()
in_delete = getattr(pyinotify, 'IN_DELETE')
in_modify = getattr(pyinotify, 'IN_MODIFY')
in_close_write = getattr(pyinotify, 'IN_CLOSE_WRITE')
in_moved_to = getattr(pyinotify, 'IN_MOVED_TO')
in_moved_from = getattr(pyinotify, 'IN_MOVED_FROM')
mask = in_delete | in_modify | in_close_write | in_moved_to | in_moved_from  # watched events
