import argparse
import configparser
import os
import queue
import threading
import time
import traceback
import webbrowser
from functools import partial

import bottle
import pyinotify
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException
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


def upload_queue_processor():
    """

    :return:
    """
    while True:
        time.sleep(2)
        if upload_queue.not_empty:
            callable_up = upload_queue.get()
            num_retries = 15
            for x in range(15):
                try:
                    callable_up()
                    break
                except (ConnectionError, BrokenPipeError, ProtocolError):
                    time.sleep(3)
                    print(traceback.format_exc())
                    if x >= num_retries - 1:
                        print('Upload giving up on: {}'.format(callable_up))
                        # no immediate plans to do anything with this info, yet.
                        uploads_given_up_on.append(callable_up)
            upload_queue.task_done()


def download_queue_processor():
    """

    :return:
    """
    while True:
        time.sleep(2)
        if download_queue.not_empty:
            item, path = download_queue.get()
            if item['type'] == 'file':
                with open(path, 'wb') as item_handler:
                    item.download_to(item_handler)
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
        return client.folder(folder_id=folder_id).get()

    @staticmethod
    def folders_to_traverse(event_path):
        """

        :param event_path:
        :return:
        """
        folders_to_traverse = [folder for folder in os.path.split(event_path.replace(BOX_DIR, '')) if
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
            for entry in cur_box_folder['item_collection']['entries']:
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
            is_base = (event.path == BOX_DIR or event.path[:-1] == BOX_DIR)
            cur_box_folder = self.traverse_path(client, event, cur_box_folder, folders_to_traverse)
            if not is_base:
                AssertionError(cur_box_folder['name'] == os.path.split(event.path)[-1],
                               cur_box_folder['name'] + 'not equals ' + os.path.split(event.path)[-1])
            event_was_for_dir = 'IN_ISDIR'.lower() in event.maskname.lower()
            for entry in cur_box_folder['item_collection']['entries']:
                if not event_was_for_dir:
                    if entry['type'] == 'file' and entry['name'] == event.name:
                        if entry['id'] not in self.files_from_box:
                            cur_file = client.file(file_id=entry['id']).get()
                            cur_file.delete()
                        else:
                            self.files_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
                else:
                    if entry['type'] == 'folder' and entry['name'] == os.path.split(event.pathname)[-1]:
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
            is_base = (dest_event.path == BOX_DIR or dest_event.path[:-1] == BOX_DIR)
            cur_box_folder = self.traverse_path(client, dest_event, cur_box_folder, folders_to_traverse)
            src_folders_to_traverse = self.folders_to_traverse(src_event.path)
            src_box_folder = box_folder
            src_box_folder = self.traverse_path(client, src_event, src_box_folder, src_folders_to_traverse)
            is_rename = src_event.path == dest_event.path
            did_find_src_file = False
            is_a_directory = 'IN_ISDIR'.lower() in dest_event.maskname.lower()
            for entry in src_box_folder['item_collection']['entries']:
                if entry['name'] == src_event.name and entry['type'] == 'file':
                    did_find_src_file = True
                    src_file = client.file(file_id=entry['id']).get()
                    if is_rename:
                        src_file.rename(dest_event.name)
                    else:
                        src_file.move(cur_box_folder)
                        # do not yet support moving and renaming in one go
                        assert src_file['name'] == dest_event.name
            if not is_a_directory:
                if not did_find_src_file:
                    # src file [should] no longer exist[s]. this file did not originate in box, too.
                    upload_queue.put(partial(cur_box_folder.upload, dest_event.pathname, dest_event.name))
            else:
                if not did_find_src_file:
                    upload_queue.put(partial(cur_box_folder.create_subfolder, dest_event.name))
                else:
                    # TODO: support changing the folder name of a folder that was in box
                    pass

        elif operation == 'create':
            print("Creating:", event.pathname)
            folders_to_traverse = self.folders_to_traverse(event.path)
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = client.folder(folder_id='0').get()
            cur_box_folder = box_folder
            # if we're modifying in root box dir, then we've already found the folder
            is_base = (event.path == BOX_DIR or event.path[:-1] == BOX_DIR)
            cur_box_folder = self.traverse_path(client, event, cur_box_folder, folders_to_traverse)
            if not is_base:
                assert cur_box_folder['name'] == os.path.split(event.path)[-1]
            did_find_the_file = False
            for entry in cur_box_folder['item_collection']['entries']:
                if os.path.isfile(event.pathname):
                    if entry['type'] == 'file' and entry['name'] == event.name:
                        did_find_the_file = True
                        if entry['id'] not in self.files_from_box:
                            print('Update the file: ', event.pathname)
                            a_file = client.file(file_id=entry['id']).get()
                            # seem it is possible to get more than one create (without having a delete in between)
                            upload_queue.put(partial(a_file.update_contents, event.pathname))
                            # cur_box_folder.upload(event.pathname, event.name)
                        else:
                            self.files_from_box.remove(entry['id'])  # just downloaded it
                        break
                else:
                    did_find_the_file = True  # hack
                    # else:  # cannot create a sub-folder that already exists
                    #     if entry['type'] == 'folder' and entry['name'] == os.path.split(event.pathname)[-1]:
                    #         if entry['id'] not in self.folders_from_box:
                    #             print('Upload new folder: ', event.pathname)
                    #             try:
                    #                 cur_box_folder.create_subfolder(os.path.split(event.pathname)[-1])
                    #             except boxsdk.exception.BoxAPIException as e:
                    #                 print(e)
                    #         else:
                    #             self.folders_from_box.remove(entry['id'])  # just downloaded it
                    #         break
            if not did_find_the_file:
                print('Upload the file: ', event.pathname)
                upload_queue.put(partial(cur_box_folder.upload, event.pathname, event.name))
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
                except (ConnectionError, BrokenPipeError, ProtocolError):
                    print(traceback.format_exc())
            # if we're modifying in root box dir, then we've already found the folder
            is_base = (event.path == BOX_DIR or event.path[:-1] == BOX_DIR)
            cur_box_folder = self.traverse_path(client, event, cur_box_folder, folders_to_traverse)
            if not is_base:
                AssertionError(cur_box_folder['name'] == os.path.split(event.path)[-1],
                               cur_box_folder['name'] + 'not equals ' + os.path.split(event.path)[-1])
            did_find_the_file = os.path.isdir(event.pathname)  # true if we are a directory :)
            did_find_the_folder = os.path.isfile(event.pathname)  # true if we are a regular file :)
            for entry in cur_box_folder['item_collection']['entries']:
                if os.path.isfile(event.pathname):
                    if entry['type'] == 'file' and entry['name'] == event.name:
                        did_find_the_file = True
                        if entry['id'] not in self.files_from_box:
                            cur_file = client.file(file_id=entry['id']).get()
                            upload_queue.put(partial(cur_file.update_contents, event.pathname))
                        else:
                            self.files_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
                else:

                    if entry['type'] == 'folder' and entry['name'] == os.path.split(event.pathname)[-1]:
                        did_find_the_folder = True
                        if entry['id'] not in self.folders_from_box:
                            print('Cannot create a subfolder when it already exists: ', event.pathname)
                            # cur_folder = client.folder(folder_id=entry['id']).get()
                            # upload_queue.put(partial(cur_folder.update_contents, event.pathname))
                        else:
                            self.folders_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
            if not did_find_the_file:
                print('Uploading contents...', event.pathname)
                upload_queue.put(partial(cur_box_folder.upload, event.pathname, event.name))
                # operation(event, do_event=True)
            if not did_find_the_folder:
                print('Creating a sub-folder...', event.pathname)
                upload_queue.put(partial(cur_box_folder.create_subfolder, event.name))
                wm.add_watch(event.pathname, rec=True, mask=mask)

    def process_IN_CREATE(self, event, do_event=False):
        """
        Overrides the super.
        :param do_event:
        :param event:
        :return:
        """
        if do_event:
            pass
        else:
            self.operations.append([event, 'create'])

    def process_IN_DELETE(self, event, do_event=False):
        """
        Overrides the super.
        :param do_event:
        :param event:
        :return:
        """
        if do_event:
            print("Removing:", event.pathname)
        else:
            self.operations.append([event, 'delete'])

    def process_IN_MODIFY(self, event, do_event=False):
        """
        Overrides the super.
        :param do_event:
        :param event:
        :return:
        """
        if do_event:
            pass
        else:
            self.operations.append([event, 'close'])

    def process_IN_MOVED_FROM(self, event, do_event=False):
        """
        Overrides the super.
        :param do_event:
        :param event:
        :return:
        """
        print("Moved from:", event.pathname)
        self.move_events.append(event)

    def process_IN_MOVED_TO(self, event, do_event=False):
        """
        Overrides the super.
        :param do_event:
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
    pass


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
        wm.add_watch(path, mask, rec=False)
    for entry in client.folder(folder_id=box_folder['id']).get()['item_collection']['entries']:
        if entry['type'] == 'folder':
            handler.folders_from_box.append(entry['id'])
            if entry['name'] in ('test directory', 'Downloads', 'from copy',):
                local_path = os.path.join(path, entry['name'])
                if not os.path.isdir(local_path):
                    os.mkdir(local_path)
                walk_and_notify_and_download_tree(local_path, client.folder(folder_id=entry['id']).get(), client)
        else:
            handler.files_from_box.append(entry['id'])
            download_queue.put((client.file(file_id=entry['id']).get(), os.path.join(path, entry['name'])))
            # open(os.path.join(path, entry['name']), 'wb').write(client.file(file_id=entry['id']).get().content())


@bottle_app.route('/')
def oauth_handler():
    """
    RESTful end-point for the oauth handling
    :return:
    """
    assert csrf_token == bottle.request.GET['state']
    access_token, refresh_token = oauth.authenticate(bottle.request.GET['code'])
    client = Client(oauth)
    wm.add_watch(BOX_DIR, mask, rec=False)
    box_folder = client.folder(folder_id='0').get()
    download_thread.daemon = True
    download_thread.start()
    upload_thread.daemon = True
    upload_thread.start()
    # local trash can
    wm.add_watch(trash_directory, mask=in_moved_to | in_moved_from)
    walk_and_notify_and_download_tree(BOX_DIR, box_folder, client)

    return 'OK'


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
    auth_url, csrf_token = oauth.get_authorization_url('https://diycrate.com:8080/')
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
    notifier.start()
    # notifier_thread = threading.Thread(target=notifier.loop)
    # notifier_thread.daemon = True
    # notifier_thread.start()
    bottle_app.run()
