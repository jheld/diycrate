import os
import threading
import argparse
import configparser
import webbrowser

import bottle
import pyinotify
import redis
import time
import boxsdk
from boxsdk import OAuth2, Client

cloud_provider_name = 'Box'

csrf_token = ''

bottle_app = bottle.Bottle()

# The watch manager stores the watches and provides operations on watches
wm = pyinotify.WatchManager()

mask = pyinotify.IN_DELETE | pyinotify.IN_MODIFY | pyinotify.IN_CLOSE_WRITE | \
       pyinotify.IN_MOVED_TO | pyinotify.IN_MOVED_FROM  # watched events

BOX_DIR = os.path.expanduser('~/box')


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

    def process_event(self, event, operation):
        """
        Wrapper to process the given event on the operation.
        :param event:
        :param operation:
        :return:
        """
        if operation == 'delete':
            pass
        elif operation == 'create':
            print("Creating:", event.pathname)
            folders_to_traverse = [folder for folder in os.path.split(event.path.replace(BOX_DIR, '')) if
                                   folder and folder != '/']
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = client.folder(folder_id='0').get()
            cur_box_folder = box_folder
            # if we're modifying in root box dir, then we've already found the folder
            is_base = (event.path == BOX_DIR or event.path[:-1] == BOX_DIR)
            for folder in folders_to_traverse:
                did_find_folder = False
                for entry in cur_box_folder['item_collection']['entries']:
                    if folder == entry['name'] and entry['type'] == 'folder':
                        did_find_folder = True
                        cur_box_folder = client.folder(folder_id=entry['id']).get()
                if not did_find_folder:
                    try:
                        cur_box_folder = cur_box_folder.create_subfolder(folder).get()
                        print('Subfolder creation: ', event.pathname)
                    except boxsdk.exception.BoxAPIException as e:
                        print(e)
            if not is_base:
                assert cur_box_folder['name'] == os.path.split(event.path)[-1]
            for entry in cur_box_folder['item_collection']['entries']:
                if os.path.isfile(event.pathname):
                    if entry['type'] == 'file' and entry['name'] == event.name:
                        if entry['id'] not in self.files_from_box:
                            print('Upload new file: ', event.pathname)
                            a_file = client.file(file_id=entry['id']).get()
                            # seem it is possible to get more than one create (without having a delete in between)
                            a_file.update_contents(event.pathname)
                            # cur_box_folder.upload(event.pathname, event.name)
                        else:
                            self.files_from_box.remove(entry['id'])  # just downloaded it
                        break
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
        elif operation == 'close':
            print("Closing...:", event.pathname)
            folders_to_traverse = [folder for folder in os.path.split(event.path.replace(BOX_DIR, '')) if
                                   folder and folder != '/']
            print(folders_to_traverse)
            client = Client(oauth)
            box_folder = client.folder(folder_id='0').get()
            cur_box_folder = box_folder
            # if we're modifying in root box dir, then we've already found the folder
            is_base = (event.path == BOX_DIR or event.path[:-1] == BOX_DIR)
            for folder in folders_to_traverse:
                for entry in cur_box_folder['item_collection']['entries']:
                    if folder == entry['name'] and entry['type'] == 'folder':
                        cur_box_folder = client.folder(folder_id=entry['id']).get()
            if not is_base:
                assert cur_box_folder['name'] == os.path.split(event.path)[-1]
            for entry in cur_box_folder['item_collection']['entries']:
                if os.path.isfile(event.pathname):
                    if entry['type'] == 'file' and entry['name'] == event.name:
                        if entry['id'] not in self.files_from_box:
                            cur_file = client.file(file_id=entry['id']).get()
                            if cur_file.update_contents(event.pathname):
                                print('Updating contents...', event.pathname)
                        else:
                            self.files_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
                else:
                    print('Whoa, we got here? Yikes.')
                    if entry['type'] == 'folder' and entry['name'] == os.path.split(event.pathname)[-1]:
                        if entry['id'] not in self.folders_from_box:
                            cur_folder = client.folder(folder_id=entry['id']).get()
                            cur_folder.update_contents(event.pathname)
                        else:
                            self.folders_from_box.remove(entry['id'])  # just wrote if, assuming create event didn't run
                        break
        # operation(event, do_event=True)

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
            if entry['name'] == 'test directory':
                local_path = os.path.join(path, entry['name'])
                if not os.path.isdir(local_path):
                    os.mkdir(local_path)
                walk_and_notify_and_download_tree(local_path, client.folder(folder_id=entry['id']).get(), client)
        else:
            handler.files_from_box.append(entry['id'])
            open(os.path.join(path, entry['name']), 'wb').write(client.file(file_id=entry['id']).get().content())


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
