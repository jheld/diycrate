import argparse
import configparser
import json
import os
import subprocess
import threading
import traceback
import webbrowser

import bottle
import pyinotify
import time
from bottle import ServerAdapter
from boxsdk import Client
from cherrypy import wsgiserver
from cherrypy.wsgiserver import ssl_builtin

from diycrate.cache_utils import redis_key, redis_get, r_c
from diycrate.file_operations import EventHandler, wm, in_moved_to, in_moved_from, mask
from diycrate.item_queue_io import upload_queue_processor, download_queue_processor, download_queue_monitor, \
    upload_queue_monitor, download_queue, upload_queue
from diycrate.oauth_utils import setup_oauth, store_tokens_callback
from diycrate.path_utils import re_walk

cloud_provider_name = 'Box'

csrf_token = ''

bottle_app = bottle.Bottle()

# The watch manager stores the watches and provides operations on watches

# keep the lint-ing & introspection from complaining that these attributes don't exist before run-time.

BOX_DIR = os.path.expanduser('~/box')


def notify_user_with_gui(message):
    """
    Sends the message to the user
    :param message:
    :return:
    """
    proc = subprocess.Popen(['notify-send', message])
    if proc.returncode:
        print('Tried sending a message to user, return code: {}'.format(proc.returncode))


download_thread = threading.Thread(target=download_queue_processor)
upload_thread = threading.Thread(target=upload_queue_processor)

trash_directory = os.path.expanduser('~/.local/share/Trash/files')

upload_mon_thread = threading.Thread(target=upload_queue_monitor)
upload_mon_thread.daemon = True
upload_mon_thread.start()

download_mon_thread = threading.Thread(target=download_queue_monitor)
download_mon_thread.daemon = True
download_mon_thread.start()

handler = EventHandler(upload_queue=upload_queue)

notifier = pyinotify.ThreadedNotifier(wm, handler, read_freq=10)
notifier.coalesce_events()


def long_poll_event_listener():
    """
    Receive and process remote cloud item events in real-time
    :return:
    """
    oauth = handler.oauth
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
                            path = '{}'.format(os.path.sep).join([folder['name']
                                                                  for folder in
                                                                  event['source']['path_collection']['entries'][
                                                                  1:]])
                        else:
                            path = ''
                        path = os.path.join(BOX_DIR, path)
                        file_path = os.path.join(path, event['source']['name'])
                        file_obj = client.file(file_id=obj_id).get()
                        src_file_path = None if not r_c.exists(redis_key(obj_id)) else redis_get(r_c, file_obj)['file_path']
                        if src_file_path and os.path.exists(src_file_path):
                            version_info = redis_get(r_c, obj=file_obj)
                            src_file_path = version_info['file_path']
                            os.rename(src_file_path, file_path)
                            version_info['file_path'] = file_path
                            version_info['etag'] = file_obj['etag']
                            r_c.set(redis_key(obj_id), json.dumps(version_info))
                            r_c.set('diy_crate.last_save_time_stamp', int(time.time()))
                        else:
                            download_queue.put([file_obj, file_path, client._oauth])
                elif event['event_type'] == 'ITEM_UPLOAD':
                    obj_id = event['source']['id']
                    obj_type = event['source']['type']
                    if obj_type == 'file':
                        if int(event['source']['path_collection']['total_count']) > 1:
                            path = '{}'.format(os.path.sep).join([folder['name']
                                                                  for folder in
                                                                  event['source']['path_collection']['entries'][
                                                                  1:]])
                        else:
                            path = ''
                        path = os.path.join(BOX_DIR, path)
                        if not os.path.exists(path):  # just in case this is a file in a new subfolder
                            os.makedirs(path)
                        download_queue.put(
                            [client.file(file_id=obj_id).get(), os.path.join(path, event['source']['name']),
                             client._oauth])
                        # was_versioned = r_c.exists(redis_key(obj_id))
                        # if not was_versioned:
                        #     if int(event['source']['path_collection']['total_count']) > 1:
                        #         path = '{}'.format(os.path.sep).join([folder['name']
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
                        item_info = r_c.get(redis_key(obj_id))
                        if item_info:
                            item_info = json.loads(str(item_info, encoding='utf-8', errors='strict'))
                        if int(event['source']['path_collection']['total_count']) > 1:
                            path = '{}'.format(os.path.sep).join([folder['name']
                                                                  for folder in
                                                                  event['source']['path_collection']['entries'][
                                                                  1:]])
                        else:
                            path = ''
                        path = os.path.join(BOX_DIR, path)
                        file_path = os.path.join(path, event['source']['name']) if not item_info else item_info[
                            'file_path']
                        if os.path.exists(file_path):
                            os.unlink(file_path)
                        if r_c.exists(redis_key(obj_id)):
                            r_c.delete(redis_key(obj_id))
                            r_c.set('diy_crate.last_save_time_stamp', int(time.time()))
                        notify_user_with_gui('Box message: Deleted {}'.format(file_path))
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
    oauth = bottle_app.oauth
    access_token, refresh_token = oauth.authenticate(bottle.request.GET['code'])
    start_cloud_threads(oauth)

    return 'OK'


def start_cloud_threads(oauth):
    """

    :return:
    """
    client = Client(oauth)
    handler.oauth = oauth
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
        server.ssl_adapter = ssl_builtin.BuiltinSSLAdapter(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cacert.pem'),
                                                           os.path.join(os.path.dirname(os.path.dirname(__file__)), 'privkey.pem'))
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
        oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
        bottle_app.oauth = oauth
        auth_url, csrf_token = oauth.get_authorization_url('https://localhost:8080/')
        webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
    else:
        oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
        start_cloud_threads(oauth)
    notifier.start()
    # notifier_thread = threading.Thread(target=notifier.loop)
    # notifier_thread.daemon = True
    # notifier_thread.start()
    if not r_c.exists('diy_crate.auth.access_token') and not r_c.exists('diy_crate.auth.refresh_token'):
        bottle_app.run(server=SSLCherryPyServer)
