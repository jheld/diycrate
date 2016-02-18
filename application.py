import os
import threading
import argparse
import configparser
import webbrowser

import bottle
import pyinotify
from boxsdk import OAuth2

cloud_provider_name = 'Box'

csrf_token = ''

def store_tokens_callback(access_token, refresh_token):
    pass


@bottle.route('/')
def oauth_handler():
    assert csrf_token == bottle.request.GET['state']
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
    if not conf_obj.has_section('oauth2'):
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('client_id', type=str, help='Client ID provided by {}'.format(cloud_provider_name))
        arg_parser.add_argument('client_secret', type=str, help='Client Secret provided by {}'.format(cloud_provider_name))
        args = arg_parser.parse_args()
        conf_obj.add_section('oauth2')
        conf_obj['oauth2'] = {
            'client_id': args.client_id,
            'client_secret': args.client_secret
        }
        conf_obj.write(open(cloud_credentials_file_path, 'w'))
    oauth = OAuth2(
        client_id=conf_obj['oauth2']['client_id'],
        client_secret=conf_obj['oauth2']['client_secret'],
        store_tokens=store_tokens_callback,
    )

    auth_url, csrf_token = oauth.get_authorization_url('https://diycrate.com:8080/')
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
    bottle.run()
