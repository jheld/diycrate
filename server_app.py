#!/usr/bin/env python3
import argparse
import configparser
import json
import logging
import os
from logging import handlers

import bottle
from bottle import ServerAdapter
from cherrypy import wsgiserver
from cherrypy.wsgiserver import ssl_builtin

from diycrate.cache_utils import r_c
from diycrate.oauth_utils import setup_oauth, store_tokens_callback

crate_logger = logging.getLogger('diy_crate_logger')
crate_logger.setLevel(logging.DEBUG)

l_handler = handlers.SysLogHandler(address='/dev/log')

crate_logger.addHandler(l_handler)

log_format = 'diycrate' + ' %(levelname)-9s %(name)-15s %(threadName)-14s +%(lineno)-4d %(message)s'
log_format = logging.Formatter(log_format)
l_handler.setFormatter(log_format)

cloud_provider_name = 'Box'

bottle_app = bottle.Bottle()


# The watch manager stores the watches and provides operations on watches

# keep the lint-ing & introspection from complaining that these attributes don't exist before run-time.


@bottle_app.route('/auth_url')
def auth_url():
    bottle_app.oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
    return json.dumps(bottle_app.oauth.get_authorization_url(redirect_url=bottle.request.query.redirect_url))


@bottle_app.route('/authenticate', method='POST')
def auth_url():
    bottle_app.oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
    secret_keys = r_c.get('secret_keys')
    if not secret_keys:
        secret_keys = []
    else:
        secret_keys = json.loads(secret_keys)
    secret_keys.append(bottle.request.POST.get('diycrate_secret_key'))
    r_c.set('secret_keys', json.dumps(secret_keys))
    return json.dumps([el.decode(encoding='utf-8', errors='strict') if isinstance(el, bytes) else el for el in bottle_app.oauth.authenticate(auth_code=bottle.request.POST.get('code'))])


@bottle_app.route('/new_access', method='POST')
def new_access():
    bottle_app.oauth = setup_oauth(r_c, conf_obj, store_tokens_callback)
    secret_keys = r_c.get('secret_keys')
    if not secret_keys:
        secret_keys = []
    else:
        secret_keys = json.loads(secret_keys)
    if bottle.request.query.diycrate_secret_key not in secret_keys:
        raise ValueError('No matching secret key; we are being secure!')
    return json.dumps([el.decode(encoding='utf-8', errors='strict') if isinstance(el, bytes) else el for el in bottle_app.oauth.refresh(bottle.request.POST.get('access_token'))])


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
        server.ssl_adapter = ssl_builtin.BuiltinSSLAdapter(conf_obj['ssl']['cacert_pem_path'],
                                                           conf_obj['ssl']['privkey_pem_path'])
        try:
            server.start()
        finally:
            server.stop()


if __name__ == '__main__':
    conf_obj = configparser.ConfigParser()
    conf_dir = os.path.abspath(os.path.expanduser('~/.config/diycrate_server'))
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
    arg_parser.add_argument('--cacert_pem_path', type=str, help='filepath to where the cacert.pem is located',
                            default='')
    arg_parser.add_argument('--privkey_pem_path', type=str, help='filepath to where the privkey.pem is located',
                            default='')
    args = arg_parser.parse_args()
    had_oauth2 = conf_obj.has_section('oauth2')
    if not had_oauth2:
        conf_obj.add_section('oauth2')
    conf_obj['oauth2'] = {
        'client_id': args.client_id or conf_obj['oauth2']['client_id'],
        'client_secret': args.client_secret or conf_obj['oauth2']['client_secret']
    }
    if 'ssl' not in conf_obj:
        if not args.cacert_pem_path:
            raise ValueError('Need a valid cacert_pem_path')
        if not args.privkey_pem_path:
            raise ValueError('Need a valid privkey_pem_path')
        conf_obj['ssl'] = {
            'cacert_pem_path': os.path.abspath(os.path.expanduser(args.cacert_pem_path)),
            'privkey_pem_path': os.path.abspath(os.path.expanduser(args.privkey_pem_path)),
        }
    conf_obj['ssl'] = {
        'cacert_pem_path': os.path.abspath(os.path.expanduser(args.cacert_pem_path)) if args.cacert_pem_path else
        conf_obj['ssl']['cacert_pem_path'],
        'privkey_pem_path': os.path.abspath(os.path.expanduser(args.privkey_pem_path)) if args.privkey_pem_path else
        conf_obj['ssl']['privkey_pem_path'],
    }

    conf_obj.write(open(cloud_credentials_file_path, 'w'))
    bottle_app.run(server=SSLCherryPyServer, port=8081)
