import time
import uuid
import webbrowser


import requests

from boxsdk import OAuth2, Client, exception
from boxsdk.auth import RemoteOAuth2

from diycrate.cache_utils import r_c



import logging
from logging import handlers
crate_logger = logging.getLogger('diy_crate_logger')
crate_logger.setLevel(logging.INFO)

l_handler = handlers.SysLogHandler(address='/dev/log')

crate_logger.addHandler(l_handler)

log_format = 'diycrate' + ' %(levelname)-9s %(name)-15s %(threadName)-14s +%(lineno)-4d %(message)s'
log_format = logging.Formatter(log_format)
l_handler.setFormatter(log_format)


def setup_oauth(cache_client, conf_object, callback):
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param conf_object:
    :param callback:
    :return:
    """
    oauth = OAuth2(
        client_id=conf_object['oauth2']['client_id'],
        client_secret=conf_object['oauth2']['client_secret'],
        store_tokens=callback,
        access_token=cache_client.get('diy_crate.auth.access_token'),
        refresh_token=cache_client.get('diy_crate.auth.refresh_token')
    )
    return oauth


def get_access_token(access_token):
    """

    :param access_token: token we will try to refresh/get anew
    :return:
    """
    from diycrate.file_operations import conf_obj
    conf_object = conf_obj
    remote_url = conf_object['box']['token_url']
    diycrate_secret_key = str(r_c.get('diycrate_secret_key') or b'', encoding='utf-8', errors='strict') or str(uuid.uuid4())
    if not r_c.exists('diycrate_secret_key'):
        r_c.set('diycrate_secret_key', diycrate_secret_key)
    access_token, refresh_token = requests.post(remote_url, data={'access_token': str(access_token), 'diycrate_secret_key': str(diycrate_secret_key)}, verify=False).json()
    r_c.set('diy_crate.auth.access_token', access_token)
    r_c.set('diy_crate.auth.refresh_token', refresh_token)
    return access_token


def setup_remote_oauth(cache_client, retrieve_access_token=get_access_token):
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param conf_object:
    :param callback:
    :param retrieve_access_token:
    :return:
    """
    oauth = RemoteOAuth2(
        client_id='',
        client_secret='',
        access_token=cache_client.get('diy_crate.auth.access_token'),
        refresh_token=cache_client.get('diy_crate.auth.refresh_token'), retrieve_access_token=retrieve_access_token
    )
    return oauth


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


def oauth_dance(redis_client, conf, bottle_app, file_event_handler, bottle_thread=None):
    diycrate_secret_key = redis_client.get('diycrate_secret_key') or str(uuid.uuid4())
    if not redis_client.exists('diycrate_secret_key'):
        redis_client.set('diycrate_secret_key', diycrate_secret_key)
    bottle_app.oauth = file_event_handler.oauth = setup_remote_oauth(redis_client)
    import requests
    auth_url, bottle_app.csrf_token = requests.get(conf['box']['authorization_url'], data={'redirect_url': 'https://localhost:8080/', }, verify=False).json()
    # auth_url, bottle_app.csrf_token = bottle_app.oauth.get_authorization_url(auth_url)
    if bottle_thread and not bottle_thread.is_alive():
        bottle_thread.start()
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth


def oauth_dance_retry(oauth_instance, cache_client, oauth_meta_info, conf_obj, bottle_app, file_event_handler, oauth_lock_instance, bottle_thread=None):
    with oauth_lock_instance:
        temp_client = Client(oauth_instance)
        while True:
            try:
                temp_client.folder(folder_id='0').get()
                break  # sweet, we should have valid oauth access, now
            except (exception.BoxAPIException, AttributeError):
                try:
                    oauth_meta_info['diy_crate.auth.oauth_dance_retry'] = True
                    # might as well get a new set of tokens
                    cache_client.delete('diy_crate.auth.access_token', 'diy_crate.auth.refresh_token',)
                    oauth_dance(cache_client, conf_obj, bottle_app, file_event_handler, bottle_thread=bottle_thread)
                    # wait until the oauth dance has completed
                    while not (cache_client.get('diy_crate.auth.access_token') and cache_client.get('diy_crate.auth.refresh_token')):
                        time.sleep(15)
                        crate_logger.info('Just slept, waiting on the oauth dance')

                finally:
                    oauth_meta_info.pop('diy_crate.auth.oauth_dance_retry')
            except requests.exceptions.ConnectionError:
                time.sleep(5)
                crate_logger.warn('had a connection error...sleeping for 5')
