import time
import webbrowser


import requests

from boxsdk import OAuth2, Client, exception
from boxsdk.auth import RemoteOAuth2

from diycrate.cache_utils import r_c

import logging

from diycrate.log_utils import setup_logger
setup_logger()

crate_logger = logging.getLogger(__name__)


def setup_oauth(cache_client, conf_object, callback):
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param conf_object:
    :param callback:
    :return:
    """
    access_token = cache_client.get('diy_crate.auth.access_token')
    refresh_token = cache_client.get('diy_crate.auth.refresh_token')
    if access_token:
        access_token = access_token.decode(encoding='utf-8') if isinstance(access_token, bytes) else access_token
    if refresh_token:
        refresh_token = refresh_token.decode(encoding='utf-8') if isinstance(refresh_token, bytes) else refresh_token
    oauth = OAuth2(
        client_id=conf_object['oauth2']['client_id'],
        client_secret=conf_object['oauth2']['client_secret'],
        store_tokens=callback,
        access_token=access_token,
        refresh_token=refresh_token,
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
    refresh_token = r_c.get('diy_crate.auth.refresh_token')
    access_token, refresh_token = requests.post(remote_url,
                                                data={
                                                    'refresh_token': refresh_token.decode(
                                                        'utf-8') if refresh_token and isinstance(refresh_token, bytes) else refresh_token,
                                                    'access_token': access_token.decode(
                                                        'utf-8') if access_token and isinstance(access_token, bytes) else access_token,
                                                },
                                                verify=True).json()
    r_c.set('diy_crate.auth.access_token', access_token)
    r_c.set('diy_crate.auth.refresh_token', refresh_token)
    crate_logger.debug(
        "new access_token: {access_token}, refresh_token: {refresh_token}".format(
            access_token=access_token, refresh_token=refresh_token
        )
    )
    return access_token


def setup_remote_oauth(cache_client, retrieve_access_token=get_access_token):
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param retrieve_access_token:
    :return:
    """
    access_token = cache_client.get('diy_crate.auth.access_token')
    refresh_token = cache_client.get('diy_crate.auth.refresh_token')
    oauth = RemoteOAuth2(
        client_id='',
        client_secret='',
        access_token=access_token.decode('utf-8') if access_token and isinstance(access_token, bytes) else access_token,
        refresh_token=refresh_token.decode('utf-8') if refresh_token and isinstance(refresh_token, bytes) else refresh_token,
        retrieve_access_token=retrieve_access_token
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
    bottle_app.oauth = file_event_handler.oauth = setup_remote_oauth(redis_client)
    import requests
    auth_url, bottle_app.csrf_token = requests.get(conf['box']['authorization_url'], data={'redirect_url': 'https://localhost:{port}/'.format(port=conf["box"]["web_server_port"]), }, verify=True).json()
    if bottle_thread and not bottle_thread.is_alive():
        bottle_thread.start()
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
