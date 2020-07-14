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
    auth_url, bottle_app.csrf_token = requests.get(conf['box']['authorization_url'], data={'redirect_url': 'https://localhost:8080/', }, verify=True).json()
    if bottle_thread and not bottle_thread.is_alive():
        bottle_thread.start()
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth


def oauth_dance_retry(oauth_instance, cache_client, oauth_meta_info, conf_obj, bottle_app, file_event_handler, oauth_lock_instance, bottle_thread=None):
    with oauth_lock_instance:
        temp_client = Client(oauth_instance)
        while True:
            try:
                temp_client.auth._access_token = cache_client.get('diy_crate.auth.access_token')
                temp_client.auth._refresh_token = cache_client.get('diy_crate.auth.refresh_token')
                if temp_client.auth._access_token:
                    temp_client.auth._access_token = temp_client.auth._access_token.decode(encoding='utf-8') if isinstance(temp_client.auth._access_token, bytes) else temp_client.auth._access_token
                if temp_client.auth._refresh_token:
                    temp_client.auth._refresh_token = temp_client.auth._refresh_token.decode(encoding='utf-8') if isinstance(temp_client.auth._refresh_token, bytes) else temp_client.auth._refresh_token
                temp_client.folder(folder_id='0').get()
                break  # sweet, we should have valid oauth access, now
            except (exception.BoxAPIException, AttributeError):
                crate_logger.warning("dance api exc or attr error", exc_info=True)
                try:
                    oauth_meta_info['diy_crate.auth.oauth_dance_retry'] = True
                    try:
                        get_access_token(cache_client.get(u'diy_crate.auth.access_token'))
                        crate_logger.info(u'got new access and refresh tokens')
                    # might as well get a new set of tokens
                    except Exception:
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
                crate_logger.warning('dancy retry had a connection error...sleeping for 5', exc_info=True)
            except Exception:
                crate_logger.warning("dance retry, error", exc_info=True)
