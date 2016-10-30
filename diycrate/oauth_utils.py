import uuid
import webbrowser

from boxsdk import OAuth2
from boxsdk.auth import RemoteOAuth2

from diycrate.cache_utils import r_c


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
    import requests
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


def oauth_dance(redis_client, conf, bottle_app):
    diycrate_secret_key = redis_client.get('diycrate_secret_key') or str(uuid.uuid4())
    if not redis_client.exists('diycrate_secret_key'):
        redis_client.set('diycrate_secret_key', diycrate_secret_key)
    bottle_app.oauth = setup_remote_oauth(redis_client)
    import requests
    auth_url, bottle_app.csrf_token = requests.get(conf['box']['authorization_url'], data={'redirect_url': 'https://localhost:8080/', }, verify=False).json()
    # auth_url, bottle_app.csrf_token = bottle_app.oauth.get_authorization_url(auth_url)
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
