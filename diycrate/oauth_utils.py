from boxsdk import OAuth2

from diycrate.cache_utils import r_c


def setup_oauth(r_c, conf_obj, callback):
    oauth = OAuth2(
        client_id=conf_obj['oauth2']['client_id'],
        client_secret=conf_obj['oauth2']['client_secret'],
        store_tokens=callback,
        access_token=r_c.get('diy_crate.auth.access_token'),
        refresh_token=r_c.get('diy_crate.auth.refresh_token')
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