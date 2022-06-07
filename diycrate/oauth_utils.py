import webbrowser
from configparser import ConfigParser
from typing import Union

import httpx

from boxsdk import OAuth2
from boxsdk.auth import RemoteOAuth2

from .cache_utils import r_c

import logging

from .log_utils import setup_logger

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
    access_token = cache_client.get("diy_crate.auth.access_token")
    refresh_token = cache_client.get("diy_crate.auth.refresh_token")
    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")
    if isinstance(refresh_token, bytes):
        refresh_token = refresh_token.decode(encoding="utf-8")
    oauth = OAuth2(
        client_id=conf_object["oauth2"]["client_id"],
        client_secret=conf_object["oauth2"]["client_secret"],
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
    from .file_operations import conf_obj

    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")

    conf_object = conf_obj
    remote_url = conf_object["box"]["token_url"]
    refresh_token = r_c.get("diy_crate.auth.refresh_token")

    if isinstance(refresh_token, bytes):
        refresh_token = refresh_token.decode(encoding="utf-8")

    access_token, refresh_token = httpx.post(
        remote_url,
        data={"refresh_token": refresh_token, "access_token": access_token},
        verify=True,
    ).json()
    r_c.set("diy_crate.auth.access_token", access_token)
    r_c.set("diy_crate.auth.refresh_token", refresh_token)
    crate_logger.debug(
        f"new access_token: {access_token}, refresh_token: {refresh_token}"
    )
    return access_token


def setup_remote_oauth(cache_client, retrieve_access_token=get_access_token, conf=None):
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param retrieve_access_token:
    :param conf:
    :return:
    """
    conf: Union[ConfigParser, dict] = conf or {}
    access_token = cache_client.get("diy_crate.auth.access_token")

    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")
    refresh_token = cache_client.get("diy_crate.auth.refresh_token")

    if isinstance(refresh_token, bytes):
        refresh_token = refresh_token.decode(encoding="utf-8")
    if isinstance(conf, ConfigParser):
        client_id = (
            (conf["oauth2"]["client_id"] or "") if conf.has_section("oauth2") else ""
        )
    else:
        # raise Exception
        client_id = conf.get("oauth2", {}).get("client_id", "")
    oauth = RemoteOAuth2(
        client_id=client_id,
        client_secret="",
        access_token=access_token,
        refresh_token=refresh_token,
        retrieve_access_token=retrieve_access_token,
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
    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")

    if isinstance(refresh_token, bytes):
        refresh_token = refresh_token.decode(encoding="utf-8")
    r_c.set("diy_crate.auth.access_token", access_token)
    r_c.set("diy_crate.auth.refresh_token", refresh_token)


def oauth_dance(redis_client, conf, bottle_app, file_event_handler, bottle_thread=None):
    bottle_app.oauth = file_event_handler.oauth = setup_remote_oauth(
        redis_client, conf=conf
    )
    redirect_url_domain = "127.0.0.1"
    (auth_url, csrf_token) = bottle_app.oauth.get_authorization_url(
        redirect_url=f"https://{redirect_url_domain}:{conf['box']['web_server_port']}/"
    )
    print(auth_url, csrf_token)
    bottle_app.csrf_token = csrf_token
    if bottle_thread and not bottle_thread.is_alive():
        bottle_thread.start()
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
