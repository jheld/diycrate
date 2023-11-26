import typing
import webbrowser
from configparser import ConfigParser
import sys
from typing import List, Union

import httpx

from boxsdk import OAuth2
from boxsdk.auth import RemoteOAuth2

from diycrate.utils import Bottle

from .cache_utils import r_c
from .gui import notify_user_with_gui

import logging

from .log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)


def setup_oauth(cache_client, conf_object, callback: typing.Callable[[str], str]):
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


def get_access_token(
    access_token: str | bytes | None,
    bottle_app: Union[None, Bottle] = None,
    oauth: OAuth2 | RemoteOAuth2 | None = None,
) -> str:
    """

    :param access_token: token we will try to refresh/get anew
    :return:
    """
    from .file_operations import conf_obj

    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")

    conf_object = conf_obj
    remote_url = conf_object["box"]["token_url"]
    refresh_token_from_cache: Union[bytes, str, None] = r_c.get(
        "diy_crate.auth.refresh_token"
    )

    if isinstance(refresh_token_from_cache, bytes):
        refresh_token = refresh_token_from_cache.decode(encoding="utf-8")
    elif isinstance(refresh_token_from_cache, str):
        refresh_token = refresh_token_from_cache
    else:
        refresh_token = ""
    response = httpx.post(
        remote_url,
        data={"refresh_token": refresh_token, "access_token": access_token},
        verify=True,
    )
    try:
        response.raise_for_status()
        response_json: List[str] = response.json()
        access_token_resolved, refresh_token = response_json
        oauth_resolved = bottle_app.oauth if bottle_app else oauth
        if oauth_resolved:
            oauth_resolved._update_current_tokens(access_token_resolved, refresh_token)
        else:
            raise AttributeError(
                "oauth_resolved has no attribute `None` (by hand exception)"
            )
    except AttributeError as e:
        crate_logger.warn("Bad oauth object passed in.", exc_info=e)
        if r_c.exists("diy_crate.auth.access_token") and r_c.exists(
            "diy_crate.auth.refresh_token"
        ):
            r_c.delete("diy_crate.auth.access_token", "diy_crate.auth.refresh_token")
        sys.exit(1)
    except (ValueError, httpx.HTTPStatusError) as e:
        response_error_body = response.content.decode()
        crate_logger.warn("Error on response: %s", response_error_body, exc_info=e)
        notify_user_with_gui("Error retrieving new token", "Will clear out.")
        if r_c.exists("diy_crate.auth.access_token") and r_c.exists(
            "diy_crate.auth.refresh_token"
        ):
            r_c.delete("diy_crate.auth.access_token", "diy_crate.auth.refresh_token")
        if bottle_app:
            handler = None
            oauth_dance(
                redis_client=r_c,
                conf=conf_obj,
                bottle_app=bottle_app,
                file_event_handler=handler,
            )
            oauth_updated: OAuth2 | RemoteOAuth2 = bottle_app.oauth
            access_token_resolved: str = typing.cast(str, oauth_updated.access_token)
            refresh_token: str = typing.cast(str, oauth_updated._refresh_token)
            bottle_app.oauth._update_current_tokens(
                access_token_resolved, refresh_token
            )
            crate_logger.debug(
                f"from 'regular' oauth_dance new"
                f"access_token: {access_token_resolved},"
                f"refresh_token: {refresh_token}"
            )
            return access_token_resolved

        else:
            crate_logger.warn("Bad bottle_app object passed in, exiting", exc_info=e)
            sys.exit(1)
    else:
        crate_logger.debug(
            f"new access_token: {access_token_resolved}, refresh_token: {refresh_token}"
        )
        return access_token_resolved


def setup_remote_oauth(
    cache_client,
    retrieve_access_token: typing.Callable = get_access_token,
    conf: Union[ConfigParser, dict, None] = None,
    bottle_app: Union[Bottle, None] = None,
    oauth_obj=None,
) -> RemoteOAuth2:
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param retrieve_access_token:
    :param conf:
    :return:
    """
    conf_obj: Union[ConfigParser, dict] = conf or {}
    access_token = cache_client.get("diy_crate.auth.access_token")

    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")
    refresh_token = cache_client.get("diy_crate.auth.refresh_token")

    if isinstance(refresh_token, bytes):
        refresh_token = refresh_token.decode(encoding="utf-8")
    if isinstance(conf_obj, ConfigParser):
        client_id = (
            (conf_obj["oauth2"]["client_id"] or "")
            if conf_obj.has_section("oauth2")
            else ""
        )
    else:
        # raise Exception
        client_id = conf_obj.get("oauth2", {}).get("client_id", "")

    def wrapper_retrieve_access_token(access_token_arg):
        retrieve_access_token(
            access_token_arg,
            bottle_app=bottle_app,
            oauth=bottle_app.oauth if bottle_app else oauth_obj,
        )

    oauth = RemoteOAuth2(
        client_id=client_id,
        client_secret="",
        access_token=access_token,
        refresh_token=refresh_token,
        retrieve_access_token=wrapper_retrieve_access_token,
    )
    return oauth


def store_tokens_callback(
    access_token: Union[str, bytes], refresh_token: Union[str, bytes]
):
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


def oauth_dance(
    redis_client, conf, bottle_app: Bottle, file_event_handler, bottle_thread=None
):
    bottle_app.oauth = setup_remote_oauth(
        redis_client, conf=conf, bottle_app=bottle_app
    )
    if file_event_handler:
        file_event_handler.oauth = bottle_app.oauth
    redirect_url_domain = "127.0.0.1"
    (auth_url, csrf_token) = bottle_app.oauth.get_authorization_url(
        redirect_url=f"https://{redirect_url_domain}:{conf['box']['web_server_port']}/"
    )
    print(auth_url, csrf_token)
    bottle_app.csrf_token = csrf_token
    if bottle_thread and not bottle_thread.is_alive():
        bottle_thread.start()
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth
