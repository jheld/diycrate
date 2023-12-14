import threading
import typing
import webbrowser
from configparser import ConfigParser
import sys
from typing import List, Union

import httpx

from boxsdk import OAuth2
from boxsdk.auth import RemoteOAuth2
from pyinotify import ProcessEvent
from redis import Redis

from diycrate.utils import FastAPI
from diycrate.cache_utils import r_c


import logging

from .log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)


def setup_oauth(cache_client, conf_object, callback: typing.Callable[[str, str], None]):
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param conf_object:
    :param callback:
    :return:
    """
    global oauth
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
    app: Union[None, FastAPI] = None,
) -> str:
    """

    :param access_token: token we will try to refresh/get anew
    :return:
    """
    from .file_operations import conf_obj

    global oauth

    if isinstance(access_token, bytes):
        access_token = access_token.decode(encoding="utf-8")

    conf_object = conf_obj
    remote_url = conf_object["box"]["token_url"]
    refresh_token_from_cache: Union[bytes, str, None] = r_c.get(
        "diy_crate.auth.refresh_token"
    )

    refresh_token: str
    if isinstance(refresh_token_from_cache, bytes):
        refresh_token = refresh_token_from_cache.decode(encoding="utf-8")
    elif isinstance(refresh_token_from_cache, str):
        refresh_token = typing.cast(str, refresh_token_from_cache)
    else:
        refresh_token = ""
    if app:
        acquired = app.processing_oauth_refresh_lock.acquire()
        if acquired:
            if (access_token, refresh_token) != (
                app.oauth.access_token,
                app.oauth._refresh_token,
            ):
                if app.oauth.access_token and app.oauth._refresh_token:
                    app.processing_oauth_refresh_lock.release()
                    crate_logger.info(
                        "Acquired oauth refresh lock, "
                        "and the tokens appear to have changed already, %s, %s, %s, %s"
                        "so releasing early, skipping refresh function.",
                        access_token,
                        app.oauth.access_token,
                        refresh_token,
                        app.oauth._refresh_token,
                    )
                    access_token_resolved = app.oauth.access_token
                    store_tokens_callback(
                        access_token_resolved, app.oauth._refresh_token
                    )
                    return access_token_resolved

    post_data = {"refresh_token": refresh_token, "access_token": access_token}
    post_kwargs = {
        (
            "json" if bool(int(conf_obj["box"]["auth_data_as_json"])) else "data"
        ): post_data
    }
    response = httpx.post(
        remote_url,
        verify=True,
        timeout=10.0,
        **post_kwargs,
    )
    try:
        response.raise_for_status()
        response_json: List[str] = response.json()
        access_token_resolved, refresh_token = response_json
        oauth_resolved = oauth
        if oauth_resolved:
            oauth_resolved._update_current_tokens(access_token_resolved, refresh_token)
            oauth._update_current_tokens(access_token_resolved, refresh_token)
            if app:
                app.oauth._update_current_tokens(access_token_resolved, refresh_token)
                store_tokens_callback(access_token_resolved, refresh_token)
                app.processing_oauth_refresh_lock.release()
        else:
            raise AttributeError(
                "oauth_resolved has no attribute `None` (by hand exception)"
            )
    except AttributeError as e:
        if app:
            app.processing_oauth_refresh_lock.release()
        crate_logger.warn("Bad oauth object passed in.", exc_info=e)
        if r_c.exists("diy_crate.auth.access_token") and r_c.exists(
            "diy_crate.auth.refresh_token"
        ):
            r_c.delete("diy_crate.auth.access_token", "diy_crate.auth.refresh_token")
        sys.exit(1)
    except (ValueError, httpx.HTTPStatusError) as e:
        if app:
            app.processing_oauth_refresh_lock.release()
        response_error_body = response.content.decode()
        crate_logger.warn(
            "Error on response: %s, refresh_token len: %d, access_token len: %d",
            response_error_body,
            len(refresh_token),
            len(access_token or ""),
            exc_info=e,
        )

        # if r_c.exists("diy_crate.auth.access_token") and r_c.exists(
        #     "diy_crate.auth.refresh_token"
        # ):
        #     r_c.delete("diy_crate.auth.access_token", "diy_crate.auth.refresh_token")
        if app:
            handler = None
            crate_logger.info("Trying the regular oauth_dance.")
            oauth_dance(
                redis_client=r_c,
                conf=conf_obj,
                app=app,
                file_event_handler=handler,
            )
            raise e

        else:
            crate_logger.warn("Bad app object passed in, exiting", exc_info=e)
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
    app: Union[FastAPI, None] = None,
) -> RemoteOAuth2:
    """
    sets up the oauth instance with credentials and runtime callback.
    :param cache_client:
    :param retrieve_access_token:
    :param conf:
    :return:
    """
    global oauth
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
        return retrieve_access_token(
            access_token_arg,
            app=app,
        )

    oauth = RemoteOAuth2(
        client_id=client_id,
        client_secret="",
        access_token=access_token,
        refresh_token=refresh_token,
        retrieve_access_token=wrapper_retrieve_access_token,
    )
    return oauth


def setup_remote_oauth_initial_basic(
    cache_client,
    retrieve_access_token: typing.Callable = get_access_token,
    conf: Union[ConfigParser, dict, None] = None,
    app: Union[FastAPI, None] = None,
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

    def wrapper_retrieve_access_token_function(access_token_arg):
        crate_logger.info(
            "This shim temporary function in a hack function was called, fun."
        )
        return retrieve_access_token(
            access_token_arg,
            app=app,
        )

    oauth_obj = RemoteOAuth2(
        client_id=client_id,
        client_secret="",
        access_token=access_token,
        refresh_token=refresh_token,
        retrieve_access_token=wrapper_retrieve_access_token_function,
    )
    return oauth_obj


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
    redis_client: Redis,
    conf: Union[ConfigParser, dict, None],
    app: FastAPI,
    file_event_handler: Union[ProcessEvent, None],
    web_server_thread: Union[threading.Thread, None] = None,
):
    global oauth
    if True:
        access_token, refresh_token = (
            app.oauth.access_token,
            app.oauth._refresh_token,
        )
        acquired = app.processing_oauth_browser_lock.acquire()
        if acquired:
            if (access_token, refresh_token) != (
                app.oauth.access_token,
                app.oauth._refresh_token,
            ):
                crate_logger.info(
                    f"Acquired oauth browser lock, "
                    f"and the tokens appear to have changed already, {access_token=}, "
                    f"{app.oauth.access_token=}, {refresh_token=}, "
                    f"{app.oauth._refresh_token=} "
                    f"so releasing early, skipping browser new oauth."
                )
                if oauth:
                    oauth._update_current_tokens(
                        app.oauth.access_token, app.oauth._refresh_token
                    )

                app.processing_oauth_browser_lock.release()
                return

    app.oauth = setup_remote_oauth(redis_client, conf=conf, app=app)
    if file_event_handler:
        file_event_handler.oauth = app.oauth
    redirect_url_domain = "127.0.0.1"
    (auth_url, csrf_token) = app.oauth.get_authorization_url(
        redirect_url=f"https://{redirect_url_domain}:{conf['box']['web_server_port']}/"
    )
    app.csrf_token = csrf_token
    if web_server_thread and not web_server_thread.is_alive():
        web_server_thread.start()
    webbrowser.open_new_tab(auth_url)  # make it easy for the end-user to start auth


oauth = setup_remote_oauth_initial_basic(cache_client=r_c)
