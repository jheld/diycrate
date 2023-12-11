import argparse
import configparser
import logging
import os
import threading
import time
from pathlib import Path
from typing import Union
import typing

import fastapi
import uvicorn
from boxsdk.object.folder import Folder
import httpx
import pyinotify
from bottle import ServerAdapter
from boxsdk import BoxAPIException, Client, exception
from cheroot import wsgi as wsgiserver
from cheroot.ssl.builtin import BuiltinSSLAdapter
import cherrypy

from diycrate.cache_utils import r_c
from diycrate.file_operations import (
    EventHandler,
    wm,
    in_moved_to,
    in_moved_from,
    mask,
)

from diycrate.long_poll_processing import long_poll_event_listener
from diycrate.oauth_utils import (
    get_access_token,
    store_tokens_callback,
    setup_remote_oauth,
    oauth_dance,
)
from diycrate.oauth_utils import oauth  # noqa: F401
from diycrate.path_utils import re_walk

from diycrate.log_utils import setup_logger
from diycrate.utils import Bottle, FastAPI

setup_logger()

crate_logger = logging.getLogger(__name__)

cloud_provider_name = "Box"

bottle_app = Bottle()
bottle_app.processing_oauth_browser_lock = threading.Lock()
bottle_app.processing_oauth_refresh_lock = threading.Lock()

# The watch manager stores the watches and provides operations on watches

# keep the lint-ing & introspection from complaining
# that these attributes don't exist before run-time.

BOX_DIR = Path()

trash_directory = Path("~/.local/share/Trash/files").expanduser()
wait_time = os.environ.get("DIY_CRATE_FILE_IO_OPERATIONS_WAIT_TIME_MS")
if wait_time:
    wait_time = int(wait_time) / 1000
else:
    wait_time = 1

handler = EventHandler(bottle_app=bottle_app, wait_time=wait_time)

file_notify_read_freq = 3

notifier = pyinotify.ThreadedNotifier(wm, handler, read_freq=file_notify_read_freq)
notifier.coalesce_events()

long_poll_thread = threading.Thread(target=long_poll_event_listener)
long_poll_thread.daemon = True

walk_thread = threading.Thread(target=re_walk)
walk_thread.daemon = True

app = FastAPI()
app.processing_oauth_browser_lock = threading.Lock()
app.processing_oauth_refresh_lock = threading.Lock()


# only re-enable when wishing to test token refresh or access dance
# @bottle_app.route('/kill_tokens')
# def kill_tokens():
#     # will get all of the oauth instances, since they are all the same reference
#     bottle_app.oauth._update_current_tokens(None, None)
#     r_c.delete('diy_crate.auth.refresh_token', 'diy_crate.auth.access_token')
#     return 'OK'

conf_obj = configparser.ConfigParser()


# @bottle_app.route("index")
@app.get("/index/", response_class=fastapi.responses.PlainTextResponse)
def index():
    """
    Good to have a simple end-point.
    :return:
    """
    return "Hello, World!"


# @bottle_app.route("/")
@app.get("/", response_class=fastapi.responses.PlainTextResponse)
def oauth_handler(state: str, code: str):
    """
    RESTful end-point for the oauth handling
    :return:
    """
    # request = typing.cast(LocalRequest, bottle.request)
    # assert bottle_app.csrf_token == request.GET["state"]
    assert app.csrf_token == state
    access_token, refresh_token = httpx.post(
        conf_obj["box"]["authenticate_url"],
        data={"code": code},
        verify=True,
    ).json()
    store_tokens_callback(access_token, refresh_token)
    app.oauth._update_current_tokens(access_token, refresh_token)
    if not getattr(bottle_app, "started_cloud_threads", False):
        start_cloud_threads()
        app.started_cloud_threads = True
    app.processing_oauth_browser = False
    app.processing_oauth_browser_lock.release()
    return "OK"


def start_cloud_threads():
    """

    :param client_oauth:
    :return:
    """
    global oauth
    client = Client(oauth)
    handler.oauth = oauth
    # bottle_app.oauth = oauth
    app.oauth = oauth
    wm.add_watch(BOX_DIR.as_posix(), mask, rec=True, auto_add=True)
    client.auth._access_token = r_c.get("diy_crate.auth.access_token")
    client.auth._refresh_token = r_c.get("diy_crate.auth.refresh_token")
    if client.auth._access_token:
        client.auth._access_token = (
            client.auth._access_token.decode(encoding="utf-8")
            if isinstance(client.auth._access_token, bytes)
            else client.auth._access_token
        )
    if client.auth._refresh_token:
        client.auth._refresh_token = (
            client.auth._refresh_token.decode(encoding="utf-8")
            if isinstance(client.auth._refresh_token, bytes)
            else client.auth._refresh_token
        )
    failed = False
    box_folder: Union[Folder, None] = None
    loop_index = 0
    while not failed:
        try:
            box_folder_initial = typing.cast(Folder, client.folder(folder_id="0"))
            box_folder = typing.cast(Folder, box_folder_initial.get())  # type: ignore
        except BoxAPIException as e:
            crate_logger.info("Bad box api response.", exc_info=e)
            get_access_token(client.auth._access_token, app=app)
            client._auth = oauth
            time.sleep(min([pow(2, loop_index), 8]))
            loop_index += 1
        except Exception:
            crate_logger.warning(
                "Encountered error getting root box folder", exc_info=True
            )
            time.sleep(min([pow(2, loop_index), 8]))
            loop_index += 1
        else:
            failed = True
    # local trash can
    wm.add_watch(
        trash_directory.as_posix(),
        mask=in_moved_to | in_moved_from,
        rec=True,
        auto_add=True,
    )

    if not long_poll_thread.is_alive():  # start before doing anything else
        long_poll_thread._args = (handler, app)
        long_poll_thread.start()
    if not walk_thread.is_alive():
        walk_thread._args = (BOX_DIR, box_folder, oauth, app, handler)
        walk_thread.start()


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
        server = wsgiserver.Server((self.host, self.port), server_handler)
        # Uses the following github page's recommendation for setting up the cert:
        # https://github.com/nickbabcock/bottle-ssl
        server.ssl_adapter = BuiltinSSLAdapter(
            conf_obj["ssl"]["cacert_pem_path"], conf_obj["ssl"]["privkey_pem_path"]
        )
        try:
            server.start()
        except KeyboardInterrupt:
            walk_thread.join()
            long_poll_thread.join()
            notifier.join()
        finally:
            server.stop()

    @cherrypy.tools.register("before_finalize", priority=60)
    def secureheaders():
        headers = cherrypy.response.headers
        headers["X-Frame-Options"] = "DENY"
        headers["X-XSS-Protection"] = "1; mode=block"
        headers["Content-Security-Policy"] = "default-src 'self';"
        if (
            cherrypy.server.ssl_certificate is not None
            and cherrypy.server.ssl_private_key is not None
        ):
            headers["Strict-Transport-Security"] = "max-age=31536000"  # one year


def main():
    global oauth
    global BOX_DIR

    conf_dir = Path("~/.config/diycrate").expanduser().resolve()
    if not conf_dir.is_dir():
        conf_dir.mkdir()
    cloud_credentials_file_path = conf_dir / "box.ini"
    if not cloud_credentials_file_path.is_file():
        cloud_credentials_file_path.write_text("")
    conf_obj.read(cloud_credentials_file_path)
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--box_dir", type=str, help="directory for the root of box", default="~/box"
    )
    arg_parser.add_argument(
        "--cacert_pem_path",
        type=str,
        help="filepath to where the cacert.pem is located",
        default="",
    )
    arg_parser.add_argument(
        "--privkey_pem_path",
        type=str,
        help="filepath to where the privkey.pem is located",
        default="",
    )
    arg_parser.add_argument(
        "--authenticate_url",
        type=str,
        help="Full URL to perform remote Oauth2 authentication",
        default="",
    )
    arg_parser.add_argument(
        "--token_url",
        type=str,
        help="Full URL to do remote Oauth2 token refresh/retrieve",
        default="",
    )
    arg_parser.add_argument(
        "--authorization_url",
        type=str,
        help="Full URL to retrieve the " "Oauth2-generated authorization url",
        default="",
    )
    arg_parser.add_argument("--port", type=int, help="local web server port")
    arg_parser.add_argument(
        "--web-server-port",
        type=int,
        help="remote web server port",
        required=False,
        default=8080,
    )
    args = arg_parser.parse_args()
    had_oauth2 = conf_obj.has_section("oauth2")
    if had_oauth2:  # this was likely before we used the RemoteOauth2 workflow,
        # so we don't want this info hanging around
        # but if there is only the client_id defined, leave it alone
        if conf_obj["oauth2"].keys() - {"client_id"}:
            conf_obj.remove_section("oauth2")
    if "ssl" not in conf_obj:
        if not args.cacert_pem_path:
            raise ValueError("Need a valid cacert_pem_path")
        if not args.privkey_pem_path:
            raise ValueError("Need a valid privkey_pem_path")
        conf_obj["ssl"] = {
            "cacert_pem_path": Path(args.cacert_pem_path)
            .expanduser()
            .resolve()
            .as_posix(),
            "privkey_pem_path": Path(args.privkey_pem_path)
            .expanduser()
            .resolve()
            .as_posix(),
        }
    conf_obj["ssl"] = {
        "cacert_pem_path": Path(args.cacert_pem_path).expanduser().resolve().as_posix()
        if args.cacert_pem_path
        else conf_obj["ssl"]["cacert_pem_path"],
        "privkey_pem_path": Path(args.privkey_pem_path)
        .expanduser()
        .resolve()
        .as_posix()
        if args.privkey_pem_path
        else conf_obj["ssl"]["privkey_pem_path"],
    }

    with open(cloud_credentials_file_path, "w") as fh:
        conf_obj.write(fh)
    had_box = conf_obj.has_section("box")
    if not had_box:
        conf_obj.add_section("box")

    conf_obj["box"] = {
        "directory": args.box_dir or conf_obj["box"]["directory"],
        "authenticate_url": args.authenticate_url
        or conf_obj["box"].get(
            "authenticate_url", "https://localhost:8081/authenticate"
        ),
        "authorization_url": args.authorization_url
        or conf_obj["box"].get("authorization_url", "https://localhost:8081/auth_url"),
        "token_url": args.token_url
        or conf_obj["box"].get("token_url", "https://localhost:8081/new_access"),
        "web_server_port": args.port or args.web_server_port,
    }
    web_server_port: int = int(conf_obj["box"]["web_server_port"])
    bottle_thread = threading.Thread(
        # target=bottle_app.run,
        target=uvicorn.run,
        kwargs=dict(
            app=app,
            port=web_server_port,
            host="0.0.0.0",
            ssl_certfile=conf_obj["ssl"]["cacert_pem_path"],
            ssl_keyfile=conf_obj["ssl"]["privkey_pem_path"],
        ),
    )
    bottle_thread.daemon = True
    with cloud_credentials_file_path.open("w") as fh:
        conf_obj.write(fh)
    BOX_DIR = Path(conf_obj["box"]["directory"]).expanduser().resolve()
    if not BOX_DIR.is_dir():
        BOX_DIR.mkdir()
    if bottle_thread and not bottle_thread.is_alive():
        bottle_thread.start()

    if not (
        r_c.exists("diy_crate.auth.access_token")
        and r_c.exists("diy_crate.auth.refresh_token")
    ):
        app.oauth = oauth
        oauth_dance(
            redis_client=r_c,
            conf=conf_obj,
            app=app,
            file_event_handler=handler,
        )
        oauth = app.oauth
    else:
        try:
            oauth = setup_remote_oauth(r_c, conf=conf_obj, app=app)
            app.oauth = oauth
            handler.oauth = oauth
            start_cloud_threads()
            app.started_cloud_threads = True
        except exception.BoxOAuthException:
            r_c.delete("diy_crate.auth.access_token", "diy_crate.auth.refresh_token")
            oauth_dance(
                redis_client=r_c,
                conf=conf_obj,
                app=app,
                file_event_handler=handler,
            )
    notifier.start()
    # notifier_thread = threading.Thread(target=notifier.loop)
    # notifier_thread.daemon = True
    # notifier_thread.start()

    while threading.active_count() > 1:
        time.sleep(0.3)
    notifier.stop()


if __name__ == "__main__":
    main()
