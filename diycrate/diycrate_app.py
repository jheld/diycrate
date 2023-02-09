import argparse
import configparser
import logging
import os
import threading
import time
from pathlib import Path
from typing import Union, Optional

import bottle
import httpx
import pyinotify
from bottle import ServerAdapter
from boxsdk import Client, exception, OAuth2
from boxsdk.auth import RemoteOAuth2
from cheroot import wsgi as wsgiserver
from cheroot.ssl.builtin import BuiltinSSLAdapter

from diycrate.cache_utils import r_c
from diycrate.file_operations import (
    EventHandler,
    wm,
    in_moved_to,
    in_moved_from,
    mask,
)
from diycrate.item_queue_io import (
    upload_queue_processor,
    download_queue_processor,
    upload_queue,
)
from diycrate.long_poll_processing import long_poll_event_listener
from diycrate.oauth_utils import store_tokens_callback, setup_remote_oauth, oauth_dance
from diycrate.path_utils import re_walk

from diycrate.log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)

cloud_provider_name = "Box"

bottle_app = bottle.Bottle()

# The watch manager stores the watches and provides operations on watches

# keep the lint-ing & introspection from complaining
# that these attributes don't exist before run-time.

BOX_DIR = Path()

download_thread = threading.Thread(target=download_queue_processor)
upload_thread = threading.Thread(target=upload_queue_processor)

trash_directory = Path("~/.local/share/Trash/files").expanduser()
wait_time = os.environ.get("DIY_CRATE_FILE_IO_OPERATIONS_WAIT_TIME_MS")
if wait_time:
    wait_time = int(wait_time) / 1000
else:
    wait_time = 1

handler = EventHandler(
    upload_queue=upload_queue, bottle_app=bottle_app, wait_time=wait_time
)

file_notify_read_freq = 3

notifier = pyinotify.ThreadedNotifier(wm, handler, read_freq=file_notify_read_freq)
notifier.coalesce_events()

long_poll_thread = threading.Thread(target=long_poll_event_listener)
long_poll_thread.daemon = True

walk_thread = threading.Thread(target=re_walk)
walk_thread.daemon = True


# only re-enable when wishing to test token refresh or access dance
# @bottle_app.route('/kill_tokens')
# def kill_tokens():
#     # will get all of the oauth instances, since they are all the same reference
#     bottle_app.oauth._update_current_tokens(None, None)
#     r_c.delete('diy_crate.auth.refresh_token', 'diy_crate.auth.access_token')
#     return 'OK'

oauth: Optional[Union[RemoteOAuth2, OAuth2]] = None
conf_obj = configparser.ConfigParser()


@bottle_app.route("index")
def index():
    """
    Good to have a simple end-point.
    :return:
    """
    return "Hello, World!"


@bottle_app.route("/")
def oauth_handler():
    """
    RESTful end-point for the oauth handling
    :return:
    """
    assert bottle_app.csrf_token == bottle.request.GET["state"]
    access_token, refresh_token = httpx.post(
        conf_obj["box"]["authenticate_url"],
        data={"code": bottle.request.GET["code"]},
        verify=True,
    ).json()
    store_tokens_callback(access_token, refresh_token)
    bottle_app.oauth._update_current_tokens(access_token, refresh_token)
    if not getattr(bottle_app, "started_cloud_threads", False):
        start_cloud_threads(bottle_app.oauth)
        bottle_app.started_cloud_threads = True
    return "OK"


def start_cloud_threads(client_oauth):
    """

    :param client_oauth:
    :return:
    """
    client = Client(client_oauth)
    handler.oauth = client_oauth
    bottle_app.oauth = client_oauth
    watches = wm.add_watch(BOX_DIR.as_posix(), mask, rec=True, auto_add=True)
    crate_logger.debug(watches)
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
    box_folder = None
    while not failed:
        try:
            box_folder = client.folder(folder_id="0").get()
        except Exception:
            crate_logger.warning(
                "Encountered error getting root box folder", exc_info=True
            )
            time.sleep(2)
        else:
            failed = True
    if not download_thread.is_alive():
        download_thread.daemon = True
        download_thread.start()
    if not upload_thread.is_alive():
        upload_thread.daemon = True
        upload_thread.start()
    # local trash can
    wm.add_watch(
        trash_directory.as_posix(),
        mask=in_moved_to | in_moved_from,
        rec=True,
        auto_add=True,
    )
    if not long_poll_thread.is_alive():  # start before doing anything else
        long_poll_thread._args = (handler,)
        long_poll_thread.start()
    if not walk_thread.is_alive():
        walk_thread._args = (BOX_DIR, box_folder, client_oauth, bottle_app, handler)
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
        "web_server_port": args.port or conf_obj["box"].get("web_server_port", 8080),
    }
    web_server_port = conf_obj["box"]["web_server_port"]
    bottle_thread = threading.Thread(
        target=bottle_app.run,
        kwargs=dict(server=SSLCherryPyServer, port=web_server_port, host="0.0.0.0"),
    )
    bottle_thread.daemon = True
    with cloud_credentials_file_path.open("w") as fh:
        conf_obj.write(fh)
    BOX_DIR = Path(conf_obj["box"]["directory"]).expanduser().resolve()
    if not BOX_DIR.is_dir():
        BOX_DIR.mkdir()
    if not (
        r_c.exists("diy_crate.auth.access_token")
        and r_c.exists("diy_crate.auth.refresh_token")
    ):
        oauth_dance(
            redis_client=r_c,
            conf=conf_obj,
            bottle_app=bottle_app,
            file_event_handler=handler,
        )
        oauth = bottle_app.oauth
    else:
        try:
            oauth = setup_remote_oauth(r_c, conf=conf_obj)
            start_cloud_threads(oauth)
            bottle_app.started_cloud_threads = True
        except exception.BoxOAuthException:
            r_c.delete("diy_crate.auth.access_token", "diy_crate.auth.refresh_token")
            oauth_dance(
                redis_client=r_c,
                conf=conf_obj,
                bottle_app=bottle_app,
                file_event_handler=handler,
            )
    notifier.start()
    # notifier_thread = threading.Thread(target=notifier.loop)
    # notifier_thread.daemon = True
    # notifier_thread.start()
    if bottle_thread and not bottle_thread.is_alive():
        bottle_thread.start()


if __name__ == "__main__":
    main()
