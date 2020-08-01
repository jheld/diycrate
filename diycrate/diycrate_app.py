#!/usr/bin/env python3
import argparse
import configparser
import json
import logging
import os
import shutil
import threading
import time
from functools import partial

import bottle
import requests
import pyinotify
from bottle import ServerAdapter
from boxsdk import Client, exception
from cheroot import wsgi as wsgiserver
from cheroot.ssl.builtin import BuiltinSSLAdapter

from diycrate.cache_utils import redis_key, redis_get, r_c, redis_set
from diycrate.file_operations import EventHandler, wm, in_moved_to, in_moved_from, mask
from diycrate.gui import notify_user_with_gui
from diycrate.item_queue_io import (
    upload_queue_processor,
    download_queue_processor,
    download_queue,
    upload_queue,
)
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

BOX_DIR = os.path.expanduser("~/box")

download_thread = threading.Thread(target=download_queue_processor)
upload_thread = threading.Thread(target=upload_queue_processor)

trash_directory = os.path.expanduser("~/.local/share/Trash/files")

oauth_meta = {}

handler = EventHandler(
    upload_queue=upload_queue, bottle_app=bottle_app, oauth_meta_info=oauth_meta
)

notifier = pyinotify.ThreadedNotifier(wm, handler, read_freq=10)
notifier.coalesce_events()


def long_poll_event_listener():
    """
    Receive and process remote cloud item events in real-time
    :return:
    """
    while True:
        try:
            client = Client(oauth=handler.oauth)
            long_poll_streamer = client.events()
            stream_position = long_poll_streamer.get_latest_stream_position()
            event_stream = long_poll_streamer.generate_events_with_long_polling(
                stream_position
            )
            for event in event_stream:
                event_message = "{} happened! {} {}".format(
                    str(event), event.event_type, event.created_at
                )
                crate_logger.debug(event_message)
                if event.get("message", "").lower() == "reconnect":
                    break
                process_long_poll_event(client, event)
        except (exception.BoxAPIException, AttributeError):
            crate_logger.warning("Box or AttributeError occurred.", exc_info=True)
        except Exception:
            crate_logger.debug("General error occurred.", exc_info=True)


def process_long_poll_event(client, event):
    if event["event_type"] == "ITEM_CREATE":
        process_item_create_long_poll(client, event)
    if event["event_type"] == "ITEM_MOVE":
        process_item_move_long_poll(event)
    if event["event_type"] == "ITEM_RENAME":
        process_item_rename_long_poll(client, event)
    elif event["event_type"] == "ITEM_UPLOAD":
        process_item_upload_long_poll(client, event)
    elif event["event_type"] == "ITEM_TRASH":
        process_item_trash_long_poll(event)
    elif event["event_type"] == "ITEM_DOWNLOAD":
        pass


def process_item_create_long_poll(client, event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if int(event["source"]["path_collection"]["total_count"]) > 1:
        path = "{}".format(os.path.sep).join(
            [
                folder["name"]
                for folder in event["source"]["path_collection"]["entries"][1:]
            ]
        )
    else:
        path = ""
    path = os.path.join(BOX_DIR, path)
    if not os.path.isdir(path):
        os.makedirs(path)
    file_path = os.path.join(path, event["source"]["name"])
    if obj_type == "folder":
        if not os.path.isdir(file_path):
            os.makedirs(file_path)
        redis_set(
            r_c,
            client.folder(folder_id=obj_id),
            os.path.getmtime(file_path),
            BOX_DIR,
            True,
            path,
        )
    elif obj_type == "file":
        if not os.path.isfile(file_path):
            download_queue.put(
                [
                    client.file(file_id=obj_id).get(),
                    os.path.join(path, event["source"]["name"]),
                    client._oauth,
                ]
            )


def process_item_trash_long_poll(event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type == "file":
        process_item_trash_file(event, obj_id)
    elif obj_type == "folder":
        process_item_trash_folder(event, obj_id)


def process_item_trash_file(event, obj_id):
    item_info = r_c.get(redis_key(obj_id))
    if item_info:
        item_info = json.loads(str(item_info, encoding="utf-8", errors="strict"))
    if int(event["source"]["path_collection"]["total_count"]) > 1:
        path = "{}".format(os.path.sep).join(
            [
                folder["name"]
                for folder in event["source"]["path_collection"]["entries"][1:]
            ]
        )
    else:
        path = ""
    path = os.path.join(BOX_DIR, path)
    file_path = (
        os.path.join(path, event["source"]["name"])
        if not item_info
        else item_info["file_path"]
    )
    if os.path.exists(file_path):
        os.unlink(file_path)
    if r_c.exists(redis_key(obj_id)):
        r_c.delete(redis_key(obj_id))
        r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
    notify_user_with_gui(
        "Box message: Deleted {}".format(file_path), crate_logger, expire_time=10000
    )


def process_item_trash_folder(event, obj_id):
    item_info = r_c.get(redis_key(obj_id))
    if item_info:
        item_info = json.loads(str(item_info, encoding="utf-8", errors="strict"))
    if int(event["source"]["path_collection"]["total_count"]) > 1:
        path = "{}".format(os.path.sep).join(
            [
                folder["name"]
                for folder in event["source"]["path_collection"]["entries"][1:]
            ]
        )
    else:
        path = ""
    path = os.path.join(BOX_DIR, path)
    file_path = (
        os.path.join(path, event["source"]["name"])
        if not item_info
        else item_info["file_path"]
    )
    if os.path.isdir(file_path):
        # still need to get the parent_id
        for box_id in get_sub_ids(obj_id):
            r_c.delete(redis_key(box_id))
        r_c.delete(redis_key(obj_id))
        shutil.rmtree(file_path)
        parent_id = r_c.get(redis_key(obj_id)).get("parent_id")
        if parent_id:
            parent_folder = r_c.get(redis_key(parent_id))
            sub_ids = parent_folder.get("sub_ids", [])
            if sub_ids:
                sub_ids.remove(obj_id)
                r_c.set(redis_key(parent_id), json.dumps(parent_folder))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
        notify_user_with_gui("Box message: Deleted {}".format(file_path), crate_logger)


def process_item_upload_long_poll(client, event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type == "file":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = "{}".format(os.path.sep).join(
                [
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                ]
            )
        else:
            path = ""
        path = os.path.join(BOX_DIR, path)
        if not os.path.exists(path):  # just in case this is a file in a new subfolder
            os.makedirs(path)
        download_queue.put(
            [
                client.file(file_id=obj_id).get(),
                os.path.join(path, event["source"]["name"]),
                client._oauth,
            ]
        )
    elif obj_type == "folder":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = "{}".format(os.path.sep).join(
                [
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                ]
            )
        else:
            path = ""
        path = os.path.join(BOX_DIR, path)
        if not os.path.exists(path):  # just in case this is a file in a new subfolder
            os.makedirs(path)
        file_path = os.path.join(path, event["source"]["name"])
        box_message = "new version"
        if not os.path.exists(file_path):
            os.makedirs(file_path)
            redis_set(
                r_c,
                client.folder(folder_id=obj_id),
                os.path.getmtime(file_path),
                BOX_DIR,
                True,
                path,
            )
            box_message = "new folder"
        notify_user_with_gui(
            "Box message: {} {}".format(box_message, file_path), crate_logger
        )


def process_item_rename_long_poll(client, event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type == "file":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = "{}".format(os.path.sep).join(
                [
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                ]
            )
        else:
            path = ""
        path = os.path.join(BOX_DIR, path)
        file_path = os.path.join(path, event["source"]["name"])
        file_obj = client.file(file_id=obj_id).get()
        src_file_path = (
            None
            if not r_c.exists(redis_key(obj_id))
            else redis_get(r_c, file_obj)["file_path"]
        )
        if src_file_path and os.path.exists(src_file_path):
            version_info = redis_get(r_c, obj=file_obj)
            os.rename(src_file_path, file_path)
            version_info["file_path"] = file_path
            version_info["etag"] = file_obj["etag"]
            r_c.set(redis_key(obj_id), json.dumps(version_info))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
        else:
            download_queue.put([file_obj, file_path, client._oauth])
    elif obj_type == "folder":
        if int(event["source"]["path_collection"]["total_count"]) > 1:
            path = "{}".format(os.path.sep).join(
                [
                    folder["name"]
                    for folder in event["source"]["path_collection"]["entries"][1:]
                ]
            )
        else:
            path = ""
        path = os.path.join(BOX_DIR, path)
        file_path = os.path.join(path, event["source"]["name"])
        folder_obj = client.folder(folder_id=obj_id).get()
        src_file_path = (
            None
            if not r_c.exists(redis_key(obj_id))
            else redis_get(r_c, folder_obj)["file_path"]
        )
        if src_file_path and os.path.exists(src_file_path):
            os.rename(src_file_path, file_path)
            version_info = redis_get(r_c, obj=folder_obj)
            version_info["file_path"] = file_path
            version_info["etag"] = folder_obj["etag"]
            r_c.set(redis_key(obj_id), json.dumps(version_info))
            r_c.set("diy_crate.last_save_time_stamp", int(time.time()))


def process_item_move_long_poll(event):
    obj_id = event["source"]["id"]
    obj_type = event["source"]["type"]
    if obj_type in ("file", "folder"):
        if r_c.exists(redis_key(obj_id)):
            item_info = json.loads(
                str(r_c.get(redis_key(obj_id)), encoding="utf-8", errors="strict")
            )
            src_file_path = json.loads(
                str(r_c.get(redis_key(obj_id)), encoding="utf-8", errors="strict")
            )["file_path"]
            if int(event["source"]["path_collection"]["total_count"]) > 1:
                path = "{}".format(os.path.sep).join(
                    [
                        folder["name"]
                        for folder in event["source"]["path_collection"]["entries"][1:]
                    ]
                )
            else:
                path = ""
            path = os.path.join(BOX_DIR, path)
            file_path = os.path.join(path, event["source"]["name"])
            if os.path.exists(src_file_path):
                to_set = []
                if obj_type == "folder":
                    for sub_id in get_sub_ids(obj_id):
                        if r_c.exists(redis_key(sub_id)):
                            sub_item_info = json.loads(
                                str(
                                    r_c.get(redis_key(sub_id)),
                                    encoding="utf-8",
                                    errors="strict",
                                )
                            )
                            orig_len = len(src_file_path.split(os.path.sep))
                            tail = os.path.sep.join(
                                sub_item_info["file_path"].split(os.path.sep)[orig_len:]
                            )
                            new_sub_path = os.path.join(file_path, tail)
                            sub_item_info["file_path"] = new_sub_path
                            to_set.append(
                                partial(
                                    r_c.set,
                                    redis_key(sub_id),
                                    json.dumps(sub_item_info),
                                )
                            )
                shutil.move(src_file_path, file_path)
                for item in to_set:
                    item()
                item_info["file_path"] = file_path
                item_info["etag"] = event["source"]["etag"]
                r_c.set(redis_key(obj_id), json.dumps(item_info))


def get_sub_ids(box_id):
    """
    Retrieve the box item ids that are stored in this sub path
    :param box_id:
    :return:
    """
    ids = []
    item_info = json.loads(
        str(r_c.get(redis_key(box_id)), encoding="utf-8", errors="strict")
    )
    for sub_id in item_info["sub_ids"]:
        sub_item_info = json.loads(
            str(r_c.get(redis_key(sub_id)), encoding="utf-8", errors="strict")
        )
        if os.path.isdir(sub_item_info["file_path"]):
            ids.extend(get_sub_ids(sub_id))
        ids.append(sub_id)
    return ids


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

oauth = None
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
    access_token, refresh_token = requests.post(
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
    wm.add_watch(BOX_DIR, mask, rec=False)
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
    wm.add_watch(trash_directory, mask=in_moved_to | in_moved_from)
    if not long_poll_thread.is_alive():  # start before doing anything else
        long_poll_thread.start()
    if not walk_thread.is_alive():
        walk_thread._args = (
            BOX_DIR,
            box_folder,
            client_oauth,
            oauth_meta,
            bottle_app,
            handler,
        )
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

    conf_dir = os.path.abspath(os.path.expanduser("~/.config/diycrate"))
    if not os.path.isdir(conf_dir):
        os.mkdir(conf_dir)
    cloud_credentials_file_path = os.path.join(conf_dir, "box.ini")
    if not os.path.isfile(cloud_credentials_file_path):
        open(cloud_credentials_file_path, "w").write("")
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
    if had_oauth2:  # this was before we used the RemoteOauth2 workflow,
        # so we don't want this info hanging around
        conf_obj.remove_section("oauth2")
    if "ssl" not in conf_obj:
        if not args.cacert_pem_path:
            raise ValueError("Need a valid cacert_pem_path")
        if not args.privkey_pem_path:
            raise ValueError("Need a valid privkey_pem_path")
        conf_obj["ssl"] = {
            "cacert_pem_path": os.path.abspath(
                os.path.expanduser(args.cacert_pem_path)
            ),
            "privkey_pem_path": os.path.abspath(
                os.path.expanduser(args.privkey_pem_path)
            ),
        }
    conf_obj["ssl"] = {
        "cacert_pem_path": os.path.abspath(os.path.expanduser(args.cacert_pem_path))
        if args.cacert_pem_path
        else conf_obj["ssl"]["cacert_pem_path"],
        "privkey_pem_path": os.path.abspath(os.path.expanduser(args.privkey_pem_path))
        if args.privkey_pem_path
        else conf_obj["ssl"]["privkey_pem_path"],
    }

    conf_obj.write(open(cloud_credentials_file_path, "w"))
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
    conf_obj.write(open(cloud_credentials_file_path, "w"))
    BOX_DIR = os.path.expanduser(conf_obj["box"]["directory"])
    if not os.path.isdir(BOX_DIR):
        os.mkdir(BOX_DIR)
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
            oauth = setup_remote_oauth(r_c)
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
