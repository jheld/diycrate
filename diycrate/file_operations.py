import configparser
import json
import os
import queue
import threading
import logging
import time
from datetime import timedelta
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Iterable, Optional, TypedDict, Union, List, Dict, cast
import typing
from boxsdk import Client, OAuth2


import boxsdk.object.file
from boxsdk.object.file import File
from boxsdk.object.folder import Folder
from boxsdk.object.item import Item
import pyinotify

from boxsdk.exception import BoxAPIException
from boxsdk.util.chunked_uploader import ChunkedUploader
from dateutil import tz
from dateutil.tz import tzutc
from dateutil.parser import parse
from requests.exceptions import ConnectionError
from urllib3.exceptions import ProtocolError

from diycrate.utils import FastAPI

from .cache_utils import (
    redis_key,
    redis_get,
    r_c,
    local_or_box_file_m_time_key_func,
    redis_set,
    redis_path_for_object_id_key,
)
from .item_queue_io import (
    UploadQueueItem,
    UploadQueueItemReal,
    upload_pool_executor,
    upload_queue_processor,
)
from .iter_utils import SafeIter
from .log_utils import setup_logger
from .oauth_utils import get_access_token, setup_remote_oauth

setup_logger()

crate_logger = logging.getLogger(__name__)

trash_directory = Path("~/.local/share/Trash/files").expanduser().resolve()

conf_obj = configparser.ConfigParser()
conf_dir = Path("~/.config/diycrate").expanduser().resolve()
if not conf_dir.is_dir():
    conf_dir.mkdir()
cloud_credentials_file_path = conf_dir / "box.ini"
if not cloud_credentials_file_path.is_file():
    cloud_credentials_file_path.write_text("")
conf_obj.read(cloud_credentials_file_path)
BOX_DIR = Path(conf_obj["box"]["directory"]).expanduser().resolve()


class Event(pyinotify.Event):
    wd: int
    mask: int
    maskname: str
    path: str
    name: Optional[str]
    pathname: str
    src_pathname: str
    cookie: int
    dir: bool


# noinspection PyUnresolvedReferences
class EventHandler(pyinotify.ProcessEvent):
    """
    EventHandler to manage cloud storage synchronization.
    """

    oauth: OAuth2
    wait_time: int

    # noinspection SpellCheckingInspection
    def my_init(self, **kargs):
        """
        Extends the super to add cloud storage state.
        :return:
        """
        our_keys = ["oauth", "app", "wait_time"]
        our_kargs = {k: kargs.get(k) for k in our_keys}
        for key in our_keys:
            kargs.pop(key, None)
        super().my_init(**kargs)
        kargs = our_kargs
        self.move_events = []
        self.files_from_box = []
        self.folders_from_box = []
        self.incoming_operations = queue.Queue()
        self.operations = []
        setattr(self, "wait_time", kargs.get("wait_time", 1))
        setattr(self, "oauth", kargs.get("oauth"))
        self.operations_thread = threading.Thread(
            target=self.incoming_operation_coalesce
        )

        self.app: Optional[FastAPI] = kargs.get("app")
        self.operations_thread.daemon = True
        self.operations_thread.start()

    def incoming_operation_coalesce(self):
        while True:
            item = self.incoming_operations.get()
            self.operations.append(item)
            self.incoming_operations.task_done()
            self.operation_coalesce()

    def operation_coalesce(self):
        """
        Coalesce and process the operations in a more timely-fashion.
        :return:
        """
        # while True:
        #     # time.sleep(self.wait_time)
        cur_num_operations = len(self.operations)
        if cur_num_operations:
            operations_to_perform = self.operations[
                :cur_num_operations
            ]  # keep a local copy for this loop-run
            # operations list could have changed since the previous two instructions
            # pycharm complained that I was re-assigning the instance
            # variable outside the __init__.
            self.operations.clear()
            self.operations.extend(
                self.operations[cur_num_operations:]
                if len(self.operations) > cur_num_operations
                else []
            )
            for operation in operations_to_perform:
                self.process_event(*operation)

    @staticmethod
    def get_folder(client: Client, folder_id: str) -> Folder:
        """

        :param client:
        :param folder_id:
        :return:
        """
        folder: Folder
        num_retry = 15
        for x in range(num_retry):
            try:
                raw_folder = client.folder(folder_id=folder_id)
                folder = raw_folder.get()
                return folder
            except (
                ConnectionError,
                BrokenPipeError,
                ProtocolError,
                ConnectionResetError,
                BoxAPIException,
            ):
                crate_logger.warning(
                    "Error getting box folder id: {}".format(folder_id), exc_info=True
                )
                if x >= num_retry - 1:
                    crate_logger.debug(
                        "Failed for the last time to get the folder: {}".format(
                            folder_id
                        )
                    )
                    raise

        raise Exception("Should not be here")

    @staticmethod
    def folders_to_traverse(event_path: Union[str, Path]) -> List[str]:
        """

        :param event_path:
        :return:
        """
        if isinstance(event_path, str):
            event_path = Path(event_path)
        folders_to_traverse = [
            folder
            for folder in event_path.relative_to(BOX_DIR).parts
            if folder and folder != "/"
        ]
        return folders_to_traverse

    @staticmethod
    def traverse_path(
        client: Client,
        event: Event,
        cur_box_folder: Folder,
        folders_to_traverse: List["str"],
    ):
        """

        :param cur_box_folder:
        :param client:
        :param event:
        :param folders_to_traverse:
        :return:
        """
        for folder in folders_to_traverse:
            did_find_folder = False
            limit = 100
            offset = 0
            raw_item_iter = cur_box_folder.get_items(offset=offset, limit=limit)  # type: ignore
            folder_items_generator = cast(Iterable[Item], raw_item_iter)
            loop_idx = 0
            while True:
                try:
                    for entry in folder_items_generator:
                        if folder == entry["name"] and entry["type"] == "folder":
                            did_find_folder = True
                            cur_box_folder = EventHandler.get_folder(
                                client, entry.object_id
                            )  # type: ignore
                            break  # found this piece of the path, keep going
                except BoxAPIException:
                    time.sleep(min([pow(2, loop_idx), 8]))
                    loop_idx += 1
                else:
                    break
            if not did_find_folder:
                try:
                    cur_box_folder = cur_box_folder.create_subfolder(folder).get()
                    crate_logger.debug(f"Sub-folder creation: {event.pathname}")
                except BoxAPIException:
                    crate_logger.debug(
                        "Box exception as we try to create a sub-folder {folder}".format(
                            folder=folder
                        ),
                        exc_info=True,
                    )
                    raise
        return cur_box_folder

    def process_event(self, event: Event, operation: str):
        """
        Wrapper to process the given event on the operation.
        :param event:
        :param operation:
        :return:
        """
        crate_logger.debug(f"{operation=}, {event=}")
        try:
            if operation == "delete":
                self.process_delete_event(event)
            elif operation == "move":
                self.process_move_event(cast(List, event))
            elif operation == "create":
                self.process_create_event(event)
            elif operation == "modify":
                self.process_modify_event(event, operation)
            elif operation == "real_close":
                self.process_real_close_event(event)
        except Exception:
            crate_logger.warning(
                "Operation: {operation} on event: {event} encountered an error.".format(
                    operation=operation, event=event
                ),
                exc_info=True,
            )

    def process_real_close_event(self, event: Event):
        file_path = Path(event.pathname)
        crate_logger.debug(f"Real close...: {file_path.as_posix()}")
        folders_to_traverse = self.folders_to_traverse(file_path.parent.as_posix())
        client = Client(self.oauth)
        cur_box_folder = get_box_folder(client, "0", 5, app=self.app)
        # if we're modifying in root box dir, then we've already found the folder
        try:
            is_base = any(
                Path(evp).relative_to(BOX_DIR) for evp in [event.path, event.path[:-1]]
            )
        except ValueError:
            is_base = False
        cur_box_folder = self.traverse_path(
            client, event, cur_box_folder, folders_to_traverse
        )
        last_dir = Path(event.path).parent.name
        if not is_base:
            AssertionError(
                cur_box_folder["name"] == last_dir,
                cur_box_folder["name"] + "not equals " + last_dir,
            )
        did_find_the_file = os.path.isdir(file_path)  # true if we are a directory :)
        did_find_the_folder = os.path.isfile(
            file_path
        )  # true if we are a regular file :)
        is_file = file_path.is_file()
        is_dir = file_path.is_dir()
        limit = 100
        offset = 0
        items_iter = cur_box_folder.get_items(offset=offset, limit=limit)
        safe_iter = SafeIter(items_iter, path=event.pathname)
        for entry in safe_iter:
            if entry is None:
                continue
            did_find_the_file = (
                is_file and entry["type"] == "file" and entry["name"] == file_path.name
            )
            did_find_the_folder = (
                is_dir and entry["type"] == "folder" and entry["name"] == file_path.name
            )
            if did_find_the_file:
                break
        # not a box file/folder (though could have been copied from a local box item)
        if is_file and not did_find_the_file:
            last_modified_time = (
                datetime.fromtimestamp(file_path.stat().st_mtime)
                .astimezone(tzutc())
                .timestamp()
            )
            try:
                if file_path.stat().st_size < 20000000:
                    raise BoxAPIException(
                        **dict(code="file_size_too_small", status=400)
                    )
                box_uploader: ChunkedUploader = cur_box_folder.get_chunked_uploader(
                    file_path.as_posix()
                )
                uploader_callable = partial(
                    box_uploader.start,
                )
            except BoxAPIException as e:
                if e.status == 400 and e.code == "file_size_too_small":
                    uploader_callable = partial(
                        cur_box_folder.upload,
                        file_path.as_posix(),
                        file_path.name,
                        preflight_check=True,
                        upload_using_accelerator=True,
                    )
                else:
                    raise
            queue_item = UploadQueueItemReal(
                last_modified_time,
                uploader_callable,
                self.oauth,
                file_path.as_posix(),
            )

            upload_pool_executor.apply_async(
                upload_queue_processor, kwds=dict(queue_item=queue_item)
            )
        elif is_dir and not did_find_the_folder:
            queue_item: UploadQueueItem = partial(
                cur_box_folder.create_subfolder, file_path.name
            )

            upload_pool_executor.apply_async(
                upload_queue_processor, kwds=dict(queue_item=queue_item)
            )

    def process_modify_event(self, event: Event, operation: str):
        file_path = Path(event.pathname)
        crate_logger.debug(f"{operation}...: {file_path.as_posix()}")
        if r_c.exists(f"{event.pathname}:created:enqueue"):
            crate_logger.info(
                f"Skipping {operation} logic because cache key "
                f"`created:enqueue` for {file_path.as_posix()}"
            )
            return
        try:
            r_c.set(
                local_or_box_file_m_time_key_func(file_path.as_posix(), False),
                datetime.fromtimestamp(file_path.stat().st_mtime)
                .astimezone(tzutc())
                .timestamp(),
            )
        except FileNotFoundError:
            pass
        folders_to_traverse = self.folders_to_traverse(file_path.parent.as_posix())
        client = Client(self.oauth)

        folder_id = "0"
        retry_limit = 5
        cur_box_folder = get_box_folder(client, folder_id, retry_limit, app=self.app)
        # if we're modifying in root box dir, then we've already found the folder
        try:
            is_base = any(
                Path(evp).relative_to(BOX_DIR) for evp in [event.path, event.path[:-1]]
            )
        except ValueError:
            is_base = False
        cur_box_folder = self.traverse_path(
            client, event, cur_box_folder, folders_to_traverse
        )
        last_dir = Path(event.path).parent.name
        if not is_base:
            AssertionError(
                cur_box_folder["name"] == last_dir,
                cur_box_folder["name"] + "not equals " + last_dir,
            )
        did_find_the_file = os.path.isdir(file_path)  # true if we are a directory :)
        did_find_the_folder = os.path.isfile(
            file_path
        )  # true if we are a regular file :)
        is_file = os.path.isfile(file_path)
        is_dir = os.path.isdir(file_path)
        limit = 100
        offset = 0

        for entry in cur_box_folder.get_items(offset=offset, limit=limit):
            did_find_the_file = (
                is_file and entry["type"] == "file" and entry["name"] == file_path.name
            )
            did_find_the_folder = (
                is_dir and entry["type"] == "folder" and entry["name"] == file_path.name
            )
            if did_find_the_file:
                last_modified_time = file_path.stat().st_mtime
                if entry.object_id not in self.files_from_box:
                    cur_file = client.file(file_id=entry.object_id).get()
                    can_update = True
                    was_versioned = r_c.exists(redis_key(cur_file.object_id))
                    try:

                        class ItemVersion(TypedDict):
                            time_stamp: int
                            fresh_download: bool
                            etag: str

                        raw_info: ItemVersion | None = (
                            redis_get(r_c, cur_file) if was_versioned else None
                        )
                        if was_versioned:
                            info = cast(ItemVersion, raw_info)
                        else:
                            info = ItemVersion(
                                **{"fresh_download": True, "etag": "0", "time_stamp": 0}
                            )

                        item_version = info
                        if cur_file["etag"] == item_version["etag"] and (
                            (
                                item_version["fresh_download"]
                                and (item_version["time_stamp"] >= last_modified_time)
                            )
                            or (
                                not item_version["fresh_download"]
                                and item_version["time_stamp"] >= last_modified_time
                            )
                        ):
                            can_update = False
                        if can_update:
                            queue_item = UploadQueueItemReal(
                                datetime.fromtimestamp(last_modified_time)
                                .astimezone(tzutc())
                                .timestamp(),
                                partial(cur_file.update_contents, file_path.as_posix()),
                                self.oauth,
                                file_path.as_posix(),
                            )

                            upload_pool_executor.apply_async(
                                upload_queue_processor, kwds=dict(queue_item=queue_item)
                            )
                        else:
                            is_new_time_stamp = (
                                item_version["time_stamp"] >= last_modified_time
                            )
                            crate_logger.debug(
                                "Skipping the update because not "
                                "versioned: {not_versioned}, "
                                "fresh_download: {fresh_download}, "
                                "version time_stamp >= "
                                "new time stamp: {new_time_stamp}, "
                                "event pathname: {path_name}, "
                                "cur file id: {obj_id}".format(
                                    not_versioned=not was_versioned,
                                    fresh_download=item_version["fresh_download"],
                                    new_time_stamp=is_new_time_stamp,
                                    path_name=event.pathname,
                                    obj_id=cur_file.object_id,
                                )
                            )
                    except TypeError:
                        crate_logger.debug("Error occurred", exc_info=True)
                    except Exception:
                        crate_logger.debug("Error occurred", exc_info=True)

                else:
                    self.files_from_box.remove(
                        entry.object_id
                    )  # just wrote if, assuming create event didn't run
                break
            elif did_find_the_folder:
                if entry.object_id not in self.folders_from_box:
                    crate_logger.debug(
                        "Cannot create a subfolder when it already exists: {}".format(
                            event.pathname
                        )
                    )
                else:
                    self.folders_from_box.remove(
                        entry.object_id
                    )  # just wrote if, assuming create event didn't run
                break
        if is_file and not did_find_the_file:
            crate_logger.debug("Uploading contents...: {}".format(file_path.as_posix()))
            last_modified_time = Path(event.pathname).stat().st_mtime
            queue_item = UploadQueueItemReal(
                datetime.fromtimestamp(last_modified_time)
                .astimezone(tzutc())
                .timestamp(),
                partial(
                    cur_box_folder.upload,
                    file_path.as_posix(),
                    file_path.name,
                    preflight_check=True,
                    upload_using_accelerator=True,
                ),
                self.oauth,
                file_path.as_posix(),
            )

            upload_pool_executor.apply_async(
                upload_queue_processor, kwds=dict(queue_item=queue_item)
            )
        if is_dir and not did_find_the_folder:
            crate_logger.debug(
                "Creating a sub-folder...: {}".format(file_path.as_posix())
            )
            queue_item: UploadQueueItem = partial(
                cur_box_folder.create_subfolder, file_path.name
            )

            upload_pool_executor.apply_async(
                upload_queue_processor, kwds=dict(queue_item=queue_item)
            )
            # wm.add_watch(file_path.as_posix(), rec=True, mask=mask, auto_add=True)

    def process_create_event(self, event: Event):
        crate_logger.debug("Creating: {}".format(event.pathname))
        folders_to_traverse = self.folders_to_traverse(event.path)
        client = Client(self.oauth)
        box_folder: Union[Folder, None] = None
        loop_index = 0
        while not box_folder:
            try:
                box_folder = client.folder(folder_id="0").get()
            except BoxAPIException as e:
                crate_logger.warning("Error getting box root folder.", exc_info=e)
                get_access_token(client.auth.access_token, app=self.app)
                client = Client(cast(FastAPI, self.app).oauth)
                time.sleep(min([pow(2, loop_index), 8]))
                loop_index += 1
            except Exception:
                crate_logger.warning("Error getting box root folder.", exc_info=True)
                time.sleep(min([pow(2, loop_index), 8]))
                loop_index += 1
        cur_box_folder = box_folder
        # if we're modifying in root box dir, then we've already found the folder
        try:
            is_base = any(
                Path(evp).relative_to(BOX_DIR) for evp in [event.path, event.path[:-1]]
            )
        except ValueError:
            is_base = False
        cur_box_folder = self.traverse_path(
            client, event, cur_box_folder, folders_to_traverse
        )
        last_dir = Path(event.path).parent.name
        if not is_base:
            assert cur_box_folder["name"] == last_dir
        did_find_the_file = os.path.isdir(
            event.pathname
        )  # true if we are a directory :)
        did_find_the_folder = os.path.isfile(
            event.pathname
        )  # true if we are a regular file :)
        is_file = os.path.isfile(event.pathname)
        is_dir = os.path.isdir(event.pathname)
        limit = 100
        offset = 0
        for entry in cur_box_folder.get_items(offset=offset, limit=limit):
            did_find_the_file = (
                is_file
                and entry["type"] == "file"
                and entry["name"] == os.path.basename(event.pathname)
            )
            did_find_the_folder = (
                is_dir
                and entry["type"] == "folder"
                and entry["name"] == os.path.basename(event.pathname)
            )
            if did_find_the_file:
                dwld_key = "diy_crate.breadcrumb.create_from_box.{path}".format(
                    path=event.pathname
                )
                file_created_from_download = r_c.exists(dwld_key)
                if file_created_from_download:
                    self.files_from_box.append(entry.object_id)
                    r_c.delete(dwld_key)
                if entry.object_id not in self.files_from_box:
                    # more accurately, was this created offline?
                    AssertionError(
                        False,
                        "We should not be able to create a "
                        "file that exists in box; should be a close/modify.",
                    )
                    crate_logger.debug("Update the file: {}".format(event.pathname))
                    a_file = client.file(file_id=entry["id"]).get()
                    # seem it is possible to get more than one create
                    # (without having a delete in between)
                    queue_item: UploadQueueItem = partial(
                        a_file.update_contents, event.pathname
                    )

                    upload_pool_executor.apply_async(
                        upload_queue_processor, kwds=dict(queue_item=queue_item)
                    )
                    # cur_box_folder.upload(event.pathname, event.name)
                else:
                    crate_logger.debug(
                        "Created from box: {fpath}, but create flag ran on "
                        "our fs too, so we are skipping the logic to upload.".format(
                            fpath=event.pathname
                        )
                    )
                    self.files_from_box.remove(entry["id"])  # just downloaded it
                break
            elif did_find_the_folder:
                # we are not going to re-create the folder, but we are
                # also not checking if the contents in this
                # local creation are different from the contents in box.
                if entry["id"] in self.folders_from_box:
                    self.folders_from_box.remove(entry["id"])  # just downloaded it
                break
        if is_file and not did_find_the_file:
            crate_logger.debug("Upload the file: {}".format(event.pathname))
            last_modified_time = os.path.getctime(event.pathname)
            r_c.set(
                f"{event.pathname}:created:enqueue", value=1, ex=timedelta(minutes=1)
            )
            queue_item = UploadQueueItemReal(
                datetime.fromtimestamp(last_modified_time)
                .astimezone(tzutc())
                .timestamp(),
                partial(
                    cur_box_folder.upload,
                    event.pathname,
                    os.path.basename(event.pathname),
                    preflight_check=True,
                    upload_using_accelerator=True,
                ),
                self.oauth,
                event.pathname,
            )

            upload_pool_executor.apply_async(
                upload_queue_processor, kwds=dict(queue_item=queue_item)
            )
        elif is_dir and not did_find_the_folder:
            crate_logger.debug("Upload the folder: {}".format(event.pathname))
            queue_item: UploadQueueItem = partial(
                cur_box_folder.create_subfolder,
                os.path.basename(event.pathname),
            )

            upload_pool_executor.apply_async(
                upload_queue_processor, kwds=dict(queue_item=queue_item)
            )
            # wm.add_watch(event.pathname, rec=True, mask=mask, auto_add=True)

    def process_move_event(self, event: List[Event]):
        crate_logger.debug("Doing a move on: {}".format(event))
        src_event, dest_event = event
        folders_to_traverse = self.folders_to_traverse(dest_event.path)
        client = Client(self.oauth)

        box_folder = None
        loop_idx = 0
        while not box_folder:
            try:
                box_folder = client.folder(folder_id="0").get()
            except Exception:
                crate_logger.warning("Error getting root box folder.", exc_info=True)
                time.sleep(min([pow(2, loop_idx), 8]))
                loop_idx += 1
        cur_box_folder = box_folder
        # if we're modifying in root box dir, then we've already found the folder
        cur_box_folder = self.traverse_path(
            client, dest_event, cur_box_folder, folders_to_traverse
        )
        src_folders_to_traverse = self.folders_to_traverse(src_event.path)
        src_box_folder = box_folder
        src_box_folder = self.traverse_path(
            client, src_event, src_box_folder, src_folders_to_traverse
        )
        is_rename = src_event.path == dest_event.path
        # is_a_directory = 'IN_ISDIR'.lower() in dest_event.maskname.lower()
        did_find_src_file = os.path.isdir(
            dest_event.pathname
        )  # true if we are a directory :)
        did_find_src_folder = os.path.isfile(
            dest_event.pathname
        )  # true if we are a regular file :)
        is_file = os.path.isfile(dest_event.pathname)
        is_dir = os.path.isdir(dest_event.pathname)
        move_from_remote = False
        limit = 100
        offset = 0
        folder_gen = src_box_folder.get_items(offset=offset, limit=limit)
        safe_iter = SafeIter(folder_gen, path=dest_event.name)
        for entry in safe_iter:
            if entry is None:
                continue
            did_find_src_file = (
                is_file and entry["name"] == src_event.name and entry["type"] == "file"
            )
            did_find_src_folder = (
                is_dir and entry["name"] == src_event.name and entry["type"] == "folder"
            )
            if did_find_src_file:
                src_file: File = client.file(file_id=entry["id"]).get()
                if is_rename:
                    crate_logger.debug(f"Processing as a rename {dest_event.pathname=}")
                    last_modified_time = (
                        datetime.fromtimestamp(
                            Path(dest_event.pathname).stat().st_mtime
                        )
                        .astimezone(tzutc())
                        .timestamp()
                    )
                    file_obj = client.file(file_id=src_file.object_id).get()
                    data_sent_to_cache = redis_set(
                        r_c, file_obj, last_modified_time, box_dir_path=BOX_DIR
                    )
                    version_info = data_sent_to_cache[redis_key(src_file.object_id)]
                    r_c.set(
                        local_or_box_file_m_time_key_func(dest_event.pathname, False),
                        datetime.fromtimestamp(
                            Path(dest_event.pathname).stat().st_mtime
                        )
                        .astimezone(tzutc())
                        .timestamp(),
                    )
                    src_file.rename(dest_event.name)
                    file_obj: File = client.file(file_id=src_file.object_id).get()
                    version_info["file_path"] = dest_event.pathname
                    version_info["etag"] = file_obj["etag"]
                    r_c.set(
                        redis_path_for_object_id_key(Path(dest_event.pathname)),
                        src_file.object_id,
                    )
                    r_c.delete(
                        redis_path_for_object_id_key(
                            Path(src_event.path) / src_file.name
                        )
                    )
                    r_c.set(redis_key(src_file.object_id), json.dumps(version_info))
                    r_c.set("diy_crate.last_save_time_stamp", int(time.time()))
                    path_builder = BOX_DIR
                    oauth = setup_remote_oauth(r_c, conf=conf_obj, app=self.app)
                    client = Client(oauth)
                    parent_folders: list[Folder] = (
                        client.file(file_obj.object_id)
                        .get(fields=["path_collection"])
                        .path_collection["entries"]
                    )
                    for updated_entry in parent_folders:
                        if updated_entry.id == "0":
                            continue
                        path_builder /= updated_entry.name
                        folder_entry: Folder = client.folder(updated_entry.id).get(
                            fields=["modified_at"]
                        )
                        if not r_c.exists(redis_path_for_object_id_key(path_builder)):
                            r_c.set(
                                redis_path_for_object_id_key(path_builder),
                                updated_entry.object_id,
                            )
                        r_c.set(
                            local_or_box_file_m_time_key_func(path_builder, True),
                            parse(folder_entry.modified_at)
                            .astimezone(tzutc())
                            .timestamp(),
                        )

                else:
                    crate_logger.debug(f"Processing as a move {dest_event.pathname=}.")
                    did_find_cur_file = os.path.isdir(
                        dest_event.pathname
                    )  # should check box instead
                    cur_offset = 0
                    cur_box_folder_items = list(
                        cur_box_folder.get_items(offset=cur_offset, limit=limit)
                    )
                    for cur_entry in cur_box_folder_items:
                        matching_name = cur_entry["name"] == os.path.basename(
                            dest_event.pathname
                        )
                        did_find_cur_file = (
                            is_file and matching_name and isinstance(cur_entry, File)
                        )
                        did_find_cur_folder = (
                            is_dir and matching_name and isinstance(cur_entry, Folder)
                        )
                        if did_find_cur_file:
                            crate_logger.info(
                                f"Did find cur file (moving to {dest_event.pathname=})"
                            )
                            queue_item = UploadQueueItemReal(
                                datetime.fromtimestamp(
                                    Path(dest_event.pathname).stat().st_mtime
                                )
                                .astimezone(tzutc())
                                .timestamp(),
                                partial(
                                    cast(File, cur_entry).update_contents,
                                    dest_event.pathname,
                                ),
                                self.oauth,
                                dest_event.pathname,
                            )

                            upload_pool_executor.apply_async(
                                upload_queue_processor, kwds=dict(queue_item=queue_item)
                            )
                            queue_item: UploadQueueItem = partial(src_file.delete)

                            upload_pool_executor.apply_async(
                                upload_queue_processor, kwds=dict(queue_item=queue_item)
                            )
                            break
                        elif did_find_cur_folder:
                            crate_logger.debug(
                                "do not currently support moving a same name folder "
                                "into parent with"
                                "folder inside of the same name -- would may need "
                                "to update the contents"
                            )
                            break
                    if is_file and not did_find_cur_file and did_find_src_file:
                        crate_logger.debug(
                            f"Did not find cur file {dest_event.pathname=} "
                            f"so we call Box API's src_file.move to cur box folder."
                        )

                        def complex_move():
                            before_move_path_collection_entries = list(
                                src_file.path_collection["entries"]
                            )
                            src_file_move_ret_value = src_file.move(cur_box_folder)

                            # oauth = setup_remote_oauth(r_c, conf=conf_obj)
                            # client = Client(oauth)
                            def and_after():
                                complex_path_builder = BOX_DIR
                                for (
                                    complex_entry
                                ) in before_move_path_collection_entries:
                                    if complex_entry.id == "0":
                                        continue
                                    complex_path_builder /= complex_entry.name
                                    complex_folder_entry = client.folder(
                                        complex_entry.id
                                    ).get(fields=["modified_at"])
                                    r_c.set(
                                        local_or_box_file_m_time_key_func(
                                            complex_path_builder, True
                                        ),
                                        parse(complex_folder_entry.modified_at)
                                        .astimezone(tzutc())
                                        .timestamp(),
                                    )
                                    r_c.set(
                                        redis_path_for_object_id_key(
                                            complex_path_builder
                                        ),
                                        complex_folder_entry.object_id,
                                    )

                            return src_file_move_ret_value, partial(and_after)

                        queue_item = UploadQueueItemReal(
                            datetime.fromtimestamp(
                                Path(dest_event.pathname).stat().st_mtime
                            )
                            .astimezone(tzutc())
                            .timestamp(),
                            partial(complex_move),
                            self.oauth,
                            dest_event.pathname,
                        )

                        upload_pool_executor.apply_async(
                            upload_queue_processor, kwds=dict(queue_item=queue_item)
                        )
                        # src_file.move(cur_box_folder)
                        # do not yet support moving and renaming in one go
                        assert src_file["name"] == dest_event.name
            elif did_find_src_folder:
                src_folder = client.folder(folder_id=entry["id"]).get()
                if is_rename:
                    src_folder.rename(dest_event.name)
                else:
                    src_folder.move(cur_box_folder)
                    # do not yet support moving and renaming in one go
                    assert src_folder["name"] == dest_event.name
            elif entry["name"] == dest_event.name:
                if (
                    Path(src_event.path) == Path(dest_event.path)
                    and (
                        Path(src_event.name).stem == dest_event.name
                        or (
                            Path(src_event.name).suffix == ".tmp"
                            and Path(cast(str, dest_event.name)).suffix != ".tmp"
                        )
                    )
                    and src_event.name != dest_event.name
                ):
                    crate_logger.info(
                        f"Detected conceptual same directory temp file rename from "
                        f"{src_event.name=} to {dest_event.name=}"
                    )
                    cur_offset = 0
                    cur_entry: Union[boxsdk.object.file.File, boxsdk.object.file.Item]
                    for cur_entry in cur_box_folder.get_items(
                        offset=cur_offset, limit=limit
                    ):
                        matching_name = cur_entry["name"] == os.path.basename(
                            dest_event.pathname
                        )
                        if matching_name:
                            crate_logger.debug(f"{cur_entry=} {type(cur_entry)=}")

                        did_find_cur_file = (
                            is_file and matching_name and isinstance(cur_entry, File)
                        )
                        did_find_cur_folder = (
                            is_dir and matching_name and isinstance(cur_entry, Folder)
                        )
                        if did_find_cur_file:
                            queue_item = UploadQueueItemReal(
                                datetime.fromtimestamp(
                                    Path(dest_event.pathname).stat().st_mtime
                                )
                                .astimezone(tzutc())
                                .timestamp(),
                                partial(
                                    cast(File, cur_entry).update_contents,
                                    dest_event.pathname,
                                ),
                                self.oauth,
                                dest_event.pathname,
                            )

                            upload_pool_executor.apply_async(
                                upload_queue_processor, kwds=dict(queue_item=queue_item)
                            )
                            break
                        elif did_find_cur_folder:
                            crate_logger.debug(
                                "do not currently support moving a same name folder "
                                "into parent with"
                                "folder inside of the same name -- would may need "
                                "to update the contents"
                            )
                            break
                else:
                    move_from_remote = True
        if (
            not move_from_remote
        ):  # if it was moved from a different folder on remote, could be false still
            limit = 100
            offset = 0
            for entry in cur_box_folder.get_items(offset=offset, limit=limit):
                if entry["name"] == dest_event.name:
                    move_from_remote = True
                    break
            if not move_from_remote:
                if is_file and not did_find_src_file:
                    # src file [should] no longer exist[s].
                    # this file did not originate in box, too.
                    last_modified_time = Path(dest_event.pathname).stat().st_mtime
                    queue_item = UploadQueueItemReal(
                        datetime.fromtimestamp(last_modified_time)
                        .astimezone(tzutc())
                        .timestamp(),
                        partial(
                            cur_box_folder.upload,
                            dest_event.pathname,
                            os.path.basename(dest_event.pathname),
                            preflight_check=True,
                            upload_using_accelerator=True,
                        ),
                        self.oauth,
                        dest_event.pathname,
                    )

                    upload_pool_executor.apply_async(
                        upload_queue_processor, kwds=dict(queue_item=queue_item)
                    )
                elif is_dir and not did_find_src_folder:
                    queue_item: UploadQueueItem = partial(
                        cur_box_folder.create_subfolder,
                        os.path.basename(dest_event.pathname),
                    )

                    upload_pool_executor.apply_async(
                        upload_queue_processor, kwds=dict(queue_item=queue_item)
                    )
                    # wm.add_watch(dest_event.pathname, rec=True, mask=mask, auto_add=True)

    def process_delete_event(self, event: Event):
        crate_logger.debug(f"Doing a delete on {event=}")
        folders_to_traverse = self.folders_to_traverse(event.path)
        client = Client(self.oauth)
        box_folder = None
        loop_index = 0
        while not box_folder:
            try:
                box_folder = client.folder(folder_id="0").get()
            except Exception:
                crate_logger.warning("Error getting box root folder.", exc_info=True)
                time.sleep(min([pow(2, loop_index), 8]))
                loop_index += 1

        cur_box_folder = box_folder
        # if we're modifying in root box dir, then we've already found the folder
        try:
            is_base = any(
                Path(evp).relative_to(BOX_DIR) for evp in [event.path, event.path[:-1]]
            )
        except ValueError:
            is_base = False
        cur_box_folder = self.traverse_path(
            client, event, cur_box_folder, folders_to_traverse
        )
        last_dir = Path(event.path).parent.name
        if not is_base:
            AssertionError(
                cur_box_folder["name"] == last_dir,
                cur_box_folder["name"] + "not equals " + last_dir,
            )
        event_was_for_dir = "IN_ISDIR".lower() in event.maskname.lower()
        limit = 100
        offset = 0
        for entry in cur_box_folder.get_items(offset=offset, limit=limit):
            if (
                not event_was_for_dir
                and entry["type"] == "file"
                and entry["name"] == os.path.basename(event.pathname)
            ):
                if entry["id"] not in self.files_from_box:
                    cur_file = client.file(file_id=entry["id"]).get()
                    if (
                        cur_file.delete()
                    ):  # does not actually check correctly...unless not "ok" is false
                        # del version_info[cur_file['id']]
                        r_c.delete(redis_key(cur_file["id"]))
                else:
                    self.files_from_box.remove(
                        entry["id"]
                    )  # just wrote if, assuming create event didn't run
                break
            elif (
                event_was_for_dir
                and entry["type"] == "folder"
                and entry["name"] == os.path.basename(event.pathname)
            ):
                if entry["id"] not in self.folders_from_box:
                    self.get_folder(client, entry["id"]).delete()
                else:
                    self.folders_from_box.remove(
                        entry["id"]
                    )  # just wrote if, assuming create event didn't run
                break

    # noinspection PyPep8Naming
    def process_IN_CREATE(self, event: Event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        if not os.path.basename(event.pathname).startswith(
            ".~lock"
        ):  # avoid propagating lock files
            self.incoming_operations.put([event, "create"])

    # noinspection PyPep8Naming
    def process_IN_DELETE(self, event: Event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        crate_logger.debug(event)
        self.incoming_operations.put([event, "delete"])

    # noinspection PyPep8Naming
    def process_IN_MODIFY(self, event: Event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        if not os.path.basename(event.pathname).startswith(
            ".~lock"
        ):  # avoid propagating lock files
            self.incoming_operations.put([event, "modify"])

    # noinspection PyPep8Naming
    def process_IN_MOVED_FROM(self, event: Event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        crate_logger.debug(event)
        if not os.path.basename(event.pathname).startswith(
            ".~lock"
        ):  # avoid propagating lock files
            crate_logger.debug("Moved from: {}".format(event.pathname))
            self.move_events.append(event)

    # noinspection PyPep8Naming
    def process_IN_MOVED_TO(self, event: Event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        crate_logger.debug(event)
        found_from = False
        to_trash = trash_directory == Path(event.pathname).parent
        to_box = BOX_DIR in Path(event.pathname).parents
        move_event_processed = None
        for move_event in self.move_events:
            was_moved_from = "in_moved_from" in move_event.maskname.lower()
            if (
                move_event.cookie == event.cookie  # noqa
                and was_moved_from
                and BOX_DIR in Path(move_event.pathname).parents
            ):
                found_from = True
                # only count deletes that come from within the box path --
                # though this should always be the case
                if to_trash:
                    self.incoming_operations.put([move_event, "delete"])
                else:
                    self.incoming_operations.put([[move_event, event], "move"])
                move_event_processed = move_event
                break
        if move_event_processed:
            try:
                self.move_events.remove(move_event_processed)
            except ValueError:
                crate_logger.warning(
                    f"Found a matching src moved from {move_event_processed=}, "
                    f"but move_events no longer contained when we tried to remove, hmm.",
                    exc_info=True,
                )
        if not found_from and (not to_trash and to_box):
            self.incoming_operations.put(
                [event, "modify"]
            )  # "close"/"modify" seems appropriate
            # allow moving from a ~.lock file...I guess that may be okay
            crate_logger.debug("Moved to: {}".format(event.pathname))

    # noinspection PyPep8Naming,PyMethodMayBeStatic
    def process_IN_DELETE_SELF(self, event: Event):
        crate_logger.debug(event)

    # noinspection PyPep8Naming,PyMethodMayBeStatic
    def process_ALL_EVENTS(self, event: Event):
        crate_logger.debug(event)

    # noinspection PyPep8Naming
    def process_IN_CLOSE(self, event: Event):
        """
        Overrides the super.
        :param event:
        :return:
        """
        if not os.path.basename(event.pathname).startswith(
            ".~lock"
        ):  # avoid propagating lock files
            crate_logger.debug("Had a close on: {}".format(event))
            self.incoming_operations.put([event, "real_close"])
        else:
            crate_logger.debug(f"Skipping close on: {event.pathname=} due to `.~lock`")


def get_box_folder(
    client: Client,
    folder_id: str,
    retry_limit: int,
    app: Union[FastAPI, None] = None,
) -> Folder:
    """

    :param client:
    :param cur_box_folder:
    :param folder_id:
    :param retry_limit:
    :return:
    """
    cur_box_folder: Folder
    for i in range(retry_limit):
        try:
            box_folder_skeleton: Folder = typing.cast(
                Folder, client.folder(folder_id=folder_id)
            )
            box_folder: Folder = typing.cast(Folder, box_folder_skeleton.get())
            cur_box_folder = box_folder
            break
        except BoxAPIException as e:
            get_access_token(client.auth._access_token, app=app)
            if app:
                client._auth = app.oauth  # type: ignore
            if i == retry_limit - 1:
                crate_logger.info("Bad box api response.", exc_info=e)
                raise e
        except (
            ConnectionError,
            BrokenPipeError,
            ProtocolError,
            ConnectionResetError,
        ) as e:
            if i + 1 >= retry_limit:
                crate_logger.warning(
                    "Attempt ({retry_count}) out of ({max_count}); Going to give "
                    "up on the write event".format(
                        retry_count=i, max_count=retry_limit
                    ),
                    exc_info=True,
                )
                raise e
            else:
                crate_logger.warning(
                    "Attempt ({retry_count}) "
                    "out of ({max_count})".format(retry_count=i, max_count=retry_limit),
                    exc_info=True,
                )
    return cur_box_folder  # type: ignore[reportUnboundVariable]


def path_time_recurse_func(
    cur_path: Union[str, Path], inotify_wm: Optional[pyinotify.WatchManager] = None
) -> Dict[str, float]:
    cur_data_map = {
        Path(cur_path)
        .resolve()
        .as_posix(): datetime.fromtimestamp(os.path.getmtime(cur_path))
        .astimezone(tz.UTC)
        .timestamp()
    }
    cur_path = Path(cur_path)
    for sub_cur_path in cur_path.iterdir():
        if inotify_wm:
            pass
            # wm.add_watch(sub_cur_path.as_posix(), mask, rec=True, auto_add=True)
        if sub_cur_path.is_file():
            cur_data_map[sub_cur_path.resolve().as_posix()] = (
                datetime.fromtimestamp(sub_cur_path.stat().st_mtime)
                .astimezone(tzutc())
                .timestamp()
            )
        else:
            if inotify_wm:
                pass
                # wm.add_watch(sub_cur_path.as_posix(), mask, rec=True, auto_add=True)
            cur_data_map.update(
                **path_time_recurse_func(sub_cur_path, inotify_wm=inotify_wm)
            )
    return cur_data_map


wm = pyinotify.WatchManager()
in_delete = getattr(pyinotify, "IN_DELETE")
in_delete_self = getattr(pyinotify, "IN_DELETE_SELF")
in_move_self = getattr(pyinotify, "IN_MOVE_SELF")
in_create = getattr(pyinotify, "IN_CREATE")
in_modify = getattr(pyinotify, "IN_MODIFY")
in_close_write = getattr(pyinotify, "IN_CLOSE_WRITE")
in_moved_to = getattr(pyinotify, "IN_MOVED_TO")
in_moved_from = getattr(pyinotify, "IN_MOVED_FROM")
all_events = getattr(pyinotify, "ALL_EVENTS")
mask = (
    in_delete | in_modify | in_close_write | in_moved_to | in_moved_from
)  # watched events
