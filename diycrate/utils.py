from io import BufferedWriter
import threading
from typing import Iterable, List, Optional, Tuple
import typing

from boxsdk import OAuth2
from boxsdk.object.file_version import FileVersion
import fastapi
import boxsdk


class FastAPI(fastapi.FastAPI):
    oauth: OAuth2
    csrf_token: str
    started_cloud_threads: bool = False
    processing_oauth_browser: bool = False
    processing_oauth_browser_lock: threading.Lock
    processing_oauth_refresh_lock: threading.Lock


class User(boxsdk.object.user.User):
    space_used: float
    space_amount: float

    def get(self, fields: Optional[List[str]] = None) -> "User":
        ret = typing.cast("User", super().get(fields=fields))  # type: ignore
        return ret


class Item(boxsdk.object.item.Item):
    name: str
    modified_at: str
    id: str
    object_id: str

    def get(
        self, fields: Optional[List[str]] = None, etag: Optional[str] = None
    ) -> "Item":
        ret = typing.cast("Item", super().get(fields=fields, etag=etag))  # type: ignore
        return ret


class Folder(boxsdk.object.folder.Folder):
    modified_at: str
    name: str
    id: str
    object_id: str

    def create_subfolder(self, folder_name: str) -> "Folder":
        ret = typing.cast("Folder", super().create_subfolder(folder_name))  # type: ignore
        return ret

    def get(
        self, fields: Optional[List[str]] = None, etag: Optional[str] = None
    ) -> "Folder":
        ret = typing.cast(Folder, super().get(fields=fields, etag=etag))  # type: ignore
        return ret

    def get_items(
        self, offset: Optional[int] = None, limit: Optional[int] = None
    ) -> Iterable[Item]:
        items_kwargs = dict(offset=offset, limit=limit)
        ret = typing.cast(Iterable[Item], super().get_items(**items_kwargs))  # type: ignore
        return ret

    def delete(self) -> bool:
        ret = typing.cast(bool, super().delete())  # type: ignore
        return ret


class File(boxsdk.object.file.File):
    modified_at: str
    path_collection: dict[str, List[Folder]]
    name: str
    id: str
    object_id: str

    def get(
        self, fields: Optional[List[str]] = None, etag: Optional[str] = None
    ) -> "File":
        ret = typing.cast(File, super().get(fields=fields, etag=etag))  # type: ignore
        return ret

    def download_to(
        self,
        writeable_stream: BufferedWriter,
        file_version: Optional[FileVersion] = None,
        byte_range: Optional[Tuple[int, int]] = None,
    ) -> None:
        func_kwargs = dict(
            writeable_stream=writeable_stream,
            file_version=file_version,
            byte_range=byte_range,
        )
        ret = typing.cast(None, super().download_to(**func_kwargs))  # type: ignore
        return ret

    def delete(self) -> bool:
        ret = typing.cast(bool, super().delete())  # type: ignore
        return ret

    def update_contents(self, **kwargs):
        ret = typing.cast(None, super().update_contents(**kwargs))  # type: ignore
        return ret


class Client(boxsdk.Client):
    def user(self) -> User:
        ret = typing.cast(User, super().user())
        return ret

    def folder(self, folder_id: str) -> Folder:
        ret = typing.cast(Folder, super().folder(folder_id))
        return ret

    def file(self, file_id: str) -> File:
        ret = typing.cast(File, super().folder(file_id))
        return ret

    @property
    def auth(self) -> OAuth2:
        ret = typing.cast(OAuth2, super().auth)
        return ret
