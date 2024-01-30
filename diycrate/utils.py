from io import BufferedWriter
import threading
from typing import Iterable, List, Optional, Tuple

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
        ...


class Item(boxsdk.object.item.Item):
    name: str
    modified_at: str
    id: str
    object_id: str

    def get(
        self, fields: Optional[List[str]] = None, etag: Optional[str] = None
    ) -> "Item":
        ...


class Folder(boxsdk.object.folder.Folder):
    modified_at: str
    name: str
    id: str
    object_id: str

    def create_subfolder(self, folder_name: str) -> "Folder":
        ...

    def get(
        self, fields: Optional[List[str]] = None, etag: Optional[str] = None
    ) -> "Folder":
        ...

    def get_items(
        self, offset: Optional[int] = None, limit: Optional[int] = None
    ) -> Iterable[Item]:
        ...

    def delete(self) -> bool:
        ...


class File(boxsdk.object.file.File):
    modified_at: str
    path_collection: dict[str, List[Folder]]
    name: str
    id: str
    object_id: str

    def get(
        self, fields: Optional[List[str]] = None, etag: Optional[str] = None
    ) -> "File":
        ...

    def download_to(
        self,
        writeable_stream: BufferedWriter,
        file_version: Optional[FileVersion] = None,
        byte_range: Optional[Tuple[int, int]] = None,
    ) -> None:
        ...

    def delete(self) -> bool:
        ...

    def update_contents(self, **kwargs):
        ...


class Client(boxsdk.Client):
    def user(self) -> User:
        ...

    def folder(self, folder_id: str) -> Folder:
        ...

    def file(self, file_id: str) -> File:
        ...

    @property
    def auth(self) -> OAuth2:
        ...
