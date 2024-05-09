from _typeshed import Incomplete
from typing import Iterable
from boxsdk.object.group import Group as Group
from boxsdk.object.item import Item as Item
from boxsdk.object.user import User as User
from boxsdk.pagination.limit_offset_based_object_collection import (
    LimitOffsetBasedObjectCollection as LimitOffsetBasedObjectCollection,
)
from boxsdk.pagination.marker_based_object_collection import (
    MarkerBasedObjectCollection as MarkerBasedObjectCollection,
)
from boxsdk.util.api_call_decorator import api_call as api_call
from boxsdk.util.text_enum import TextEnum as TextEnum

class FolderSyncState(TextEnum):
    IS_SYNCED: str
    NOT_SYNCED: str
    PARTIALLY_SYNCED: str

class _CollaborationType(TextEnum):
    USER: str
    GROUP: str

class _Collaborator:
    def __init__(self, collaborator) -> None: ...
    @property
    def access(self): ...
    @property
    def type(self): ...

class Folder(Item):
    def preflight_check(self, size, name): ...
    def create_upload_session(self, file_size, file_name): ...
    def get_chunked_uploader(self, file_path): ...
    def get(
        self, fields: Incomplete | None = None, etag: Incomplete | None = None
    ) -> "Folder": ...
    def get_items(
        self,
        limit: Incomplete | None = None,
        offset: int = 0,
        marker: Incomplete | None = None,
        use_marker: bool = False,
        sort: Incomplete | None = None,
        direction: Incomplete | None = None,
        fields: Incomplete | None = None,
    ) -> Iterable[Item]: ...
    def upload_stream(
        self,
        file_stream,
        file_name,
        file_description: Incomplete | None = None,
        preflight_check: bool = False,
        preflight_expected_size: int = 0,
        upload_using_accelerator: bool = False,
        content_created_at: Incomplete | None = None,
        content_modified_at: Incomplete | None = None,
        additional_attributes: Incomplete | None = None,
        sha1: Incomplete | None = None,
        etag: Incomplete | None = None,
    ): ...
    def upload(
        self,
        file_path: Incomplete | None = None,
        file_name: Incomplete | None = None,
        file_description: Incomplete | None = None,
        preflight_check: bool = False,
        preflight_expected_size: int = 0,
        upload_using_accelerator: bool = False,
        content_created_at: Incomplete | None = None,
        content_modified_at: Incomplete | None = None,
        additional_attributes: Incomplete | None = None,
        sha1: Incomplete | None = None,
        etag: Incomplete | None = None,
    ): ...
    def create_subfolder(self, name): ...
    def update_sync_state(self, sync_state): ...
    def add_collaborator(
        self, collaborator, role, notify: bool = False, can_view_path: bool = False
    ): ...
    def create_web_link(
        self,
        target_url,
        name: Incomplete | None = None,
        description: Incomplete | None = None,
    ): ...
    def delete(self, recursive: bool = True, etag: Incomplete | None = None): ...
    def get_metadata_cascade_policies(
        self,
        owner_enterprise: Incomplete | None = None,
        limit: Incomplete | None = None,
        marker: Incomplete | None = None,
        fields: Incomplete | None = None,
    ): ...
    def cascade_metadata(self, metadata_template): ...
    def create_lock(self): ...
    def get_locks(self): ...
