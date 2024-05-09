# from ..pagination.limit_offset_based_object_collection import LimitOffsetBasedObjectCollection as LimitOffsetBasedObjectCollection
# from ..pagination.marker_based_object_collection import MarkerBasedObjectCollection as MarkerBasedObjectCollection
# from ..util.api_call_decorator import api_call as api_call
# from ..util.deprecation_decorator import deprecated as deprecated
from typing import List
from .item import Item as Item
from .folder import Folder as Folder
from _typeshed import Incomplete

class File(Item):
    path_collection: dict[str, List[Folder]]
    def preflight_check(self, size, name: Incomplete | None = None): ...
    def create_upload_session(self, file_size, file_name: Incomplete | None = None): ...
    def get_chunked_uploader(self, file_path, rename_file: bool = False): ...
    def content(
        self,
        file_version: Incomplete | None = None,
        byte_range: Incomplete | None = None,
    ): ...
    def download_to(
        self,
        writeable_stream,
        file_version: Incomplete | None = None,
        byte_range: Incomplete | None = None,
    ) -> None: ...
    def get(
        self, fields: Incomplete | None = None, etag: Incomplete | None = None
    ) -> "File": ...
    def get_download_url(self, file_version: Incomplete | None = None): ...
    def update_contents_with_stream(
        self,
        file_stream,
        etag: Incomplete | None = None,
        preflight_check: bool = False,
        preflight_expected_size: int = 0,
        upload_using_accelerator: bool = False,
        file_name: Incomplete | None = None,
        content_modified_at: Incomplete | None = None,
        additional_attributes: Incomplete | None = None,
        sha1: Incomplete | None = None,
    ): ...
    def update_contents(
        self,
        file_path,
        etag: Incomplete | None = None,
        preflight_check: bool = False,
        preflight_expected_size: int = 0,
        upload_using_accelerator: bool = False,
        file_name: Incomplete | None = None,
        content_modified_at: Incomplete | None = None,
        additional_attributes: Incomplete | None = None,
        sha1: Incomplete | None = None,
    ): ...
    def lock(
        self, prevent_download: bool = False, expire_time: Incomplete | None = None
    ): ...
    def unlock(self): ...
    def get_shared_link_download_url(
        self,
        access: Incomplete | None = None,
        etag: Incomplete | None = None,
        unshared_at: Incomplete | None = None,
        allow_preview: Incomplete | None = None,
        password: Incomplete | None = None,
        vanity_name: Incomplete | None = None,
    ): ...
    def get_comments(
        self,
        limit: Incomplete | None = None,
        offset: int = 0,
        fields: Incomplete | None = None,
    ): ...
    def add_comment(self, message): ...
    def create_task(
        self,
        message: Incomplete | None = None,
        due_at: Incomplete | None = None,
        action: str = "review",
        completion_rule: Incomplete | None = None,
    ): ...
    def get_tasks(self, fields: Incomplete | None = None): ...
    def get_previous_versions(
        self,
        limit: Incomplete | None = None,
        offset: Incomplete | None = None,
        fields: Incomplete | None = None,
    ): ...
    def promote_version(self, file_version): ...
    def delete_version(self, file_version, etag: Incomplete | None = None): ...
    def get_embed_url(self): ...
    def get_representation_info(self, rep_hints: Incomplete | None = None): ...
    def get_thumbnail(
        self,
        extension: str = "png",
        min_width: Incomplete | None = None,
        min_height: Incomplete | None = None,
        max_width: Incomplete | None = None,
        max_height: Incomplete | None = None,
    ): ...
    def get_thumbnail_representation(self, dimensions, extension: str = "png"): ...
    def copy(
        self,
        parent_folder,
        name: Incomplete | None = None,
        file_version: Incomplete | None = None,
    ): ...
