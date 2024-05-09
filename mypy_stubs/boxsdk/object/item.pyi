# from ..exception import BoxAPIException as BoxAPIException
# from ..pagination.marker_based_dict_collection import MarkerBasedDictCollection as MarkerBasedDictCollection
# from ..pagination.marker_based_object_collection import MarkerBasedObjectCollection as MarkerBasedObjectCollection
# from ..util.api_call_decorator import api_call as api_call
# from ..util.default_arg_value import SDK_VALUE_NOT_SET as SDK_VALUE_NOT_SET
from .base_item import BaseItem as BaseItem

# from .metadata import Metadata as Metadata
from _typeshed import Incomplete
from boxsdk.util.text_enum import TextEnum as TextEnum

class ClassificationType(TextEnum):
    PUBLIC: str
    INTERNAL: str
    CONFIDENTIAL: str
    NONE: str

class Item(BaseItem):
    name: str
    modified_at: str
    id: str
    object_id: str

    def update_info(self, data, etag: Incomplete | None = None): ...
    def get(
        self, fields: Incomplete | None = None, etag: Incomplete | None = None
    ) -> "Item": ...
    def create_shared_link(
        self,
        access: Incomplete | None = None,
        etag: Incomplete | None = None,
        unshared_at=...,
        allow_download: Incomplete | None = None,
        allow_preview: Incomplete | None = None,
        password: Incomplete | None = None,
        vanity_name: Incomplete | None = None,
        **kwargs
    ): ...
    def get_shared_link(
        self,
        access: Incomplete | None = None,
        etag: Incomplete | None = None,
        unshared_at=...,
        allow_download: Incomplete | None = None,
        allow_preview: Incomplete | None = None,
        password: Incomplete | None = None,
        vanity_name: Incomplete | None = None,
        **kwargs
    ): ...
    def remove_shared_link(self, etag: Incomplete | None = None, **kwargs): ...
    def delete(
        self, params: Incomplete | None = None, etag: Incomplete | None = None
    ): ...
    def metadata(self, scope: str = "global", template: str = "properties"): ...
    def get_all_metadata(self): ...
    def get_watermark(self): ...
    def apply_watermark(self): ...
    def delete_watermark(self): ...
    def collaborate(
        self,
        accessible_by,
        role,
        can_view_path: Incomplete | None = None,
        notify: Incomplete | None = None,
        fields: Incomplete | None = None,
    ): ...
    def collaborate_with_login(
        self,
        login,
        role,
        can_view_path: Incomplete | None = None,
        notify: Incomplete | None = None,
        fields: Incomplete | None = None,
    ): ...
    def get_collaborations(
        self,
        limit: Incomplete | None = None,
        marker: Incomplete | None = None,
        fields: Incomplete | None = None,
    ): ...
    def add_classification(self, classification): ...
    def update_classification(self, classification): ...
    def set_classification(self, classification): ...
    def get_classification(self): ...
    def remove_classification(self): ...
