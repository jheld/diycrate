from ..exception import BoxValueError as BoxValueError
from ..util.api_call_decorator import api_call as api_call
from ..util.default_arg_value import SDK_VALUE_NOT_SET as SDK_VALUE_NOT_SET
from .base_object import BaseObject as BaseObject
from _typeshed import Incomplete

class BaseItem(BaseObject):
    def copy(self, parent_folder, name: Incomplete | None = None): ...
    def move(self, parent_folder, name: Incomplete | None = None): ...
    def rename(self, name): ...
    def create_shared_link(self, **kwargs): ...
    def get_shared_link(self, **kwargs): ...
    def remove_shared_link(self, **kwargs): ...
    def add_to_collection(self, collection): ...
    def remove_from_collection(self, collection): ...
    @staticmethod
    def validate_item_id(item_id) -> None: ...