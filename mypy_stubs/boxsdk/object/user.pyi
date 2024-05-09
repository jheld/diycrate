# from ..pagination.limit_offset_based_object_collection import LimitOffsetBasedObjectCollection as LimitOffsetBasedObjectCollection
# from ..pagination.marker_based_object_collection import MarkerBasedObjectCollection as MarkerBasedObjectCollection
# from ..util.api_call_decorator import api_call as api_call
from .base_object import BaseObject as BaseObject
from _typeshed import Incomplete

class User(BaseObject):
    space_used: float
    space_amount: float
    def add_email_alias(self, email): ...
    def get_email_aliases(
        self, limit: Incomplete | None = None, fields: Incomplete | None = None
    ): ...
    def remove_email_alias(self, email_alias): ...
    def transfer_content(
        self,
        destination_user,
        notify: Incomplete | None = None,
        fields: Incomplete | None = None,
    ): ...
    def get(
        self, fields: Incomplete | None = None, headers: Incomplete | None = None
    ) -> "User": ...
    def get_storage_policy_assignment(self): ...
    def get_group_memberships(
        self,
        limit: Incomplete | None = None,
        offset: Incomplete | None = None,
        fields: Incomplete | None = None,
    ): ...
    def get_avatar(self): ...
    def delete(self, notify: bool = True, force: bool = False): ...
