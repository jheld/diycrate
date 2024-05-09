from ..config import API as API
from ..exception import (
    BoxAPIException as BoxAPIException,
    BoxOAuthException as BoxOAuthException,
)
from ..object.base_api_json_object import BaseAPIJSONObject as BaseAPIJSONObject
from ..session.session import Session as Session
from ..util.json import is_json_response as is_json_response
from ..util.text_enum import TextEnum as TextEnum
from _typeshed import Incomplete
from collections.abc import Generator

class TokenScope(TextEnum):
    ITEM_READ: str
    ITEM_READWRITE: str
    ITEM_PREVIEW: str
    ITEM_UPLOAD: str
    ITEM_SHARE: str
    ITEM_DELETE: str
    ITEM_DOWNLOAD: str

class TokenResponse(BaseAPIJSONObject): ...

class OAuth2:
    def __init__(
        self,
        client_id,
        client_secret,
        store_tokens: Incomplete | None = None,
        box_device_id: str = "0",
        box_device_name: str = "",
        access_token: Incomplete | None = None,
        refresh_token: Incomplete | None = None,
        session: Incomplete | None = None,
        refresh_lock: Incomplete | None = None,
    ) -> None: ...
    @property
    def access_token(self): ...
    @property
    def closed(self): ...
    @property
    def api_config(self): ...
    def get_authorization_url(self, redirect_url): ...
    def authenticate(self, auth_code): ...
    def refresh(self, access_token_to_refresh): ...
    def send_token_request(
        self, data, access_token, expect_refresh_token: bool = True
    ): ...
    def revoke(self) -> None: ...
    def close(self, revoke: bool = True) -> None: ...
    def closing(self, **close_kwargs) -> Generator[Incomplete, None, None]: ...
