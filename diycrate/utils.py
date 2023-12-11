import threading
from typing import Any

import bottle
from boxsdk import OAuth2
import fastapi


class DictProperty(bottle.DictProperty):
    def __getitem__(self: "DictProperty", key: Any) -> Any:
        pass

    def get(self: "DictProperty", key: Any) -> Any:
        pass

    def __getattribute__(self, __name: str) -> Any:
        pass


class LocalRequest(bottle.LocalRequest):
    query: DictProperty
    GET: DictProperty
    POST: DictProperty


class Bottle(bottle.Bottle):
    oauth: OAuth2
    csrf_token: str
    started_cloud_threads: bool = False
    processing_oauth_browser: bool = False
    processing_oauth_browser_lock: threading.Lock
    processing_oauth_refresh_lock: threading.Lock


class FastAPI(fastapi.FastAPI):
    oauth: OAuth2
    csrf_token: str
    started_cloud_threads: bool = False
    processing_oauth_browser: bool = False
    processing_oauth_browser_lock: threading.Lock
    processing_oauth_refresh_lock: threading.Lock
