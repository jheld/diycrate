import threading

from boxsdk import OAuth2
import fastapi


class FastAPI(fastapi.FastAPI):
    oauth: OAuth2
    csrf_token: str
    started_cloud_threads: bool = False
    processing_oauth_browser: bool = False
    processing_oauth_browser_lock: threading.Lock
    processing_oauth_refresh_lock: threading.Lock
