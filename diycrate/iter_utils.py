import logging
import time
from typing import Union

from boxsdk import BoxAPIException
from boxsdk.object.file import File
from boxsdk.object.folder import Folder

from .log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)


class SafeIter:
    def __init__(self, iterable, path=None, tries=5):
        self.iterable = iterable
        self.path = path
        self.tries = tries or 5

    def __iter__(self):
        return self

    def __next__(self) -> Union[File, Folder, None]:
        tries = self.tries
        buffered_exc = None
        for _ in range(tries):
            try:
                value = next(self.iterable)
            except BoxAPIException as e:
                buffered_exc = e
                crate_logger.warning(
                    "Box API exception in folder: {path}".format(path=self.path),
                    exc_info=True,
                )
                time.sleep(5)
            else:
                break
        else:
            crate_logger.warning(
                "Could not recover from Box API issues after {tries} "
                "tries on folder: {path}".format(tries=tries, path=self.path),
                exc_info=buffered_exc,
            )
            value = None
        return value
