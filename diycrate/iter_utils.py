import logging
import time

from boxsdk import BoxAPIException

from .log_utils import setup_logger

setup_logger()

crate_logger = logging.getLogger(__name__)


class SafeIter:
    def __init__(self, iterable, path=None):
        self.iterable = iterable
        self.path = path

    def __iter__(self):
        return self

    def __next__(self):
        tries = 5
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
