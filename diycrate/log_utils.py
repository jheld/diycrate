from logging.config import dictConfig

from .logging_config import LOGGING


def setup_logger(config=None):
    if config is None:
        config = LOGGING
    dictConfig(config)
