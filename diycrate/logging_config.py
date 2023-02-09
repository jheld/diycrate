import os

LOG_LEVEL = os.environ.get("DIY_CRATE_LOGGING_LEVEL")
if not LOG_LEVEL:
    LOG_LEVEL = "INFO"
LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "verbose": {
            "format": "%(levelname)s %(asctime)s %(module)s:%(lineno)d "
            "%(process)d %(thread)d %(message)s"
        },
        "simple": {"format": "%(levelname)s %(message)s"},
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        }
    },
    "loggers": {
        "diycrate": {"handlers": ["console"], "propagate": True, "level": LOG_LEVEL},
        "diycrate_app": {
            "handlers": ["console"],
            "propagate": True,
            "level": LOG_LEVEL,
        },
        "__main__": {"handlers": ["console"], "propagate": True, "level": LOG_LEVEL},
    },
}
