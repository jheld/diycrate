import os

LOG_LEVEL = os.environ.get("DIY_CRATE_LOGGING_LEVEL")
LOG_TO_CONSOLE = os.environ.get("DIY_CRATE_LOGGING_CONSOLE")

if not LOG_LEVEL:
    LOG_LEVEL = "INFO"

try:
    from systemd import journal
except ImportError:
    journal = None


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
            "level": LOG_LEVEL,
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        }
    },
    "loggers": {
        "diycrate": {
            "handlers": ["console"],
            "propagate": True,
            "level": LOG_LEVEL,
        },
        "diycrate_app": {
            "handlers": ["console"],
            "propagate": True,
            "level": LOG_LEVEL,
        },
        "__main__": {
            "handlers": ["console"],
            "propagate": True,
            "level": LOG_LEVEL,
        },
    },
}
#
if journal:
    LOGGING["handlers"]["journal"] = {
        "level": LOG_LEVEL,
        "class": "systemd.journal.JournalHandler",
        "formatter": "verbose",
    }

    LOGGING["loggers"]["root"] = {
        "handlers": ["journal"],
    }
    if not LOG_TO_CONSOLE:
        LOGGING["loggers"]["diycrate_app"]["handlers"] = []
        LOGGING["loggers"]["__main__"]["handlers"] = []
        LOGGING["loggers"]["diycrate"]["handlers"] = []
