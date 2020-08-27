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
        "diycrate": {"handlers": ["console"], "propagate": True, "level": "INFO"},
        "diycrate_app": {"handlers": ["console"], "propagate": True, "level": "INFO"},
        "__main__": {"handlers": ["console"], "propagate": True, "level": "INFO"},
    },
}
