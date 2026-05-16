import logging
import logging.config
import os


def configure_logging() -> None:
    log_level = os.getenv("TTL_LOG_LEVEL", "info").upper()
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "console": {
                    "format": "%(levelname)s %(asctime)s %(name)s.%(funcName)s:%(lineno)s- %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "console",
                }
            },
            "loggers": {
                "uvicorn.access": {"level": "WARNING"},
                "botocore": {"level": "WARNING"},
                "httpx": {"level": "WARNING"},
            },
            "root": {
                "handlers": ["console"],
                "level": log_level,
            },
        }
    )
