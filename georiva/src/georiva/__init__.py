from georiva.config.celery import app as celery_app

from .utils.version import get_semver_version, get_version
from .version import VERSION

__version__ = get_version(VERSION)

__semver__ = get_semver_version(VERSION)

__all__ = ["celery_app", "__version__", "__semver__"]
