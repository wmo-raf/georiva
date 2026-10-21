from .base import BaseFetchStrategy, FailureScope, FetchMode, FetchResult, FileRequest
from .ftp import FTPFetchStrategy
from .http import HTTPFetchStrategy

__all__ = [
    "FileRequest",
    "FailureScope",
    "FetchMode",
    "FetchResult",
    "BaseFetchStrategy",
    "HTTPFetchStrategy",
    "FTPFetchStrategy",
]
