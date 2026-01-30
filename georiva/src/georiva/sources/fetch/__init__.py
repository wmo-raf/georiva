from .base import FileRequest, FetchMode, FetchResult, BaseFetchStrategy
from .ftp import FTPFetchStrategy
from .http import HTTPFetchStrategy

__all__ = [
    'FileRequest',
    'FetchMode',
    'FetchResult',
    'BaseFetchStrategy',
    'HTTPFetchStrategy',
    'FTPFetchStrategy',
]
