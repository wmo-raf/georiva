"""
GeoRiva Loaders

Automated data loading from various external sources.
"""

from .base import BaseLoader, FetchResult, LoaderRunResult, RemoteFile
from .ftp import FTPLoader
from .http import HTTPLoader, THREDDSLoader
from .s3 import GCSLoader, S3Loader

__all__ = [
    # Base
    'BaseLoader',
    'RemoteFile',
    'FetchResult',
    'LoaderRunResult',
    # Implementations
    'FTPLoader',
    'HTTPLoader',
    'THREDDSLoader',
    'S3Loader',
    'GCSLoader',
    # Registry
    'get_loader_for_config',
]


def get_loader_for_config(config, collection) -> BaseLoader:
    """
    Factory function to get the appropriate loader for a config.
    
    Args:
        config: A LoaderConfig subclass instance
        collection: The collection to which data will be loaded
        
    Returns:
        Instantiated loader for the config
    """
    # Import here to avoid circular imports
    from georiva.loaders.models import (
        FTPLoaderConfig,
        HTTPLoaderConfig,
        S3LoaderConfig,
    )
    
    loader_map = {
        FTPLoaderConfig: FTPLoader,
        HTTPLoaderConfig: HTTPLoader,
        S3LoaderConfig: S3Loader,
    }
    
    config_class = config.__class__
    
    # Handle polymorphic - get real class
    if hasattr(config, 'get_real_instance'):
        config = config.get_real_instance()
        config_class = config.__class__
    
    loader_class = loader_map.get(config_class)
    
    if loader_class is None:
        raise ValueError(f"No loader registered for {config_class.__name__}")
    
    return loader_class(config, collection)
