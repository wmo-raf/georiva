"""
GeoRiva Storage Manager

Unified interface for file storage operations.
Supports local filesystem, S3, MinIO
"""

import logging
from typing import BinaryIO, Optional

from django.core.files.storage import storages

logger = logging.getLogger(__name__)


class StorageManager:
    """
    Unified storage interface for GeoRiva.
    
    Provides consistent methods for file operations across different
    storage backends (local, S3/MinIO, GCS).
    """
    
    def __init__(self, storage_name: str = 'georiva'):
        """
        Initialize with a Django storage backend.
        
        Args:
            storage_name: Name of the storage in Django STORAGES setting
        """
        self.storage_name = storage_name
        self._storage = None
    
    @property
    def storage(self):
        """Get the Django storage backend."""
        if self._storage is None:
            self._storage = storages[self.storage_name]
        return self._storage
    
    @property
    def is_s3(self) -> bool:
        """Check if using S3-compatible storage."""
        return hasattr(self.storage, 'bucket')
    
    @property
    def is_local(self) -> bool:
        """Check if using local filesystem."""
        return hasattr(self.storage, 'location')
    
    # =========================================================================
    # Core file operations
    # =========================================================================
    
    def exists(self, path: str) -> bool:
        """Check if a file exists."""
        return self.storage.exists(path)
    
    def save(self, path: str, content: BinaryIO) -> str:
        """
        Save content to a file.
        
        Args:
            path: Destination path
            content: File-like object with content
        
        Returns:
            The actual path where the file was saved
        """
        from django.core.files.base import ContentFile
        
        # Read content if it's a file-like object
        if hasattr(content, 'read'):
            data = content.read()
            content = ContentFile(data)
        
        return self.storage.save(path, content)
    
    def save_bytes(self, path: str, data: bytes) -> str:
        """
        Save bytes to a file.
        
        Args:
            path: Destination path
            data: Bytes to save
        
        Returns:
            The actual path where the file was saved
        """
        from django.core.files.base import ContentFile
        return self.storage.save(path, ContentFile(data))
    
    def read_bytes(self, path: str) -> bytes:
        """Read a file as bytes."""
        with self.storage.open(path, 'rb') as f:
            return f.read()
    
    def open(self, path: str, mode: str = 'rb') -> BinaryIO:
        """Open a file and return a file-like object."""
        return self.storage.open(path, mode)
    
    def delete(self, path: str) -> bool:
        """
        Delete a file.
        
        Returns:
            True if deleted, False if didn't exist
        """
        if self.exists(path):
            self.storage.delete(path)
            return True
        return False
    
    def url(self, path: str) -> str:
        """Get URL for a file."""
        return self.storage.url(path)
    
    def size(self, path: str) -> int:
        """Get file size in bytes."""
        return self.storage.size(path)
    
    def modified_time(self, path: str):
        """Get file modification time."""
        return self.storage.get_modified_time(path)
    
    # =========================================================================
    # Directory operations
    # =========================================================================
    
    def list_files(self, path: str = '', recursive: bool = False) -> list[dict]:
        """
        List files in a directory.
        
        Args:
            path: Directory path
            recursive: Include subdirectories
        
        Returns:
            List of dicts with file info: {'path': ..., 'size': ..., 'modified': ...}
        """
        files = []
        
        try:
            dirs, filenames = self.storage.listdir(path)
            
            for filename in filenames:
                file_path = f"{path}/{filename}" if path else filename
                try:
                    files.append({
                        'path': file_path,
                        'size': self.storage.size(file_path),
                        'modified': self.storage.get_modified_time(file_path),
                    })
                except Exception as e:
                    logger.warning(f"Could not get info for {file_path}: {e}")
                    files.append({'path': file_path})
            
            if recursive:
                for dir_name in dirs:
                    dir_path = f"{path}/{dir_name}" if path else dir_name
                    files.extend(self.list_files(dir_path, recursive=True))
        
        except Exception as e:
            logger.error(f"Failed to list files in {path}: {e}")
        
        return files
    
    def list_directories(self, path: str = '') -> list[str]:
        """List subdirectories in a directory."""
        try:
            dirs, _ = self.storage.listdir(path)
            return [f"{path}/{d}" if path else d for d in dirs]
        except Exception as e:
            logger.error(f"Failed to list directories in {path}: {e}")
            return []
    
    # =========================================================================
    # S3-specific operations
    # =========================================================================
    
    def get_presigned_url(
            self,
            path: str,
            expiration: int = 3600,
            method: str = 'get_object'
    ) -> Optional[str]:
        """
        Generate a presigned URL for S3/MinIO.
        
        Args:
            path: File path
            expiration: URL expiration in seconds
            method: 'get_object' for download, 'put_object' for upload
        
        Returns:
            Presigned URL or None if not S3
        """
        if not self.is_s3:
            return None
        
        try:
            client = self.storage.connection.meta.client
            
            return client.generate_presigned_url(
                method,
                Params={
                    'Bucket': self.storage.bucket_name,
                    'Key': path,
                },
                ExpiresIn=expiration,
            )
        except Exception as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return None
    
    def copy(self, src_path: str, dest_path: str) -> str:
        """
        Copy a file within storage.
        
        For S3, uses server-side copy (efficient for large files).
        """
        if self.is_s3:
            try:
                bucket = self.storage.bucket
                bucket.copy(
                    {'Bucket': self.storage.bucket_name, 'Key': src_path},
                    dest_path
                )
                return dest_path
            except Exception as e:
                logger.error(f"S3 copy failed: {e}")
                # Fall back to read/write
        
        # Non-S3 or fallback: read and write
        content = self.read_bytes(src_path)
        return self.save_bytes(dest_path, content)
    
    def move(self, src_path: str, dest_path: str) -> str:
        """Move a file (copy then delete source)."""
        saved_path = self.copy(src_path, dest_path)
        self.delete(src_path)
        return saved_path


# Singleton instance
storage_manager = StorageManager()
