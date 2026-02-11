"""
GeoRiva Storage Manager

Multi-bucket storage interface for GeoRiva's data pipeline.

Bucket layout:
    georiva-incoming/   User-uploaded raw data      {catalog}/{collection}/file.ext
    georiva-sources/    Plugin-collected data        {catalog}/{collection}/file.ext
    georiva-archive/    Raw copy before processing   {incoming|sources}/{catalog}/{collection}/file.ext
    georiva-assets/     Final processed datasets     {catalog}/{collection}/{variable}/{year}/{month}/{day}/file.ext

Flow:
    incoming/sources → process → assets
                     └→ archive (raw copy)
"""

import logging
from datetime import datetime
from typing import BinaryIO, Optional

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import storages

logger = logging.getLogger(__name__)


# =============================================================================
# Bucket Registry
# =============================================================================

class BucketType:
    """Known bucket types in GeoRiva."""
    
    INCOMING = "incoming"
    SOURCES = "sources"
    ARCHIVE = "archive"
    ASSETS = "assets"
    
    ALL = [INCOMING, SOURCES, ARCHIVE, ASSETS]


def get_bucket_config() -> dict[str, str]:
    """
    Get bucket name mapping from settings.

    Falls back to defaults if not configured.
    """
    return getattr(settings, "GEORIVA_BUCKETS", {
        BucketType.INCOMING: "georiva-incoming",
        BucketType.SOURCES: "georiva-sources",
        BucketType.ARCHIVE: "georiva-archive",
        BucketType.ASSETS: "georiva-assets",
    })


# =============================================================================
# Single Bucket Handle
# =============================================================================

class Bucket:
    """
    Interface to a single storage bucket.

    Wraps a Django storage backend and provides file operations
    scoped to one bucket.
    """
    
    def __init__(self, bucket_type: str, storage_name: str):
        self.bucket_type = bucket_type
        self.storage_name = storage_name
        self._storage = None
    
    def __repr__(self):
        return f"Bucket({self.bucket_type!r}, storage={self.storage_name!r})"
    
    @property
    def storage(self):
        """Lazy-load the Django storage backend."""
        if self._storage is None:
            self._storage = storages[self.storage_name]
        return self._storage
    
    @property
    def is_s3(self) -> bool:
        return hasattr(self.storage, "bucket")
    
    @property
    def is_local(self) -> bool:
        return hasattr(self.storage, "location")
    
    @property
    def bucket_name(self) -> str:
        """Return the S3 bucket name, or the local storage root."""
        if self.is_s3:
            return self.storage.bucket_name
        return getattr(self.storage, "location", self.storage_name)
    
    # ---- Core file operations -----------------------------------------------
    
    def exists(self, path: str) -> bool:
        return self.storage.exists(path)
    
    def save(self, path: str, content) -> str:
        """
        Save content to a path in this bucket.

        Args:
            path: Destination path relative to bucket root.
            content: bytes, file-like object, or Django File.

        Returns:
            The actual saved path.
        """
        if isinstance(content, bytes):
            content = ContentFile(content)
        elif hasattr(content, "read"):
            content = ContentFile(content.read())
        
        return self.storage.save(path, content)
    
    def read_bytes(self, path: str) -> bytes:
        with self.storage.open(path, "rb") as f:
            return f.read()
    
    def open(self, path: str, mode: str = "rb") -> BinaryIO:
        return self.storage.open(path, mode)
    
    def delete(self, path: str) -> bool:
        """Delete a file. Returns True if it existed."""
        if self.exists(path):
            self.storage.delete(path)
            return True
        return False
    
    def url(self, path: str) -> str:
        return self.storage.url(path)
    
    def size(self, path: str) -> int:
        return self.storage.size(path)
    
    def modified_time(self, path: str):
        return self.storage.get_modified_time(path)
    
    # ---- Listing ------------------------------------------------------------
    
    def list_files(self, path: str = "", recursive: bool = False) -> list[dict]:
        """
        List files under a path.

        Returns:
            List of dicts: {'path', 'size', 'modified'}
        """
        files = []
        
        try:
            dirs, filenames = self.storage.listdir(path)
            
            for filename in filenames:
                file_path = f"{path}/{filename}" if path else filename
                try:
                    files.append({
                        "path": file_path,
                        "size": self.storage.size(file_path),
                        "modified": self.storage.get_modified_time(file_path),
                    })
                except Exception as e:
                    logger.warning("Could not get info for %s: %s", file_path, e)
                    files.append({"path": file_path})
            
            if recursive:
                for dir_name in dirs:
                    dir_path = f"{path}/{dir_name}" if path else dir_name
                    files.extend(self.list_files(dir_path, recursive=True))
        
        except Exception as e:
            logger.error("Failed to list files in %s: %s", path, e)
        
        return files
    
    def list_directories(self, path: str = "") -> list[str]:
        try:
            dirs, _ = self.storage.listdir(path)
            return [f"{path}/{d}" if path else d for d in dirs]
        except Exception as e:
            logger.error("Failed to list directories in %s: %s", path, e)
            return []
    
    # ---- Presigned URLs (S3/MinIO only) -------------------------------------
    
    def get_presigned_url(
            self,
            path: str,
            expiration: int = 3600,
            method: str = "get_object",
    ) -> Optional[str]:
        """Generate a presigned URL for download or upload."""
        if not self.is_s3:
            return None
        
        try:
            client = self.storage.connection.meta.client
            return client.generate_presigned_url(
                method,
                Params={"Bucket": self.storage.bucket_name, "Key": path},
                ExpiresIn=expiration,
            )
        except Exception as e:
            logger.error("Failed to generate presigned URL: %s", e)
            return None
    
    def get_upload_url(self, path: str, expiration: int = 3600) -> Optional[str]:
        """Convenience: presigned URL for PUT upload."""
        return self.get_presigned_url(path, expiration, method="put_object")
    
    # ---- Intra-bucket operations --------------------------------------------
    
    def copy(self, src_path: str, dest_path: str) -> str:
        """Copy a file within this bucket."""
        if self.is_s3:
            try:
                self.storage.bucket.copy(
                    {"Bucket": self.storage.bucket_name, "Key": src_path},
                    dest_path,
                )
                return dest_path
            except Exception as e:
                logger.warning("S3 copy failed, falling back to read/write: %s", e)
        
        data = self.read_bytes(src_path)
        return self.save(dest_path, data)
    
    def move(self, src_path: str, dest_path: str) -> str:
        """Move a file within this bucket."""
        saved = self.copy(src_path, dest_path)
        self.delete(src_path)
        return saved


# =============================================================================
# Multi-Bucket Storage Manager
# =============================================================================

class StorageManager:
    """
    Central interface to all GeoRiva storage buckets.

    Usage:
        storage = StorageManager()

        # Access buckets
        storage.incoming.save("sat-imagery/ndvi/file.tif", data)
        storage.sources.list_files("weather-stations/synop/")

        # Cross-bucket operations
        storage.archive_and_process(
            source=storage.incoming,
            path="sat-imagery/ndvi/raw.tif",
        )
    """
    
    def __init__(self):
        self._buckets: dict[str, Bucket] = {}
    
    def _get_bucket(self, bucket_type: str) -> Bucket:
        """Get or create a Bucket handle."""
        if bucket_type not in self._buckets:
            storage_name = f"georiva-{bucket_type}"
            self._buckets[bucket_type] = Bucket(bucket_type, storage_name)
        return self._buckets[bucket_type]
    
    # ---- Named bucket accessors ---------------------------------------------
    
    @property
    def incoming(self) -> Bucket:
        """User-uploaded raw data."""
        return self._get_bucket(BucketType.INCOMING)
    
    @property
    def sources(self) -> Bucket:
        """Plugin-collected data."""
        return self._get_bucket(BucketType.SOURCES)
    
    @property
    def archive(self) -> Bucket:
        """Long-term raw data preservation."""
        return self._get_bucket(BucketType.ARCHIVE)
    
    @property
    def assets(self) -> Bucket:
        """Final processed datasets."""
        return self._get_bucket(BucketType.ASSETS)
    
    def bucket(self, bucket_type: str) -> Bucket:
        """Get a bucket by type string."""
        if bucket_type not in BucketType.ALL:
            raise ValueError(
                f"Unknown bucket type: {bucket_type!r}. "
                f"Valid types: {BucketType.ALL}"
            )
        return self._get_bucket(bucket_type)
    
    # ---- Cross-bucket operations --------------------------------------------
    
    def transfer(
            self,
            source: Bucket,
            dest: Bucket,
            src_path: str,
            dest_path: Optional[str] = None,
    ) -> str:
        """
        Copy a file between buckets.

        Uses S3 server-side copy when both buckets are S3-backed.

        Args:
            source: Source bucket.
            dest: Destination bucket.
            src_path: Path in source bucket.
            dest_path: Path in dest bucket (defaults to src_path).

        Returns:
            The path in the destination bucket.
        """
        dest_path = dest_path or src_path
        
        if source.is_s3 and dest.is_s3:
            try:
                client = source.storage.connection.meta.client
                client.copy_object(
                    Bucket=dest.storage.bucket_name,
                    Key=dest_path,
                    CopySource={
                        "Bucket": source.storage.bucket_name,
                        "Key": src_path,
                    },
                )
                logger.info(
                    "S3 copy: %s/%s → %s/%s",
                    source.bucket_name, src_path,
                    dest.bucket_name, dest_path,
                )
                return dest_path
            except Exception as e:
                logger.warning("S3 cross-bucket copy failed, falling back: %s", e)
        
        # Fallback: read from source, write to dest
        data = source.read_bytes(src_path)
        saved = dest.save(dest_path, data)
        logger.info(
            "Copied: %s/%s → %s/%s",
            source.bucket_name, src_path,
            dest.bucket_name, saved,
        )
        return saved
    
    def move_between(
            self,
            source: Bucket,
            dest: Bucket,
            src_path: str,
            dest_path: Optional[str] = None,
    ) -> str:
        """Copy between buckets, then delete from source."""
        saved = self.transfer(source, dest, src_path, dest_path)
        source.delete(src_path)
        return saved
    
    # ---- Pipeline helpers ---------------------------------------------------
    
    def archive_raw(self, source: Bucket, path: str) -> str:
        """
        Archive a raw file before processing.

        Preserves origin by prefixing with the source bucket type:
            georiva-archive/{incoming|sources}/{catalog}/{collection}/file.ext
        """
        archive_path = f"{source.bucket_type}/{path}"
        return self.transfer(source, self.archive, path, archive_path)
    
    def ingest(
            self,
            source: Bucket,
            path: str,
            asset_path: str,
            processed_data: bytes,
            delete_source: bool = False,
    ) -> dict:
        """
        Full ingest pipeline: archive raw → save processed asset.

        Args:
            source: The bucket the raw file is in (incoming or sources).
            path: Path to the raw file in the source bucket.
            asset_path: Destination path in the assets bucket.
                        e.g. "sat-imagery/ndvi/temperature/2025/01/15/20250115T060000_temp.tif"
            processed_data: The processed bytes to save as an asset.
            delete_source: Whether to remove the raw file after archiving.

        Returns:
            Dict with archive_path and asset_path.
        """
        # 1. Archive the raw file
        archived = self.archive_raw(source, path)
        logger.info("Archived: %s/%s → archive/%s", source.bucket_type, path, archived)
        
        # 2. Save processed data to assets
        saved_asset = self.assets.save(asset_path, processed_data)
        logger.info("Asset saved: %s", saved_asset)
        
        # 3. Optionally clean up source
        if delete_source:
            source.delete(path)
            logger.info("Deleted source: %s/%s", source.bucket_type, path)
        
        return {
            "archive_path": archived,
            "asset_path": saved_asset,
            "source": f"{source.bucket_type}/{path}",
        }
    
    # ---- Asset path builder -------------------------------------------------
    
    @staticmethod
    def build_asset_path(
            catalog: str,
            collection: str,
            variable: str,
            timestamp: datetime,
            filename: str,
    ) -> str:
        """
        Build a time-partitioned asset path.

        Returns:
            "{catalog}/{collection}/{variable}/{year}/{month}/{day}/{filename}"

        Example:
            >>> StorageManager.build_asset_path(
            ...     "satellite-imagery", "ndvi", "temperature",
            ...     datetime(2025, 1, 15, 6, 0, 0), "20250115T060000_temp.tif"
            ... )
            "satellite-imagery/ndvi/temperature/2025/01/15/20250115T060000_temp.tif"
        """
        return (
            f"{catalog}/{collection}/{variable}/"
            f"{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/"
            f"{filename}"
        )
    
    # ---- Bucket initialization (for Docker/startup) -------------------------
    
    def ensure_buckets(self) -> list[str]:
        """
        Ensure all required buckets exist.

        For S3/MinIO, creates buckets if missing.
        For local storage, creates directories.

        Returns:
            List of bucket names that were created.
        """
        created = []
        
        for bucket_type in BucketType.ALL:
            bucket = self._get_bucket(bucket_type)
            
            if bucket.is_s3:
                try:
                    client = bucket.storage.connection.meta.client
                    try:
                        client.head_bucket(Bucket=bucket.storage.bucket_name)
                    except client.exceptions.NoSuchBucket:
                        client.create_bucket(Bucket=bucket.storage.bucket_name)
                        created.append(bucket.storage.bucket_name)
                        logger.info("Created S3 bucket: %s", bucket.storage.bucket_name)
                except Exception as e:
                    # MinIO may raise differently than AWS
                    try:
                        client.create_bucket(Bucket=bucket.storage.bucket_name)
                        created.append(bucket.storage.bucket_name)
                    except Exception:
                        logger.debug("Bucket %s likely exists: %s", bucket.bucket_name, e)
            
            elif bucket.is_local:
                import os
                location = bucket.storage.location
                if not os.path.exists(location):
                    os.makedirs(location, exist_ok=True)
                    created.append(location)
                    logger.info("Created local directory: %s", location)
        
        return created


# =============================================================================
# Singleton
# =============================================================================

storage = StorageManager()
