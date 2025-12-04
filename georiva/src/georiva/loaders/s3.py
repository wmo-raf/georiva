"""
GeoRiva S3 Loader

Loader implementation for S3-compatible storage (AWS S3, MinIO, etc.).
Uses boto3 for AWS S3 and minio for MinIO/generic S3.
"""

import fnmatch
from pathlib import Path
from typing import Iterator

from .base import BaseLoader, FetchResult, RemoteFile


class S3Loader(BaseLoader):
    """
    Loader for S3-compatible object storage.
    
    Supports:
    - AWS S3
    - MinIO
    - Other S3-compatible services (Wasabi, DigitalOcean Spaces, etc.)
    - Public buckets (no credentials)
    - Prefix (folder) filtering
    - Glob patterns for object keys
    - Requester pays buckets
    """
    
    def __init__(self, config, collection):
        super().__init__(config, collection)
        self._client = None
        self._use_minio = False
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    def connect(self) -> None:
        """Create S3 client."""
        # Decide whether to use minio or boto3
        # MinIO client is better for MinIO servers and simple use cases
        # boto3 is more feature-complete for AWS
        
        if self.config.endpoint_url:
            # Custom endpoint (MinIO, etc.)
            self._connect_minio()
        else:
            # AWS S3
            self._connect_boto3()
    
    def _connect_minio(self) -> None:
        """Connect using MinIO client."""
        try:
            from minio import Minio
            from urllib.parse import urlparse
        except ImportError:
            raise ImportError("minio is required. Install with: pip install minio")
        
        # Parse endpoint URL
        parsed = urlparse(self.config.endpoint_url)
        endpoint = parsed.netloc or parsed.path
        secure = parsed.scheme == 'https'
        
        # Create client
        self._client = Minio(
            endpoint,
            access_key=self.config.access_key or None,
            secret_key=self.config.secret_key or None,
            secure=secure,
        )
        
        self._use_minio = True
        
        # Verify bucket exists
        if not self._client.bucket_exists(self.config.bucket_name):
            raise ValueError(f"Bucket '{self.config.bucket_name}' does not exist")
        
        self.logger.info(f"Connected to MinIO: {endpoint}/{self.config.bucket_name}")
    
    def _connect_boto3(self) -> None:
        """Connect using boto3."""
        try:
            import boto3
            from botocore.config import Config as BotoConfig
        except ImportError:
            raise ImportError("boto3 is required. Install with: pip install boto3")
        
        # Build client config
        client_config = BotoConfig(
            signature_version='s3v4',
            retries={'max_attempts': 3, 'mode': 'standard'},
        )
        
        # Client kwargs
        kwargs = {
            'service_name': 's3',
            'region_name': self.config.region,
            'config': client_config,
        }
        
        # Credentials (if provided)
        if self.config.access_key:
            kwargs['aws_access_key_id'] = self.config.access_key
            kwargs['aws_secret_access_key'] = self.config.secret_key
        
        # Custom endpoint
        if self.config.endpoint_url:
            kwargs['endpoint_url'] = self.config.endpoint_url
        
        self._client = boto3.client(**kwargs)
        self._use_minio = False
        
        # Verify bucket
        try:
            self._client.head_bucket(Bucket=self.config.bucket_name)
        except Exception as e:
            raise ValueError(f"Cannot access bucket '{self.config.bucket_name}': {e}")
        
        self.logger.info(f"Connected to S3: {self.config.bucket_name}")
    
    def disconnect(self) -> None:
        """Close S3 client."""
        self._client = None
    
    # =========================================================================
    # File Listing
    # =========================================================================
    
    def list_files(self) -> Iterator[RemoteFile]:
        """List objects in the configured bucket/prefix."""
        prefix = self.config.prefix.strip('/') + '/' if self.config.prefix else ''
        
        if self._use_minio:
            yield from self._list_minio(prefix)
        else:
            yield from self._list_boto3(prefix)
    
    def _list_minio(self, prefix: str) -> Iterator[RemoteFile]:
        """List objects using MinIO client."""
        objects = self._client.list_objects(
            self.config.bucket_name,
            prefix=prefix,
            recursive=True,
        )
        
        for obj in objects:
            # Skip "directories"
            if obj.object_name.endswith('/'):
                continue
            
            filename = Path(obj.object_name).name
            
            # Apply filename pattern
            if not self._matches_pattern(filename):
                continue
            
            yield RemoteFile(
                path=obj.object_name,
                filename=filename,
                size=obj.size,
                modified=obj.last_modified,
                checksum=obj.etag.strip('"') if obj.etag else None,
                metadata={
                    'bucket': self.config.bucket_name,
                    'key': obj.object_name,
                },
            )
    
    def _list_boto3(self, prefix: str) -> Iterator[RemoteFile]:
        """List objects using boto3 paginator."""
        paginator = self._client.get_paginator('list_objects_v2')
        
        page_config = {
            'Bucket': self.config.bucket_name,
            'Prefix': prefix,
        }
        
        # Requester pays
        if self.config.requester_pays:
            page_config['RequestPayer'] = 'requester'
        
        for page in paginator.paginate(**page_config):
            for obj in page.get('Contents', []):
                key = obj['Key']
                
                # Skip "directories"
                if key.endswith('/'):
                    continue
                
                filename = Path(key).name
                
                # Apply filename pattern
                if not self._matches_pattern(filename):
                    continue
                
                yield RemoteFile(
                    path=key,
                    filename=filename,
                    size=obj.get('Size'),
                    modified=obj.get('LastModified'),
                    checksum=obj.get('ETag', '').strip('"'),
                    metadata={
                        'bucket': self.config.bucket_name,
                        'key': key,
                        'storage_class': obj.get('StorageClass'),
                    },
                )
    
    def _matches_pattern(self, filename: str) -> bool:
        """Check if filename matches configured pattern."""
        pattern = self.config.filename_pattern
        
        if not pattern or pattern == '*':
            return True
        
        return fnmatch.fnmatch(filename, pattern)
    
    # =========================================================================
    # File Fetching
    # =========================================================================
    
    def fetch_file(self, remote_file: RemoteFile, local_path: Path) -> FetchResult:
        """Download an object from S3."""
        result = FetchResult(remote_file=remote_file, local_path=local_path)
        
        key = remote_file.metadata.get('key', remote_file.path)
        
        try:
            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            if self._use_minio:
                self._fetch_minio(key, local_path)
            else:
                self._fetch_boto3(key, local_path)
            
            result.success = True
            result.bytes_transferred = local_path.stat().st_size
        
        except Exception as e:
            self.logger.error(f"Failed to fetch {key}: {e}")
            result.success = False
            result.error = str(e)
        
        return result
    
    def _fetch_minio(self, key: str, local_path: Path) -> None:
        """Download using MinIO client."""
        self._client.fget_object(
            self.config.bucket_name,
            key,
            str(local_path),
        )
    
    def _fetch_boto3(self, key: str, local_path: Path) -> None:
        """Download using boto3."""
        extra_args = {}
        if self.config.requester_pays:
            extra_args['RequestPayer'] = 'requester'
        
        self._client.download_file(
            self.config.bucket_name,
            key,
            str(local_path),
            ExtraArgs=extra_args if extra_args else None,
        )
    
    # =========================================================================
    # S3-Specific Methods
    # =========================================================================
    
    def get_presigned_url(self, remote_file: RemoteFile, expires_in: int = 3600) -> str:
        """
        Generate a presigned URL for direct download.
        
        Useful for passing to external services or clients.
        """
        key = remote_file.metadata.get('key', remote_file.path)
        
        if self._use_minio:
            from datetime import timedelta
            return self._client.presigned_get_object(
                self.config.bucket_name,
                key,
                expires=timedelta(seconds=expires_in),
            )
        else:
            return self._client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.config.bucket_name,
                    'Key': key,
                },
                ExpiresIn=expires_in,
            )
    
    def stream_object(self, remote_file: RemoteFile):
        """
        Get a streaming response for an object.
        
        Returns a file-like object for streaming reads.
        """
        key = remote_file.metadata.get('key', remote_file.path)
        
        if self._use_minio:
            response = self._client.get_object(self.config.bucket_name, key)
            return response
        else:
            response = self._client.get_object(
                Bucket=self.config.bucket_name,
                Key=key,
            )
            return response['Body']


class GCSLoader(BaseLoader):
    """
    Loader for Google Cloud Storage.
    
    Uses google-cloud-storage client library.
    """
    
    def __init__(self, config, collection):
        super().__init__(config, collection)
        self._client = None
        self._bucket = None
    
    def connect(self) -> None:
        """Create GCS client."""
        try:
            from google.cloud import storage
            from google.oauth2 import service_account
        except ImportError:
            raise ImportError(
                "google-cloud-storage is required. "
                "Install with: pip install google-cloud-storage"
            )
        
        # Create client
        if hasattr(self.config, 'credentials_file') and self.config.credentials_file:
            credentials = service_account.Credentials.from_service_account_file(
                self.config.credentials_file
            )
            self._client = storage.Client(credentials=credentials)
        else:
            # Use default credentials (ADC)
            self._client = storage.Client()
        
        # Get bucket
        self._bucket = self._client.bucket(self.config.bucket_name)
        
        # Verify bucket exists
        if not self._bucket.exists():
            raise ValueError(f"Bucket '{self.config.bucket_name}' does not exist")
        
        self.logger.info(f"Connected to GCS: {self.config.bucket_name}")
    
    def disconnect(self) -> None:
        """Close GCS client."""
        self._client = None
        self._bucket = None
    
    def list_files(self) -> Iterator[RemoteFile]:
        """List blobs in the bucket."""
        prefix = self.config.prefix.strip('/') + '/' if self.config.prefix else None
        
        blobs = self._bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            # Skip "directories"
            if blob.name.endswith('/'):
                continue
            
            filename = Path(blob.name).name
            
            # Apply filename pattern
            if not self._matches_pattern(filename):
                continue
            
            yield RemoteFile(
                path=blob.name,
                filename=filename,
                size=blob.size,
                modified=blob.updated,
                checksum=blob.md5_hash,
                metadata={
                    'bucket': self.config.bucket_name,
                    'blob_name': blob.name,
                    'content_type': blob.content_type,
                },
            )
    
    def _matches_pattern(self, filename: str) -> bool:
        """Check if filename matches configured pattern."""
        pattern = getattr(self.config, 'filename_pattern', '*')
        
        if not pattern or pattern == '*':
            return True
        
        return fnmatch.fnmatch(filename, pattern)
    
    def fetch_file(self, remote_file: RemoteFile, local_path: Path) -> FetchResult:
        """Download a blob from GCS."""
        result = FetchResult(remote_file=remote_file, local_path=local_path)
        
        blob_name = remote_file.metadata.get('blob_name', remote_file.path)
        
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            blob = self._bucket.blob(blob_name)
            blob.download_to_filename(str(local_path))
            
            result.success = True
            result.bytes_transferred = local_path.stat().st_size
        
        except Exception as e:
            self.logger.error(f"Failed to fetch {blob_name}: {e}")
            result.success = False
            result.error = str(e)
        
        return result
