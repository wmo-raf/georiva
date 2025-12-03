"""
GeoRiva S3 Event Listener

Listens for S3 bucket notifications when new files are added to incoming paths.
Triggers the ingestion pipeline for matching collections.
"""

import json
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class S3EventListener:
    """
    Listens for S3/MinIO bucket notifications.
    
    Currently, Can be configured to use:
    - MinIO webhook notifications
    
    When a new file appears in incoming/{collection_id}/,
    triggers the ingestion pipeline.
    """
    
    def __init__(self, callback: Optional[Callable] = None):
        """
        Initialize the listener.
        
        Args:
            callback: Function to call when a new file is detected.
                     Receives (file_path: str, collection_id: str)
        """
        self.callback = callback or self._default_callback
        self.logger = logging.getLogger("georiva.s3_events")
    
    @property
    def storage(self):
        """Get the storage manager."""
        from georiva.core.storage import storage_manager
        return storage_manager
    
    def _default_callback(self, file_path: str, collection_id: str):
        """Default callback - queues a Celery task."""
        from georiva.core.tasks import process_incoming_file_task
        process_incoming_file_task.delay(file_path, collection_id)
        self.logger.info(f"Queued ingestion task for {file_path}")
    
    def handle_webhook(self, request_body: bytes) -> dict:
        """
        Handle MinIO webhook notification.
        
        MinIO sends notifications in a specific JSON format when
        objects are created/deleted.
        
        Args:
            request_body: Raw HTTP request body from MinIO webhook
        
        Returns:
            Dict with processing results
        """
        try:
            payload = json.loads(request_body)
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in webhook payload: {e}")
            return {'error': 'Invalid JSON'}
        
        results = []
        
        # MinIO sends Records array
        records = payload.get('Records', [])
        
        for record in records:
            event_name = record.get('eventName', '')
            
            # Only process object creation events
            if not event_name.startswith('s3:ObjectCreated'):
                continue
            
            # Extract bucket and key
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name', '')
            key = s3_info.get('object', {}).get('key', '')
            
            if not key:
                continue
            
            # Check if it's in an incoming path
            if not key.startswith('incoming/'):
                continue
            
            # Extract collection ID
            parts = key.split('/')
            if len(parts) < 3:  # incoming/{collection_id}/{filename}
                continue
            
            collection_id = parts[1]
            
            self.logger.info(f"New file in incoming: {key} (collection: {collection_id})")
            
            # Trigger ingestion
            try:
                self.callback(key, collection_id)
                results.append({
                    'file': key,
                    'collection': collection_id,
                    'status': 'queued',
                })
            except Exception as e:
                self.logger.error(f"Failed to trigger ingestion for {key}: {e}")
                results.append({
                    'file': key,
                    'collection': collection_id,
                    'status': 'error',
                    'error': str(e),
                })
        
        return {'processed': len(results), 'results': results}
    
    def _is_already_processed(self, file_path: str) -> bool:
        """
        Check if a file has already been processed.
        
        Uses a simple marker file approach for now.
        Could be enhanced to use Redis or database.
        """
        from django.core.cache import cache
        
        cache_key = f"georiva:incoming:processed:{file_path}"
        return cache.get(cache_key) is not None
    
    def _mark_as_processed(self, file_path: str):
        """Mark a file as processed."""
        from django.core.cache import cache
        
        cache_key = f"georiva:incoming:processed:{file_path}"
        # Keep the marker for 24 hours
        cache.set(cache_key, True, timeout=86400)
