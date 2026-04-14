"""
Management command to consume MinIO bucket events from Redis and dispatch
Celery ingestion tasks.

MinIO publishes S3 event notifications to a Redis list when files are
uploaded to georiva-incoming or georiva-sources buckets. This command
blocks on that list and queues a Celery task for each event.

Usage:
    python manage.py minio_event_consumer
"""

import signal
import threading

from django.core.management.base import BaseCommand

from georiva.ingestion.consumer import run_minio_consumer


class Command(BaseCommand):
    help = "Consume MinIO bucket events from Redis and dispatch ingestion tasks"
    
    def handle(self, *args, **options):
        self.stdout.write("Starting MinIO event consumer...")
        
        # Allow clean shutdown on SIGTERM (Docker stop) and SIGINT (Ctrl+C)
        stop_event = threading.Event()
        
        def _handle_signal(signum, frame):
            self.stdout.write("Shutdown signal received, stopping consumer...")
            stop_event.set()
        
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
        
        run_minio_consumer(stop_event)
        
        self.stdout.write(self.style.SUCCESS("Consumer stopped."))
