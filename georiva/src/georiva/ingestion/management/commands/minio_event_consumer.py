import signal
import threading

from django.core.management.base import BaseCommand

from georiva.ingestion.consumer import run_minio_consumer


class Command(BaseCommand):
    help = "Consume MinIO bucket events from Redis and dispatch ingestion tasks"

    def handle(self, *args, **options):
        self.stdout.write("Starting MinIO event consumer...")

        stop_event = threading.Event()

        def _handle_signal(signum, frame):
            self.stdout.write("Shutdown signal received, stopping consumer...")
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

        run_minio_consumer(stop_event)

        self.stdout.write(self.style.SUCCESS("Consumer stopped."))
