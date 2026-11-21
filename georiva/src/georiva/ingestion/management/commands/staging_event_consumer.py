import signal
import threading

from django.core.management.base import BaseCommand

from georiva.ingestion.staging_consumer import run_staging_consumer


class Command(BaseCommand):
    help = "Consume MinIO STAGING bucket events from Redis and register StagingItems"

    def handle(self, *args, **options):
        self.stdout.write("Starting staging event consumer...")

        stop_event = threading.Event()

        def _handle_signal(signum, frame):
            self.stdout.write("Shutdown signal received, stopping staging consumer...")
            stop_event.set()

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

        run_staging_consumer(stop_event)

        self.stdout.write(self.style.SUCCESS("Staging consumer stopped."))
