import logging

from django.core.management.base import BaseCommand

from georiva.core.palette_cache import warm_all

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Warm the palette cache"
    
    def handle(self, *args, **kwargs):
        self.stdout.write("Warming palette cache...")
        warm_all()
        self.stdout.write(self.style.SUCCESS("Done."))
