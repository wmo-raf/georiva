from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Safety net sweep: resets stale locks, queues untracked files, "
        "and retries failed ingestions."
    )
    
    def add_arguments(self, parser):
        parser.add_argument(
            "--async",
            action="store_true",
            dest="async_mode",
            default=False,
            help="Queue files via Celery instead of processing synchronously.",
        )
    
    def handle(self, *args, **options):
        from georiva.ingestion.tasks import sweep_unprocessed
        
        self.stdout.write("Running sweep_unprocessed...")
        sweep_unprocessed(sync=not options["async_mode"])
        self.stdout.write(self.style.SUCCESS("Sweep complete."))
