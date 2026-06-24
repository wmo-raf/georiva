from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Reconcile the STAGING bucket against StagingItems: register any staged "
        "object that has no StagingItem (event missed, consumer down, or DB reset "
        "with the bucket intact), without re-downloading."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--async",
            action="store_true",
            dest="async_mode",
            default=False,
            help="Queue files via Celery instead of registering synchronously.",
        )

    def handle(self, *args, **options):
        from georiva.ingestion.tasks import sweep_staging

        self.stdout.write("Running sweep_staging...")
        count = sweep_staging(sync=not options["async_mode"])
        self.stdout.write(self.style.SUCCESS(
            f"Sweep complete: {count} new staging file(s) registered."
        ))
