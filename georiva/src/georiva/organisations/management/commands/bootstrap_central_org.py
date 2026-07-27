from django.core.management.base import BaseCommand

from georiva.organisations.provisioning import bootstrap_central_org


class Command(BaseCommand):
    help = (
        "Bootstrap the central organisation on Wagtail's default Site at the base domain. "
        "Idempotent: does nothing if the default Site already belongs to an organisation."
    )

    def add_arguments(self, parser):
        parser.add_argument("--slug", default="central", help="Slug for the central organisation.")
        parser.add_argument("--name", default=None, help="Display name (defaults to the default Site's name).")

    def handle(self, *args, **options):
        organisation = bootstrap_central_org(slug=options["slug"], name=options["name"])
        self.stdout.write(
            self.style.SUCCESS(f"Central organisation: {organisation.name} ({organisation.slug}) at {organisation.hostname}")
        )
