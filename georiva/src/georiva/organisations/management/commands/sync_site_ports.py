from django.core.management.base import BaseCommand

from georiva.organisations.provisioning import resolve_site_port, sync_site_ports


class Command(BaseCommand):
    help = (
        "Move every organisation's Wagtail Site onto GEORIVA_SITE_PORT. Run this after "
        "putting an existing instance behind TLS: the port decides the scheme of every "
        "absolute URL the APIs advertise, and provisioning only sets it once. Idempotent."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--port",
            type=int,
            default=None,
            help="Port to move Sites to. Defaults to GEORIVA_SITE_PORT.",
        )

    def handle(self, *args, **options):
        target = resolve_site_port(options["port"])
        updated = sync_site_ports(options["port"])
        if not updated:
            self.stdout.write(f"Every organisation Site is already on port {target}.")
            return
        self.stdout.write(
            self.style.SUCCESS(f"Moved {updated} organisation Site(s) to port {target}.")
        )
