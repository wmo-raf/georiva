from django.core.management.base import BaseCommand, CommandError

from georiva.organisations.provisioning import resolve_base_domain, sync_site_domains


class Command(BaseCommand):
    help = (
        "Move every organisation's Wagtail Site from a previous domain onto "
        "GEORIVA_BASE_DOMAIN. Run this after correcting the base domain of an instance "
        "that is already initialized: hostnames are written once at provisioning time, "
        "so until they are moved the new domain matches no organisation and every "
        "request 404s. Each organisation keeps its own label. Idempotent."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--from",
            dest="old_domain",
            required=True,
            help="Domain the organisations are currently served on (e.g. wrong.example).",
        )
        parser.add_argument(
            "--base-domain",
            default=None,
            help="Domain to move them to. Defaults to GEORIVA_BASE_DOMAIN.",
        )

    def handle(self, *args, **options):
        target = resolve_base_domain(options["base_domain"])
        try:
            moves = sync_site_domains(options["old_domain"], options["base_domain"])
        except ValueError as exc:
            raise CommandError(str(exc))

        if not moves:
            self.stdout.write(f"No organisation Site is on {options['old_domain']}; nothing to move.")
            return

        for old_hostname, new_hostname in moves:
            self.stdout.write(f"  {old_hostname} -> {new_hostname}")
        self.stdout.write(self.style.SUCCESS(f"Moved {len(moves)} organisation Site(s) onto {target}."))
