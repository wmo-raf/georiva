from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError

from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation


class Command(BaseCommand):
    help = "Provision an organisation: its Site, root page and page-permission group, in one transaction."

    def add_arguments(self, parser):
        parser.add_argument("slug", help="Immutable organisation slug (lowercase, hyphen-separated).")
        parser.add_argument("--name", required=True, help="Display name of the institution.")
        parser.add_argument("--contact-email", default="")
        parser.add_argument("--website", default="")
        parser.add_argument("--country", default="", help="ISO 3166-1 alpha-2 code.")
        parser.add_argument(
            "--admin",
            default=None,
            help="Username of an existing user to add as the organisation's first org admin.",
        )

    def handle(self, *args, **options):
        try:
            organisation = provision_organisation(
                name=options["name"],
                slug=options["slug"],
                contact_email=options["contact_email"],
                website=options["website"],
                country=options["country"],
            )
        except ValidationError as exc:
            raise CommandError(
                "; ".join(f"{field}: {', '.join(errors)}" for field, errors in exc.message_dict.items())
            ) from exc

        self.stdout.write(self.style.SUCCESS(f"Provisioned {organisation.name} at {organisation.hostname}"))

        if options["admin"]:
            user = get_user_model().objects.filter(username=options["admin"]).first()
            if user is None:
                raise CommandError(f"No user named “{options['admin']}”.")
            OrganisationMembership.objects.create(
                user=user,
                organisation=organisation,
                role=OrganisationMembership.Role.ADMIN,
            )
            self.stdout.write(self.style.SUCCESS(f"Added {user} as org admin of {organisation.slug}."))
