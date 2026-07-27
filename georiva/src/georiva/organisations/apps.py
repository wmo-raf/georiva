import logging

from django.apps import AppConfig
from django.conf import settings
from django.db.models.signals import post_migrate

logger = logging.getLogger(__name__)


def bootstrap_on_migrate(sender, **kwargs):
    """First setup gets a working central organisation, without ceremony.

    A single-institution install should never have to think about tenancy: after
    the first ``migrate`` its default Site is an ordinary organisation on the
    base domain, and the instance behaves exactly as it did before tenancy
    existed. Idempotent, and silent once any organisation exists.
    """
    if not getattr(settings, "GEORIVA_BOOTSTRAP_CENTRAL_ORG", True):
        return

    from georiva.organisations.models import Organisation
    from georiva.organisations.provisioning import bootstrap_central_org

    if Organisation.objects.exists():
        return

    try:
        organisation = bootstrap_central_org(
            name=getattr(settings, "WAGTAIL_SITE_NAME", None),
            slug=getattr(settings, "GEORIVA_CENTRAL_ORG_SLUG", "central"),
        )
    except Exception:
        # Never block a migration on it: an operator can run
        # `bootstrap_central_org` by hand once the cause is fixed.
        logger.exception("Could not bootstrap the central organisation.")
        return

    logger.info("Bootstrapped central organisation %s at %s", organisation.slug, organisation.hostname)


class OrganisationsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "georiva.organisations"
    label = "organisations"
    verbose_name = "Organisations"

    def ready(self):
        post_migrate.connect(bootstrap_on_migrate, sender=self)
