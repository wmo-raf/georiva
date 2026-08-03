"""Provisioning an organisation: everything it needs to exist, in one transaction.

An Organisation is not usable on its own. It needs a Site to be reachable at, a
root page to serve and author under, and a group holding page permissions over
that root so its staff can edit their portal without seeing anyone else's. All
four are created together or not at all — a half-provisioned org is a tenant
nobody can reach and nobody can fix from the admin.

The central org (first-setup bootstrap) is provisioned through the same path,
adopting Wagtail's existing default Site and root page instead of creating new
ones. It is otherwise an entirely ordinary organisation: no code anywhere treats
it as a fallback.
"""
import logging

from django.conf import settings
from django.contrib.auth.models import Group, Permission
from django.db import transaction
from wagtail.models import GroupPagePermission, Page, Site, get_default_page_content_type

from .models import Organisation

logger = logging.getLogger(__name__)

# Page permissions the per-org group holds over its own root page. Enough to
# author and publish a portal; deliberately not `bulk_delete` (deleting the org
# root is an instance-admin action).
ORG_PAGE_PERMISSION_TYPES = ("add", "change", "publish")


def resolve_base_domain(base_domain=None):
    """The domain organisations hang off; the central org sits on it directly."""
    return base_domain or settings.GEORIVA_BASE_DOMAIN


def resolve_site_port(port=None):
    """The port organisation Sites are created on.

    More than a port: Wagtail derives a Site's ``root_url`` scheme from it (80 →
    ``http://``, 443 → ``https://``), and ``root_url`` is what
    ``core.utils.get_full_url_by_request`` builds every absolute URL from — STAC
    and EDR ``self`` links included. An instance behind TLS whose Sites are left
    on port 80 advertises ``http://`` links to callers who reached it over
    ``https://``, and that is not cosmetic: STAC Browser compares every link
    against its configured catalog URL and reads an unexpected scheme as another
    origin entirely, so the whole catalog becomes unbrowsable.
    """
    return port if port is not None else getattr(settings, "GEORIVA_SITE_PORT", 80)


def build_org_hostname(slug, base_domain=None):
    """The hostname an organisation's Site is served from."""
    return f"{slug}.{resolve_base_domain(base_domain)}"


def org_page_group_name(slug):
    """Deterministic name of an organisation's page-permission group.

    Derived from the immutable slug rather than the editable name so the group
    stays findable after a rename.
    """
    return f"{slug} editors"


def _grant_page_permissions(group, root_page):
    for permission_type in ORG_PAGE_PERMISSION_TYPES:
        permission = Permission.objects.get(
            content_type=get_default_page_content_type(),
            codename=f"{permission_type}_page",
        )
        GroupPagePermission.objects.get_or_create(group=group, page=root_page, permission=permission)


def _create_root_page(name, slug):
    from georiva.pages.home.models import HomePage

    wagtail_root = Page.get_first_root_node()
    home = HomePage(title=name, slug=slug, hero_heading=name)
    wagtail_root.add_child(instance=home)
    return home


@transaction.atomic
def provision_organisation(*, name, slug, site=None, port=None, base_domain=None, **fields):
    """Create an Organisation with its Site, root page and page-permission group.

    Pass ``site`` to bind the organisation to an existing Site (the central-org
    bootstrap case); its root page is adopted as-is. Otherwise a Site at
    ``<slug>.<GEORIVA_BASE_DOMAIN>`` and a dedicated root ``HomePage`` are
    created. Extra keyword arguments set the organisation's lean settings.

    Raises ``ValidationError`` if the slug is invalid or already taken.
    """
    organisation = Organisation(name=name, slug=slug, **fields)
    organisation.full_clean(exclude=["site"])

    if site is None:
        root_page = _create_root_page(name, slug)
        site = Site.objects.create(
            hostname=build_org_hostname(slug, base_domain),
            port=resolve_site_port(port),
            site_name=name,
            root_page=root_page,
        )
    else:
        root_page = site.root_page

    organisation.site = site
    organisation.save()

    group, _ = Group.objects.get_or_create(name=org_page_group_name(slug))
    _grant_page_permissions(group, root_page)

    logger.info("Provisioned organisation %s at %s", slug, site.hostname)
    return organisation


def sync_site_ports(port=None):
    """Move every organisation's Site onto ``GEORIVA_SITE_PORT``. Returns the count.

    New organisations get the port at provisioning time, but an instance that
    gains TLS after its organisations exist has no other way to correct them —
    the bootstrap is idempotent and returns early once any organisation exists,
    so it will never revisit them. See :func:`resolve_site_port` for why the
    stale value is worth a command of its own rather than a note in the docs.
    """
    target = resolve_site_port(port)
    stale = Site.objects.filter(organisation__isnull=False).exclude(port=target)
    updated = [site.hostname for site in stale]
    stale.update(port=target)
    if updated:
        logger.info("Moved %d organisation Site(s) to port %d: %s", len(updated), target, ", ".join(updated))
    return len(updated)


def bootstrap_central_org(*, name=None, slug="central", base_domain=None, port=None):
    """First-setup bootstrap: an ordinary org on Wagtail's default Site.

    Idempotent — returns the existing organisation if the default Site already
    has one. The default Site's hostname is moved onto the base domain so the
    apex serves the central org's portal, and its port onto
    :func:`resolve_site_port` for the same reason an ordinary organisation is
    provisioned there: Wagtail reads the scheme of every absolute URL off it, and
    the default Site ships on port 80.
    """
    site = Site.objects.filter(is_default_site=True).first()
    if site is None:
        raise RuntimeError("No default Wagtail Site exists; cannot bootstrap the central organisation.")

    existing = Organisation.objects.filter(site=site).first()
    if existing is not None:
        return existing

    base = resolve_base_domain(base_domain)
    site_port = resolve_site_port(port)
    with transaction.atomic():
        changed = []
        if site.hostname != base:
            site.hostname = base
            changed.append("hostname")
        if site.port != site_port:
            site.port = site_port
            changed.append("port")
        if changed:
            site.save(update_fields=changed)
        return provision_organisation(name=name or site.site_name or "Central", slug=slug, site=site)
