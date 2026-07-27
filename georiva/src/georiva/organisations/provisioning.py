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


def build_org_hostname(slug, base_domain=None):
    """The hostname an organisation's Site is served from."""
    base = base_domain or settings.GEORIVA_BASE_DOMAIN
    return f"{slug}.{base}"


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
            port=port if port is not None else getattr(settings, "GEORIVA_SITE_PORT", 80),
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


def bootstrap_central_org(*, name=None, slug="central", base_domain=None):
    """First-setup bootstrap: an ordinary org on Wagtail's default Site.

    Idempotent — returns the existing organisation if the default Site already
    has one. The default Site's hostname is moved onto the base domain so the
    apex serves the central org's portal.
    """
    site = Site.objects.filter(is_default_site=True).first()
    if site is None:
        raise RuntimeError("No default Wagtail Site exists; cannot bootstrap the central organisation.")

    existing = Organisation.objects.filter(site=site).first()
    if existing is not None:
        return existing

    base = base_domain or settings.GEORIVA_BASE_DOMAIN
    with transaction.atomic():
        if site.hostname != base:
            site.hostname = base
            site.save(update_fields=["hostname"])
        return provision_organisation(name=name or site.site_name or "Central", slug=slug, site=site)
