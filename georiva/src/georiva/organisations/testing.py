"""Organisation fixtures for tests.

``Catalog.organisation`` is non-null with no default, so every test that builds
a catalog needs an organisation first. Provisioning a real one is heavier than
most tests want (a Site, a root page, a permission group), so this builds the
minimum a catalog needs — an Organisation on a Site — and reuses it across calls
with the same slug.

Not a test module itself: it lives beside the app so any app's tests can import
it without reaching into another app's ``tests`` package.
"""
from wagtail.models import Page, Site

from .models import Organisation

DEFAULT_TEST_ORG_SLUG = "test-org"


def _root_page():
    default_site = Site.objects.filter(is_default_site=True).first()
    if default_site is not None:
        return default_site.root_page
    return Page.get_first_root_node()


def org_host(slug=DEFAULT_TEST_ORG_SLUG):
    """The Host header that ``make_organisation(slug)`` answers on.

    Admin views owning tenant data resolve their organisation from the request
    Host and refuse to guess one. A test driving such a view therefore has to
    dial the org that owns its fixtures: the bare ``testserver`` default belongs
    to the bootstrap organisation, not to anything ``make_organisation`` built.
    """
    return f"{slug}.testserver"


def dial_org(client, slug=DEFAULT_TEST_ORG_SLUG):
    """Point ``client`` at ``slug``'s host for every request it makes.

    Creates the organisation if it does not exist yet: an unknown host is a 404
    on every URL, and a test that only drives admin views has no other reason to
    build one.
    """
    make_organisation(slug)
    client.defaults["HTTP_HOST"] = org_host(slug)
    return client


def join_org(user, slug=DEFAULT_TEST_ORG_SLUG, role=None):
    """Give ``user`` a membership row in ``slug``, defaulting to Member.

    Non-superusers need one to reach any organisation's admin at all — the
    middleware turns a signed-in stranger away before any view runs.
    """
    from .models import OrganisationMembership

    return OrganisationMembership.objects.create(
        user=user,
        organisation=make_organisation(slug),
        role=role or OrganisationMembership.Role.MEMBER,
    )


def make_organisation(slug=DEFAULT_TEST_ORG_SLUG, name=None, **fields):
    """An Organisation with ``slug``, created once and returned thereafter."""
    existing = Organisation.objects.filter(slug=slug).first()
    if existing is not None:
        return existing

    site, _ = Site.objects.get_or_create(
        hostname=f"{slug}.testserver",
        port=80,
        defaults={"site_name": name or slug, "root_page": _root_page()},
    )
    return Organisation.objects.create(
        name=name or slug, slug=slug, site=site, **fields
    )
