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
