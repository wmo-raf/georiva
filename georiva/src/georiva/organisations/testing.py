"""Organisation fixtures for tests.

``Catalog.organisation`` is non-null with no default, so every test that builds
a catalog needs an organisation first. Provisioning a real one is heavier than
most tests want (a Site, a root page, a permission group), so this builds the
minimum a catalog needs — an Organisation on a Site — and reuses it across calls
with the same slug.

Not a test module itself: it lives beside the app so any app's tests can import
it without reaching into another app's ``tests`` package.
"""

from datetime import UTC, datetime

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
    return Organisation.objects.create(name=name or slug, slug=slug, site=site, **fields)


#: The catalog/collection/variable slug :func:`make_org_tree` defaults to. It is
#: the *same* string for every organisation on purpose: slugs are unique only
#: within an organisation (#267), so a fixture that gave each one a distinct
#: slug would pass every scoping test by accident.
SHARED_TREE_SLUG = "forecast"


def make_org_tree(organisation, *, name=None, slug=SHARED_TREE_SLUG):
    """One organisation's data chain: catalog → collection → variable → item → asset.

    The fixture almost every tenancy test needs, and the one it is easy to build
    subtly wrong. Two organisations' trees must be distinguishable *only* by
    their organisation — same slugs, differing names — because that is the
    arrangement in which a dropped org filter stops being invisible: it either
    collides or serves the other institution's row, and the name in the response
    says which.

    Returns a dict rather than a tuple so a caller takes the two or three pieces
    it cares about by name.
    """
    from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable

    name = name or f"{organisation.slug} {slug}"
    catalog = Catalog.objects.create(
        organisation=organisation,
        name=name,
        slug=slug,
        file_format=Catalog.FileFormat.GEOTIFF,
    )
    collection = Collection.objects.create(catalog=catalog, name=name, slug=slug)
    unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
    variable = Variable.objects.create(
        collection=collection,
        name=name,
        slug=slug,
        unit=unit,
        value_min=0,
        value_max=50,
    )
    item = Item.objects.create(
        collection=collection,
        time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
    )
    asset = Asset.objects.create(item=item, variable=variable, href="x.tif")
    return {
        "organisation": organisation,
        "catalog": catalog,
        "collection": collection,
        "variable": variable,
        "item": item,
        "asset": asset,
    }
