"""Turning a storage key's leading segments back into the objects that own it.

Every bucket key starts ``{org}/{catalog}/…``. This module is the only place
those two segments are resolved to a ``Catalog``, and it is deliberately
unforgiving: an unknown organisation, or a catalog whose slug matches but which
belongs to a *different* organisation, resolves to nothing. There is no default
organisation to fall back on — a wrong answer here files one institution's
national data under another's prefix.

Catalog slugs are unique per organisation, so ``(org, catalog)`` is the only
lookup that identifies a catalog unambiguously; a bare ``slug=`` lookup would
now match an arbitrary org's catalog.
"""
from georiva.core.models import Catalog
from georiva.organisations.models import Organisation


def org_slug_from_key(key):
    """The organisation slug a bucket key is filed under, or ``None``.

    The first segment of every key on every bucket is the owning organisation's
    slug, which makes the path itself a usable owner marker for records that
    have no FK chain to lean on — a ``FileIngestion`` written before its
    collections are known, for instance.
    """
    if not key:
        return None
    head = str(key).lstrip("/").split("/", 1)[0]
    return head or None


def resolve_org_catalog(org_slug, catalog_slug, *, require_active=True):
    """Resolve ``(org_slug, catalog_slug)`` to a Catalog.

    Returns ``(catalog, None)`` on success and ``(None, message)`` on failure,
    where the message names precisely which half of the pair did not resolve so
    an operator reading an ``IngestionLog`` can act on it.
    """
    if not org_slug:
        return None, "Cannot determine the organisation from the file path."
    if not catalog_slug:
        return None, "Cannot determine the catalog from the file path."

    organisation = Organisation.objects.filter(slug=org_slug).first()
    if organisation is None:
        return None, f"Unknown organisation '{org_slug}'."

    catalogs = Catalog.objects.select_related("boundary", "organisation")
    catalog = catalogs.filter(organisation=organisation, slug=catalog_slug).first()
    if catalog is None:
        return None, (
            f"Catalog '{catalog_slug}' does not belong to organisation '{org_slug}'."
        )
    if require_active and not catalog.is_active:
        return None, f"Catalog '{org_slug}/{catalog_slug}' is inactive."

    return catalog, None
