"""How EDR names a collection (ADR 0045).

An EDR collection id is ``{catalog}.{collection}``. A collection slug is unique
only within its catalog and a catalog slug only within its organisation, so the
pair is unique on an organisation's host — and a dot cannot occur in a slug, so
the id splits back into the pair without ambiguity.

Every EDR id and every link to an EDR collection is spelt here, so the API, the
dataset pages and the Connect panel cannot drift apart.
"""

from django.urls import reverse

SEPARATOR = "."


def edr_collection_id(collection) -> str:
    """The id EDR serves ``collection`` under."""
    return f"{collection.catalog.slug}{SEPARATOR}{collection.slug}"


def edr_collection_href(base_url: str, collection) -> str:
    """``collection``'s EDR document under ``base_url`` (the EDR root, ending in ``/``)."""
    return f"{base_url}collections/{edr_collection_id(collection)}/"


def edr_collection_path(collection) -> str:
    """The host-relative path of ``collection``'s EDR document."""
    return reverse("edr:collection-detail", args=[edr_collection_id(collection)])


class EDRCollectionIdConverter:
    """Path converter for an EDR collection id: a qualified id, or a bare slug (which redirects).

    Registered as ``edrid``. Anything else — a second dot, a character no slug
    holds — matches no route at all.
    """

    regex = r"[-a-zA-Z0-9_]+(?:\.[-a-zA-Z0-9_]+)?"

    def to_python(self, value: str) -> str:
        return value

    def to_url(self, value: str) -> str:
        return value


def split_edr_collection_id(collection_id: str) -> tuple[str, str] | None:
    """``(catalog_slug, collection_slug)`` for a qualified id, ``None`` for a bare slug."""
    if SEPARATOR not in collection_id:
        return None
    catalog_slug, _, collection_slug = collection_id.partition(SEPARATOR)
    return catalog_slug, collection_slug
