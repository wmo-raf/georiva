"""
Ingestion tracking for the feed-scoped Ingestion Activity page (PRD #217).

FileIngestion has no FK to acquisition records or feeds (ADR-0003) — the
feed's records are found by the catalog path prefix (file_path is always
{catalog}/{collection}/{filename}), which also surfaces failed Ingestions
that never got associated with any Collection.
"""
from __future__ import annotations

#: Sentinel filter value selecting records with an empty collections M2M —
#: the orphaned-failure view.
NO_COLLECTION = "none"


def feed_file_ingestions(feed, *, status=None, collection=None):
    """A feed's FileIngestions, newest activity first.

    `status` narrows to one FileIngestion status. `collection` narrows via
    the collections M2M: pass a Collection (or its pk), or NO_COLLECTION for
    records linked to no collection at all.
    """
    from georiva.ingestion.models import FileIngestion

    records = FileIngestion.objects.filter(
        file_path__startswith=f"{feed.catalog.slug}/"
    )
    if status:
        records = records.filter(status=status)
    if collection == NO_COLLECTION:
        records = records.filter(collections__isnull=True)
    elif collection is not None:
        records = records.filter(collections=collection)
    return records.order_by("-updated_at")
