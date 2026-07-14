"""
Per-file re-fetch (PRD #217, issue #225).

A failed FetchedFile is retried in place: the stored request is rebuilt,
the file re-fetched with no skip-existing shortcut, the SAME record walks
pending → fetching → stored/failed again, and the parent FetchRun's
counters are recomputed from its children so the run reflects current
truth. Per-file collection context comes from file_path (ADR-0003).
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class RetryNotPossible(Exception):
    """This FetchedFile cannot be re-fetched (e.g. no stored request)."""


def _collection_for(fetched_file, feed):
    """Resolve the collection from the storage path's
    {catalog}/{collection}/... shape (ADR-0003)."""
    from georiva.core.models import Collection

    parts = fetched_file.file_path.split("/")
    if len(parts) < 3:
        raise RetryNotPossible(
            f"file path '{fetched_file.file_path}' has no collection segment"
        )
    return Collection.objects.get(catalog=feed.catalog, slug=parts[1])


def retry_fetch(fetched_file):
    """Re-fetch one FetchedFile in place and recompute its run's counters."""
    from georiva.sources.fetch.base import FileRequest

    if not fetched_file.request_payload:
        raise RetryNotPossible("no stored request on this record")

    run = fetched_file.fetch_run
    feed = run.data_feed
    request = FileRequest.from_dict(fetched_file.request_payload)
    loader = feed.get_loader(_collection_for(fetched_file, feed))

    # Reset the stale outcome so the record walks the state machine again.
    fetched_file.error = ""
    fetched_file.skip_reason = ""
    fetched_file.completed_at = None
    fetched_file.save(update_fields=["error", "skip_reason", "completed_at"])
    fetched_file.mark_fetching()

    result = loader.fetch_one(request)
    if result.success:
        fetched_file.mark_stored(bytes_transferred=result.bytes_transferred or 0)
    else:
        fetched_file.mark_failed(error=result.error or "fetch failed")

    run.recompute_counters()
    return fetched_file
