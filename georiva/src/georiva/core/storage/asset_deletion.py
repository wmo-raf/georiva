"""Published asset objects follow their rows out, on commit (ADR 0032).

The platform wrote every object an ``Asset.href`` names in the assets bucket,
so deleting the row deletes the object — but only once the delete has
committed, and never from inside the request that deleted it:

1. ``on_asset_deleted`` (an ``Asset`` ``post_delete`` receiver) adds the href to
   a buffer held on the connection for the current transaction. The first href
   registers one ``on_commit`` flush; a rollback discards the flush, and the
   buffer with it.
2. The flush stamps the commit time and dispatches the hrefs in chunks of
   :data:`CHUNK_SIZE` — one task per chunk, not per asset, so deleting a
   collection of 50,000 assets is 100 tasks rather than 50,000.
3. ``delete_objects`` (the task body) re-checks every key against
   ``deletion_verdict`` before deleting it: a key a live Asset names again, or
   an object rewritten since the commit, is spared.

Whatever this misses — a chunk that exhausted its retries, a key spared by the
margin that turns out to be an orphan after all — is what
``cleanup_asset_orphans`` sweeps. Raw inputs on the staging tier are not
covered: they are evidence of what was received, and keep their files.
"""

from __future__ import annotations

import logging

from django.db import connections, transaction
from django.utils import timezone

from .asset_cleanup import DELETE, MISSING, SKIP_LIVE, SKIP_RECENT, deletion_verdict

logger = logging.getLogger(__name__)

#: Hrefs per deletion task.
CHUNK_SIZE = 500

_BUFFER_ATTR = "_georiva_pending_asset_deletions"


class _PendingDeletions:
    """The hrefs one transaction has deleted rows for, flushed on its commit."""

    def __init__(self, using: str):
        self.using = using
        self.hrefs: list[str] = []

    def flush(self):
        connection = connections[self.using]
        if getattr(connection, _BUFFER_ATTR, None) is self:
            delattr(connection, _BUFFER_ATTR)
        dispatch(self.hrefs, timezone.now())


def _pending_for(using: str) -> _PendingDeletions:
    """This transaction's buffer, registering its flush on first use.

    A buffer outlives its transaction on the connection when the transaction
    rolls back — Django drops the flush but knows nothing of the buffer — so a
    buffer whose flush is no longer pending is stale and replaced, not reused.
    """
    connection = connections[using]
    pending = getattr(connection, _BUFFER_ATTR, None)
    if pending is not None and any(func == pending.flush for _, func, _ in connection.run_on_commit):
        return pending

    pending = _PendingDeletions(using)
    setattr(connection, _BUFFER_ATTR, pending)
    return pending


def on_asset_deleted(sender, instance, using, **kwargs):
    if not instance.href:
        return
    pending = _pending_for(using)
    first = not pending.hrefs
    pending.hrefs.append(instance.href)
    # Registered after the append: outside a transaction on_commit runs at once.
    if first:
        transaction.on_commit(pending.flush, using=using)


def dispatch(hrefs, deleted_at):
    from georiva.core.tasks import delete_asset_objects

    unique = list(dict.fromkeys(hrefs))
    for start in range(0, len(unique), CHUNK_SIZE):
        delete_asset_objects.delay(unique[start : start + CHUNK_SIZE], deleted_at.isoformat())


def delete_objects(hrefs, deleted_at, *, bucket=None) -> dict:
    """Delete the objects ``hrefs`` name, sparing any that came back to life.

    Per key, a failure is logged and counted, never raised — one bad key must
    not strand the other 499. Raises only when the live-href lookup fails, which
    is the caller's cue to retry the chunk.
    """
    from georiva.core.models import Asset

    from .manager import storage

    bucket = bucket or storage.assets
    live = set(Asset.objects.filter(href__in=hrefs).values_list("href", flat=True))
    summary = {DELETE: 0, SKIP_LIVE: 0, SKIP_RECENT: 0, MISSING: 0, "failed": 0}

    for href in hrefs:
        try:
            modified = None if href in live else _modified_or_none(bucket, href)
            verdict = deletion_verdict(href, live, modified, deleted_at)
            if verdict == DELETE:
                bucket.delete(href)
            summary[verdict] += 1
        except Exception as exc:
            summary["failed"] += 1
            logger.warning("Could not delete asset object %s: %s", href, exc)

    summary["deleted"] = summary.pop(DELETE)
    return summary


def _modified_or_none(bucket, href):
    if not bucket.exists(href):
        return None
    return bucket.modified_time(href)
