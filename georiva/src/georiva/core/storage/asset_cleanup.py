"""
Orphaned-asset-object selection — the pure core of the ``cleanup_asset_orphans``
management command.

An **orphan** is a raster/visual object in the assets bucket that no live
``Asset.href`` references. These accumulate when a re-derivation rewrites an
asset's ``href`` in place (e.g. a filename-scheme change) while the old object
lingers in the bucket, or when an on-commit deletion of a deleted row's object
(ADR 0032) gave up or spared it — and from before that existed.

Only object types that correspond to an ``Asset`` (COG/GeoTIFF, PNG, WebP, JPEG)
are ever selected — a non-asset object such as the legacy ``.json`` metadata
sidecars ingestion wrote alongside each asset before ADR 0024 is deliberately
left alone, so a sweep can never remove a legitimate file that simply isn't
modelled as an ``Asset`` row.

Deleting an object is guarded the same way whoever asks for it — the
on-commit deletion that follows an Asset row out (``asset_deletion``) and the
sweep alike — by :func:`deletion_verdict`: never a key a live ``href`` names,
and never an object written at or after the cutoff, since a key is
deterministic and a re-ingest may already have put new bytes under it before
its row exists (ADR 0032).

Kept dependency-free so it is unit-testable without a live bucket; the callers
supply the object listing, the live-href set and the modified times.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

# Extensions of objects that ARE registered as Assets (mirrors the format→ext
# mapping the engine/ingestion writers use). Anything else in the bucket — most
# notably the legacy ``.json`` metadata sidecars written before ADR 0024 —
# is never a deletion candidate.
DELETABLE_EXTENSIONS = (".tif", ".tiff", ".png", ".webp", ".jpeg", ".jpg")


def select_orphan_objects(object_paths, live_hrefs, deletable_extensions):
    """Return the object paths that are safe to delete: those whose extension is
    a known asset type **and** which no live ``href`` references.

    ``object_paths`` — the keys currently in the (scoped) bucket.
    ``live_hrefs`` — the set of ``Asset.href`` values still in the database.
    ``deletable_extensions`` — lowercase extensions eligible for deletion.
    """
    live = set(live_hrefs)
    exts = tuple(e.lower() for e in deletable_extensions)
    orphans = []
    for path in object_paths:
        if path in live:
            continue
        if os.path.splitext(path)[1].lower() not in exts:
            continue
        orphans.append(path)
    return orphans


#: How far before the cutoff an object's modified time must fall for it to be
#: deleted. Absorbs clock skew between the app and the object store; anything
#: inside the margin is left for the sweep.
DELETION_MARGIN = timedelta(seconds=60)

DELETE = "delete"
SKIP_LIVE = "skipped_live"
SKIP_RECENT = "skipped_recent"
MISSING = "missing"


def deletion_verdict(path: str, live_hrefs, modified: datetime | None, cutoff: datetime) -> str:
    """Whether the object at ``path`` may be deleted, and if not, why.

    ``live_hrefs`` — the ``Asset.href`` values that still exist.
    ``modified`` — the object's modified time, or ``None`` when it is gone.
    ``cutoff`` — the moment the object stopped being wanted (a delete's commit
    time, or *now* for the sweep); objects modified within
    :data:`DELETION_MARGIN` of it, or after it, are kept.
    """
    if path in live_hrefs:
        return SKIP_LIVE
    if modified is None:
        return MISSING
    if modified >= cutoff - DELETION_MARGIN:
        return SKIP_RECENT
    return DELETE
