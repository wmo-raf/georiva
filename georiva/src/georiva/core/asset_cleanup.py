"""
Orphaned-asset-object selection — the pure core of the ``cleanup_asset_orphans``
management command.

An **orphan** is a raster/visual object in the assets bucket that no live
``Asset.href`` references. These accumulate when a re-derivation rewrites an
asset's ``href`` in place (e.g. a filename-scheme change) while the old object
lingers in the bucket, or when items/assets are deleted without a storage sweep.

Only object types that correspond to an ``Asset`` (COG/GeoTIFF, PNG, WebP, JPEG)
are ever selected — a non-asset sidecar such as the ``.json`` metadata ingestion
writes alongside each asset is deliberately left alone, so a sweep can never
remove a legitimate file that simply isn't modelled as an ``Asset`` row.

Kept dependency-free so it is unit-testable without a live bucket; the command
supplies the object listing and the live-href set.
"""
from __future__ import annotations

import os

# Extensions of objects that ARE registered as Assets (mirrors the format→ext
# mapping the engine/ingestion writers use). Anything else in the bucket — most
# notably the ``.json`` metadata sidecar — is never a deletion candidate.
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
