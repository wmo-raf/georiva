"""Objects in buckets, and the keys that name them.

Every key on every bucket opens with the owning organisation's slug —
``{org}/{catalog}/{collection}/{variable}/{year}/{month}/{day}/``. This package
holds both directions of that grammar and the conventions that fill it in:

``manager``          the ``Bucket`` facade and the ``StorageManager`` singleton;
                     ``build_asset_path`` writes keys
``path_resolution``  reads a key's leading segments back to the objects that own
                     it — the inverse of ``build_asset_path``, and deliberately
                     unforgiving, since a wrong answer files one institution's
                     data under another's prefix
``filename``         the ``GR--{YYYYMMDDTHHMM}--`` naming convention for the
                     objects that land in those buckets
``asset_cleanup``    pure selection of objects no live ``Asset.href`` names

``core.machine_plane`` mirrors this grammar for service addresses rather than
object keys, so a Titiler tile URL and the COG key behind it differ only by
prefix.

The re-exports below are the surface the old top-level ``core/storage.py``
offered, kept so its callers — the singleton has 27 of them — did not have to
move with it. The other three modules are imported by their own paths.
"""

from .manager import (  # noqa: F401
    Bucket,
    BucketType,
    StorageManager,
    get_bucket_config,
    storage,
)
