"""What values an asset holds, wherever its bytes are (ADR 0046 §10).

``core.storage.asset_location`` answers *where* an asset's bytes are; this
package answers *what values* they hold. Every Django-side reader of values
reads through it — derivation, timeseries, zonal statistics — and a core test
fails any other way of opening raster bytes (ADR 0046 §19,
``core/tests/test_values_guard.py``).

Reads happen in a session::

    from georiva.core import values

    with values.session() as s:
        field = s.field(asset)                   # the whole field
        part = s.window(asset, (w, s_, e, n))    # the cells under a WGS84 box
        v = s.point(asset, lon, lat)             # one cell
        series = s.point_series(assets, lon, lat)

and each call has a one-call helper (``values.field(asset)`` …) that is a
session of its own. A field is north-up float32 with NaN for nodata, with its
georeferencing (:class:`Field`).

What is read: a core ``Asset`` or its :class:`AssetLocation` — a local COG,
read by range from the assets bucket, or a Remote COG, read by range from its
href — a ``StagingAsset`` (a key on the staging bucket), or any object in any
bucket named as :class:`Stored`. A Store asset is not read here yet.
"""

from .session import (
    Field,
    Session,
    SessionClosed,
    Stored,
    area,
    field,
    point,
    session,
    window,
)

__all__ = [
    "Field",
    "Session",
    "SessionClosed",
    "Stored",
    "area",
    "field",
    "point",
    "session",
    "window",
]
