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
bucket named as :class:`Stored`. A Store asset — or a :class:`StoreSlice`, the
field it names — is read from its Upstream store, opened with its harvest feed's
Pinned locations only, once per session and at one snapshot for the session's
life (``store.py``).

In what unit: every value is held in its Variable's source unit and returned
in its served unit, or in the unit a session names —
``values.session(in_unit="degC")``, ``values.point(asset, lon, lat,
in_unit="K")`` — refused with :class:`ConversionRefused` before anything is
read when the source unit cannot reach it (ADR 0046 §11, ``units.py``). Bytes
no Variable describes (an :class:`AssetLocation`, a :class:`Stored` key, a bare
:class:`StoreSlice`) are read as they are, and refuse an ``in_unit``.
"""

from georiva.core.unit_utils import ConversionRefused

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
from .store import StepNotHeld, StoreDataNotPinned, StoreSlice, StoreUnreadable
from .units import Conversion, conversion_for

__all__ = [
    "Conversion",
    "ConversionRefused",
    "Field",
    "Session",
    "SessionClosed",
    "StepNotHeld",
    "StoreDataNotPinned",
    "StoreSlice",
    "StoreUnreadable",
    "Stored",
    "area",
    "conversion_for",
    "field",
    "point",
    "session",
    "window",
]
