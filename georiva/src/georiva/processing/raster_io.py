"""
Reading stored rasters back for derivation.

One reader, one contract: single band, float32, nodata mapped to NaN, and
always north-up — a south-up raster (positive row pitch, ``transform.e > 0``)
is flipped so row 0 is north, the orientation every downstream consumer
assumes (parity with ``formats/geotiff.py`` at ingestion time). The contract is
the read module's (``core.values``, ADR 0046 §10); this is its derivation-shaped
caller.
"""


def read_north_up(bucket_type, href):
    """Read a stored single-band raster into
    ``(data, bounds, crs, width, height)`` under the contract above."""
    from georiva.core import values
    from georiva.core.storage import BucketType
    from georiva.core.storage.asset_location import AssetLocation

    source = AssetLocation(href) if bucket_type == BucketType.ASSETS else values.Stored(bucket_type, href)
    field = values.field(source)
    return field.data, list(field.bounds), field.crs, field.width, field.height
