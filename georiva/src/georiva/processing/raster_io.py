"""
Reading stored rasters back for derivation.

One reader, one contract: single band, float32, nodata mapped to NaN, and
always north-up — a south-up raster (positive row pitch, ``transform.e > 0``)
is flipped so row 0 is north, the orientation every downstream consumer
assumes (parity with ``formats/geotiff.py`` at ingestion time).
"""

import numpy as np


def read_north_up(bucket_type, href):
    """Read a stored single-band raster into
    ``(data, bounds, crs, width, height)`` under the contract above."""
    import rasterio  # noqa: F401 — registers drivers for MemoryFile
    from rasterio.io import MemoryFile

    from georiva.core.storage import storage

    raw = storage.bucket(bucket_type).read_bytes(href)
    with MemoryFile(raw) as memfile, memfile.open() as src:
        data = src.read(1).astype("float32")
        if src.nodata is not None:
            data = np.where(data == src.nodata, np.nan, data)
        if src.transform.e > 0:
            data = np.flipud(data)
        bounds = list(src.bounds)
        crs = src.crs.to_string() if src.crs else "EPSG:4326"
        return data, bounds, crs, src.width, src.height
