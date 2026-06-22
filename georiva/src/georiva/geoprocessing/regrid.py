"""
Regridding — resample a raster from a source grid onto a target grid.

Uses ``rasterio.warp.reproject`` (already in the image) so the library needs
no rioxarray/scipy. Operates on numpy arrays with affine transforms + CRS; the
caller decides the target grid (the recipe's "target grid of production").
"""
from __future__ import annotations

import numpy as np
from rasterio.crs import CRS as RasterioCRS
from rasterio.enums import Resampling
from rasterio.warp import reproject

_RESAMPLING = {
    "nearest": Resampling.nearest,
    "bilinear": Resampling.bilinear,
    "cubic": Resampling.cubic,
    "average": Resampling.average,
}


def regrid_array(
        data,
        src_transform,
        src_crs,
        dst_transform,
        dst_crs,
        dst_shape,
        resampling: str = "bilinear",
):
    """
    Reproject/resample a 2-D array onto a target grid.

    Parameters
    ----------
    data : np.ndarray
        2-D source array (NaN = nodata).
    src_transform, dst_transform : affine.Affine
        Source and target affine transforms.
    src_crs, dst_crs : str or CRS
        Source and target CRS.
    dst_shape : tuple[int, int]
        Target (height, width).
    resampling : {"nearest", "bilinear", "cubic", "average"}

    Returns
    -------
    np.ndarray
        2-D array on the target grid, NaN where unmapped.
    """
    if resampling not in _RESAMPLING:
        raise ValueError(f"unknown resampling: {resampling!r}")

    dst_height, dst_width = dst_shape
    destination = np.full((dst_height, dst_width), np.nan, dtype="float32")

    reproject(
        source=np.asarray(data, dtype="float32"),
        destination=destination,
        src_transform=src_transform,
        src_crs=RasterioCRS.from_user_input(src_crs),
        dst_transform=dst_transform,
        dst_crs=RasterioCRS.from_user_input(dst_crs),
        src_nodata=np.nan,
        dst_nodata=np.nan,
        resampling=_RESAMPLING[resampling],
    )
    return destination
