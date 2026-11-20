"""The numbers this service answers with, in the Variable's served unit (ADR 0046 §14–§16).

Every value we hold is in its Variable's source unit, and so is every number a
tile is drawn with: Django hands over the raw range and raw stops, and a raw
pixel drawn with them is the same colour a converted one drawn with converted
ones would be. So no tile or encoded-texture pixel is converted here.

What leaves as a number is. The render config carries ``scale``, ``offset`` and
the served ``unit`` symbol beside the range, and a point read, GetFeatureInfo
and an encoded texture's ``X-Image-Unscale`` header are served as
``value·scale + offset``. A config without them — written before units were
served — converts nothing.

Decoded store fields stay cached raw, so a served-unit change neither empties
the field cache nor reopens a store.
"""

from typing import Annotated

import rasterio
from fastapi import Depends, Path
from rio_tiler.constants import WGS84_CRS
from titiler.core.dependencies import CoordCRSParams
from titiler.core.factory import TilerFactory
from titiler.core.models.responses import Point
from titiler.core.resources.responses import JSONResponse

from app.dependencies import SemanticTileConfig


def conversion(config: dict) -> tuple[float, float]:
    """``(scale, offset)`` from a render config; the identity when it names none."""
    return float(config.get("scale", 1.0)), float(config.get("offset", 0.0))


def served(value, config: dict):
    """A raw value in the served unit; ``None`` stays ``None``."""
    if value is None:
        return None
    scale, offset = conversion(config)
    return value * scale + offset


def unit(config: dict) -> str | None:
    """The served unit's symbol, or ``None`` when the config names none."""
    return config.get("unit") or None


class ServedPoint(Point):
    """A point read, its values in the served unit, which it names."""

    unit: str | None = None


class ServedTilerFactory(TilerFactory):
    """The tile factory, its point read answering in the served unit (ADR 0046 §15).

    Every other route is the factory's own: they draw with the raw range and
    stops the render config already holds.
    """

    def point(self):
        @self.router.get(
            "/point/{lon},{lat}",
            response_model=ServedPoint,
            response_class=JSONResponse,
            responses={200: {"description": "The value at a point, in the variable's served unit"}},
            operation_id=f"{self.operation_prefix}getDataForPoint",
        )
        def point(
            lon: Annotated[float, Path(description="Longitude")],
            lat: Annotated[float, Path(description="Latitude")],
            src_path=Depends(self.path_dependency),
            tile_config: dict = Depends(SemanticTileConfig),
            reader_params=Depends(self.reader_dependency),
            coord_crs=Depends(CoordCRSParams),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            env=Depends(self.environment_dependency),
        ):
            with rasterio.Env(**env):
                with self.reader(src_path, **reader_params.as_dict()) as src_dst:
                    pts = src_dst.point(
                        lon,
                        lat,
                        coord_crs=coord_crs or WGS84_CRS,
                        **layer_params.as_dict(),
                        **dataset_params.as_dict(),
                    )

            return {
                "coordinates": [lon, lat],
                "values": [served(value, tile_config) for value in pts.array.tolist()],
                "band_names": pts.band_names,
                "band_descriptions": pts.band_descriptions,
                "unit": unit(tile_config),
            }
