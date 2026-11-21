"""The numbers this service answers with, in the Variable's served unit (ADR 0046 §14–§16).

Every value we hold is in its Variable's source unit, and so is every number a
tile is drawn with: Django hands over the raw range and raw stops, and a raw
pixel drawn with them is the same colour a converted one drawn with converted
ones would be. So no tile or encoded-texture pixel is converted here.

What leaves as a number is. The render config carries ``scale``, ``offset`` and
the served ``unit`` symbol beside the range, and a point read, GetFeatureInfo,
statistics and an encoded texture's ``X-Image-Unscale`` header are served as
``value·scale + offset`` (a spread by ``|scale|``, a sum gaining the offset once
per value). ``/info`` names the served unit. A config without them — written
before units were served — converts nothing.

Decoded store fields stay cached raw, so a served-unit change neither empties
the field cache nor reopens a store.
"""

from typing import Annotated

import rasterio
from fastapi import Body, Depends, Path
from geojson_pydantic.features import Feature, FeatureCollection
from rio_tiler.constants import WGS84_CRS
from titiler.core.dependencies import CoordCRSParams, CoverScaleParams, DstCRSParams
from titiler.core.factory import TilerFactory
from titiler.core.models.responses import Point
from titiler.core.resources.responses import GeoJSONResponse, JSONResponse

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


#: A band's statistics that are values, and so convert like one.
_VALUES = ("min", "max", "mean", "median", "majority", "minority")


def served_statistics(statistics: dict, config: dict) -> dict:
    """rio-tiler's per-band statistics of raw values, in the served unit.

    Values (min, max, mean, median, percentiles, majority, minority, the
    histogram's bin edges) convert as values; ``std`` by ``|scale|`` alone;
    ``sum`` gains the offset once per value counted. Counts are counts.
    """
    scale, offset = conversion(config)
    served_bands = {}
    for name, band in statistics.items():
        band = band.model_dump() if hasattr(band, "model_dump") else dict(band)
        for key in list(band):
            if key in _VALUES or key.startswith("percentile_"):
                band[key] = served(band[key], config)
        band["std"] = band["std"] * abs(scale)
        band["sum"] = band["sum"] * scale + offset * band["count"]
        counts, edges = band["histogram"]
        band["histogram"] = [counts, [served(edge, config) for edge in edges]]
        band["unit"] = unit(config)
        served_bands[name] = band
    return served_bands


class ServedPoint(Point):
    unit: str | None = None


class ServedTilerFactory(TilerFactory):
    """The tile factory, its numbers answering in the served unit (ADR 0046 §15).

    The point read and statistics are served; ``/info`` names the unit. Every
    other route is the factory's own: they draw with the raw range and stops
    the render config already holds.
    """

    def info(self):
        @self.router.get(
            "/info",
            response_class=JSONResponse,
            responses={200: {"description": "The dataset's basic info, naming the served unit"}},
            operation_id=f"{self.operation_prefix}getInfo",
        )
        def info(
            src_path=Depends(self.path_dependency),
            tile_config: dict = Depends(SemanticTileConfig),
            reader_params=Depends(self.reader_dependency),
            env=Depends(self.environment_dependency),
        ):
            with rasterio.Env(**env):
                with self.reader(src_path, **reader_params.as_dict()) as src_dst:
                    answer = src_dst.info().model_dump(exclude_none=True)
            return {**answer, "unit": unit(tile_config)}

    def statistics(self):
        @self.router.get(
            "/statistics",
            response_class=JSONResponse,
            responses={200: {"description": "The values' statistics, in the variable's served unit"}},
            operation_id=f"{self.operation_prefix}getStatistics",
        )
        def statistics(
            src_path=Depends(self.path_dependency),
            tile_config: dict = Depends(SemanticTileConfig),
            reader_params=Depends(self.reader_dependency),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            image_params=Depends(self.img_preview_dependency),
            stats_params=Depends(self.stats_dependency),
            histogram_params=Depends(self.histogram_dependency),
            env=Depends(self.environment_dependency),
        ):
            # Of the values, never of the colours: no rescale is applied.
            with rasterio.Env(**env):
                with self.reader(src_path, **reader_params.as_dict()) as src_dst:
                    image = src_dst.preview(
                        **layer_params.as_dict(), **image_params.as_dict(), **dataset_params.as_dict()
                    )
                    stats = image.statistics(**stats_params.as_dict(), hist_options=histogram_params.as_dict())
            return served_statistics(stats, tile_config)

        @self.router.post(
            "/statistics",
            response_class=GeoJSONResponse,
            responses={200: {"description": "The values' statistics in each feature, in the served unit"}},
            operation_id=f"{self.operation_prefix}postStatisticsForGeoJSON",
        )
        def geojson_statistics(
            geojson: Annotated[FeatureCollection | Feature, Body(description="GeoJSON Feature or FeatureCollection.")],
            src_path=Depends(self.path_dependency),
            tile_config: dict = Depends(SemanticTileConfig),
            reader_params=Depends(self.reader_dependency),
            coord_crs=Depends(CoordCRSParams),
            dst_crs=Depends(DstCRSParams),
            layer_params=Depends(self.layer_dependency),
            dataset_params=Depends(self.dataset_dependency),
            image_params=Depends(self.img_part_dependency),
            cover_scale=Depends(CoverScaleParams),
            stats_params=Depends(self.stats_dependency),
            histogram_params=Depends(self.histogram_dependency),
            env=Depends(self.environment_dependency),
        ):
            features = geojson.features if isinstance(geojson, FeatureCollection) else [geojson]
            answered = []
            with rasterio.Env(**env):
                with self.reader(src_path, **reader_params.as_dict()) as src_dst:
                    for feature in features:
                        shape = feature.model_dump(exclude_none=True)
                        image = src_dst.feature(
                            shape,
                            shape_crs=coord_crs or WGS84_CRS,
                            dst_crs=dst_crs,
                            align_bounds_with_dataset=True,
                            **layer_params.as_dict(),
                            **image_params.as_dict(),
                            **dataset_params.as_dict(),
                        )
                        coverage = image.get_coverage_array(
                            shape, shape_crs=coord_crs or WGS84_CRS, cover_scale=cover_scale
                        )
                        stats = image.statistics(
                            **stats_params.as_dict(), hist_options=histogram_params.as_dict(), coverage=coverage
                        )
                        properties = {**(shape.get("properties") or {})}
                        properties["statistics"] = served_statistics(stats, tile_config)
                        answered.append({**shape, "properties": properties})
            if isinstance(geojson, Feature):
                return answered[0]
            return {"type": "FeatureCollection", "features": answered}

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
