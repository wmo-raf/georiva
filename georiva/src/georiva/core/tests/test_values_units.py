"""Values are held in their Variable's source unit and converted when read (ADR 0046 §1–§3, §11; #742).

A file is ingested as it came — Kelvin stays Kelvin — and a reading session
answers in the Variable's served unit, or in the unit it was asked for. Nothing
here opens a COG; every value is read through ``georiva.core.values``.
"""

import math
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import numpy as np
import rasterio
from django.test import override_settings
from rasterio.transform import from_origin

from georiva.core import values
from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.core.storage.asset_location import AssetLocation
from georiva.core.tests.test_asset_deletion import AssetsBucketTestCase
from georiva.formats.geotiff import GeoTIFFFormatPlugin
from georiva.ingestion.asset_writer import AssetWriter
from georiva.ingestion.extractor import VariableExtractor
from georiva.ingestion.handlers.asset_handler import AssetHandler
from georiva.organisations.testing import make_organisation

TIME = datetime(2026, 3, 1, 12, tzinfo=UTC)
SIZE = 8
#: Kelvin, as a model writes 2 m temperature: 270 K in the first cell, rising by one per cell.
KELVIN = (270.0 + np.arange(SIZE * SIZE, dtype="float32")).reshape(SIZE, SIZE)
TRANSFORM = from_origin(30.0, 4.0, 1.0, 1.0)
#: Inside the cell at row 2, column 3 — 270 + 19 = 289 K.
LON, LAT = 33.5, 1.5
AREA = {
    "type": "Polygon",
    "coordinates": [[[30.0, -4.0], [32.0, -4.0], [32.0, 4.0], [30.0, 4.0], [30.0, -4.0]]],
}


def unit(symbol):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


def kelvin_tiff(folder: Path) -> Path:
    path = folder / "t2m_20260301T120000.tif"
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=SIZE,
        height=SIZE,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=TRANSFORM,
    ) as dst:
        dst.write(KELVIN, 1)
    return path


class IngestedInSourceUnit(AssetsBucketTestCase):
    """A GeoTIFF in Kelvin, ingested for a Variable stored in K and served in °C."""

    def setUp(self):
        super().setUp()
        folder = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, folder, ignore_errors=True)
        self.enterContext(override_settings(GEORIVA_TEMP_DIR=str(folder)))
        reading = mock.patch("georiva.core.storage.asset_location.storage", mock.Mock(assets=self.bucket))
        reading.start()
        self.addCleanup(reading.stop)

        catalog = Catalog.objects.create(
            organisation=make_organisation("kenya"), name="Models", slug="models", file_format="geotiff"
        )
        collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface")
        self.variable = Variable.objects.create(
            collection=collection,
            name="2 m temperature",
            slug="t2m",
            source_unit=unit("K"),
            unit=unit("degC"),
            sources=[("primary", {"source_name": "band_1"})],
            value_min=233.15,
            value_max=323.15,
        )
        item = Item.objects.create(collection=collection, time=TIME)
        handler = AssetHandler(AssetWriter(self.bucket), VariableExtractor(GeoTIFFFormatPlugin()))
        (self.asset,) = handler.process_variable(
            item=item,
            variable=self.variable,
            local_path=kelvin_tiff(folder),
            timestamp=TIME,
            bounds=(30.0, -4.0, 38.0, 4.0),
            crs="EPSG:4326",
            width=SIZE,
            height=SIZE,
        )


class IngestionStoresTheSourceUnitTests(IngestedInSourceUnit):
    def test_read_in_the_source_unit_the_values_are_the_files_own(self):
        with values.session(in_unit="K") as s:
            np.testing.assert_array_equal(s.field(self.asset).data, KELVIN)

    def test_read_by_default_the_values_are_in_the_served_unit(self):
        with values.session() as s:
            np.testing.assert_allclose(s.field(self.asset).data, KELVIN - 273.15, atol=1e-4)
            self.assertAlmostEqual(s.point(self.asset, LON, LAT), 289.0 - 273.15, places=4)

    def test_the_assets_statistics_are_in_the_source_unit(self):
        self.asset.refresh_from_db()
        self.assertEqual((self.asset.stats_min, self.asset.stats_max), (270.0, 270.0 + SIZE * SIZE - 1))

    def test_another_unit_can_be_asked_for(self):
        self.assertAlmostEqual(values.point(self.asset, LON, LAT, in_unit="degF"), 289.0 * 9 / 5 - 459.67, places=3)

    def test_changing_the_served_unit_changes_what_is_read_and_not_what_is_stored(self):
        self.variable.unit = unit("degF")
        self.variable.save()

        self.assertAlmostEqual(values.point(self.asset, LON, LAT), 289.0 * 9 / 5 - 459.67, places=3)
        self.assertAlmostEqual(values.point(self.asset, LON, LAT, in_unit="K"), 289.0, places=4)

    def test_an_area_is_aggregated_over_the_converted_cells(self):
        cells = KELVIN[:, 0:2].astype("float64")
        with values.session() as s:
            self.assertAlmostEqual(s.area(self.asset, AREA, "sum"), float((cells - 273.15).sum()), places=2)
            self.assertAlmostEqual(s.area(self.asset, AREA, "std"), float(cells.std()), places=4)

    def test_a_series_is_converted_step_by_step(self):
        with values.session(in_unit="degC") as s:
            (value,) = s.point_series([self.asset], LON, LAT)
        self.assertAlmostEqual(value, 289.0 - 273.15, places=4)


class UnreachableUnitTests(IngestedInSourceUnit):
    def test_a_unit_the_source_unit_cannot_reach_is_refused_before_anything_is_read(self):
        with (
            mock.patch.object(AssetLocation, "open_raster", side_effect=AssertionError("read")) as opened,
            values.session(in_unit="mm") as s,
        ):
            for read in (
                lambda: s.field(self.asset),
                lambda: s.point(self.asset, LON, LAT),
                lambda: s.area(self.asset, AREA, "mean"),
                lambda: s.point_series([self.asset, self.asset], LON, LAT),
            ):
                with self.subTest(read=read), self.assertRaises(values.ConversionRefused):
                    read()
        opened.assert_not_called()

    def test_a_unit_pint_does_not_know_is_refused_too(self):
        with self.assertRaises(values.ConversionRefused):
            values.field(self.asset, in_unit="not-a-unit")

    def test_a_unit_is_not_asked_of_bytes_whose_unit_nobody_states(self):
        with self.assertRaises(values.ConversionRefused):
            values.field(AssetLocation(self.asset.href), in_unit="K")

    def test_bytes_whose_unit_nobody_states_are_read_as_they_are(self):
        self.assertTrue(math.isclose(values.point(AssetLocation(self.asset.href), LON, LAT), 289.0))
