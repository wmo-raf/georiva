"""Org ownership of catalogs, and the org-first storage path grammar (#267)."""
from datetime import datetime
from unittest.mock import MagicMock

import pytz
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase

from georiva.core.storage.filename import parse_path, validate_path
from georiva.core.models import Catalog
from georiva.core.storage.path_resolution import resolve_org_catalog
from georiva.core.storage import StorageManager
from georiva.organisations.testing import make_organisation


class CatalogOwnershipTests(TestCase):
    """A catalog belongs to exactly one organisation, and its slug is only
    unique within it."""

    def test_catalog_requires_an_organisation(self):
        catalog = Catalog(name="Orphan", slug="orphan", file_format="grib2")
        with self.assertRaises(ValidationError) as ctx:
            catalog.full_clean()
        self.assertIn("organisation", ctx.exception.message_dict)

    def test_two_organisations_may_each_own_the_same_slug(self):
        kenya = make_organisation("kenya")
        uganda = make_organisation("uganda")

        Catalog.objects.create(
            organisation=kenya, name="Forecast", slug="forecast", file_format="grib2"
        )
        Catalog.objects.create(
            organisation=uganda, name="Forecast", slug="forecast", file_format="grib2"
        )

        self.assertEqual(Catalog.objects.filter(slug="forecast").count(), 2)

    def test_one_organisation_may_not_reuse_a_slug(self):
        kenya = make_organisation("kenya")
        Catalog.objects.create(
            organisation=kenya, name="Forecast", slug="forecast", file_format="grib2"
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            Catalog.objects.create(
                organisation=kenya, name="Forecast again", slug="forecast",
                file_format="grib2",
            )

    def test_storage_prefix_is_org_then_catalog(self):
        catalog = Catalog.objects.create(
            organisation=make_organisation("kenya"), name="CHIRPS", slug="chirps",
            file_format="geotiff",
        )
        self.assertEqual(catalog.storage_prefix, "kenya/chirps")


class ResolveOrgCatalogTests(TestCase):
    """The only lookup that turns a path's leading segments into a Catalog —
    and it never falls back to a default organisation."""

    def setUp(self):
        self.kenya = make_organisation("kenya")
        self.uganda = make_organisation("uganda")
        self.catalog = Catalog.objects.create(
            organisation=self.kenya, name="CHIRPS", slug="chirps", file_format="geotiff"
        )

    def test_resolves_a_catalog_within_its_own_organisation(self):
        catalog, error = resolve_org_catalog("kenya", "chirps")
        self.assertEqual(catalog, self.catalog)
        self.assertIsNone(error)

    def test_unknown_organisation_resolves_to_nothing(self):
        catalog, error = resolve_org_catalog("atlantis", "chirps")
        self.assertIsNone(catalog)
        self.assertIn("atlantis", error)

    def test_catalog_of_another_organisation_is_not_reachable(self):
        catalog, error = resolve_org_catalog("uganda", "chirps")
        self.assertIsNone(catalog)
        self.assertIn("does not belong to organisation 'uganda'", error)

    def test_inactive_catalog_is_refused_with_a_precise_message(self):
        Catalog.objects.filter(pk=self.catalog.pk).update(is_active=False)
        catalog, error = resolve_org_catalog("kenya", "chirps")
        self.assertIsNone(catalog)
        self.assertIn("inactive", error)


class PathGrammarTests(TestCase):
    """``{org}/{catalog}/[{collection}/]{file}`` on every drop zone."""

    def test_parses_org_catalog_and_collection(self):
        meta = parse_path("kenya/weather/gfs/GR--20250115T0600--gfs.grib2")
        self.assertEqual(meta["org"], "kenya")
        self.assertEqual(meta["catalog"], "weather")
        self.assertEqual(meta["collection"], "gfs")
        self.assertEqual(meta["original_name"], "gfs.grib2")

    def test_parses_a_collectionless_drop(self):
        meta = parse_path("kenya/weather/gfs.grib2")
        self.assertEqual(meta["org"], "kenya")
        self.assertEqual(meta["catalog"], "weather")
        self.assertIsNone(meta["collection"])

    def test_a_path_without_an_org_segment_is_invalid(self):
        with self.assertRaises(ValueError):
            validate_path("weather/gfs.grib2")

    def test_the_org_is_not_inferred_from_a_short_path(self):
        meta = parse_path("gfs.grib2")
        self.assertIsNone(meta["org"])
        self.assertIsNone(meta["catalog"])


class AssetPathTests(TestCase):
    """``build_asset_path`` cannot be called without saying which org."""

    TIMESTAMP = datetime(2025, 1, 15, 6, 0, tzinfo=pytz.utc)

    def test_org_is_the_first_segment(self):
        path = StorageManager.build_asset_path(
            org="kenya", catalog="chirps", collection="rainfall",
            variable="precip", timestamp=self.TIMESTAMP, filename="precip_060000.tif",
        )
        self.assertEqual(
            path, "kenya/chirps/rainfall/precip/2025/01/15/precip_060000.tif"
        )

    def test_org_is_a_required_argument(self):
        with self.assertRaises(TypeError):
            StorageManager.build_asset_path(
                catalog="chirps", collection="rainfall", variable="precip",
                timestamp=self.TIMESTAMP, filename="precip_060000.tif",
            )

    def test_an_empty_org_is_refused_rather_than_written_at_the_root(self):
        with self.assertRaises(ValueError):
            StorageManager.build_asset_path(
                org="", catalog="chirps", collection="rainfall", variable="precip",
                timestamp=self.TIMESTAMP, filename="precip_060000.tif",
            )


class ArchivePathTests(TestCase):
    """The archive names its origin bucket without displacing the org segment."""

    def _archive(self, path):
        manager = StorageManager()
        source = MagicMock()
        source.bucket_type = "sources"
        manager.transfer = MagicMock(side_effect=lambda s, d, sp, dp: dp)
        return manager.archive_raw(source, path)

    def test_origin_is_recorded_after_the_org(self):
        self.assertEqual(
            self._archive("kenya/chirps/rainfall/rain.tif"),
            "kenya/sources/chirps/rainfall/rain.tif",
        )

    def test_a_path_with_no_org_segment_is_refused(self):
        with self.assertRaises(ValueError):
            self._archive("rain.tif")
