from datetime import timedelta

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.ingestion.models import PushFeed
from georiva.organisations.testing import make_organisation


def _make_catalog(slug="cat"):
    return Catalog.objects.create(organisation=make_organisation(), name=slug, slug=slug, file_format="grib2")


def _make_collection(catalog, slug="col"):
    return Collection.objects.create(name=slug, slug=slug, catalog=catalog)


def _make_feed(catalog, name="Surface variables", fmt="YYYYMMDD", is_forecast=False):
    return PushFeed.objects.create(
        catalog=catalog,
        name=name,
        valid_time_format=fmt,
        is_forecast=is_forecast,
    )


class PushFeedCreationTests(TestCase):
    def test_feed_can_be_created_with_required_fields(self):
        catalog = _make_catalog()
        feed = PushFeed.objects.create(
            catalog=catalog,
            name="Surface variables",
            valid_time_format="YYYYMMDD",
        )
        self.assertEqual(feed.catalog, catalog)
        self.assertEqual(feed.name, "Surface variables")
        self.assertFalse(feed.is_forecast)

    def test_a_catalog_has_at_most_one_push_feed(self):
        catalog = _make_catalog()
        _make_feed(catalog, name="Surface variables")
        with self.assertRaises(IntegrityError), transaction.atomic():
            _make_feed(catalog, name="Pressure levels")

    def test_files_per_run_defaults_to_one_and_interval_to_none(self):
        feed = _make_feed(_make_catalog())
        self.assertEqual(feed.files_per_run, 1)
        self.assertIsNone(feed.expected_interval)

    def test_files_per_run_must_be_at_least_one(self):
        feed = PushFeed(catalog=_make_catalog(), name="x", valid_time_format="YYYYMMDD", files_per_run=0)
        with self.assertRaises(ValidationError) as ctx:
            feed.full_clean()
        self.assertIn("files_per_run", ctx.exception.message_dict)

    def test_expected_interval_is_stored(self):
        feed = _make_feed(_make_catalog())
        feed.expected_interval = timedelta(hours=6)
        feed.save()
        feed.refresh_from_db()
        self.assertEqual(feed.expected_interval, timedelta(hours=6))

    def test_is_forecast_defaults_to_false(self):
        feed = _make_feed(_make_catalog())
        self.assertFalse(feed.is_forecast)

    def test_is_forecast_can_be_set_true(self):
        feed = _make_feed(_make_catalog(), is_forecast=True)
        self.assertTrue(feed.is_forecast)


class ValidTimeFormatChoicesTests(TestCase):
    def test_YYYYMMDD_maps_to_correct_strptime_pattern(self):
        feed = _make_feed(_make_catalog(), fmt="YYYYMMDD")
        self.assertEqual(feed.strptime_pattern(), "%Y%m%d")

    def test_DDMMYYYY_maps_to_correct_strptime_pattern(self):
        feed = _make_feed(_make_catalog(), fmt="DDMMYYYY")
        self.assertEqual(feed.strptime_pattern(), "%d%m%Y")

    def test_YYYYMMDDHH_maps_to_correct_strptime_pattern(self):
        feed = _make_feed(_make_catalog(), fmt="YYYYMMDDHH")
        self.assertEqual(feed.strptime_pattern(), "%Y%m%d%H")

    def test_YYYYMMDDHHMM_maps_to_correct_strptime_pattern(self):
        feed = _make_feed(_make_catalog(), fmt="YYYYMMDDHHMM")
        self.assertEqual(feed.strptime_pattern(), "%Y%m%d%H%M")

    def test_DDMMYY_maps_to_correct_strptime_pattern(self):
        feed = _make_feed(_make_catalog(), fmt="DDMMYY")
        self.assertEqual(feed.strptime_pattern(), "%d%m%y")

    def test_YYMMDD_maps_to_correct_strptime_pattern(self):
        feed = _make_feed(_make_catalog(), fmt="YYMMDD")
        self.assertEqual(feed.strptime_pattern(), "%y%m%d")


class PushFeedVariablesTests(TestCase):
    def test_variables_are_the_active_variables_of_the_catalog(self):
        catalog = _make_catalog()
        feed = _make_feed(catalog)
        collection = _make_collection(catalog)
        unit = Unit.objects.get_or_create(name="Kelvin", symbol="K")[0]
        active = Variable.objects.create(collection=collection, name="Temp", slug="t2m", unit=unit)
        Variable.objects.create(collection=collection, name="Old", slug="old", is_active=False, unit=unit)
        other = _make_collection(_make_catalog("other"), slug="other-col")
        Variable.objects.create(collection=other, name="Elsewhere", slug="elsewhere", unit=unit)

        self.assertEqual(list(feed.variables()), [active])
