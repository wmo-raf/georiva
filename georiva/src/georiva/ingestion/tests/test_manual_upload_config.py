from django.db import IntegrityError
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable


def _make_catalog(slug="cat"):
    return Catalog.objects.create(name=slug, slug=slug, file_format="grib2")


def _make_collection(catalog, slug="col"):
    return Collection.objects.create(name=slug, slug=slug, catalog=catalog)


def _make_config(catalog, name="Surface variables", fmt="YYYYMMDD", is_forecast=False):
    return ManualUploadConfig.objects.create(
        catalog=catalog,
        name=name,
        valid_time_format=fmt,
        is_forecast=is_forecast,
    )


class ManualUploadConfigCreationTests(TestCase):

    def test_config_can_be_created_with_required_fields(self):
        catalog = _make_catalog()
        config = ManualUploadConfig.objects.create(
            catalog=catalog,
            name="Surface variables",
            valid_time_format="YYYYMMDD",
        )
        self.assertEqual(config.catalog, catalog)
        self.assertEqual(config.name, "Surface variables")
        self.assertFalse(config.is_forecast)

    def test_multiple_configs_allowed_per_catalog(self):
        catalog = _make_catalog()
        _make_config(catalog, name="Surface variables")
        _make_config(catalog, name="Pressure levels")
        self.assertEqual(ManualUploadConfig.objects.filter(catalog=catalog).count(), 2)

    def test_is_forecast_defaults_to_false(self):
        config = _make_config(_make_catalog())
        self.assertFalse(config.is_forecast)

    def test_is_forecast_can_be_set_true(self):
        config = _make_config(_make_catalog(), is_forecast=True)
        self.assertTrue(config.is_forecast)


class ValidTimeFormatChoicesTests(TestCase):

    def test_YYYYMMDD_maps_to_correct_strptime_pattern(self):
        config = _make_config(_make_catalog(), fmt="YYYYMMDD")
        self.assertEqual(config.strptime_pattern(), "%Y%m%d")

    def test_DDMMYYYY_maps_to_correct_strptime_pattern(self):
        config = _make_config(_make_catalog(), fmt="DDMMYYYY")
        self.assertEqual(config.strptime_pattern(), "%d%m%Y")

    def test_YYYYMMDDHH_maps_to_correct_strptime_pattern(self):
        config = _make_config(_make_catalog(), fmt="YYYYMMDDHH")
        self.assertEqual(config.strptime_pattern(), "%Y%m%d%H")

    def test_YYYYMMDDHHMM_maps_to_correct_strptime_pattern(self):
        config = _make_config(_make_catalog(), fmt="YYYYMMDDHHMM")
        self.assertEqual(config.strptime_pattern(), "%Y%m%d%H%M")

    def test_DDMMYY_maps_to_correct_strptime_pattern(self):
        config = _make_config(_make_catalog(), fmt="DDMMYY")
        self.assertEqual(config.strptime_pattern(), "%d%m%y")

    def test_YYMMDD_maps_to_correct_strptime_pattern(self):
        config = _make_config(_make_catalog(), fmt="YYMMDD")
        self.assertEqual(config.strptime_pattern(), "%y%m%d")


class ManualUploadConfigVariableTests(TestCase):

    def setUp(self):
        catalog = _make_catalog()
        self.collection = _make_collection(catalog)
        self.config = _make_config(catalog)

    def test_variable_can_be_created_and_retrieved_via_related_manager(self):
        ManualUploadConfigVariable.objects.create(
            config=self.config,
            collection=self.collection,
            variable_name="2t",
            long_name="2m temperature",
            units="K",
        )
        variables = self.config.variables.all()
        self.assertEqual(variables.count(), 1)
        var = variables.first()
        self.assertEqual(var.variable_name, "2t")
        self.assertEqual(var.long_name, "2m temperature")
        self.assertEqual(var.units, "K")

    def test_long_name_and_units_default_to_empty_string(self):
        var = ManualUploadConfigVariable.objects.create(
            config=self.config,
            collection=self.collection,
            variable_name="tp",
        )
        self.assertEqual(var.long_name, "")
        self.assertEqual(var.units, "")

    def test_unique_constraint_on_config_and_variable_name(self):
        ManualUploadConfigVariable.objects.create(
            config=self.config,
            collection=self.collection,
            variable_name="2t",
        )
        with self.assertRaises(IntegrityError):
            ManualUploadConfigVariable.objects.create(
                config=self.config,
                collection=self.collection,
                variable_name="2t",
            )

    def test_same_variable_name_allowed_on_different_configs(self):
        other_config = _make_config(self.config.catalog, name="Other")
        ManualUploadConfigVariable.objects.create(
            config=self.config, collection=self.collection, variable_name="2t"
        )
        ManualUploadConfigVariable.objects.create(
            config=other_config, collection=self.collection, variable_name="2t"
        )
        self.assertEqual(ManualUploadConfigVariable.objects.filter(variable_name="2t").count(), 2)
