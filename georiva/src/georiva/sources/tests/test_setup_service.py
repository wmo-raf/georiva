"""
Tests for SourceSetupService variable provisioning, focused on the
source_units -> units split that drives ingestion-time unit conversion.
"""
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.core.unit_utils import ureg
from georiva.sources.collection_definitions import (
    CollectionDefinition,
    CollectionVariable,
)
from georiva.sources.models import DataFeed
from georiva.sources.parameters import SourceKey
from georiva.sources.setup_service import SourceSetupService
from georiva.organisations.testing import make_organisation


def _collection():
    catalog = Catalog.objects.create(organisation=make_organisation(), name="Cat", slug="cat", file_format="grib2")
    return Collection.objects.create(name="Col", slug="col", catalog=catalog)


class ProvisionCollectionSlugTests(TestCase):
    """The provisioned raw collection's slug is derived from the definition key
    alone — no catalog prefix (ADR-0010 §5), so the slug matches the key the
    derived-product declarations reference."""

    def test_slug_is_the_definition_key_without_a_catalog_prefix(self):
        service = SourceSetupService()
        catalog = Catalog.objects.create(organisation=make_organisation(), 
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        feed = DataFeed.objects.create(name="Rain Feed", catalog=catalog)
        definition = CollectionDefinition(
            key="chirps-monthly",
            name="CHIRPS Monthly",
            time_resolution="monthly",
            variables=(CollectionVariable(
                key="precip", name="Precipitation", source_units="mm",
                source_variable=SourceKey(name="band_1"),
            ),),
        )

        collection = service.provision_collection(
            catalog=catalog, definition=definition, data_feed=feed,
            config_values={},
        )

        self.assertEqual(collection.slug, "chirps-monthly")


class UpsertVariableUnitsTests(TestCase):
    def setUp(self):
        self.service = SourceSetupService()
        self.collection = _collection()

    def test_source_units_creates_distinct_source_and_output_units(self):
        var_def = CollectionVariable(
            key="2t",
            name="2m Temperature",
            source_units="K",
            output_units="degC",
            source_variable=SourceKey(name="2t"),
            value_range=(-60.0, 60.0),
        )

        variable = self.service._upsert_variable(self.collection, var_def)

        self.assertEqual(variable.unit.symbol, "degC")
        self.assertEqual(variable.source_unit.symbol, "K")
        self.assertNotEqual(variable.unit_id, variable.source_unit_id)

    def test_omitted_output_units_defaults_output_to_source_unit(self):
        var_def = CollectionVariable(
            key="wind",
            name="10m Wind Speed",
            source_units="m/s",
            source_variable=SourceKey(name="10u"),
        )

        variable = self.service._upsert_variable(self.collection, var_def)

        # No output_units declared -> the variable is exposed in its source
        # unit, so source and output units resolve to the same row (no-op).
        self.assertEqual(variable.unit_id, variable.source_unit_id)
        self.assertEqual(variable.unit.symbol, "m/s")

    def test_geopotential_context_converts_specific_energy_to_decametres(self):
        # m2/s2 -> gpdam rides the global geopotential context (divide by g)
        # plus the gpdam definition (divide by 10), in a single pint conversion.
        q = ureg.Quantity(54000.0, "m2 s-2")
        self.assertAlmostEqual(q.to("gpdam").magnitude, 550.65, places=1)


class UpsertVariableSeedVsTuneTests(TestCase):
    """Provisioning seeds value_min/value_max only on create (ADR 0022) — a
    re-run must never overwrite ranges the operator tuned afterwards."""

    def setUp(self):
        self.service = SourceSetupService()
        self.collection = _collection()
        self.var_def = CollectionVariable(
            key="precip",
            name="Precipitation",
            source_units="mm",
            source_variable=SourceKey(name="band_1"),
            value_range=(0.0, 500.0),
        )

    def test_create_seeds_range_from_plugin_declaration(self):
        variable = self.service._upsert_variable(self.collection, self.var_def)

        self.assertEqual(variable.value_min, 0.0)
        self.assertEqual(variable.value_max, 500.0)

    def test_create_falls_back_to_default_range_when_plugin_omits_one(self):
        var_def = CollectionVariable(
            key="ndvi",
            name="NDVI",
            source_units="dimensionless",
            source_variable=SourceKey(name="band_1"),
        )

        variable = self.service._upsert_variable(self.collection, var_def)

        self.assertEqual(variable.value_min, 0.0)
        self.assertEqual(variable.value_max, 1.0)

    def test_reprovision_leaves_operator_tuned_range_untouched(self):
        variable = self.service._upsert_variable(self.collection, self.var_def)
        variable.value_min = -5.0
        variable.value_max = 1200.0
        variable.save()

        self.service._upsert_variable(self.collection, self.var_def)

        variable.refresh_from_db()
        self.assertEqual(variable.value_min, -5.0)
        self.assertEqual(variable.value_max, 1200.0)

    def test_reprovision_still_updates_non_styling_attributes(self):
        variable = self.service._upsert_variable(self.collection, self.var_def)
        variable.value_min = -5.0
        variable.value_max = 1200.0
        variable.save()

        renamed = CollectionVariable(
            key="precip",
            name="Total Precipitation",
            source_units="mm",
            source_variable=SourceKey(name="band_1"),
            value_range=(0.0, 500.0),
        )
        self.service._upsert_variable(self.collection, renamed)

        variable.refresh_from_db()
        self.assertEqual(variable.name, "Total Precipitation")
        self.assertEqual(variable.value_min, -5.0)
        self.assertEqual(variable.value_max, 1200.0)
