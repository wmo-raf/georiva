"""
Tests for SourceSetupService variable provisioning: the source_units -> units
split that drives ingestion-time unit conversion, and the seed-vs-tune policy
(ADR 0022) for value ranges and styling seeds.
"""

from django.test import TestCase

from georiva.core.models import Catalog, Collection, ColorRamp, ColorRampStop
from georiva.core.models.visualization import generate_stops
from georiva.core.unit_utils import ureg
from georiva.organisations.testing import make_organisation
from georiva.sources.collection_definitions import (
    CollectionDefinition,
    CollectionVariable,
    parse_collection_defs,
)
from georiva.sources.models import DataFeed
from georiva.sources.parameters import SourceKey
from georiva.sources.setup_service import SourceSetupService


def _collection():
    catalog = Catalog.objects.create(organisation=make_organisation(), name="Cat", slug="cat", file_format="grib2")
    return Collection.objects.create(name="Col", slug="col", catalog=catalog)


class ProvisionCollectionSlugTests(TestCase):
    """The provisioned raw collection's slug is derived from the definition key
    alone — no catalog prefix (ADR-0010 §5), so the slug matches the key the
    derived-product declarations reference."""

    def test_slug_is_the_definition_key_without_a_catalog_prefix(self):
        service = SourceSetupService()
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        feed = DataFeed.objects.create(name="Rain Feed", catalog=catalog)
        definition = CollectionDefinition(
            key="chirps-monthly",
            name="CHIRPS Monthly",
            time_resolution="monthly",
            variables=(
                CollectionVariable(
                    key="precip",
                    name="Precipitation",
                    source_units="mm",
                    source_variable=SourceKey(name="band_1"),
                ),
            ),
        )

        collection = service.provision_collection(
            catalog=catalog,
            definition=definition,
            data_feed=feed,
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

    def test_reprovision_never_touches_styles_through_public_seam(self):
        # The full acceptance regression at the public entry point: a re-run
        # of provision_collection must leave both the tuned range and the
        # operator's style set exactly as it found them.
        feed = DataFeed.objects.create(name="Feed", catalog=self.collection.catalog)
        var_def = CollectionVariable(
            key="precip",
            name="Precipitation",
            source_units="mm",
            source_variable=SourceKey(name="band_1"),
            value_range=(0.0, 500.0),
            palette="viridis",
        )
        definition = CollectionDefinition(
            key="rain",
            name="Rain",
            time_resolution="daily",
            variables=(var_def,),
        )
        collection = self.service.provision_collection(
            catalog=self.collection.catalog,
            definition=definition,
            data_feed=feed,
            config_values={},
        )
        variable = collection.variables.get(slug="precip")
        style = variable.styles.get()
        tuned_stops = [{"value": -5.0, "color": "#123456"}, {"value": 1200.0, "color": "#654321"}]
        style.stops = tuned_stops
        style.save()

        self.service.provision_collection(
            catalog=self.collection.catalog,
            definition=definition,
            data_feed=feed,
            config_values={},
        )

        style.refresh_from_db()
        self.assertEqual(style.stops, tuned_stops)
        self.assertEqual(variable.styles.count(), 1)

    def test_reprovision_through_public_seam_preserves_tuned_range(self):
        # Same guarantee proven at the public entry point the add-collection
        # action uses, so a refactor of the provisioning path can't silently
        # bypass the seed-only behaviour.
        feed = DataFeed.objects.create(name="Feed", catalog=self.collection.catalog)
        definition = CollectionDefinition(
            key="rain",
            name="Rain",
            time_resolution="daily",
            variables=(self.var_def,),
        )
        collection = self.service.provision_collection(
            catalog=self.collection.catalog,
            definition=definition,
            data_feed=feed,
            config_values={},
        )
        variable = collection.variables.get(slug="precip")
        self.assertEqual(variable.value_max, 500.0)

        variable.value_min = -5.0
        variable.value_max = 1200.0
        variable.save()

        self.service.provision_collection(
            catalog=self.collection.catalog,
            definition=definition,
            data_feed=feed,
            config_values={},
        )

        variable.refresh_from_db()
        self.assertEqual(variable.value_min, -5.0)
        self.assertEqual(variable.value_max, 1200.0)


class UpsertVariableStyleSeedTests(TestCase):
    """Provisioning seeds a new Variable's default style from the plugin
    contract (ADR 0022), precedence ``palette_stops`` > ``palette`` >
    grayscale (no style row). All create-only; degradation warns, never fails."""

    def setUp(self):
        self.service = SourceSetupService()
        self.collection = _collection()

    def _var_def(self, **overrides):
        fields = dict(
            key="precip",
            name="Precipitation",
            source_units="mm",
            source_variable=SourceKey(name="band_1"),
            value_range=(0.0, 500.0),
        )
        fields.update(overrides)
        return CollectionVariable(**fields)

    # -------- palette: ramp name stretched over the declared range --------

    def test_palette_seeds_default_style_from_the_catalog_ramp(self):
        variable = self.service._upsert_variable(self.collection, self._var_def(palette="viridis"))

        style = variable.styles.get()
        self.assertTrue(style.is_default)
        self.assertEqual(style.ramp.name, "viridis")
        self.assertEqual(style.stops, generate_stops(style.ramp, 0.0, 500.0))
        # tile-config serves the default style's snapshot
        self.assertEqual(variable.weather_layers_palette, style.as_weatherlayers_palette())
        # ... all the way down to the payload Titiler reads.
        from georiva.core.machine_plane.palette_cache import build_variable_payload

        payload = build_variable_payload(variable)
        self.assertIn("colormap", payload)
        self.assertEqual(payload["vmin"], 0.0)
        self.assertEqual(payload["vmax"], 500.0)

    def test_palette_prefers_the_org_tier_ramp_over_the_instance_wide_one(self):
        org = self.collection.catalog.organisation
        org_ramp = ColorRamp.objects.create(organisation=org, name="viridis")
        ColorRampStop.objects.create(ramp=org_ramp, hex_value="#111111", sort_order=0)
        ColorRampStop.objects.create(ramp=org_ramp, hex_value="#eeeeee", sort_order=1)

        variable = self.service._upsert_variable(self.collection, self._var_def(palette="viridis"))

        self.assertEqual(variable.styles.get().ramp, org_ramp)

    def test_unknown_ramp_degrades_to_grayscale_with_a_warning(self):
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(self.collection, self._var_def(palette="no-such-ramp"))

        # No style row: serving falls back to grayscale.
        self.assertEqual(variable.styles.count(), 0)
        self.assertEqual(variable.value_max, 500.0)

    # -------- palette_stops: canonical stops, materialized verbatim --------

    def test_palette_stops_seed_the_default_style_verbatim(self):
        stops = ((0.0, "#000000"), (250.0, "#ff0000"), (500.0, "#ffffff"))
        variable = self.service._upsert_variable(
            self.collection,
            self._var_def(value_range=None, palette_stops=stops),
        )

        style = variable.styles.get()
        self.assertTrue(style.is_default)
        self.assertIsNone(style.ramp)
        self.assertEqual(
            style.stops,
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 250.0, "color": "#ff0000"},
                {"value": 500.0, "color": "#ffffff"},
            ],
        )
        # Range is derived from the stops.
        self.assertEqual(variable.value_min, 0.0)
        self.assertEqual(variable.value_max, 500.0)

    def test_palette_stops_take_precedence_over_palette(self):
        stops = ((0.0, "#000000"), (10.0, "#ffffff"))
        variable = self.service._upsert_variable(
            self.collection,
            self._var_def(palette="viridis", palette_stops=stops, value_range=None),
        )

        style = variable.styles.get()
        self.assertIsNone(style.ramp)
        self.assertEqual(len(style.stops), 2)

    def test_declaring_both_range_and_stops_warns_on_disagreement_and_stops_win(self):
        stops = ((0.0, "#000000"), (400.0, "#ffffff"))
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(value_range=(0.0, 500.0), palette_stops=stops),
            )

        self.assertEqual(variable.value_max, 400.0)
        self.assertEqual(variable.styles.count(), 1)

    def test_agreeing_range_and_stops_do_not_warn(self):
        stops = ((0.0, "#000000"), (500.0, "#ffffff"))
        with self.assertNoLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(value_range=(0.0, 500.0), palette_stops=stops),
            )

        self.assertEqual(variable.value_max, 500.0)

    def test_malformed_stops_fall_back_to_the_declared_ramp_with_a_warning(self):
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(palette="viridis", palette_stops=(("low", "#000000"),)),
            )

        style = variable.styles.get()
        self.assertEqual(style.ramp.name, "viridis")
        # Range comes from the declared value_range, not the bad stops.
        self.assertEqual(variable.value_max, 500.0)

    def test_malformed_stops_without_a_ramp_leave_grayscale_with_a_warning(self):
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(palette_stops="not-a-list"),
            )

        self.assertEqual(variable.styles.count(), 0)

    def test_a_single_stop_is_malformed(self):
        # One stop cannot derive a min < max range.
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(palette_stops=((3.0, "#000000"),)),
            )

        self.assertEqual(variable.styles.count(), 0)
        self.assertEqual(variable.value_max, 500.0)

    def test_unsorted_stops_are_normalized_to_ascending_order(self):
        stops = ((500.0, "#ffffff"), (0.0, "#000000"))
        variable = self.service._upsert_variable(self.collection, self._var_def(value_range=None, palette_stops=stops))

        self.assertEqual([s["value"] for s in variable.styles.get().stops], [0.0, 500.0])

    # -------- create-only: re-provision never touches styles --------

    def test_reprovision_never_modifies_an_existing_style(self):
        variable = self.service._upsert_variable(self.collection, self._var_def(palette="viridis"))
        style = variable.styles.get()
        tuned = [{"value": 0.0, "color": "#123456"}, {"value": 42.0, "color": "#654321"}]
        style.stops = tuned
        style.save()

        self.service._upsert_variable(self.collection, self._var_def(palette="plasma"))

        style.refresh_from_db()
        self.assertEqual(style.stops, tuned)
        self.assertEqual(style.ramp.name, "viridis")
        self.assertEqual(variable.styles.count(), 1)

    def test_reprovision_does_not_re_emit_seed_warnings(self):
        # Degradation warnings belong to the create that actually seeds; a
        # re-provision applies no styling, so it must not nag either.
        bad = self._var_def(palette_stops=(("low", "#000000"),))
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            self.service._upsert_variable(self.collection, bad)

        with self.assertNoLogs("georiva.sources.setup_service", level="WARNING"):
            self.service._upsert_variable(self.collection, bad)

    def test_reprovision_does_not_resurrect_a_style_the_operator_deleted(self):
        variable = self.service._upsert_variable(self.collection, self._var_def(palette="viridis"))
        variable.styles.all().delete()

        self.service._upsert_variable(self.collection, self._var_def(palette="viridis"))

        self.assertEqual(variable.styles.count(), 0)


class ParseCollectionDefsPaletteTests(TestCase):
    """The dict shorthand carries the styling seed fields through."""

    def test_palette_and_palette_stops_survive_parsing(self):
        defs = parse_collection_defs(
            {
                "rain": {
                    "name": "Rain",
                    "time_resolution": "daily",
                    "variables": [
                        {
                            "name": "Precipitation",
                            "source_units": "mm",
                            "source_variable": "band_1",
                            "palette": "viridis",
                            "palette_stops": [(0.0, "#000000"), (500.0, "#ffffff")],
                        }
                    ],
                },
            }
        )

        var = defs[0].variables[0]
        self.assertEqual(var.palette, "viridis")
        self.assertEqual(var.palette_stops, ((0.0, "#000000"), (500.0, "#ffffff")))
