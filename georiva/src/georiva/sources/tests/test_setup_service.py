"""
Tests for SourceSetupService variable provisioning: the source_units -> units
split that drives ingestion-time unit conversion, and the seed-vs-tune policy
(ADR 0022) for value ranges and styling seeds.
"""

from django.test import TestCase

from georiva.core.models import Catalog, Collection, ColorRamp, ColorRampStop, StyleTemplate, Unit
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

    def test_a_reprovision_never_changes_the_source_unit_of_a_variable_with_data(self):
        """The bytes already held say what they hold (ADR 0046 §8): a plugin
        that now declares another source unit does not relabel them."""
        from datetime import UTC, datetime

        from georiva.core.models import Asset, Item

        declared = self._kelvin_served_in_celsius()
        variable = self.service._upsert_variable(self.collection, declared)
        item = Item.objects.create(collection=self.collection, time=datetime(2026, 3, 1, tzinfo=UTC))
        Asset.objects.create(item=item, variable=variable, href="x/1.tif")

        redeclared = CollectionVariable(
            key="2t",
            name="2m Temperature",
            source_units="degC",
            output_units="degF",
            source_variable=SourceKey(name="2t"),
        )
        with self.assertLogs("georiva.sources.setup_service", "WARNING") as logged:
            self.service._upsert_variable(self.collection, redeclared)

        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "K")
        self.assertEqual(variable.unit.symbol, "degF")
        self.assertIn("Correct source unit", "\n".join(logged.output))

    def test_a_plugin_declares_a_variable_of_water(self):
        """kg m-2 is mm only of water (ADR 0046 §6); the plugin's declaration is the confirmation."""
        variable = self.service._upsert_variable(
            self.collection,
            CollectionVariable(
                key="tp",
                name="Total precipitation",
                source_units="kg m-2",
                output_units="mm",
                water=True,
                source_variable=SourceKey(name="tp"),
                value_range=(0.0, 100.0),
            ),
        )

        self.assertTrue(variable.water)
        self.assertEqual(variable.value_range, (0.0, 100.0))

    def test_a_reprovision_updates_the_source_unit_of_a_variable_without_data(self):
        variable = self.service._upsert_variable(self.collection, self._kelvin_served_in_celsius())

        self.service._upsert_variable(
            self.collection,
            CollectionVariable(
                key="2t", name="2m Temperature", source_units="degC", source_variable=SourceKey(name="2t")
            ),
        )

        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "degC")

    # -------- seeds are served-unit values, stored converted (ADR 0046 §3) --------

    def _kelvin_served_in_celsius(self, **fields):
        return CollectionVariable(
            key="2t",
            name="2m Temperature",
            source_units="K",
            output_units="degC",
            source_variable=SourceKey(name="2t"),
            **fields,
        )

    def test_a_seeded_range_is_read_in_the_output_unit_and_stored_in_the_source_unit(self):
        variable = self.service._upsert_variable(
            self.collection, self._kelvin_served_in_celsius(value_range=(-40.0, 50.0))
        )

        self.assertEqual(variable.value_range, (233.15, 323.15))
        self.assertEqual(variable.served_range, (-40.0, 50.0))

    def test_seeded_stops_are_read_in_the_output_unit_and_stored_in_the_source_unit(self):
        variable = self.service._upsert_variable(
            self.collection,
            self._kelvin_served_in_celsius(palette_stops=((0.0, "#0000ff"), (30.0, "#ff0000"))),
        )

        style = variable.styles.get()
        self.assertEqual(style.stops, [{"value": 273.15, "color": "#0000ff"}, {"value": 303.15, "color": "#ff0000"}])
        self.assertEqual(variable.value_range, (273.15, 303.15))

    def test_a_seeded_ramp_is_laid_over_the_output_unit_range_and_stored_converted(self):
        variable = self.service._upsert_variable(
            self.collection, self._kelvin_served_in_celsius(value_range=(-40.0, 50.0), palette="viridis")
        )

        stops = variable.styles.get().stops
        self.assertEqual((stops[0]["value"], stops[-1]["value"]), (233.15, 323.15))

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


class UpsertVariableStyleTemplateSeedTests(TestCase):
    """A plugin names a Style template in its collection definition (#792):
    a new Variable gets the template's stepped default style, in its source
    unit, and the template's range and curve. Precedence ``palette_stops`` >
    ``style_template`` > ``palette`` > grayscale; create-only."""

    def setUp(self):
        self.service = SourceSetupService()
        self.collection = _collection()

    def _var_def(self, **overrides):
        fields = dict(
            key="precip",
            name="Precipitation",
            source_units="mm",
            source_variable=SourceKey(name="band_1"),
        )
        fields.update(overrides)
        return CollectionVariable(**fields)

    def test_a_template_seeds_the_default_style_range_and_curve(self):
        variable = self.service._upsert_variable(
            self.collection, self._var_def(style_template="precipitation-0-5-to-200mm")
        )

        style = variable.styles.get()
        self.assertTrue(style.is_default)
        self.assertEqual(style.mode, "stepped")
        self.assertEqual(style.template.slug, "precipitation-0-5-to-200mm")
        self.assertEqual(style.steps, 17)
        self.assertEqual(
            style.stops[:3],
            [
                {"value": 0.5, "color": "#f2a6f2"},
                {"value": 1.0, "color": "#f2a6f2"},
                {"value": 1.0, "color": "#d98cd9"},
            ],
        )
        self.assertEqual(style.stops[-1], {"value": 200.0, "color": "#cc0000"})
        self.assertEqual((style.under, style.over), ("transparent", "clamp"))
        self.assertEqual((variable.value_min, variable.value_max), (0.5, 200.0))
        self.assertEqual(variable.scale_type, "sqrt")

    def test_the_template_is_converted_into_the_source_unit(self):
        # A °C template on a Variable stored in kelvin and served in °C.
        variable = self.service._upsert_variable(
            self.collection,
            self._var_def(
                key="t2m",
                name="2 m temperature",
                source_units="K",
                output_units="°C",
                style_template="temperature-4c-bands",
            ),
        )

        style = variable.styles.get()
        self.assertAlmostEqual(style.stops[0]["value"], 241.15)
        self.assertAlmostEqual(style.stops[-1]["value"], 317.15)
        self.assertAlmostEqual(variable.value_min, 241.15)
        self.assertAlmostEqual(variable.value_max, 317.15)
        self.assertEqual(variable.scale_type, "linear")

    def test_palette_stops_take_precedence_over_a_template(self):
        stops = ((0.0, "#000000"), (10.0, "#ffffff"))
        variable = self.service._upsert_variable(
            self.collection,
            self._var_def(palette_stops=stops, style_template="precipitation-0-5-to-200mm"),
        )

        style = variable.styles.get()
        self.assertIsNone(style.template)
        self.assertEqual(len(style.stops), 2)
        self.assertEqual((variable.value_min, variable.value_max), (0.0, 10.0))
        self.assertEqual(variable.scale_type, "linear")

    def test_a_template_takes_precedence_over_a_palette(self):
        variable = self.service._upsert_variable(
            self.collection,
            self._var_def(palette="viridis", style_template="precipitation-0-5-to-200mm"),
        )

        style = variable.styles.get()
        self.assertIsNone(style.ramp)
        self.assertEqual(style.template.slug, "precipitation-0-5-to-200mm")

    def test_the_org_template_wins_over_an_instance_wide_one_of_the_same_slug(self):
        organisation = self.collection.catalog.organisation
        own = StyleTemplate.objects.create(
            organisation=organisation,
            name="House rain",
            slug="precipitation-0-5-to-200mm",
            levels=[1, 10, 100],
            colors=["#00ff00", "#0000ff"],
            unit=Unit.objects.get(symbol="mm"),
            scale_type="log",
        )

        variable = self.service._upsert_variable(
            self.collection, self._var_def(style_template="precipitation-0-5-to-200mm")
        )

        self.assertEqual(variable.styles.get().template, own)
        self.assertEqual((variable.value_min, variable.value_max), (1.0, 100.0))
        self.assertEqual(variable.scale_type, "log")

    def test_another_organisations_template_is_not_resolved(self):
        StyleTemplate.objects.create(
            organisation=make_organisation("other-org"),
            name="Theirs",
            slug="theirs",
            levels=[1, 10, 100],
            colors=["#00ff00", "#0000ff"],
            unit=Unit.objects.get(symbol="mm"),
        )

        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(self.collection, self._var_def(style_template="theirs"))

        self.assertEqual(variable.styles.count(), 0)

    def test_a_single_band_template_seeds_no_style_with_a_warning(self):
        # A valid template, but a stepped style needs at least two bands:
        # provisioning never fails on styling (ADR 0022).
        StyleTemplate.objects.create(
            organisation=self.collection.catalog.organisation,
            name="One band",
            slug="one-band",
            levels=[1, 10],
            colors=["#00ff00"],
            unit=Unit.objects.get(symbol="mm"),
        )

        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(self.collection, self._var_def(style_template="one-band"))

        self.assertEqual(variable.styles.count(), 0)

    def test_an_unknown_template_falls_back_to_the_palette_with_a_warning(self):
        with self.assertLogs("georiva.sources.setup_service", level="WARNING") as logs:
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(palette="viridis", value_range=(0.0, 50.0), style_template="no-such-template"),
            )

        self.assertIn("no-such-template", "\n".join(logs.output))
        style = variable.styles.get()
        self.assertEqual(style.ramp.name, "viridis")
        self.assertEqual((variable.value_min, variable.value_max), (0.0, 50.0))
        self.assertEqual(variable.scale_type, "linear")

    def test_a_template_whose_unit_does_not_convert_falls_back_with_a_warning(self):
        # A precipitation (mm) template on a temperature Variable.
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(
                    source_units="K",
                    value_range=(200.0, 330.0),
                    style_template="precipitation-0-5-to-200mm",
                ),
            )

        self.assertEqual(variable.styles.count(), 0)
        self.assertEqual((variable.value_min, variable.value_max), (200.0, 330.0))
        self.assertEqual(variable.scale_type, "linear")

    def test_a_declared_range_that_disagrees_with_the_template_warns_and_the_template_wins(self):
        with self.assertLogs("georiva.sources.setup_service", level="WARNING"):
            variable = self.service._upsert_variable(
                self.collection,
                self._var_def(value_range=(0.0, 500.0), style_template="precipitation-0-5-to-200mm"),
            )

        self.assertEqual((variable.value_min, variable.value_max), (0.5, 200.0))

    def test_reprovision_never_touches_the_seeded_style_range_or_curve(self):
        variable = self.service._upsert_variable(
            self.collection, self._var_def(style_template="precipitation-0-5-to-200mm")
        )
        style = variable.styles.get()
        tuned = [{"value": 0.0, "color": "#123456"}, {"value": 42.0, "color": "#654321"}]
        style.stops = tuned
        style.save()
        variable.value_min, variable.value_max, variable.scale_type = 0.0, 42.0, "linear"
        variable.save()

        self.service._upsert_variable(self.collection, self._var_def(style_template="temperature-4c-bands"))

        variable.refresh_from_db()
        style.refresh_from_db()
        self.assertEqual(style.stops, tuned)
        self.assertEqual((variable.value_min, variable.value_max), (0.0, 42.0))
        self.assertEqual(variable.scale_type, "linear")
        self.assertEqual(variable.styles.count(), 1)

    def test_style_template_survives_parsing(self):
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
                            "style_template": "precipitation-0-5-to-200mm",
                        }
                    ],
                },
            }
        )

        self.assertEqual(defs[0].variables[0].style_template, "precipitation-0-5-to-200mm")


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
