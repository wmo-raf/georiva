"""The Variable style layer (ADR 0022): applied, per-variable style snapshots.

Two subjects, each at the model seam:

* the ``VariableStyle`` row — exactly one default per variable at the DB
  level, slugs unique within a variable, the stops snapshot as the
  materialized value→color contract;
* snapshot generation — applying a ramp over a variable's range produces
  absolute stops, in both continuous and stepped modes, and re-applying is an
  explicit act that regenerates from the ramp.
"""

from importlib import import_module

from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase

from georiva.core.models import ColorRamp, ColorRampStop, VariableStyle
from georiva.core.models.visualization import generate_stops
from georiva.organisations.testing import make_org_tree, make_organisation


def make_ramp(hexes, *, ramp_type=ColorRamp.RampType.SEQUENTIAL, positions=None, name="Test ramp"):
    ramp = ColorRamp.objects.create(name=name, ramp_type=ramp_type)
    positions = positions or [None] * len(hexes)
    for i, (hex_value, position) in enumerate(zip(hexes, positions, strict=True)):
        ColorRampStop.objects.create(ramp=ramp, hex_value=hex_value, position=position, sort_order=i)
    return ramp


class VariableStyleRowTests(TestCase):
    """The row itself: default uniqueness, slug uniqueness, validation."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = make_organisation("kenya")
        cls.variable = make_org_tree(cls.kenya)["variable"]

    def test_exactly_one_default_per_variable_is_enforced_by_the_database(self):
        VariableStyle.objects.create(variable=self.variable, name="Official", slug="official", is_default=True)
        with transaction.atomic():
            with self.assertRaises(IntegrityError):
                VariableStyle.objects.create(variable=self.variable, name="Rival", slug="rival", is_default=True)

    def test_any_number_of_non_default_styles_may_coexist(self):
        VariableStyle.objects.create(variable=self.variable, name="Official", slug="official", is_default=True)
        VariableStyle.objects.create(variable=self.variable, name="Analyst", slug="analyst")
        VariableStyle.objects.create(variable=self.variable, name="Draft", slug="draft")
        self.assertEqual(self.variable.styles.count(), 3)

    def test_each_variable_carries_its_own_default(self):
        other = make_org_tree(make_organisation("uganda"))["variable"]
        VariableStyle.objects.create(variable=self.variable, name="Official", slug="official", is_default=True)
        VariableStyle.objects.create(variable=other, name="Official", slug="official", is_default=True)

    def test_a_slug_is_unique_within_its_variable(self):
        VariableStyle.objects.create(variable=self.variable, name="Official", slug="official")
        with transaction.atomic():
            with self.assertRaises(IntegrityError):
                VariableStyle.objects.create(variable=self.variable, name="Official again", slug="official")

    def test_stepped_mode_requires_a_class_count(self):
        style = VariableStyle(
            variable=self.variable,
            name="Classes",
            slug="classes",
            mode=VariableStyle.Mode.STEPPED,
        )
        with self.assertRaises(ValidationError):
            style.full_clean()

    def test_a_malformed_stops_snapshot_is_rejected(self):
        style = VariableStyle(
            variable=self.variable,
            name="Broken",
            slug="broken",
            stops=[{"value": 0.0}],
        )
        with self.assertRaises(ValidationError):
            style.full_clean()

    def test_default_style_resolves_the_default_among_several(self):
        VariableStyle.objects.create(variable=self.variable, name="Analyst", slug="analyst")
        official = VariableStyle.objects.create(
            variable=self.variable, name="Official", slug="official", is_default=True
        )
        self.assertEqual(self.variable.default_style, official)

    def test_a_variable_with_no_styles_has_no_default(self):
        self.assertIsNone(self.variable.default_style)


class SnapshotGenerationTests(TestCase):
    """Applying a ramp over a range materializes absolute value→color stops."""

    def test_continuous_stretches_evenly_spread_colors_over_the_range(self):
        ramp = make_ramp(["#000000", "#808080", "#ffffff"])
        self.assertEqual(
            generate_stops(ramp, 0.0, 100.0),
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#808080"},
                {"value": 100.0, "color": "#ffffff"},
            ],
        )

    def test_continuous_honours_explicit_ramp_positions(self):
        ramp = make_ramp(["#000000", "#ff0000", "#ffffff"], positions=[0, 0.9, 1])
        self.assertEqual(
            generate_stops(ramp, 0.0, 10.0),
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 9.0, "color": "#ff0000"},
                {"value": 10.0, "color": "#ffffff"},
            ],
        )

    def test_continuous_preserves_an_alpha_channel(self):
        # A fully opaque alpha says nothing and is canonicalized away; a
        # meaningful one travels into the snapshot.
        ramp = make_ramp(["#00000000", "#ffffffff"])
        self.assertEqual(
            generate_stops(ramp, 0.0, 1.0),
            [
                {"value": 0.0, "color": "#00000000"},
                {"value": 1.0, "color": "#ffffff"},
            ],
        )

    def test_stepped_duplicates_class_boundaries_into_hard_edges(self):
        # Two classes over 0–100 from a black→white ramp: the first class ends
        # exactly where the second begins, expressed as two stops sharing the
        # boundary value — which is what keeps the edge hard through any
        # linear interpolation downstream.
        ramp = make_ramp(["#000000", "#ffffff"])
        self.assertEqual(
            generate_stops(ramp, 0.0, 100.0, mode=VariableStyle.Mode.STEPPED, steps=2),
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#000000"},
                {"value": 50.0, "color": "#ffffff"},
                {"value": 100.0, "color": "#ffffff"},
            ],
        )

    def test_stepped_samples_intermediate_class_colors_from_the_ramp(self):
        ramp = make_ramp(["#000000", "#ffffff"])
        stops = generate_stops(ramp, 0.0, 90.0, mode=VariableStyle.Mode.STEPPED, steps=3)
        # Middle class color sits halfway along the ramp.
        self.assertEqual(stops[2], {"value": 30.0, "color": "#808080"})
        self.assertEqual(stops[3], {"value": 60.0, "color": "#808080"})

    def test_stepped_cycles_a_qualitative_ramps_colors_instead_of_blending(self):
        ramp = make_ramp(["#111111", "#222222"], ramp_type=ColorRamp.RampType.QUALITATIVE)
        stops = generate_stops(ramp, 0.0, 3.0, mode=VariableStyle.Mode.STEPPED, steps=3)
        self.assertEqual(
            [s["color"] for s in stops],
            ["#111111", "#111111", "#222222", "#222222", "#111111", "#111111"],
        )

    def test_a_ramp_with_no_colors_generates_no_stops(self):
        ramp = ColorRamp.objects.create(name="Empty")
        self.assertEqual(generate_stops(ramp, 0.0, 1.0), [])


class ApplyRampTests(TestCase):
    """Re-applying is explicit: the style regenerates from its ramp and the
    variable's current range, discarding whatever tuning was in the snapshot."""

    @classmethod
    def setUpTestData(cls):
        cls.variable = make_org_tree(make_organisation("kenya"))["variable"]

    def test_apply_ramp_materializes_the_snapshot_from_the_variables_range(self):
        ramp = make_ramp(["#000000", "#ffffff"])
        style = VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            ramp=ramp,
            is_default=True,
        )
        style.apply_ramp()
        # make_org_tree's variable spans 0–50.
        self.assertEqual(
            style.stops,
            [
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#ffffff"},
            ],
        )

    def test_apply_ramp_discards_fine_tuning(self):
        ramp = make_ramp(["#000000", "#ffffff"])
        style = VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            ramp=ramp,
            stops=[{"value": 42.0, "color": "#123456"}],
        )
        style.apply_ramp()
        self.assertNotIn({"value": 42.0, "color": "#123456"}, style.stops)

    def test_apply_ramp_without_a_ramp_is_refused(self):
        style = VariableStyle.objects.create(
            variable=self.variable,
            name="Tuned by hand",
            slug="tuned",
        )
        with self.assertRaises(ValueError):
            style.apply_ramp()


class StyleAsPaletteTests(TestCase):
    """The snapshot serves the same shapes the retired palette served."""

    @classmethod
    def setUpTestData(cls):
        cls.variable = make_org_tree(make_organisation("kenya"))["variable"]

    def test_the_snapshot_converts_to_weatherlayers_pairs(self):
        style = VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            is_default=True,
            stops=[
                {"value": 0.0, "color": "#000000"},
                {"value": 50.0, "color": "#ff000080"},
            ],
        )
        self.assertEqual(
            style.as_weatherlayers_palette(),
            [[0.0, [0, 0, 0]], [50.0, [255, 0, 0, 128]]],
        )

    def test_min_max_come_from_the_snapshot(self):
        style = VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            stops=[
                {"value": 10.0, "color": "#000000"},
                {"value": 20.0, "color": "#ffffff"},
            ],
        )
        self.assertEqual(style.min_max_from_stops(), (10.0, 20.0))

    def test_the_variable_serves_its_default_styles_snapshot(self):
        VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            is_default=True,
            stops=[{"value": 0.0, "color": "#000000"}, {"value": 50.0, "color": "#ffffff"}],
        )
        self.assertEqual(
            self.variable.weather_layers_palette,
            [[0.0, [0, 0, 0]], [50.0, [255, 255, 255]]],
        )

    def test_a_styleless_variable_still_falls_back_to_grayscale(self):
        palette = self.variable.weather_layers_palette
        self.assertEqual(len(palette), 11)
        self.assertEqual(palette[0], [0.0, [0, 0, 0]])
        self.assertEqual(palette[-1], [50.0, [255, 255, 255]])

    def test_the_style_renders_a_css_gradient_for_swatches(self):
        style = VariableStyle.objects.create(
            variable=self.variable,
            name="Official",
            slug="official",
            stops=[
                {"value": 0.0, "color": "#000000"},
                {"value": 25.0, "color": "#ff0000"},
                {"value": 50.0, "color": "#ffffff"},
            ],
        )
        self.assertEqual(
            style.css_gradient(),
            "linear-gradient(to right, #000000 0%, #ff0000 50%, #ffffff 100%)",
        )


#: The palette-retirement transforms, imported from the data migration itself
#: so the tests and the migration can never drift apart. (importlib because
#: the module name starts with a digit.)
_palette_migration = import_module("georiva.core.migrations.0013_migrate_palettes_to_styles")


class PaletteMigrationTests(TestCase):
    """The pure transforms the palette-retirement migration ran.

    The legacy models are gone, so what stays testable is exactly what the
    migration module exposes: the value→position normalization, the verbatim
    stop materialization, and the vocabulary mapping.
    """

    def test_absolute_values_normalize_to_zero_one_positions(self):
        self.assertEqual(
            _palette_migration.normalized_positions([0.0, 5.0, 10.0]),
            [0.0, 0.5, 1.0],
        )

    def test_normalization_preserves_stop_order_not_value_order(self):
        self.assertEqual(_palette_migration.normalized_positions([10.0, 0.0]), [1.0, 0.0])

    def test_a_degenerate_span_spreads_evenly(self):
        self.assertEqual(
            _palette_migration.normalized_positions([7.0, 7.0, 7.0]),
            [0.0, 0.5, 1.0],
        )

    def test_the_snapshot_carries_the_palettes_stops_verbatim(self):
        self.assertEqual(
            _palette_migration.style_stops_from([(0.5, "#112233"), (11.5749, "445566aa")]),
            [
                {"value": 0.5, "color": "#112233"},
                {"value": 11.5749, "color": "#445566aa"},
            ],
        )

    def test_the_categorical_vocabulary_becomes_qualitative(self):
        self.assertEqual(
            _palette_migration.RAMP_TYPE_FOR_PALETTE_TYPE["categorical"],
            "qualitative",
        )

    def test_an_unsluggable_palette_name_still_gets_a_slug(self):
        self.assertEqual(_palette_migration.style_slug_from("温度"), "default")
        self.assertEqual(_palette_migration.style_slug_from("Kenya Rainfall"), "kenya-rainfall")

    def test_a_migrated_ramp_dodges_the_seeded_catalogs_names(self):
        # A legacy global palette named "viridis" collides with the seeded
        # catalog; the migration suffixes rather than fails or overwrites.
        # (`_unique_ramp_name` only reads `.objects`, so the live model
        # stands in for the historical one.)
        self.assertEqual(
            _palette_migration._unique_ramp_name(ColorRamp, None, "viridis"),
            "viridis (migrated)",
        )

    def test_an_org_palette_may_share_a_name_with_the_seeded_catalog(self):
        # The tiers have separate namespaces: an organisation's own "viridis"
        # does not collide with the instance-wide one.
        organisation = make_organisation("kenya")
        self.assertEqual(
            _palette_migration._unique_ramp_name(ColorRamp, organisation.pk, "viridis"),
            "viridis",
        )

    def test_an_uncontested_name_survives_unsuffixed(self):
        self.assertEqual(
            _palette_migration._unique_ramp_name(ColorRamp, None, "Kenya Rainfall"),
            "Kenya Rainfall",
        )
