"""
The Harvest Feed Wizard over a store whose fields have an extra axis — a
statistic, an ensemble member, a level — read at one value of it (#830).

Each value an operator ticks of a field becomes a Variable of its own, read at
it: its Selection. Driven through the wizard's own pages; the only fake is the
Upstream.
"""

from django.test import TestCase

from georiva.core.models import Unit, Variable
from georiva.harvest.models import HarvestFeed, VariableMapping
from georiva.harvest.upstream import CubeDimension, StoreField

from .test_harvest_wizard import step
from .test_harvest_wizard_store import StoreWizardMixin

STATISTIC_GRID = ("init_time", "statistic", "lead_time", "latitude", "longitude")
BY_STATISTIC = (1, 1, 1, 721, 1440)
STATISTICS = ("mean", "p10", "p90")
AXES = ("run", "statistic", "step", "y", "x")
DIMENSIONS = {
    "init_time": None,
    "lead_time": 49,
    "statistic": CubeDimension("statistic", kind="other", size=3, values=STATISTICS, unit="1"),
    "latitude": 721,
    "longitude": 1440,
}
MEAN, P90 = 'temperature_2m{"statistic":"mean"}', 'temperature_2m{"statistic":"p90"}'


class StatisticsWizardMixin(StoreWizardMixin):
    """Google's WeatherNext, as dynamical.org keeps it: one map per statistic per step."""

    def setUp(self):
        super().setUp()
        self.arrays = {
            "temperature_2m": (STATISTIC_GRID, BY_STATISTIC, {"title": "2 metre temperature", "unit": "°C"}),
            "wind_u_10m": (STATISTIC_GRID, BY_STATISTIC, {"unit": "m s-1"}),
            "wind_v_10m": (STATISTIC_GRID, BY_STATISTIC, {"unit": "m s-1"}),
        }
        self.store(dimensions=DIMENSIONS)
        self.reach_fields()

    def pick(self, *values, array=0, **typed):
        """Tick the field at `array` at each of `values` of its statistic."""
        picked = {f"pick_{array}_0_{STATISTICS.index(value)}": "on" for value in values}
        return self.tick(array, **picked, **typed)

    def typed(self, index, name, unit=None):
        unit = unit or self.celsius
        return {f"name_{index}": name, f"unit_{index}": unit.pk, f"low_{index}": "-40", f"high_{index}": "50"}


class AFieldWithAnExtraAxisTests(StatisticsWizardMixin, TestCase):
    def test_the_field_is_offered_with_each_value_of_its_axis(self):
        page = self.client.get(step(4))

        self.assertContains(page, 'name="pick_0_0_2"')
        self.assertContains(page, "p90")
        self.assertNotContains(page, "It holds statistics")

    def test_each_value_ticked_is_read_on_its_own(self):
        self.pick("mean", "p90")

        _href, fields, _places = self.upstream.samples_asked[-1]
        self.assertEqual(
            [(field.key, field.axes) for field in fields if field.path == "temperature_2m"],
            [(MEAN, AXES), (P90, AXES)],
        )
        self.assertEqual(fields[0], StoreField("temperature_2m", AXES, (("statistic", "mean"),)))

    def test_each_value_ticked_has_a_card_of_its_own_named_for_it(self):
        page = self.pick("mean", "p90")

        self.assertContains(page, 'data-settings="0-0"')
        self.assertContains(page, 'data-settings="0-2"')
        self.assertContains(page, "2 metre temperature (statistic p90)")
        self.assertNotContains(page, 'data-settings="0-1"')

    def test_a_field_ticked_at_no_value_is_said_so_and_does_not_go_on(self):
        page = self.tick(0)

        self.assertContains(page, "Choose at least one statistic of “2 metre temperature”")
        self.assertFalse(self.upstream.samples_asked)

    def test_each_value_is_saved_as_a_variable_read_at_it(self):
        self.pick("mean", "p90")
        self.assertRedirects(
            self.pick("mean", "p90", **self.typed("0-0", "Temperature"), **self.typed("0-2", "Temperature (warm)")),
            step(5),
        )
        self.save_store()

        feed = HarvestFeed.objects.get()
        mean, p90 = (Variable.objects.get(name=name) for name in ("Temperature", "Temperature (warm)"))
        self.assertEqual(feed.mapped_variables(feed.collection), {MEAN: mean, P90: p90})
        self.assertEqual(
            sorted(VariableMapping.objects.values_list("asset_key", "selection"), key=lambda row: row[1]["statistic"]),
            [("temperature_2m", {"statistic": "mean"}), ("temperature_2m", {"statistic": "p90"})],
        )
        self.assertNotEqual(mean.slug, p90.slug)

    def test_a_wind_is_offered_over_its_parts_at_one_value_and_saved_so(self):
        both = {f"pick_{array}_0_2": "on" for array in (1, 2)}
        self.assertContains(self.tick(1, 2, **both), 'name="speed_1-2"')
        speed = Unit.objects.get(symbol="m/s")
        typed = {**self.typed("1-2", "U", speed), **self.typed("2-2", "V", speed)}

        self.assertRedirects(self.tick(1, 2, **both, **typed, **{"speed_1-2": "on"}), step(5))
        self.save_store()

        speed = Variable.objects.get(transform_type=Variable.TransformType.VECTOR_MAGNITUDE)
        components = speed.vector_components
        self.assertEqual(
            (components.u, components.v), ('wind_u_10m{"statistic":"p90"}', 'wind_v_10m{"statistic":"p90"}')
        )
        self.assertIn("p90", speed.slug)
