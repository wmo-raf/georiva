"""
The Harvest Feed Wizard offers wind speed and direction over a store's u and v (ADR 0048).

When two fields an operator chooses are a vector's eastward and northward parts,
step 4 offers to show the wind's speed and the direction it blows from as well.
Each is a Variable of the collection, computed from the two when read.
"""

from django.test import TestCase

from georiva.core.models import Unit, Variable
from georiva.harvest.models import HarvestFeed, VariableMapping

from .test_harvest_wizard import step
from .test_harvest_wizard_store import RUN_STEP_GRID, WHOLE_FIELD, StoreWizardMixin

U, V = "wind_u_10m", "wind_v_10m"


class WindWizardMixin(StoreWizardMixin):
    #: The unit the store says its v is in.
    v_unit = "m/s"

    def setUp(self):
        super().setUp()
        self.arrays = {
            "temperature_2m": self.arrays["temperature_2m"],
            U: (RUN_STEP_GRID, WHOLE_FIELD, {"title": "10 metre U wind component", "unit": "m/s"}),
            V: (RUN_STEP_GRID, WHOLE_FIELD, {"title": "10 metre V wind component", "unit": self.v_unit}),
        }
        self.upstream.field_ranges.update({U: (-8.3, 17.9), V: (-21.4, 6.0)})
        self.store()
        self.metres_per_second = Unit.objects.get(symbol="m/s")
        self.reach_fields()

    def typed(self, *indexes, **changes):
        typed = {}
        for index in indexes:
            typed.update(
                {
                    f"name_{index}": f"Field {index}",
                    f"unit_{index}": self.metres_per_second.pk,
                    f"low_{index}": "-30",
                    f"high_{index}": "30",
                    f"about_{index}": "",
                }
            )
        typed.update(changes)
        return typed

    def choose_wind(self, **offers):
        self.tick(1, 2)
        return self.tick(1, 2, **self.typed(1, 2, **offers))


class WizardOffersWindTests(WindWizardMixin, TestCase):
    def test_choosing_both_parts_offers_speed_and_direction(self):
        page = self.tick(1, 2)

        self.assertContains(page, 'name="speed_1"')
        self.assertContains(page, 'name="direction_1"')
        self.assertContains(page, "Wind speed 10m")
        self.assertContains(page, "Wind direction 10m")

    def test_choosing_one_part_offers_neither(self):
        page = self.tick(1)

        self.assertNotContains(page, 'name="speed_1"')
        self.assertNotContains(page, 'name="direction_1"')

    def test_speed_and_direction_are_saved_as_variables_over_the_two_parts(self):
        self.assertRedirects(self.choose_wind(speed_1="on", direction_1="on"), step(5))

        self.save_store()

        feed = HarvestFeed.objects.get()
        speed = Variable.objects.get(slug="wind_speed_10m")
        direction = Variable.objects.get(slug="wind_direction_10m")
        self.assertEqual(speed.transform_type, Variable.TransformType.VECTOR_MAGNITUDE)
        self.assertEqual(direction.transform_type, Variable.TransformType.VECTOR_DIRECTION)
        for variable in (speed, direction):
            self.assertEqual((variable.vector_components.u, variable.vector_components.v), (U, V))
            self.assertEqual(variable.collection, feed.collection)
            variable.clean()
        # In the parts' unit; from calm to the strongest part's own reach, as a length of both.
        self.assertEqual((speed.source_unit, speed.unit), (self.metres_per_second, self.metres_per_second))
        self.assertEqual(speed.name, "Wind speed 10m")
        self.assertEqual((speed.value_min, speed.value_max), (0.0, 30.0))
        self.assertEqual((direction.unit.symbol, direction.value_min, direction.value_max), ("°", 0.0, 360.0))
        # Only the arrays are mapped: the two are computed from them.
        self.assertEqual(sorted(VariableMapping.objects.values_list("asset_key", flat=True)), [U, V])

    def test_only_what_is_ticked_is_saved(self):
        self.choose_wind(speed_1="on")

        self.save_store()

        self.assertTrue(Variable.objects.filter(slug="wind_speed_10m").exists())
        self.assertFalse(Variable.objects.filter(slug="wind_direction_10m").exists())

    def test_nothing_is_offered_ticked_until_the_operator_ticks_it(self):
        self.choose_wind()

        self.save_store()

        self.assertEqual(sorted(Variable.objects.values_list("slug", flat=True)), [U, V])

    def test_coming_back_to_the_step_shows_what_was_offered_and_chosen(self):
        self.choose_wind(direction_1="on")

        (pair,) = self.client.get(step(4)).context["pairs"]

        chosen = {offer["kind"]: offer["chosen"] for offer in pair["offers"]}
        self.assertEqual(chosen, {"speed": False, "direction": True})


class WizardRefusesMismatchedWindTests(WindWizardMixin, TestCase):
    v_unit = "km/h"

    def test_parts_stored_in_different_units_are_not_one_wind(self):
        page = self.choose_wind(speed_1="on")

        self.assertEqual(page.status_code, 200)
        self.assertContains(page, "different units")
