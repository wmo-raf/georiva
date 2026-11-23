"""
A store harvest indexes a vector Variable over two of its mapped arrays (ADR 0048).

Wind speed and direction are not arrays of the store: they are a collection's
Variables computed from its u and v when read. The harvest gives such a
Variable a Store asset at each step where both of its arrays are held, and
judges it as it judges every other Store asset.
"""

from django.test import TestCase

from georiva.core.models import Asset, Unit, Variable
from georiva.harvest.models import VariableMapping

from .test_store_harvest import RUN_STEP_GRID, STORE, WHOLE_FIELD, StoreFeedMixin, hours
from .test_store_runs import STEPS, StoreRunsMixin

U, V = "wind_u_10m", "wind_v_10m"


class VectorFeedMixin(StoreFeedMixin):
    arrays = {
        "temperature_2m": (RUN_STEP_GRID, WHOLE_FIELD),
        U: (RUN_STEP_GRID, WHOLE_FIELD),
        V: (RUN_STEP_GRID, WHOLE_FIELD),
    }

    def setUp(self):
        super().setUp()
        self.u = self.map_variable(U, "wind-u-10m")
        self.v = self.map_variable(V, "wind-v-10m")

    def vector(self, slug="wind-speed-10m", transform=Variable.TransformType.VECTOR_MAGNITUDE, u=U, v=V):
        unit, _ = Unit.objects.get_or_create(symbol="m/s", defaults={"name": "Metres per second"})
        return Variable.objects.create(
            collection=self.feed.collection,
            slug=slug,
            name=slug.title(),
            source_unit=unit,
            unit=unit,
            transform_type=transform,
            sources=[
                {"type": "u_component", "value": {"source_name": u}},
                {"type": "v_component", "value": {"source_name": v}},
            ],
        )


class VectorHarvestTests(VectorFeedMixin, TestCase):
    def test_a_vector_variable_has_an_asset_wherever_both_its_arrays_are_held(self):
        speed = self.vector()
        self.hold(U, 0, 3, 6)
        self.hold(V, 0, 3)

        result = self.harvest()

        self.assertEqual(self.steps(speed), [(self.run, 0), (self.run, 3)])
        self.assertEqual(result.indexed, 3 + 2 + 2)

    def test_its_asset_points_at_the_store_and_names_no_array_of_its_own(self):
        speed = self.vector()
        self.hold(U, 0)
        self.hold(V, 0)

        self.harvest()

        asset = Asset.objects.get(variable=speed)
        self.assertEqual((asset.href, asset.format, asset.store_array), (STORE, Asset.Format.ZARR, ""))
        self.assertEqual(asset.harvested_by_id, self.feed.pk)
        # Both on the one item of that step, with u's and v's own assets.
        self.assertEqual(Asset.objects.filter(item=asset.item).count(), 3)

    def test_speed_and_direction_are_both_indexed(self):
        speed = self.vector()
        direction = self.vector("wind-direction-10m", Variable.TransformType.VECTOR_DIRECTION)
        self.hold(U, 0)
        self.hold(V, 0)

        self.harvest()

        self.assertEqual(self.steps(speed), [(self.run, 0)])
        self.assertEqual(self.steps(direction), [(self.run, 0)])

    def test_a_harvest_of_the_same_window_twice_changes_nothing(self):
        self.vector()
        self.hold(U, 0, 3)
        self.hold(V, 0, 3)
        self.harvest()
        before = list(Asset.objects.order_by("pk").values_list("pk", "modified"))

        result = self.harvest()

        self.assertEqual((result.indexed, result.updated, result.removed), (0, 0, 0))
        self.assertEqual(list(Asset.objects.order_by("pk").values_list("pk", "modified")), before)

    def test_a_step_one_of_its_arrays_no_longer_holds_loses_its_row(self):
        speed = self.vector()
        self.hold(U, 0, 3)
        self.hold(V, 0, 3)
        self.harvest()

        self.upstream.release(V, self.run + hours(3), run=self.run)
        self.harvest()

        self.assertEqual(self.steps(speed), [(self.run, 0)])
        self.assertEqual(self.steps(self.u), [(self.run, 0), (self.run, 3)])

    def test_a_vector_over_an_array_the_feed_does_not_map_is_not_indexed(self):
        VariableMapping.objects.filter(variable=self.v).delete()
        speed = self.vector()
        self.hold(U, 0)
        self.hold(V, 0)

        self.harvest()

        self.assertEqual(self.steps(speed), [])

    def test_a_passthrough_variable_with_no_mapping_is_not_indexed(self):
        unit = Unit.objects.get(symbol="°C")
        loose = Variable.objects.create(
            collection=self.feed.collection, slug="loose", name="Loose", source_unit=unit, unit=unit
        )
        self.hold(U, 0)
        self.hold(V, 0)

        self.harvest()

        self.assertEqual(self.steps(loose), [])


class VectorRunTests(VectorFeedMixin, StoreRunsMixin, TestCase):
    def hold_both(self):
        self.hold_run("temperature_2m")
        self.hold_run(U)
        self.hold_run(V)

    def test_a_run_its_arrays_hold_whole_closes_with_the_vector_whole_too(self):
        speed = self.vector()
        self.hold_both()

        self.harvest()

        self.assertTrue(self.record().is_complete)
        self.assertEqual(self.steps(speed), [(self.run, 3 * n) for n in range(STEPS)])

    def test_a_vector_declared_after_its_run_closed_is_completed_on_the_next_harvest(self):
        self.hold_both()
        self.harvest()
        self.assertTrue(self.record().is_complete)

        speed = self.vector()
        self.harvest()

        self.assertEqual(self.steps(speed), [(self.run, 3 * n) for n in range(STEPS)])
        self.assertTrue(self.record().is_complete)
