"""
A store harvest indexes a vector Variable over two of its mapped arrays (ADR 0048).

Wind speed and direction are not arrays of the store: they are a collection's
Variables computed from its u and v when read. The harvest gives such a
Variable a Store asset at each step where both of its arrays are held, and
judges it as it judges every other Store asset.
"""

import json

from django.core.exceptions import ValidationError
from django.test import TestCase
from django.urls import reverse
from django_redis import get_redis_connection

from georiva.core.machine_plane import remote_assets
from georiva.core.models import Asset, Collection, Unit, Variable
from georiva.harvest.models import HarvestFeed, UpstreamShape, VariableMapping
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.testing import dial_org
from georiva.organisations.tests.factories import PASSWORD, grant_everything, make_user

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


class VectorVariableCleanTests(VectorFeedMixin, TestCase):
    """A vector Variable of a store collection names two of the feed's mapped arrays."""

    def unsaved(self, sources, transform=Variable.TransformType.VECTOR_MAGNITUDE):
        unit = Unit.objects.get(symbol="°C")
        return Variable(
            collection=self.feed.collection,
            slug="wind-speed-10m",
            name="Wind speed",
            source_unit=unit,
            unit=unit,
            transform_type=transform,
            sources=[{"type": kind, "value": {"source_name": name}} for kind, name in sources],
        )

    def errors(self, variable) -> str:
        with self.assertRaises(ValidationError) as refused:
            variable.clean()
        return " ".join(refused.exception.message_dict.get("sources", []))

    def test_one_over_two_mapped_arrays_is_clean(self):
        self.unsaved([("u_component", U), ("v_component", V)]).clean()

    def test_one_missing_its_v_is_refused(self):
        self.assertIn("v_component", self.errors(self.unsaved([("u_component", U)])))

    def test_one_over_an_array_the_feed_does_not_map_is_refused_naming_it(self):
        said = self.errors(self.unsaved([("u_component", U), ("v_component", "wind_v_100m")]))

        self.assertIn("wind_v_100m", said)

    def test_a_passthrough_variable_of_a_store_collection_still_needs_no_source(self):
        self.unsaved([], transform=Variable.TransformType.PASSTHROUGH).clean()

    def test_a_mapped_variable_cannot_be_made_a_vector(self):
        # It shows its own array as it is: a transform on it would be ignored by every reader.
        self.u.transform_type = Variable.TransformType.VECTOR_MAGNITUDE
        self.u.sources = [
            {"type": "u_component", "value": {"source_name": U}},
            {"type": "v_component", "value": {"source_name": V}},
        ]

        with self.assertRaises(ValidationError) as refused:
            self.u.clean()

        self.assertIn("transform_type", refused.exception.message_dict)

    def test_one_over_parts_stored_in_different_units_is_refused(self):
        kelvin, _ = Unit.objects.get_or_create(symbol="K", defaults={"name": "Kelvin"})
        Variable.objects.filter(pk=self.v.pk).update(source_unit=kelvin)

        for transform in (Variable.TransformType.VECTOR_MAGNITUDE, Variable.TransformType.VECTOR_DIRECTION):
            with self.subTest(transform):
                said = self.errors(self.unsaved([("u_component", U), ("v_component", V)], transform=transform))
                self.assertIn("unit", said)

    def test_a_speed_stored_in_another_unit_than_its_parts_is_refused(self):
        speed = self.unsaved([("u_component", U), ("v_component", V)])
        speed.source_unit = speed.unit = Unit.objects.get_or_create(symbol="K", defaults={"name": "Kelvin"})[0]

        with self.assertRaises(ValidationError) as refused:
            speed.clean()

        self.assertIn("source_unit", refused.exception.message_dict)


class StoreCollectionEditorTests(VectorFeedMixin, TestCase):
    """The collection editor offers a store collection's Variables a transform and its u and v."""

    def setUp(self):
        super().setUp()
        user = grant_everything(make_user("amina"))
        OrganisationMembership.objects.create(
            user=user, organisation=self.organisation, role=OrganisationMembership.Role.ADMIN
        )
        dial_org(self.client)
        self.client.login(username="amina", password=PASSWORD)

    def test_the_transform_and_its_parts_are_offered(self):
        form = self.client.get(reverse("collection:edit", args=[self.feed.collection.pk])).context["form"]
        formset = form.formsets["variables"]

        for child in [*formset.forms, formset.empty_form]:
            self.assertIn("transform_type", child.fields)
            self.assertIn("sources", child.fields)

    def test_an_items_harvest_still_offers_neither(self):
        HarvestFeed.objects.filter(pk=self.feed.pk).update(upstream_shape=UpstreamShape.ITEMS)

        form = self.client.get(reverse("collection:edit", args=[self.feed.collection.pk])).context["form"]

        for child in form.formsets["variables"].forms:
            self.assertNotIn("transform_type", child.fields)
            self.assertNotIn("sources", child.fields)


class VectorPublishedEntryTests(VectorFeedMixin, TestCase):
    """What Titiler is told of a vector Store asset: the store, and the two arrays it is computed from."""

    def setUp(self):
        super().setUp()
        self.speed = self.vector()
        self.hold(U, 0)
        self.hold(V, 0)
        self.harvest()

    def test_its_entry_names_its_u_and_v_and_which_quantity_it_is(self):
        entry = remote_assets.remote_source(self.speed, self.run, self.run)

        self.assertEqual(
            entry,
            {
                "store": STORE,
                "vector": {"u": U, "v": V, "transform": "vector_magnitude"},
                "chunk_locations": {},
                "allow_http": False,
            },
        )

    def test_an_arrays_entry_still_names_its_array(self):
        entry = remote_assets.remote_source(self.u, self.run, self.run)

        self.assertEqual(entry["array"], U)
        self.assertNotIn("vector", entry)

    def test_the_harvest_publishes_the_entry_to_the_variables_hash(self):
        published = get_redis_connection("default").hget(
            remote_assets.variable_key(self.speed), remote_assets.timestep_field(self.run, self.run)
        )

        self.assertEqual(json.loads(published)["vector"]["v"], V)


class VectorStacTests(VectorFeedMixin, TestCase):
    def test_stac_names_the_two_arrays_a_vector_asset_is_computed_from(self):
        dial_org(self.client, self.organisation.slug)
        self.vector()
        self.hold(U, 0)
        self.hold(V, 0)
        self.harvest()
        Collection.objects.filter(pk=self.feed.collection.pk).update(visibility=Collection.Visibility.PUBLIC)

        response = self.client.get(reverse("stac:items", args=["gfs", self.feed.collection.slug, "wind-speed-10m"]))

        self.assertEqual(response.status_code, 200, response.content)
        asset = response.json()["features"][0]["assets"]["wind-speed-10m_zarr"]
        self.assertEqual(asset["href"], STORE)
        self.assertNotIn("georiva:array", asset)
        self.assertEqual(asset["georiva:vector"], {"u": U, "v": V, "transform": "vector_magnitude"})
