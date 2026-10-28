import json
from datetime import UTC, datetime

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from pint.errors import DimensionalityError

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.core.provisioning import passthrough_sources
from georiva.ingestion.models import PushFeed
from georiva.ingestion.unit_range import convert_range
from georiva.organisations.testing import dial_org, make_organisation

User = get_user_model()


class ConvertRangeTests(SimpleTestCase):
    def test_offset_units_are_not_a_plain_factor(self):
        self.assertEqual(convert_range(273.15, 313.15, "K", "degC"), (0.0, 40.0))

    def test_a_decreasing_conversion_keeps_min_below_max(self):
        low, high = convert_range(0.0, 100.0, "degC", "degF")
        self.assertLess(low, high)

    def test_incompatible_units_raise(self):
        with self.assertRaises(DimensionalityError):
            convert_range(0.0, 1.0, "K", "m")


class ConvertRangeViewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("conv", "c@v.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)

    def test_returns_the_range_in_the_new_unit(self):
        response = self.client.get(
            "/admin/push-feeds/convert-range/",
            {"from_symbol": "K", "to_symbol": "degC", "min": "273.15", "max": "313.15"},
        )
        self.assertEqual(response.json(), {"value_min": 0.0, "value_max": 40.0})

    def test_incompatible_units_are_a_400(self):
        response = self.client.get(
            "/admin/push-feeds/convert-range/",
            {"from_symbol": "K", "to_symbol": "m", "min": "0", "max": "1"},
        )
        self.assertEqual(response.status_code, 400)


class PushFeedEditBase(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("editor", "e@d.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.kelvin = Unit.objects.get_or_create(symbol="K", defaults={"name": "Kelvin"})[0]
        self.celsius = Unit.objects.get_or_create(symbol="degC", defaults={"name": "Celsius"})[0]
        self.metre = Unit.objects.get_or_create(symbol="m", defaults={"name": "Metre"})[0]
        catalog = Catalog.objects.create(organisation=make_organisation(), name="Cat", slug="cat", file_format="grib2")
        self.collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface")
        self.feed = PushFeed.objects.create(catalog=catalog, name="Surface", valid_time_format="YYYYMMDD")
        self.variable = Variable.objects.create(
            collection=self.collection,
            name="2m temperature",
            slug="2t",
            source_unit=self.kelvin,
            unit=self.kelvin,
            value_min=250,
            value_max=320,
        )
        self.url = f"/admin/push-feeds/{self.feed.pk}/edit/"

    def _row(self, **overrides):
        row = {
            "key": str(self.variable.pk),
            "long_name": "2m temperature",
            "unit_id": self.kelvin.pk,
            "value_min": 250,
            "value_max": 320,
            "confirm_unit_change": False,
        }
        row.update(overrides)
        return row

    def _post(self, rows, **settings):
        data = {
            "name": "Surface",
            "valid_time_format": "YYYYMMDD",
            "files_per_run": 1,
            "variables_json": json.dumps(rows),
        }
        data.update(settings)
        return self.client.post(self.url, data)

    def _add_data(self):
        item = Item.objects.create(
            collection=self.collection, time=datetime(2026, 3, 1, tzinfo=UTC), bounds=[0, 0, 1, 1]
        )
        Asset.objects.create(item=item, variable=self.variable, href="x.tif")


class PushFeedEditTests(PushFeedEditBase):
    def test_get_renders_both_units(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "2m temperature")

    def test_feed_settings_are_saved(self):
        response = self._post([self._row()], name="Renamed", files_per_run=3, expected_interval_hours=6)
        self.assertEqual(response.status_code, 302)
        self.feed.refresh_from_db()
        self.assertEqual((self.feed.name, self.feed.files_per_run), ("Renamed", 3))
        self.assertEqual(self.feed.expected_interval.total_seconds(), 6 * 3600)

    def test_the_output_unit_and_range_are_saved(self):
        response = self._post([self._row(unit_id=self.celsius.pk, value_min=-23, value_max=47, long_name="Temp")])
        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.unit, self.celsius)
        self.assertEqual(self.variable.source_unit, self.kelvin)
        self.assertEqual((self.variable.value_min, self.variable.value_max), (-23, 47))
        self.assertEqual(self.variable.name, "Temp")

    def test_renaming_leaves_the_files_own_name_alone(self):
        Variable.objects.filter(pk=self.variable.pk).update(source_long_name="2 metre temperature")
        self.assertContains(self.client.get(self.url), "2 metre temperature")
        self._post([self._row(long_name="Air temp")])
        self.variable.refresh_from_db()
        self.assertEqual((self.variable.name, self.variable.source_long_name), ("Air temp", "2 metre temperature"))

    def test_a_variable_with_no_incoming_unit_cannot_be_saved_until_it_has_one(self):
        Variable.objects.filter(pk=self.variable.pk).update(source_unit=None)
        self.assertEqual(self._post([self._row()]).status_code, 200)

        response = self._post([self._row(incoming_unit_id=self.kelvin.pk)])
        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.source_unit, self.kelvin)

    def test_an_unconvertible_output_unit_is_refused(self):
        response = self._post([self._row(unit_id=self.metre.pk)])
        self.assertEqual(response.status_code, 200)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.unit, self.kelvin)

    def test_changing_the_unit_of_a_variable_with_data_needs_confirmation(self):
        self._add_data()
        response = self._post([self._row(unit_id=self.celsius.pk, value_min=-23, value_max=47)])
        self.assertEqual(response.status_code, 200)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.unit, self.kelvin)

        response = self._post(
            [self._row(unit_id=self.celsius.pk, value_min=-23, value_max=47, confirm_unit_change=True)]
        )
        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.unit, self.celsius)

    def test_an_unchanged_unit_needs_no_confirmation_even_with_data(self):
        self._add_data()
        self.assertEqual(self._post([self._row(value_min=240)]).status_code, 302)

    def test_a_reversed_range_is_refused(self):
        self.assertEqual(self._post([self._row(value_min=400, value_max=300)]).status_code, 200)

    def test_a_variable_of_another_feed_is_refused(self):
        other = Catalog.objects.create(
            organisation=self.feed.catalog.organisation, name="Other", slug="other", file_format="grib2"
        )
        coll = Collection.objects.create(catalog=other, name="C", slug="c")
        foreign = Variable.objects.create(collection=coll, name="x", slug="x", unit=self.kelvin)
        response = self._post([self._row(key=str(foreign.pk), value_min=0, value_max=5)])
        self.assertEqual(response.status_code, 200)
        foreign.refresh_from_db()
        self.assertEqual(foreign.value_max, 1.0)

    def test_the_edit_link_is_on_the_list_and_the_feed_page(self):
        for url in ("/admin/push-feeds/", f"/admin/push-feeds/{self.feed.pk}/"):
            self.assertContains(self.client.get(url), self.url)


class DerivedVariableEditTests(PushFeedEditBase):
    """Adding a wind speed or direction from a U and a V component, on the Edit page."""

    def setUp(self):
        super().setUp()
        self.ms = Unit.objects.get_or_create(symbol="m/s", defaults={"name": "Metre per second"})[0]
        self.kmh = Unit.objects.get_or_create(symbol="km/h", defaults={"name": "Kilometre per hour"})[0]
        for slug in ("u10", "v10"):
            Variable.objects.create(
                collection=self.collection,
                name=slug,
                slug=slug,
                unit=self.ms,
                source_unit=self.ms,
                sources=passthrough_sources(slug),
            )

    def _new(self, **overrides):
        row = {
            "key": "derived-0",
            "new_derived": True,
            "derived": {"transform": "vector_magnitude", "u_source": "u10", "v_source": "v10"},
            "collection_value": str(self.collection.pk),
            "long_name": "10 m wind speed",
            "incoming_symbol": "m/s",
            "unit_id": self.ms.pk,
            "value_min": 0,
            "value_max": 40,
        }
        row.update(overrides)
        return row

    def _existing(self):
        return [
            self._row(key=str(v.pk), long_name=v.name, unit_id=v.unit_id, value_min=v.value_min, value_max=v.value_max)
            for v in self.feed.variables()
        ]

    def test_a_wind_speed_is_created_from_the_two_components(self):
        self.assertEqual(self._post(self._existing() + [self._new()]).status_code, 302)

        speed = Variable.objects.get(slug="10-m-wind-speed")
        self.assertEqual(speed.transform_type, "vector_magnitude")
        self.assertEqual(speed.collection, self.collection)
        self.assertEqual(speed.source_unit, self.ms)
        self.assertEqual(
            [(b.block_type, b.value["source_name"]) for b in speed.sources],
            [
                ("u_component", "u10"),
                ("v_component", "v10"),
            ],
        )

    def test_the_speed_can_be_expressed_in_another_unit(self):
        self._post(self._existing() + [self._new(unit_id=self.kmh.pk, value_min=0, value_max=144)])
        speed = Variable.objects.get(slug="10-m-wind-speed")
        self.assertEqual((speed.source_unit, speed.unit), (self.ms, self.kmh))

    def test_a_direction_is_always_degrees_with_nothing_to_convert(self):
        row = self._new(long_name="10 m wind direction", incoming_symbol="", unit_id=self.ms.pk, value_max=360)
        row["derived"]["transform"] = "vector_direction"
        self.assertEqual(self._post(self._existing() + [row]).status_code, 302)

        direction = Variable.objects.get(slug="10-m-wind-direction")
        self.assertEqual(direction.transform_type, "vector_direction")
        self.assertEqual(direction.unit.name, "Degree")
        self.assertIsNone(direction.source_unit)

    def test_components_in_different_units_are_refused(self):
        Variable.objects.filter(slug="v10").update(source_unit=self.kmh)
        self.assertEqual(self._post(self._existing() + [self._new()]).status_code, 200)
        self.assertFalse(Variable.objects.filter(slug="10-m-wind-speed").exists())

    def test_the_incoming_unit_is_the_components_stored_unit(self):
        # The page may send nothing for it: both components already say what they carry.
        self._post(self._existing() + [self._new(incoming_symbol="")])
        self.assertEqual(Variable.objects.get(slug="10-m-wind-speed").source_unit, self.ms)

    def test_components_with_no_stored_unit_need_one_given(self):
        Variable.objects.filter(slug__in=("u10", "v10")).update(source_unit=None)
        self.assertEqual(self._post(self._existing() + [self._new(incoming_symbol="")]).status_code, 200)

        # Set once on the components themselves, and the derived variable can name its own.
        rows = self._existing()
        for row in rows:
            row["incoming_unit_id"] = self.ms.pk
        self.assertEqual(self._post(rows + [self._new()]).status_code, 302)

    def test_components_with_no_stored_unit_are_assumed_to_be_in_their_label(self):
        # Provisioned before conversion existed: nothing was ever converted, so the label is the truth.
        Variable.objects.filter(slug__in=("u10", "v10")).update(source_unit=None)
        rows = self._existing()
        for row in rows:
            row["incoming_unit_id"] = self.ms.pk
        self._post(rows + [self._new(incoming_symbol="")])
        self.assertEqual(Variable.objects.get(slug="10-m-wind-speed").source_unit, self.ms)

    def test_assumed_components_in_different_labels_need_the_unit_given(self):
        Variable.objects.filter(slug__in=("u10", "v10")).update(source_unit=None)
        Variable.objects.filter(slug="v10").update(unit=self.kmh)
        rows = [
            self._row(key=str(v.pk), long_name=v.name, unit_id=v.unit_id, value_min=0, value_max=10)
            | {"incoming_unit_id": v.unit_id}
            for v in self.feed.variables()
        ]
        self.assertEqual(self._post(rows + [self._new(incoming_symbol="")]).status_code, 200)

    def test_the_edit_page_offers_the_assumed_unit_of_an_unset_component(self):
        Variable.objects.filter(slug="u10").update(source_unit=None)
        options = {o["name"]: o for o in self.client.get(self.url).context["source_options"]}
        self.assertEqual((options["u10"]["symbol"], options["u10"]["assumed"]), ("m/s", True))
        self.assertEqual((options["v10"]["symbol"], options["v10"]["assumed"]), ("m/s", False))

    def test_the_same_component_twice_is_refused(self):
        row = self._new()
        row["derived"]["v_source"] = "u10"
        self.assertEqual(self._post(self._existing() + [row]).status_code, 200)
        self.assertFalse(Variable.objects.filter(slug="10-m-wind-speed").exists())

    def test_a_component_named_by_hand_is_accepted(self):
        row = self._new()
        row["derived"]["u_source"] = "u100"
        self.assertEqual(self._post(self._existing() + [row]).status_code, 302)

    def test_a_name_already_used_in_the_collection_is_refused(self):
        row = self._new(long_name="u10")
        self.assertEqual(self._post(self._existing() + [row]).status_code, 200)

    def test_a_collection_of_another_feed_is_refused(self):
        other = Catalog.objects.create(
            organisation=self.feed.catalog.organisation, name="Other", slug="other-2", file_format="grib2"
        )
        foreign = Collection.objects.create(catalog=other, name="Foreign", slug="foreign")
        row = self._new(collection_value=str(foreign.pk))
        self.assertEqual(self._post(self._existing() + [row]).status_code, 200)
        self.assertFalse(Variable.objects.filter(collection=foreign).exists())

    def test_a_stored_derived_variable_shows_its_components_and_keeps_them_when_edited(self):
        self._post(self._existing() + [self._new()])
        speed = Variable.objects.get(slug="10-m-wind-speed")
        self.assertContains(self.client.get(self.url), "u10")

        rows = [
            self._row(key=str(v.pk), long_name=v.name, unit_id=v.unit_id, value_min=v.value_min, value_max=v.value_max)
            for v in self.feed.variables()
        ]
        for row in rows:
            if row["key"] == str(speed.pk):
                row.update(long_name="Wind speed 10 m", value_max=50)
        self.assertEqual(self._post(rows).status_code, 302)
        speed.refresh_from_db()
        self.assertEqual((speed.name, speed.value_max), ("Wind speed 10 m", 50))
        self.assertEqual([b.value["source_name"] for b in speed.sources], ["u10", "v10"])
