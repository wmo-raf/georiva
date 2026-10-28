import json
from datetime import UTC, datetime

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from pint.errors import DimensionalityError

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
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


class PushFeedEditTests(TestCase):
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
