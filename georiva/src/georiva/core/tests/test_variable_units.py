"""A Variable states what its bytes hold and how to convert them (ADR 0046).

``source_unit`` is what the bytes hold, ``unit`` what readers are served; the
Variable keeps the positive affine conversion between them as ``scale`` and
``offset``, recomputed whenever it is saved, and refuses one that is not.
"""

import importlib
from datetime import UTC, datetime

from django.apps import apps
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import IntegrityError, connection
from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from wagtail.test.utils.form_data import inline_formset, nested_form_data, streamfield

from georiva.core.models import Asset, Catalog, Collection, Item, SourceUnitCorrected, Unit, Variable, VariableStyle
from georiva.core.unit_utils import Conversion, ureg
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.testing import dial_org, make_organisation
from georiva.organisations.tests.factories import PASSWORD, grant_everything, make_user

User = get_user_model()
migration = importlib.import_module("georiva.core.migrations.0023_variable_source_unit_scale_offset_water")


def unit(symbol):
    found, _ = Unit.objects.get_or_create(symbol=symbol, defaults={"name": f"test {symbol}"})
    return found


class VariableUnitsFixture:
    @classmethod
    def setUpTestData(cls):
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="Models", slug="models", file_format="grib2"
        )
        cls.collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface")

    def variable(self, source, served, slug="v", **fields):
        return Variable(
            collection=self.collection,
            slug=slug,
            name=slug,
            source_unit=unit(source),
            unit=unit(served),
            sources=[("primary", {"source_name": slug})],
            **fields,
        )

    def saved(self, source, served, **fields):
        variable = self.variable(source, served, **fields)
        variable.full_clean()
        variable.save()
        variable.refresh_from_db()
        return variable


class ScaleAndOffsetTests(VariableUnitsFixture, TestCase):
    def test_kelvin_served_in_celsius(self):
        variable = self.saved("K", "degC")

        self.assertAlmostEqual(variable.scale, 1.0)
        self.assertAlmostEqual(variable.offset, -273.15)

    def test_scale_and_offset_carry_no_float_dust(self):
        # pint gives 1.7999999999999972 and 31.999999999999936 for °C → °F; every
        # reader applies these numbers, so they are stored as the conversion is.
        variable = self.saved("degC", "degF")

        self.assertEqual((variable.scale, variable.offset), (1.8, 32.0))

    def test_pascal_served_in_hectopascal(self):
        variable = self.saved("Pa", "hPa")

        self.assertAlmostEqual(variable.scale, 0.01)
        self.assertAlmostEqual(variable.offset, 0.0)

    def test_kelvin_served_in_fahrenheit(self):
        variable = self.saved("K", "degF")

        self.assertAlmostEqual(variable.scale, 1.8)
        self.assertAlmostEqual(variable.offset, -459.67)

    def test_served_as_produced_is_the_identity(self):
        variable = self.saved("K", "K")

        self.assertEqual((variable.scale, variable.offset), (1.0, 0.0))

    def test_changing_the_served_unit_recomputes_on_save(self):
        variable = self.saved("K", "K")

        variable.unit = unit("degC")
        variable.save(update_fields=["unit"])
        variable.refresh_from_db()

        self.assertAlmostEqual(variable.offset, -273.15)

    def test_a_difference_is_scaled_and_never_offset(self):
        variable = self.saved("K", "delta_degF")

        self.assertAlmostEqual(variable.scale, 1.8)
        self.assertAlmostEqual(variable.offset, 0.0)

    def test_the_delta_units_are_among_the_unit_choices(self):
        self.assertTrue(Unit.objects.filter(symbol="delta_degC").exists())


class SourceUnitMigrationTests(VariableUnitsFixture, TestCase):
    """0023 says what every existing Variable's bytes hold: nothing is deployed,
    so none were converted, and a Variable with no source unit holds its served one."""

    def test_an_empty_source_unit_is_filled_from_the_served_unit(self):
        variable = self.saved("K", "K")
        with connection.cursor() as cursor:  # rolled back with the test: Postgres DDL is transactional
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cursor.execute(f"ALTER TABLE {Variable._meta.db_table} ALTER COLUMN source_unit_id DROP NOT NULL")
        Variable.objects.filter(pk=variable.pk).update(source_unit=None, unit=unit("degC"), offset=0.0)

        migration.fill_source_units(apps, None)

        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "degC")
        self.assertEqual((variable.scale, variable.offset), (1.0, 0.0))

    def test_every_variable_gets_its_scale_and_offset(self):
        variable = self.saved("K", "K")
        Variable.objects.filter(pk=variable.pk).update(unit=unit("degC"))

        migration.fill_source_units(apps, None)

        variable.refresh_from_db()
        self.assertAlmostEqual(variable.offset, -273.15)

    def test_a_range_and_stops_written_in_the_served_unit_keep_their_meaning(self):
        # Before ADR 0046 a range and a style's stops were written in the served
        # unit; a K source served in °C had −60…60 meaning °C.
        variable = self.saved("K", "degC")
        Variable.objects.filter(pk=variable.pk).update(value_min=-60.0, value_max=60.0, scale=1.0, offset=0.0)
        style = VariableStyle.objects.create(
            variable=variable,
            name="Default",
            slug="default",
            is_default=True,
            stops=[{"value": -60.0, "color": "#0000ff"}, {"value": 0.0, "color": "#ffffff"}],
        )

        migration.fill_source_units(apps, None)

        variable.refresh_from_db()
        style.refresh_from_db()
        self.assertEqual((variable.value_min, variable.value_max), (213.15, 333.15))
        self.assertEqual([stop["value"] for stop in style.stops], [213.15, 273.15])
        self.assertEqual(style.stops[1]["color"], "#ffffff")

    def test_a_range_already_in_the_source_unit_is_left_alone(self):
        variable = self.saved("mm", "mm")
        Variable.objects.filter(pk=variable.pk).update(value_min=0.0, value_max=500.0)

        migration.fill_source_units(apps, None)

        variable.refresh_from_db()
        self.assertEqual((variable.value_min, variable.value_max), (0.0, 500.0))


class AdminFixture(VariableUnitsFixture):
    """An organisation admin signed in, editing ``collection``."""

    def setUp(self):
        user = grant_everything(make_user("amina"))
        OrganisationMembership.objects.create(
            user=user, organisation=self.collection.catalog.organisation, role=OrganisationMembership.Role.ADMIN
        )
        dial_org(self.client)
        self.client.login(username="amina", password=PASSWORD)
        self.edit_url = reverse("collection:edit", args=[self.collection.pk])

    def with_asset(self, variable):
        item = Item.objects.create(collection=self.collection, time=datetime(2026, 3, 1, tzinfo=UTC))
        Asset.objects.create(item=item, variable=variable, href="kenya/1.tif")
        return variable


class SourceUnitLockTests(AdminFixture, TestCase):
    """The collection editor shows a source unit read-only once there are bytes it describes."""

    def variable_form(self):
        return self.client.get(self.edit_url).context["form"].formsets["variables"].forms[0]

    def post(self, variable, source_unit):
        row = {
            "id": variable.pk,
            "name": variable.name,
            "slug": variable.slug,
            "is_active": "on",
            "description": "",
            "source_unit": source_unit.pk,
            "unit": variable.unit_id,
            "transform_type": variable.transform_type,
            "time_statistic": variable.time_statistic,
            "statistic_window": variable.statistic_window,
            "sources": streamfield([("primary", {"source_name": "v", "vertical_dimension": "", "vertical_value": ""})]),
        }
        data = nested_form_data(
            {
                "name": self.collection.name,
                "catalog": self.collection.catalog_id,
                "description": "",
                "bounds": "",
                "crs": self.collection.crs,
                "time_resolution": "",
                "is_active": "on",
                "visibility": self.collection.visibility,
                "sort_order": 0,
                "forecast_horizon_hours": "",
                "boundary_stats_levels": [],
                "slug": self.collection.slug,
                "variables": inline_formset([row], initial=1),
            }
        )
        return self.client.post(self.edit_url, data)

    def test_a_variable_with_no_data_offers_its_source_unit(self):
        self.saved("K", "degC")

        self.assertFalse(self.variable_form().fields["source_unit"].disabled)

    def test_a_variable_with_data_shows_it_read_only(self):
        self.with_asset(self.saved("K", "degC"))

        field = self.variable_form().fields["source_unit"]

        self.assertTrue(field.disabled)
        self.assertIn("Correct source unit", str(field.help_text))

    def test_posting_another_source_unit_for_a_variable_with_data_changes_nothing(self):
        variable = self.with_asset(self.saved("K", "degC"))

        response = self.post(variable, unit("degC"))

        self.assertEqual(response.status_code, 302)
        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "K")

    def test_without_data_the_source_unit_can_still_be_edited(self):
        variable = self.saved("K", "degC")

        response = self.post(variable, unit("degF"))

        self.assertEqual(response.status_code, 302)
        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "degF")


class SourceUnitLockedOnEveryPathTests(AdminFixture, TestCase):
    """The lock is the Variable's, not one form's (ADR 0046 §8): whatever saves
    it — a plugin's re-provision, a feed's edit page, a script — cannot change
    what existing bytes are said to hold. Only Correct source unit can."""

    def test_saving_another_source_unit_for_a_variable_with_data_is_refused(self):
        variable = self.with_asset(self.saved("K", "degC"))

        variable.source_unit = unit("degC")
        with self.assertRaises(ValidationError) as refused:
            variable.save()

        self.assertIn("source_unit", refused.exception.error_dict)
        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "K")

    def test_without_data_the_source_unit_can_be_saved(self):
        variable = self.saved("K", "degC")

        variable.source_unit = unit("degF")
        variable.save()

        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "degF")

    def test_saving_anything_else_of_a_variable_with_data_is_allowed(self):
        variable = self.with_asset(self.saved("K", "degC"))

        variable.unit = unit("degF")
        variable.save()

        variable.refresh_from_db()
        self.assertEqual(variable.unit.symbol, "degF")

    def test_the_correction_changes_it(self):
        variable = self.with_asset(self.saved("K", "degC"))

        variable.correct_source_unit(unit("degC"))

        variable.refresh_from_db()
        self.assertEqual(variable.source_unit.symbol, "degC")


class CorrectSourceUnitTests(AdminFixture, TestCase):
    """Correct source unit: the one way to change what existing bytes are said to hold."""

    def setUp(self):
        super().setUp()
        self.variable = self.with_asset(self.saved("degC", "degC", value_min=-40.0, value_max=40.0))
        Asset.objects.filter(variable=self.variable).update(stats_min=250.0, stats_max=310.0, stats_mean=290.0)
        self.url = reverse("variable_correct_source_unit", args=[self.collection.pk, self.variable.pk])

    def test_the_confirmation_page_lists_what_is_reinterpreted(self):
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        content = response.content.decode()
        for reinterpreted in ("values", "range", "stops", "asset statistics", "zonal statistics"):
            self.assertIn(reinterpreted, content.lower())

    def test_correcting_changes_the_source_unit_and_converts_nothing(self):
        stored_asset = Asset.objects.values("href", "stats_min", "stats_max", "stats_mean").get(variable=self.variable)

        response = self.client.post(self.url, {"source_unit": unit("K").pk})

        self.assertEqual(response.status_code, 302)
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.source_unit.symbol, "K")
        self.assertAlmostEqual(self.variable.offset, -273.15)
        self.assertEqual((self.variable.value_min, self.variable.value_max), (-40.0, 40.0))
        self.assertEqual(
            Asset.objects.values("href", "stats_min", "stats_max", "stats_mean").get(variable=self.variable),
            stored_asset,
        )

    def test_the_correction_is_recorded_with_who_made_it(self):
        self.client.post(self.url, {"source_unit": unit("K").pk})

        event = SourceUnitCorrected.objects.get()
        self.assertEqual(event.actor, f"user:{User.objects.get(username='amina').pk}")
        self.assertEqual((event.from_unit, event.to_unit), ("degC", "K"))
        self.assertEqual(event.variable_id, self.variable.pk)
        self.assertIsNotNone(event.occurred_at)

    def test_a_correction_the_served_unit_cannot_follow_is_refused(self):
        response = self.client.post(self.url, {"source_unit": unit("m/s").pk})

        self.assertEqual(response.status_code, 200)
        self.assertIn("incompatible dimensions", response.content.decode())
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.source_unit.symbol, "degC")
        self.assertFalse(SourceUnitCorrected.objects.exists())

    def test_another_organisations_variable_is_not_found(self):
        other = Catalog.objects.create(organisation=make_organisation("elsewhere"), name="Other", slug="other")
        collection = Collection.objects.create(catalog=other, name="Theirs", slug="theirs")
        theirs = Variable.objects.create(
            collection=collection, slug="t", name="t", source_unit=unit("K"), unit=unit("K")
        )

        response = self.client.post(
            reverse("variable_correct_source_unit", args=[collection.pk, theirs.pk]), {"source_unit": unit("degC").pk}
        )

        self.assertEqual(response.status_code, 404)


class RefusedConversionTests(VariableUnitsFixture, TestCase):
    def refusal(self, source, served, **fields):
        with self.assertRaises(ValidationError) as ctx:
            self.variable(source, served, **fields).full_clean()
        return " ".join(ctx.exception.message_dict["unit"])

    def test_incompatible_dimensions(self):
        self.assertIn("incompatible dimensions", self.refusal("K", "m/s"))

    def test_a_logarithmic_unit(self):
        self.assertIn("not a linear conversion", self.refusal("dBZ", "mm**6/m**3"))

    def test_a_negative_scale(self):
        if "georiva_test_inverted_kelvin" not in ureg:
            ureg.define("georiva_test_inverted_kelvin = -1 * kelvin")

        self.assertIn("positive scale", self.refusal("K", "georiva_test_inverted_kelvin"))

    def test_a_rate_to_an_amount(self):
        self.assertIn("incompatible dimensions", self.refusal("mm/h", "mm"))

    def test_a_rate_to_an_amount_even_of_water(self):
        self.assertIn("incompatible dimensions", self.refusal("kg/m**2/s", "mm", water=True))

    def test_kg_per_square_metre_to_mm_needs_the_water_flag(self):
        self.assertIn("only for water", self.refusal("kg/m**2", "mm"))

    def test_kg_per_square_metre_to_mm_of_water(self):
        variable = self.saved("kg/m**2", "mm", water=True)

        self.assertAlmostEqual(variable.scale, 1.0)
        self.assertAlmostEqual(variable.offset, 0.0)

    def test_a_rate_of_water(self):
        variable = self.saved("kg/m**2/s", "mm/h", water=True)

        self.assertAlmostEqual(variable.scale, 3600.0)

    def test_the_water_flag_does_not_leak_into_the_registry(self):
        self.saved("kg/m**2", "mm", water=True)

        self.assertIn("only for water", self.refusal("kg/m**2", "mm"))

    def test_a_refused_conversion_is_never_saved(self):
        with self.assertRaises(ValidationError):
            self.variable("K", "m/s").save()

    def test_a_variable_cannot_be_saved_without_a_source_unit(self):
        variable = self.variable("K", "K")
        variable.source_unit = None

        with self.assertRaises(ValidationError) as ctx:
            variable.full_clean()
        self.assertIn("source_unit", ctx.exception.message_dict)
        with self.assertRaises(IntegrityError):
            variable.save()


class ConversionArithmeticTests(SimpleTestCase):
    """One type holds a conversion's arithmetic (ADR 0046 §4, §7): numbers go
    forward and back without float dust, and a spread is scaled, never offset."""

    def test_a_number_goes_forward_without_float_dust(self):
        self.assertEqual(Conversion.between("K", "degC").forward(300.0), 26.85)

    def test_a_number_comes_back(self):
        self.assertEqual(Conversion.between("K", "degC").inverse(0.0), 273.15)

    def test_a_spread_is_scaled_and_never_offset(self):
        self.assertEqual(Conversion.between("K", "degF").spread(2.0), 3.6)

    def test_none_stays_none(self):
        conversion = Conversion.between("K", "degC")
        self.assertEqual((conversion.forward(None), conversion.inverse(None), conversion.spread(None)), (None,) * 3)

    def test_an_array_is_converted_whole(self):
        import numpy as np

        np.testing.assert_allclose(Conversion.between("Pa", "hPa").apply(np.array([101325.0])), [1013.25])
