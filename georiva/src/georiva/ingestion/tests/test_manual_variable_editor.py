"""
Variable editor for manually-provisioned Collections.

Data managers tune variables (display name, unit, value range, palette) and
add/remove them from the Manual Uploads surface — never the raw Collection
form, which is permission-gated away from them. Operator-is-truth: edits here
are authoritative.

All tests run as a Data Managers group member (no raw model add/change
permissions), proving the editor never depends on them.
"""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.core.provisioning import passthrough_sources
from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

User = get_user_model()


class ManualVariableEditorTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user("dm", "dm@x.com", "pw")
        self.user.groups.add(Group.objects.get(name="Data Managers"))
        self.client.force_login(self.user)

        self.catalog = Catalog.objects.create(name="Local", slug="local", file_format="grib2")
        self.collection = Collection.objects.create(catalog=self.catalog, name="Surface", slug="surface")
        self.config = ManualUploadConfig.objects.create(
            catalog=self.catalog, name="Surface variables", valid_time_format="CONTENT",
        )
        self.kelvin, _ = Unit.objects.get_or_create(symbol="K", defaults={"name": "kelvin"})
        self.mcv = ManualUploadConfigVariable.objects.create(
            config=self.config, collection=self.collection,
            variable_name="2t", long_name="2m temperature", units="K",
        )
        self.variable = Variable.objects.create(
            collection=self.collection, slug="2t", name="2m temperature",
            transform_type=Variable.TransformType.PASSTHROUGH,
            unit=self.kelvin, value_min=-40.0, value_max=50.0,
            sources=passthrough_sources("2t"),
        )


class EditVariableTests(ManualVariableEditorTestCase):
    def test_data_manager_edits_display_name_unit_and_value_range(self):
        celsius, _ = Unit.objects.get_or_create(symbol="degC", defaults={"name": "degree Celsius"})
        url = reverse("manual_upload_variable_edit", args=[self.config.pk, self.mcv.pk])

        response = self.client.post(url, {
            "name": "Temperature (2 m)",
            "unit": celsius.pk,
            "value_min": "-60",
            "value_max": "60",
        })

        self.assertRedirects(
            response, reverse("manual_upload_config_edit", args=[self.config.pk]),
            fetch_redirect_response=False,
        )
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.name, "Temperature (2 m)")
        self.assertEqual(self.variable.unit, celsius)
        self.assertEqual(self.variable.value_min, -60.0)
        self.assertEqual(self.variable.value_max, 60.0)
        # Display name stays consistent on the config's own row
        self.mcv.refresh_from_db()
        self.assertEqual(self.mcv.long_name, "Temperature (2 m)")

    def test_inverted_value_range_is_rejected_and_nothing_saved(self):
        url = reverse("manual_upload_variable_edit", args=[self.config.pk, self.mcv.pk])
        response = self.client.post(url, {
            "name": "2m temperature",
            "unit": self.kelvin.pk,
            "value_min": "100",
            "value_max": "0",
        })
        self.assertEqual(response.status_code, 200)  # re-rendered with errors
        self.variable.refresh_from_db()
        self.assertEqual(self.variable.value_min, -40.0)
        self.assertEqual(self.variable.value_max, 50.0)


class AddVariableTests(ManualVariableEditorTestCase):
    def test_data_manager_adds_a_variable(self):
        url = reverse("manual_upload_variable_add", args=[self.config.pk])

        response = self.client.post(url, {
            "variable_name": "10u",
            "long_name": "10m U wind",
            "collection": self.collection.pk,
            "unit": self.kelvin.pk,
            "value_min": "-50",
            "value_max": "50",
        })

        self.assertRedirects(
            response, reverse("manual_upload_config_edit", args=[self.config.pk]),
            fetch_redirect_response=False,
        )
        variable = Variable.objects.get(collection=self.collection, slug="10u")
        self.assertEqual(variable.name, "10m U wind")
        self.assertEqual(variable.transform_type, Variable.TransformType.PASSTHROUGH)
        self.assertEqual(variable.sources[0].block_type, "primary")
        self.assertEqual(variable.sources[0].value["source_name"], "10u")
        mcv = ManualUploadConfigVariable.objects.get(config=self.config, variable_name="10u")
        self.assertEqual(mcv.collection, self.collection)
        self.assertEqual(mcv.long_name, "10m U wind")

    def test_adding_a_duplicate_variable_is_rejected(self):
        url = reverse("manual_upload_variable_add", args=[self.config.pk])
        response = self.client.post(url, {
            "variable_name": "2t",
            "long_name": "Duplicate",
            "collection": self.collection.pk,
            "unit": self.kelvin.pk,
            "value_min": "0",
            "value_max": "1",
        })
        self.assertEqual(response.status_code, 200)  # re-rendered with error
        self.assertEqual(Variable.objects.filter(collection=self.collection, slug="2t").count(), 1)
        self.assertEqual(
            ManualUploadConfigVariable.objects.filter(config=self.config, variable_name="2t").count(), 1
        )

    def test_add_with_new_unit_symbol_resolves_or_creates_the_unit(self):
        url = reverse("manual_upload_variable_add", args=[self.config.pk])
        self.client.post(url, {
            "variable_name": "msl",
            "long_name": "Mean sea level pressure",
            "collection": self.collection.pk,
            "new_unit_symbol": "hPa",
            "value_min": "900",
            "value_max": "1100",
        })
        variable = Variable.objects.get(collection=self.collection, slug="msl")
        self.assertEqual(variable.unit.symbol.lower(), "hpa")


class RemoveVariableTests(ManualVariableEditorTestCase):
    def test_data_manager_removes_a_variable(self):
        url = reverse("manual_upload_variable_remove", args=[self.config.pk, self.mcv.pk])

        # GET shows a confirmation, deletes nothing
        self.assertEqual(self.client.get(url).status_code, 200)
        self.assertTrue(Variable.objects.filter(pk=self.variable.pk).exists())

        response = self.client.post(url)

        self.assertRedirects(
            response, reverse("manual_upload_config_edit", args=[self.config.pk]),
            fetch_redirect_response=False,
        )
        self.assertFalse(Variable.objects.filter(pk=self.variable.pk).exists())
        self.assertFalse(ManualUploadConfigVariable.objects.filter(pk=self.mcv.pk).exists())

    def test_remove_survives_a_missing_core_variable(self):
        self.variable.delete()
        url = reverse("manual_upload_variable_remove", args=[self.config.pk, self.mcv.pk])
        self.client.post(url)
        self.assertFalse(ManualUploadConfigVariable.objects.filter(pk=self.mcv.pk).exists())


class ConfigPageAffordanceTests(ManualVariableEditorTestCase):
    def test_config_edit_page_links_to_the_variable_editor(self):
        html = self.client.get(
            reverse("manual_upload_config_edit", args=[self.config.pk])
        ).content.decode()
        self.assertIn(reverse("manual_upload_variable_add", args=[self.config.pk]), html)
        self.assertIn(reverse("manual_upload_variable_edit", args=[self.config.pk, self.mcv.pk]), html)
        self.assertIn(reverse("manual_upload_variable_remove", args=[self.config.pk, self.mcv.pk]), html)
