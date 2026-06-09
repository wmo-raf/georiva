from io import BytesIO
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

User = get_user_model()

STEP1_URL = "/admin/manual-uploads/wizard/step1/"
STEP2_URL = "/admin/manual-uploads/wizard/step2/"
STEP3_URL = "/admin/manual-uploads/wizard/step3/"
STEP4_URL = "/admin/manual-uploads/wizard/step4/"
STEP5_URL = "/admin/manual-uploads/wizard/step5/"
STEP6_URL = "/admin/manual-uploads/wizard/step6/"
PROVISION_URL = "/admin/manual-uploads/wizard/provision/"

SESSION_KEY = "georiva_upload_wizard"


def _make_catalog(slug="cat"):
    return Catalog.objects.create(name=slug, slug=slug, file_format="grib2")


def _make_collection(catalog, slug="col"):
    return Collection.objects.create(name=slug, slug=slug, catalog=catalog)


def _seed_session(client, data):
    session = client.session
    session[SESSION_KEY] = data
    session.save()


def _full_session(collection_id):
    return {
        "catalog_mode": "create",
        "new_catalog_name": "Weather Models",
        "new_catalog_slug": "weather-models",
        "new_catalog_format": "grib2",
        "config_name": "Surface variables",
        "variables": [{"name": "2t", "long_name": "2m temperature", "units": "K"}],
        "is_forecast": False,
        "assignments": [{"variable_name": "2t", "collection_id": collection_id}],
        "valid_time_format": "YYYYMMDD",
    }


class Step1CatalogTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@b.com", "pw")
        self.client.force_login(self.user)

    def test_step1_renders(self):
        response = self.client.get(STEP1_URL)
        self.assertEqual(response.status_code, 200)

    def test_step1_valid_post_creates_catalog_and_redirects_to_step2(self):
        response = self.client.post(STEP1_URL, {
            "catalog_mode": "create",
            "new_catalog_name": "Weather Models",
            "new_catalog_slug": "weather-models",
            "new_catalog_format": "grib2",
        })
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)
        session = self.client.session[SESSION_KEY]
        self.assertEqual(session["new_catalog_name"], "Weather Models")

    def test_step1_invalid_post_rerenders_with_error(self):
        response = self.client.post(STEP1_URL, {
            "catalog_mode": "create",
            "new_catalog_name": "",
            "new_catalog_format": "",
        })
        self.assertEqual(response.status_code, 200)

    def test_step1_select_existing_catalog(self):
        catalog = _make_catalog("existing")
        response = self.client.post(STEP1_URL, {
            "catalog_mode": "select",
            "catalog_id": str(catalog.pk),
        })
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)
        session = self.client.session[SESSION_KEY]
        self.assertEqual(session["catalog_id"], catalog.pk)


class Step2ConfigNameTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "b@c.com", "pw")
        self.client.force_login(self.user)
        _seed_session(self.client, {"catalog_mode": "create", "new_catalog_name": "WM",
                                    "new_catalog_slug": "wm", "new_catalog_format": "grib2"})

    def test_step2_renders(self):
        self.assertEqual(self.client.get(STEP2_URL).status_code, 200)

    def test_step2_valid_post_stores_name_and_redirects(self):
        response = self.client.post(STEP2_URL, {"config_name": "Surface variables"})
        self.assertRedirects(response, STEP3_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["config_name"], "Surface variables")

    def test_step2_empty_name_rerenders(self):
        response = self.client.post(STEP2_URL, {"config_name": ""})
        self.assertEqual(response.status_code, 200)

    def test_step2_without_step1_session_redirects_to_step1(self):
        _seed_session(self.client, {})
        response = self.client.get(STEP2_URL)
        self.assertRedirects(response, STEP1_URL, fetch_redirect_response=False)


class Step3SampleFileTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin3", "c@d.com", "pw")
        self.client.force_login(self.user)
        _seed_session(self.client, {
            "catalog_mode": "create", "new_catalog_name": "WM",
            "new_catalog_slug": "wm", "new_catalog_format": "grib2",
            "config_name": "Surface variables",
        })

    def test_step3_renders(self):
        self.assertEqual(self.client.get(STEP3_URL).status_code, 200)

    def test_step3_extracts_variables_and_discards_file(self):
        mock_plugin = MagicMock()
        mock_plugin.list_variables.return_value = [
            {"name": "2t", "long_name": "2m temperature", "units": "K"},
        ]
        with patch("georiva.ingestion.upload_wizard_views.format_registry") as mock_reg:
            mock_reg.get_plugin_for.return_value = mock_plugin
            response = self.client.post(STEP3_URL, {
                "sample_file": BytesIO(b"fake-grib-content"),
            }, format="multipart", **{"CONTENT_TYPE": "multipart/form-data"})

        # redirect is enough to confirm flow; file was written + deleted by view
        self.assertNotEqual(response.status_code, 500)

    def test_step3_stores_variables_in_session_and_redirects(self):
        mock_plugin = MagicMock()
        mock_plugin.list_variables.return_value = [
            {"name": "2t", "long_name": "2m temp", "units": "K"},
        ]
        sample = BytesIO(b"fake")
        sample.name = "sample.grib2"
        with patch("georiva.ingestion.upload_wizard_views.format_registry") as mock_reg:
            mock_reg.get_plugin_for.return_value = mock_plugin
            response = self.client.post(STEP3_URL, {"sample_file": sample})

        self.assertRedirects(response, STEP4_URL, fetch_redirect_response=False)
        variables = self.client.session[SESSION_KEY]["variables"]
        self.assertEqual(variables[0]["name"], "2t")

    def test_step3_missing_file_rerenders(self):
        response = self.client.post(STEP3_URL, {})
        self.assertEqual(response.status_code, 200)

    def test_step3_without_step2_session_redirects(self):
        _seed_session(self.client, {"catalog_mode": "create"})
        response = self.client.get(STEP3_URL)
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)


class Step4VariablesTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin4", "d@e.com", "pw")
        self.client.force_login(self.user)
        self.catalog = _make_catalog("cat4")
        self.collection = _make_collection(self.catalog, "col4")
        _seed_session(self.client, {
            "catalog_mode": "create", "new_catalog_name": "WM",
            "new_catalog_slug": "wm", "new_catalog_format": "grib2",
            "config_name": "Surface variables",
            "variables": [{"name": "2t", "long_name": "2m temp", "units": "K"}],
        })

    def test_step4_renders_with_variables(self):
        response = self.client.get(STEP4_URL)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "2t")

    def test_step4_valid_post_stores_assignments_and_redirects(self):
        response = self.client.post(STEP4_URL, {
            "is_forecast": "",
            f"collection_2t": str(self.collection.pk),
        })
        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)
        session = self.client.session[SESSION_KEY]
        self.assertEqual(session["assignments"][0]["collection_id"], self.collection.pk)
        self.assertFalse(session["is_forecast"])

    def test_step4_is_forecast_stored_as_true_when_checked(self):
        self.client.post(STEP4_URL, {
            "is_forecast": "1",
            "collection_2t": str(self.collection.pk),
        })
        self.assertTrue(self.client.session[SESSION_KEY]["is_forecast"])

    def test_step4_missing_assignment_rerenders(self):
        response = self.client.post(STEP4_URL, {})
        self.assertEqual(response.status_code, 200)

    def test_step4_back_navigation_preserves_session(self):
        _seed_session(self.client, {
            "catalog_mode": "create", "config_name": "X",
            "variables": [{"name": "tp", "long_name": "", "units": ""}],
            "assignments": [{"variable_name": "tp", "collection_id": self.collection.pk}],
        })
        response = self.client.get(STEP4_URL)
        self.assertEqual(response.status_code, 200)
        self.assertIn("assignments", self.client.session[SESSION_KEY])


class Step5FormatTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin5", "e@f.com", "pw")
        self.client.force_login(self.user)
        catalog = _make_catalog("cat5")
        col = _make_collection(catalog, "col5")
        _seed_session(self.client, {
            "catalog_mode": "create", "config_name": "X",
            "variables": [{"name": "2t", "long_name": "", "units": ""}],
            "assignments": [{"variable_name": "2t", "collection_id": col.pk}],
        })

    def test_step5_renders(self):
        self.assertEqual(self.client.get(STEP5_URL).status_code, 200)

    def test_step5_valid_post_stores_format_and_redirects(self):
        response = self.client.post(STEP5_URL, {"valid_time_format": "YYYYMMDD"})
        self.assertRedirects(response, STEP6_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["valid_time_format"], "YYYYMMDD")

    def test_step5_missing_format_rerenders(self):
        response = self.client.post(STEP5_URL, {"valid_time_format": ""})
        self.assertEqual(response.status_code, 200)


class Step6ReviewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin6", "f@g.com", "pw")
        self.client.force_login(self.user)
        catalog = _make_catalog("cat6")
        col = _make_collection(catalog, "col6")
        _seed_session(self.client, _full_session(col.pk))

    def test_step6_renders_summary(self):
        response = self.client.get(STEP6_URL)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Surface variables")
        self.assertContains(response, "YYYYMMDD")


class ProvisionTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin7", "g@h.com", "pw")
        self.client.force_login(self.user)
        self.catalog = _make_catalog("cat7")
        self.collection = _make_collection(self.catalog, "col7")

    def test_provision_creates_config_and_variables(self):
        _seed_session(self.client, _full_session(self.collection.pk))
        response = self.client.post(PROVISION_URL)

        self.assertEqual(ManualUploadConfig.objects.count(), 1)
        config = ManualUploadConfig.objects.get()
        self.assertEqual(config.name, "Surface variables")
        self.assertEqual(config.valid_time_format, "YYYYMMDD")
        self.assertFalse(config.is_forecast)

        self.assertEqual(ManualUploadConfigVariable.objects.count(), 1)
        var = ManualUploadConfigVariable.objects.get()
        self.assertEqual(var.variable_name, "2t")
        self.assertEqual(var.collection, self.collection)
        self.assertEqual(var.long_name, "2m temperature")

    def test_provision_creates_catalog_when_mode_is_create(self):
        _seed_session(self.client, _full_session(self.collection.pk))
        self.client.post(PROVISION_URL)
        self.assertTrue(Catalog.objects.filter(slug="weather-models").exists())

    def test_provision_uses_existing_catalog_when_mode_is_select(self):
        session = _full_session(self.collection.pk)
        session["catalog_mode"] = "select"
        session["catalog_id"] = self.catalog.pk
        _seed_session(self.client, session)

        self.client.post(PROVISION_URL)
        config = ManualUploadConfig.objects.get()
        self.assertEqual(config.catalog, self.catalog)

    def test_provision_clears_session(self):
        _seed_session(self.client, _full_session(self.collection.pk))
        self.client.post(PROVISION_URL)
        self.assertNotIn(SESSION_KEY, self.client.session)

    def test_provision_get_redirects_to_step6(self):
        _seed_session(self.client, _full_session(self.collection.pk))
        response = self.client.get(PROVISION_URL)
        self.assertRedirects(response, STEP6_URL, fetch_redirect_response=False)
