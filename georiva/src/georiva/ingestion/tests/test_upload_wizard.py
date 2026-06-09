import json
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

User = get_user_model()

CONFIG_LIST_URL = "/admin/manual-uploads/"

STEP1_URL         = "/admin/manual-uploads/wizard/step1/"
STEP2_URL         = "/admin/manual-uploads/wizard/step2/"
STEP3_URL         = "/admin/manual-uploads/wizard/step3/"
STEP4_URL         = "/admin/manual-uploads/wizard/step4/"
STEP5_URL         = "/admin/manual-uploads/wizard/step5/"
PROVISION_URL     = "/admin/manual-uploads/wizard/provision/"
UPLOAD_SAMPLE_URL = "/admin/manual-uploads/wizard/upload-sample/"

SESSION_KEY = "georiva_upload_wizard"


def _make_catalog(slug="cat", file_format="grib2"):
    return Catalog.objects.create(name=slug, slug=slug, file_format=file_format)


def _make_collection(catalog, slug="col"):
    return Collection.objects.create(name=slug, slug=slug, catalog=catalog)


def _seed_session(client, data):
    session = client.session
    session[SESSION_KEY] = data
    session.save()


def _assignment(**overrides):
    base = {
        "variable_name":  "2t",
        "long_name":      "2m temperature",
        "units":          "K",
        "unit_id":        None,
        "unit_create":    "K",
        "unit_display":   'Create unit "K"',
        "value_min":      -40.0,
        "value_max":      50.0,
        "collection_idx": 0,
    }
    base.update(overrides)
    return base


def _full_session():
    return {
        "catalog_mode":            "create",
        "new_catalog_name":        "Weather Models",
        "new_catalog_slug":        "weather-models",
        "new_catalog_format":      "grib2",
        "config_name":             "Surface variables",
        "variables":               [{"name": "2t", "long_name": "2m temperature", "units": "K"}],
        "selected_variable_names": ["2t"],
        "sample_filename":         "20250115.grib2",
        "valid_time_format":       "CONTENT",
        "is_forecast":             False,
        "collections":             [{"name": "Weather Models Collection 1", "slug": "weather-models-collection-1"}],
        "assignments":             [_assignment()],
    }


# =============================================================================
# Step 1 — Catalog
# =============================================================================

class Step1CatalogTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@b.com", "pw")
        self.client.force_login(self.user)

    def test_step1_renders(self):
        self.assertEqual(self.client.get(STEP1_URL).status_code, 200)

    def test_step1_valid_post_redirects_to_step2(self):
        response = self.client.post(STEP1_URL, {
            "catalog_mode": "create",
            "new_catalog_name": "Weather Models",
            "new_catalog_slug": "weather-models",
            "new_catalog_format": "grib2",
        })
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["new_catalog_name"], "Weather Models")

    def test_step1_description_saved_to_session(self):
        self.client.post(STEP1_URL, {
            "catalog_mode": "create",
            "new_catalog_name": "WM",
            "new_catalog_slug": "wm",
            "new_catalog_format": "grib2",
            "new_catalog_description": "A weather model catalog",
        })
        self.assertEqual(self.client.session[SESSION_KEY]["new_catalog_description"], "A weather model catalog")

    def test_step1_invalid_post_rerenders(self):
        response = self.client.post(STEP1_URL, {
            "catalog_mode": "create", "new_catalog_name": "", "new_catalog_format": "",
        })
        self.assertEqual(response.status_code, 200)

    def test_step1_select_existing_catalog(self):
        catalog = _make_catalog("existing")
        response = self.client.post(STEP1_URL, {
            "catalog_mode": "select", "catalog_id": str(catalog.pk),
        })
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["catalog_id"], catalog.pk)


# =============================================================================
# Step 2 — Config name
# =============================================================================

class Step2ConfigNameTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "b@c.com", "pw")
        self.client.force_login(self.user)
        _seed_session(self.client, {
            "catalog_mode": "create", "new_catalog_name": "WM",
            "new_catalog_slug": "wm", "new_catalog_format": "grib2",
        })

    def test_step2_renders(self):
        self.assertEqual(self.client.get(STEP2_URL).status_code, 200)

    def test_step2_default_config_name_derived_from_catalog(self):
        response = self.client.get(STEP2_URL)
        self.assertContains(response, "WM Config")

    def test_step2_valid_post_stores_name_and_redirects(self):
        response = self.client.post(STEP2_URL, {"config_name": "Surface variables"})
        self.assertRedirects(response, STEP3_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["config_name"], "Surface variables")

    def test_step2_empty_name_rerenders(self):
        response = self.client.post(STEP2_URL, {"config_name": ""})
        self.assertEqual(response.status_code, 200)

    def test_step2_without_step1_session_redirects_to_step1(self):
        _seed_session(self.client, {})
        self.assertRedirects(self.client.get(STEP2_URL), STEP1_URL, fetch_redirect_response=False)

    def test_step2_duplicate_name_for_selected_catalog_rerenders(self):
        catalog = _make_catalog("dup-cat")
        ManualUploadConfig.objects.create(
            catalog=catalog, name="Surface variables", valid_time_format="CONTENT",
        )
        _seed_session(self.client, {"catalog_mode": "select", "catalog_id": catalog.pk})
        response = self.client.post(STEP2_URL, {"config_name": "Surface variables"})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("config_name", self.client.session[SESSION_KEY])


# =============================================================================
# Upload sample AJAX endpoint
# =============================================================================

class UploadSampleAjaxTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_ajax", "x@y.com", "pw")
        self.client.force_login(self.user)

    def test_returns_405_on_get(self):
        self.assertEqual(self.client.get(UPLOAD_SAMPLE_URL).status_code, 405)

    def test_returns_error_when_no_file(self):
        response = self.client.post(UPLOAD_SAMPLE_URL, {})
        self.assertEqual(response.status_code, 200)
        self.assertIn("error", response.json())

    def _mock_plugin(self, vmin=-5.0, vmax=45.0, units="K"):
        mock_plugin = MagicMock()
        mock_plugin.list_variables.return_value = [
            {"name": "2t", "long_name": "2m temp", "units": units},
        ]
        mock_info = MagicMock()
        mock_info.data.min.return_value = vmin
        mock_info.data.max.return_value = vmax
        mock_plugin.open_variable.return_value.__enter__.return_value = mock_info
        return mock_plugin

    def _post_sample(self, mock_plugin):
        from io import BytesIO
        sample = BytesIO(b"fake")
        sample.name = "20250115.grib2"
        with patch("georiva.ingestion.upload_wizard_views.format_registry") as mock_reg:
            mock_reg.get_for_file.return_value = mock_plugin
            return self.client.post(UPLOAD_SAMPLE_URL, {"sample_file": sample})

    def test_returns_variables_on_success(self):
        response = self._post_sample(self._mock_plugin())
        data = response.json()
        self.assertNotIn("error", data)
        self.assertEqual(data["variables"][0]["name"], "2t")
        self.assertEqual(data["sample_filename"], "20250115.grib2")

    def test_returns_scanned_value_range_rounded_outward(self):
        response = self._post_sample(self._mock_plugin(vmin=-5.0, vmax=45.0))
        var = response.json()["variables"][0]
        self.assertEqual(var["value_min"], -10.0)
        self.assertEqual(var["value_max"], 50.0)

    def test_value_range_is_none_when_scan_fails(self):
        mock_plugin = self._mock_plugin()
        mock_plugin.open_variable.side_effect = RuntimeError("boom")
        var = self._post_sample(mock_plugin).json()["variables"][0]
        self.assertIsNone(var["value_min"])
        self.assertIsNone(var["value_max"])

    def test_unit_matched_to_existing_unit_by_symbol(self):
        unit = Unit.objects.get(symbol="K")  # seeded by core migration 0003
        var = self._post_sample(self._mock_plugin()).json()["variables"][0]
        self.assertEqual(var["unit_id"], unit.pk)
        self.assertFalse(var["can_create"])

    def test_unit_matched_by_pint_equivalence(self):
        unit = Unit.objects.get(symbol="K")
        var = self._post_sample(self._mock_plugin(units="kelvin")).json()["variables"][0]
        self.assertEqual(var["unit_id"], unit.pk)

    def test_unmatched_valid_unit_is_creatable(self):
        # 'knot' is pint-valid but not among the seeded units
        var = self._post_sample(self._mock_plugin(units="knot")).json()["variables"][0]
        self.assertIsNone(var["unit_id"])
        self.assertTrue(var["can_create"])

    def test_invalid_unit_string_not_creatable(self):
        mock_plugin = self._mock_plugin()
        mock_plugin.list_variables.return_value = [
            {"name": "x", "long_name": "X", "units": "code table 4.2"},
        ]
        var = self._post_sample(mock_plugin).json()["variables"][0]
        self.assertIsNone(var["unit_id"])
        self.assertFalse(var["can_create"])

    def test_returns_error_for_unsupported_format(self):
        from io import BytesIO
        sample = BytesIO(b"fake")
        sample.name = "data.csv"
        with patch("georiva.ingestion.upload_wizard_views.format_registry") as mock_reg:
            mock_reg.get_for_file.return_value = None
            response = self.client.post(UPLOAD_SAMPLE_URL, {"sample_file": sample})
        self.assertIn("error", response.json())


# =============================================================================
# Step 3 — File & variable selection
# =============================================================================

class Step3CombinedTests(TestCase):
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

    def test_step3_without_config_name_redirects_to_step2(self):
        _seed_session(self.client, {"catalog_mode": "create"})
        self.assertRedirects(self.client.get(STEP3_URL), STEP2_URL, fetch_redirect_response=False)

    def test_step3_valid_post_saves_all_fields_and_redirects(self):
        variables = [{"name": "2t", "long_name": "2m temp", "units": "K"}]
        response = self.client.post(STEP3_URL, {
            "sample_filename": "20250115.grib2",
            "variables_json": json.dumps(variables),
            "selected_variables_json": json.dumps(["2t"]),
        })
        self.assertRedirects(response, STEP4_URL, fetch_redirect_response=False)
        session = self.client.session[SESSION_KEY]
        self.assertEqual(session["valid_time_format"], "CONTENT")
        self.assertFalse(session["is_forecast"])
        self.assertEqual(session["selected_variable_names"], ["2t"])
        self.assertEqual(session["variables"][0]["name"], "2t")

    def test_step3_is_forecast_stored_as_true_when_checked(self):
        variables = [{"name": "2t", "long_name": "", "units": ""}]
        self.client.post(STEP3_URL, {
            "sample_filename": "20250115.grib2",
            "variables_json": json.dumps(variables),
            "selected_variables_json": json.dumps(["2t"]),
            "is_forecast": "1",
        })
        self.assertTrue(self.client.session[SESSION_KEY]["is_forecast"])

    def test_step3_missing_file_rerenders_with_error(self):
        response = self.client.post(STEP3_URL, {
            "variables_json": "[]",
            "selected_variables_json": "[]",
        })
        self.assertEqual(response.status_code, 200)

    def test_step3_no_selection_rerenders_with_error(self):
        variables = [{"name": "2t", "long_name": "", "units": ""}]
        response = self.client.post(STEP3_URL, {
            "sample_filename": "20250115.grib2",
            "variables_json": json.dumps(variables),
            "selected_variables_json": json.dumps([]),
        })
        self.assertEqual(response.status_code, 200)

    def test_step3_missing_format_rerenders_with_error_for_geotiff(self):
        _seed_session(self.client, {
            "catalog_mode": "create", "new_catalog_name": "Imagery",
            "new_catalog_slug": "imagery", "new_catalog_format": "geotiff",
            "config_name": "Surface variables",
        })
        variables = [{"name": "band_1", "long_name": "", "units": ""}]
        response = self.client.post(STEP3_URL, {
            "sample_filename": "20250115.tif",
            "variables_json": json.dumps(variables),
            "selected_variables_json": json.dumps(["band_1"]),
            "valid_time_format": "",
        })
        self.assertEqual(response.status_code, 200)

    def test_step3_show_filename_format_false_for_grib(self):
        response = self.client.get(STEP3_URL)
        self.assertFalse(response.context["show_filename_format"])
        self.assertContains(response, "Time will be read from the file content")

    def test_step3_show_filename_format_true_for_geotiff(self):
        _seed_session(self.client, {
            "catalog_mode": "create", "new_catalog_name": "Imagery",
            "new_catalog_slug": "imagery", "new_catalog_format": "geotiff",
            "config_name": "Bands",
        })
        response = self.client.get(STEP3_URL)
        self.assertTrue(response.context["show_filename_format"])
        self.assertContains(response, "Valid time format")


# =============================================================================
# Step 4 — Collection setup
# =============================================================================

class Step4CollectionsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin4", "d@e.com", "pw")
        self.client.force_login(self.user)
        _seed_session(self.client, {
            "catalog_mode": "create", "new_catalog_name": "WM",
            "new_catalog_slug": "wm", "new_catalog_format": "grib2",
            "config_name": "Surface variables",
            "variables": [{"name": "2t", "long_name": "2m temp", "units": "K"}],
            "selected_variable_names": ["2t"],
            "sample_filename": "20250115.grib2",
            "valid_time_format": "CONTENT",
            "is_forecast": False,
        })

    def test_step4_renders(self):
        self.assertEqual(self.client.get(STEP4_URL).status_code, 200)

    def test_step4_without_selected_variables_redirects_to_step3(self):
        _seed_session(self.client, {"catalog_mode": "create", "config_name": "X"})
        self.assertRedirects(self.client.get(STEP4_URL), STEP3_URL, fetch_redirect_response=False)

    def test_step4_valid_post_saves_and_redirects(self):
        collections = [{"name": "WM Collection 1", "slug": "wm-collection-1"}]
        assignments = [_assignment()]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)
        session = self.client.session[SESSION_KEY]
        self.assertEqual(session["collections"][0]["name"], "WM Collection 1")
        self.assertEqual(session["assignments"][0]["variable_name"], "2t")
        self.assertEqual(session["assignments"][0]["collection_idx"], 0)
        self.assertEqual(session["assignments"][0]["value_min"], -40.0)

    def test_step4_valid_post_with_existing_unit_id(self):
        unit = Unit.objects.get(symbol="K")
        collections = [{"name": "WM Collection 1", "slug": "wm-collection-1"}]
        assignments = [_assignment(unit_id=unit.pk, unit_create="")]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)

    def test_step4_empty_collection_name_rerenders(self):
        collections = [{"name": "", "slug": ""}]
        assignments = [_assignment()]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertEqual(response.status_code, 200)

    def test_step4_collection_with_no_variables_rerenders(self):
        # Two collections but only one variable — second collection is empty
        collections = [
            {"name": "Col A", "slug": "col-a"},
            {"name": "Col B", "slug": "col-b"},
        ]
        assignments = [_assignment()]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertEqual(response.status_code, 200)

    def test_step4_missing_unit_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(unit_id=None, unit_create="")]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertEqual(response.status_code, 200)

    def test_step4_invalid_create_unit_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(unit_create="not_a_real_unit_xyz")]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertEqual(response.status_code, 200)

    def test_step4_missing_value_range_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(value_min=None, value_max=None)]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertEqual(response.status_code, 200)

    def test_step4_min_not_below_max_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(value_min=50.0, value_max=50.0)]
        response = self.client.post(STEP4_URL, {
            "collections_json": json.dumps(collections),
            "assignments_json":  json.dumps(assignments),
        })
        self.assertEqual(response.status_code, 200)


# =============================================================================
# Step 5 — Review
# =============================================================================

class Step5ReviewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin5", "e@f.com", "pw")
        self.client.force_login(self.user)
        _seed_session(self.client, _full_session())

    def test_step5_renders_summary(self):
        response = self.client.get(STEP5_URL)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Surface variables")
        self.assertContains(response, "Weather Models Collection 1")
        self.assertContains(response, "2t")

    def test_step5_without_assignments_redirects_to_step4(self):
        _seed_session(self.client, {"catalog_mode": "create", "config_name": "X"})
        self.assertRedirects(self.client.get(STEP5_URL), STEP4_URL, fetch_redirect_response=False)


# =============================================================================
# Provision
# =============================================================================

class ProvisionTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin6", "g@h.com", "pw")
        self.client.force_login(self.user)

    def test_provision_creates_collection_config_and_variables(self):
        _seed_session(self.client, _full_session())
        self.client.post(PROVISION_URL)

        collection = Collection.objects.get(slug="weather-models-collection-1")
        self.assertEqual(collection.name, "Weather Models Collection 1")

        config = ManualUploadConfig.objects.get()
        self.assertEqual(config.name, "Surface variables")
        self.assertEqual(config.valid_time_format, "CONTENT")
        self.assertFalse(config.is_forecast)

        var = ManualUploadConfigVariable.objects.get()
        self.assertEqual(var.variable_name, "2t")
        self.assertEqual(var.collection, collection)
        self.assertEqual(var.long_name, "2m temperature")

    def test_provision_creates_catalog_when_mode_is_create(self):
        _seed_session(self.client, _full_session())
        self.client.post(PROVISION_URL)
        self.assertTrue(Catalog.objects.filter(slug="weather-models").exists())

    def test_provision_uses_existing_catalog_when_mode_is_select(self):
        catalog = _make_catalog("existing-cat")
        session = _full_session()
        session["catalog_mode"] = "select"
        session["catalog_id"] = catalog.pk
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)
        self.assertEqual(ManualUploadConfig.objects.get().catalog, catalog)
        self.assertEqual(Collection.objects.get().catalog, catalog)

    def test_provision_description_set_on_new_catalog(self):
        session = _full_session()
        session["new_catalog_description"] = "My description"
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)
        self.assertEqual(Catalog.objects.get(slug="weather-models").description, "My description")

    def test_provision_clears_session(self):
        _seed_session(self.client, _full_session())
        self.client.post(PROVISION_URL)
        self.assertNotIn(SESSION_KEY, self.client.session)

    def test_provision_get_redirects_to_step5(self):
        _seed_session(self.client, _full_session())
        self.assertRedirects(
            self.client.get(PROVISION_URL), STEP5_URL, fetch_redirect_response=False
        )

    def test_provision_redirects_to_config_list(self):
        _seed_session(self.client, _full_session())
        response = self.client.post(PROVISION_URL)
        self.assertRedirects(response, CONFIG_LIST_URL, fetch_redirect_response=False)

    def test_provision_creates_core_variable(self):
        _seed_session(self.client, _full_session())
        self.client.post(PROVISION_URL)

        variable = Variable.objects.get(slug="2t")
        self.assertEqual(variable.collection.slug, "weather-models-collection-1")
        self.assertEqual(variable.name, "2m temperature")
        self.assertEqual(variable.transform_type, Variable.TransformType.PASSTHROUGH)
        self.assertEqual(variable.value_min, -40.0)
        self.assertEqual(variable.value_max, 50.0)
        self.assertEqual(variable.unit.symbol, "K")
        sources = list(variable.sources)
        self.assertEqual(sources[0].block_type, "primary")
        self.assertEqual(sources[0].value["source_name"], "2t")

    def test_provision_creates_unit_from_unit_create(self):
        session = _full_session()
        session["assignments"] = [_assignment(units="knot", unit_create="knot")]
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)
        unit = Unit.objects.get(symbol="knot")
        self.assertEqual(unit.name, "knot")
        self.assertEqual(Variable.objects.get(slug="2t").unit, unit)

    def test_provision_uses_existing_unit_by_id(self):
        unit = Unit.objects.get(symbol="K")
        unit_count = Unit.objects.count()
        session = _full_session()
        session["assignments"] = [_assignment(unit_id=unit.pk, unit_create="")]
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)
        self.assertEqual(Unit.objects.count(), unit_count)
        self.assertEqual(Variable.objects.get(slug="2t").unit, unit)

    def test_provision_does_not_clobber_existing_variable(self):
        catalog = _make_catalog("weather-models")
        collection = Collection.objects.create(
            catalog=catalog, name="Tuned", slug="weather-models-collection-1",
        )
        unit = Unit.objects.get(symbol="°C")
        existing = Variable.objects.create(
            collection=collection, slug="2t", name="Hand-tuned temperature",
            transform_type=Variable.TransformType.PASSTHROUGH,
            unit=unit, value_min=-10.0, value_max=10.0,
            sources=[("primary", {"source_name": "2t"})],
        )
        session = _full_session()
        session["catalog_mode"] = "select"
        session["catalog_id"] = catalog.pk
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)

        existing.refresh_from_db()
        self.assertEqual(existing.name, "Hand-tuned temperature")
        self.assertEqual(existing.value_min, -10.0)
        self.assertEqual(Variable.objects.filter(slug="2t").count(), 1)

    def test_provision_duplicate_config_name_errors_without_partial_state(self):
        catalog = _make_catalog("existing-cat")
        ManualUploadConfig.objects.create(
            catalog=catalog, name="Surface variables", valid_time_format="CONTENT",
        )
        session = _full_session()
        session["catalog_mode"] = "select"
        session["catalog_id"] = catalog.pk
        _seed_session(self.client, session)
        response = self.client.post(PROVISION_URL)

        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)
        self.assertEqual(ManualUploadConfig.objects.count(), 1)
        self.assertFalse(Collection.objects.filter(slug="weather-models-collection-1").exists())
        self.assertFalse(Variable.objects.exists())

    def test_provision_assignment_without_unit_rolls_back_everything(self):
        session = _full_session()
        session["assignments"] = [_assignment(unit_id=None, unit_create="")]
        _seed_session(self.client, session)
        response = self.client.post(PROVISION_URL)

        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)
        self.assertFalse(Catalog.objects.filter(slug="weather-models").exists())
        self.assertFalse(ManualUploadConfig.objects.exists())
        self.assertFalse(Variable.objects.exists())
