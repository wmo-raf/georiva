import json
from unittest.mock import MagicMock, patch

import numpy as np
import xarray as xr
from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase

from georiva.core.models import Catalog, Collection, Unit, Variable
from georiva.ingestion.models import PushFeed
from georiva.ingestion.upload_wizard_views import SAMPLE_MAX_BYTES, _thinned
from georiva.organisations.testing import dial_org, join_org, make_organisation, org_host

User = get_user_model()

FEED_LIST_URL = "/admin/push-feeds/"

STEP1_URL = "/admin/push-feeds/wizard/step1/"
STEP2_URL = "/admin/push-feeds/wizard/step2/"
STEP3_URL = "/admin/push-feeds/wizard/step3/"
STEP4_URL = "/admin/push-feeds/wizard/step4/"
STEP5_URL = "/admin/push-feeds/wizard/step5/"
PROVISION_URL = "/admin/push-feeds/wizard/provision/"
UPLOAD_SAMPLE_URL = "/admin/push-feeds/wizard/upload-sample/"

SESSION_KEY = "georiva_upload_wizard"


def _make_catalog(slug="cat", file_format="grib2"):
    return Catalog.objects.create(organisation=make_organisation(), name=slug, slug=slug, file_format=file_format)


def _make_collection(catalog, slug="col"):
    return Collection.objects.create(name=slug, slug=slug, catalog=catalog)


def _seed_session(client, data):
    session = client.session
    session[SESSION_KEY] = data
    session.save()


def _assignment(**overrides):
    base = {
        "variable_name": "2t",
        "long_name": "2m temperature",
        "units": "K",
        "unit_id": None,
        "unit_create": "K",
        "unit_display": 'Create unit "K"',
        "value_min": -40.0,
        "value_max": 50.0,
        "collection_idx": 0,
    }
    base.update(overrides)
    return base


def _full_session():
    return {
        "catalog_mode": "create",
        "new_catalog_name": "Weather Models",
        "new_catalog_slug": "weather-models",
        "new_catalog_format": "grib2",
        "feed_name": "Surface variables",
        "variables": [{"name": "2t", "long_name": "2m temperature", "units": "K"}],
        "selected_variable_names": ["2t"],
        "sample_filename": "20250115.grib2",
        "valid_time_format": "CONTENT",
        "is_forecast": False,
        "collections": [{"name": "Weather Models Collection 1", "slug": "weather-models-collection-1"}],
        "assignments": [_assignment()],
    }


# =============================================================================
# Step 1 — Catalog
# =============================================================================


class Step1CatalogTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@b.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        # The wizard only offers (and accepts) catalogs of the org serving the
        # request, so dial the host that owns the fixtures.
        make_organisation()
        self.client.defaults["HTTP_HOST"] = org_host()

    def test_step1_renders(self):
        self.assertEqual(self.client.get(STEP1_URL).status_code, 200)

    def test_step1_valid_post_redirects_to_step2(self):
        response = self.client.post(
            STEP1_URL,
            {
                "catalog_mode": "create",
                "new_catalog_name": "Weather Models",
                "new_catalog_slug": "weather-models",
                "new_catalog_format": "grib2",
            },
        )
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["new_catalog_name"], "Weather Models")

    def test_step1_description_saved_to_session(self):
        self.client.post(
            STEP1_URL,
            {
                "catalog_mode": "create",
                "new_catalog_name": "WM",
                "new_catalog_slug": "wm",
                "new_catalog_format": "grib2",
                "new_catalog_description": "A weather model catalog",
            },
        )
        self.assertEqual(self.client.session[SESSION_KEY]["new_catalog_description"], "A weather model catalog")

    def test_step1_invalid_post_rerenders(self):
        response = self.client.post(
            STEP1_URL,
            {
                "catalog_mode": "create",
                "new_catalog_name": "",
                "new_catalog_format": "",
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step1_select_existing_catalog(self):
        catalog = _make_catalog("existing")
        response = self.client.post(
            STEP1_URL,
            {
                "catalog_mode": "select",
                "catalog_id": str(catalog.pk),
            },
        )
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["catalog_id"], catalog.pk)


# =============================================================================
# Step 2 — Config name
# =============================================================================


class Step2ConfigNameTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "b@c.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        _seed_session(
            self.client,
            {
                "catalog_mode": "create",
                "new_catalog_name": "WM",
                "new_catalog_slug": "wm",
                "new_catalog_format": "grib2",
            },
        )

    def test_step2_renders(self):
        self.assertEqual(self.client.get(STEP2_URL).status_code, 200)

    def test_step2_default_feed_name_derived_from_catalog(self):
        response = self.client.get(STEP2_URL)
        self.assertContains(response, "WM push feed")

    def test_step2_valid_post_stores_name_and_redirects(self):
        response = self.client.post(STEP2_URL, {"feed_name": "Surface variables"})
        self.assertRedirects(response, STEP3_URL, fetch_redirect_response=False)
        self.assertEqual(self.client.session[SESSION_KEY]["feed_name"], "Surface variables")

    def test_step2_stores_files_per_run_and_expected_interval(self):
        self.client.post(
            STEP2_URL,
            {"feed_name": "WRF", "files_per_run": "4", "expected_interval_hours": "6"},
        )
        stored = self.client.session[SESSION_KEY]
        self.assertEqual(stored["files_per_run"], 4)
        self.assertEqual(stored["expected_interval_hours"], 6.0)

    def test_step2_interval_may_be_left_empty(self):
        self.client.post(STEP2_URL, {"feed_name": "WRF", "files_per_run": "1", "expected_interval_hours": ""})
        self.assertIsNone(self.client.session[SESSION_KEY]["expected_interval_hours"])

    def test_step2_rejects_zero_files_per_run_and_non_positive_interval(self):
        for data in (
            {"feed_name": "WRF", "files_per_run": "0"},
            {"feed_name": "WRF", "expected_interval_hours": "0"},
            {"feed_name": "WRF", "expected_interval_hours": "nan"},
        ):
            with self.subTest(data=data):
                response = self.client.post(STEP2_URL, data)
                self.assertEqual(response.status_code, 200)
                self.assertNotIn("feed_name", self.client.session[SESSION_KEY])

    def test_step2_empty_name_rerenders(self):
        response = self.client.post(STEP2_URL, {"feed_name": ""})
        self.assertEqual(response.status_code, 200)

    def test_step2_without_step1_session_redirects_to_step1(self):
        _seed_session(self.client, {})
        self.assertRedirects(self.client.get(STEP2_URL), STEP1_URL, fetch_redirect_response=False)

    def test_step2_catalog_that_already_has_a_push_feed_rerenders(self):
        catalog = _make_catalog("dup-cat")
        PushFeed.objects.create(
            catalog=catalog,
            name="Surface variables",
            valid_time_format="CONTENT",
        )
        _seed_session(self.client, {"catalog_mode": "select", "catalog_id": catalog.pk})
        response = self.client.post(STEP2_URL, {"feed_name": "Surface variables"})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("feed_name", self.client.session[SESSION_KEY])


# =============================================================================
# Upload sample AJAX endpoint
# =============================================================================


class UploadSampleAjaxTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_ajax", "x@y.com", "pw")
        dial_org(self.client)
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
        mock_info.data = xr.DataArray(np.array([[vmin, vmax]]), dims=("y", "x"))
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

    def test_refuses_a_sample_over_the_size_limit_without_opening_it(self):
        mock_plugin = self._mock_plugin()
        with patch("georiva.ingestion.upload_wizard_views.SAMPLE_MAX_BYTES", 3):
            data = self._post_sample(mock_plugin).json()
        self.assertIn("larger than", data["error"])
        mock_plugin.list_variables.assert_not_called()

    def test_value_range_is_scanned_from_a_thinned_sample(self):
        mock_plugin = self._mock_plugin()
        mock_plugin.open_variable.return_value.__enter__.return_value.data = xr.DataArray(
            np.arange(100.0).reshape(10, 10), dims=("y", "x")
        )
        with patch("georiva.ingestion.upload_wizard_views.SCAN_MAX_POINTS", 25):
            var = self._post_sample(mock_plugin).json()["variables"][0]
        # Every second row and column: 0 … 88, so 99 is never read.
        self.assertEqual((var["value_min"], var["value_max"]), (0.0, 90.0))


class ThinnedTests(SimpleTestCase):
    def test_leaves_a_small_array_whole(self):
        data = xr.DataArray(np.zeros((4, 5)), dims=("y", "x"))
        self.assertIs(_thinned(data, max_points=20), data)

    def test_strides_every_dimension_to_fit_the_budget(self):
        data = xr.DataArray(np.zeros((40, 180, 360)), dims=("level", "y", "x"))
        thinned = _thinned(data, max_points=10_000)
        self.assertLessEqual(thinned.size, 10_000)
        self.assertEqual(thinned.dims, data.dims)
        self.assertTrue(all(n > 1 for n in thinned.shape))


# =============================================================================
# Step 3 — File & variable selection
# =============================================================================


class Step3CombinedTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin3", "c@d.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        _seed_session(
            self.client,
            {
                "catalog_mode": "create",
                "new_catalog_name": "WM",
                "new_catalog_slug": "wm",
                "new_catalog_format": "grib2",
                "feed_name": "Surface variables",
            },
        )

    def test_step3_renders(self):
        self.assertEqual(self.client.get(STEP3_URL).status_code, 200)

    def test_step3_refuses_oversized_samples_before_sending_them(self):
        response = self.client.get(STEP3_URL)
        self.assertContains(response, f"const MAX_BYTES    = {SAMPLE_MAX_BYTES};")
        self.assertContains(response, "file.size > MAX_BYTES")

    def test_step3_without_feed_name_redirects_to_step2(self):
        _seed_session(self.client, {"catalog_mode": "create"})
        self.assertRedirects(self.client.get(STEP3_URL), STEP2_URL, fetch_redirect_response=False)

    def test_step3_valid_post_saves_all_fields_and_redirects(self):
        variables = [{"name": "2t", "long_name": "2m temp", "units": "K"}]
        response = self.client.post(
            STEP3_URL,
            {
                "sample_filename": "20250115.grib2",
                "variables_json": json.dumps(variables),
                "selected_variables_json": json.dumps(["2t"]),
            },
        )
        self.assertRedirects(response, STEP4_URL, fetch_redirect_response=False)
        session = self.client.session[SESSION_KEY]
        self.assertEqual(session["valid_time_format"], "CONTENT")
        self.assertFalse(session["is_forecast"])
        self.assertEqual(session["selected_variable_names"], ["2t"])
        self.assertEqual(session["variables"][0]["name"], "2t")

    def test_step3_is_forecast_stored_as_true_when_checked(self):
        variables = [{"name": "2t", "long_name": "", "units": ""}]
        self.client.post(
            STEP3_URL,
            {
                "sample_filename": "20250115.grib2",
                "variables_json": json.dumps(variables),
                "selected_variables_json": json.dumps(["2t"]),
                "is_forecast": "1",
            },
        )
        self.assertTrue(self.client.session[SESSION_KEY]["is_forecast"])

    def test_step3_missing_file_rerenders_with_error(self):
        response = self.client.post(
            STEP3_URL,
            {
                "variables_json": "[]",
                "selected_variables_json": "[]",
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step3_no_selection_rerenders_with_error(self):
        variables = [{"name": "2t", "long_name": "", "units": ""}]
        response = self.client.post(
            STEP3_URL,
            {
                "sample_filename": "20250115.grib2",
                "variables_json": json.dumps(variables),
                "selected_variables_json": json.dumps([]),
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step3_missing_format_rerenders_with_error_for_geotiff(self):
        _seed_session(
            self.client,
            {
                "catalog_mode": "create",
                "new_catalog_name": "Imagery",
                "new_catalog_slug": "imagery",
                "new_catalog_format": "geotiff",
                "feed_name": "Surface variables",
            },
        )
        variables = [{"name": "band_1", "long_name": "", "units": ""}]
        response = self.client.post(
            STEP3_URL,
            {
                "sample_filename": "20250115.tif",
                "variables_json": json.dumps(variables),
                "selected_variables_json": json.dumps(["band_1"]),
                "valid_time_format": "",
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step3_show_filename_format_false_for_grib(self):
        response = self.client.get(STEP3_URL)
        self.assertFalse(response.context["show_filename_format"])
        self.assertContains(response, "Time will be read from the file content")

    def test_step3_show_filename_format_true_for_geotiff(self):
        _seed_session(
            self.client,
            {
                "catalog_mode": "create",
                "new_catalog_name": "Imagery",
                "new_catalog_slug": "imagery",
                "new_catalog_format": "geotiff",
                "feed_name": "Bands",
            },
        )
        response = self.client.get(STEP3_URL)
        self.assertTrue(response.context["show_filename_format"])
        self.assertContains(response, "Valid time format")


# =============================================================================
# Step 4 — Collection setup
# =============================================================================


class Step4CollectionsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin4", "d@e.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        _seed_session(
            self.client,
            {
                "catalog_mode": "create",
                "new_catalog_name": "WM",
                "new_catalog_slug": "wm",
                "new_catalog_format": "grib2",
                "feed_name": "Surface variables",
                "variables": [{"name": "2t", "long_name": "2m temp", "units": "K"}],
                "selected_variable_names": ["2t"],
                "sample_filename": "20250115.grib2",
                "valid_time_format": "CONTENT",
                "is_forecast": False,
            },
        )

    def test_step4_renders(self):
        self.assertEqual(self.client.get(STEP4_URL).status_code, 200)

    def test_step4_without_selected_variables_redirects_to_step3(self):
        _seed_session(self.client, {"catalog_mode": "create", "feed_name": "X"})
        self.assertRedirects(self.client.get(STEP4_URL), STEP3_URL, fetch_redirect_response=False)

    def test_step4_valid_post_saves_and_redirects(self):
        collections = [{"name": "WM Collection 1", "slug": "wm-collection-1"}]
        assignments = [_assignment()]
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
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
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)

    def test_step4_empty_collection_name_rerenders(self):
        collections = [{"name": "", "slug": ""}]
        assignments = [_assignment()]
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step4_collection_with_no_variables_rerenders(self):
        # Two collections but only one variable — second collection is empty
        collections = [
            {"name": "Col A", "slug": "col-a"},
            {"name": "Col B", "slug": "col-b"},
        ]
        assignments = [_assignment()]
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step4_missing_unit_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(unit_id=None, unit_create="")]
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step4_invalid_create_unit_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(unit_create="not_a_real_unit_xyz")]
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step4_missing_value_range_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(value_min=None, value_max=None)]
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
        self.assertEqual(response.status_code, 200)

    def test_step4_min_not_below_max_rerenders(self):
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [_assignment(value_min=50.0, value_max=50.0)]
        response = self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )
        self.assertEqual(response.status_code, 200)


# =============================================================================
# Step 4 — duplicate source_name validation
# =============================================================================


class Step4DuplicateSourceNameTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin4dup", "dup@test.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        _seed_session(
            self.client,
            {
                "catalog_mode": "create",
                "new_catalog_name": "WM",
                "new_catalog_slug": "wm",
                "new_catalog_format": "grib2",
                "feed_name": "Surface variables",
                "variables": [{"name": "2t", "long_name": "2m temp", "units": "K"}],
                "selected_variable_names": ["2t"],
                "sample_filename": "20250115.grib2",
                "valid_time_format": "CONTENT",
                "is_forecast": False,
            },
        )

    def _post(self, collections, assignments):
        return self.client.post(
            STEP4_URL,
            {
                "collections_json": json.dumps(collections),
                "assignments_json": json.dumps(assignments),
            },
        )

    def test_same_source_name_in_two_collections_rerenders_with_error(self):
        collections = [
            {"name": "Col A", "slug": "col-a"},
            {"name": "Col B", "slug": "col-b"},
        ]
        assignments = [
            _assignment(variable_name="2t", collection_idx=0),
            _assignment(variable_name="2t", collection_idx=1),
        ]
        response = self._post(collections, assignments)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "2t")
        self.assertContains(response, "Col A")
        self.assertContains(response, "Col B")

    def test_same_source_name_same_collection_is_allowed(self):
        # Two assignments pointing to the same collection slug — not a conflict.
        collections = [{"name": "Col A", "slug": "col-a"}]
        assignments = [
            _assignment(variable_name="2t", collection_idx=0),
            _assignment(variable_name="2t", collection_idx=0),
        ]
        response = self._post(collections, assignments)
        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)

    def test_conflict_with_existing_variable_in_different_collection_rerenders(self):
        catalog = _make_catalog(slug="existing-cat")
        existing_col = _make_collection(catalog, slug="existing-col")
        Variable.objects.create(
            collection=existing_col,
            slug="2t",
            name="2m temperature",
            unit=Unit.objects.get(symbol="K"),
        )
        _seed_session(
            self.client,
            {
                "catalog_mode": "existing",
                "catalog_id": catalog.pk,
                "feed_name": "New config",
                "variables": [{"name": "2t", "long_name": "2m temp", "units": "K"}],
                "selected_variable_names": ["2t"],
                "sample_filename": "20250115.grib2",
                "valid_time_format": "CONTENT",
                "is_forecast": False,
            },
        )
        collections = [{"name": "New Col", "slug": "new-col"}]
        assignments = [_assignment(variable_name="2t", collection_idx=0)]
        response = self._post(collections, assignments)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "2t")

    def test_same_source_name_same_existing_collection_is_allowed(self):
        catalog = _make_catalog(slug="reuse-cat")
        existing_col = _make_collection(catalog, slug="reuse-col")
        Variable.objects.create(
            collection=existing_col,
            slug="2t",
            name="2m temperature",
            unit=Unit.objects.get(symbol="K"),
        )
        _seed_session(
            self.client,
            {
                "catalog_mode": "existing",
                "catalog_id": catalog.pk,
                "feed_name": "New config 2",
                "variables": [{"name": "2t", "long_name": "2m temp", "units": "K"}],
                "selected_variable_names": ["2t"],
                "sample_filename": "20250115.grib2",
                "valid_time_format": "CONTENT",
                "is_forecast": False,
            },
        )
        # Assigning the same variable to the same collection slug — should pass.
        collections = [{"name": "Reuse Col", "slug": "reuse-col"}]
        assignments = [_assignment(variable_name="2t", collection_idx=0)]
        response = self._post(collections, assignments)
        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)


# =============================================================================
# Step 5 — Review
# =============================================================================


class Step5ReviewTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin5", "e@f.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        _seed_session(self.client, _full_session())

    def test_step5_renders_summary(self):
        response = self.client.get(STEP5_URL)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Surface variables")
        self.assertContains(response, "Weather Models Collection 1")
        self.assertContains(response, "2t")

    def test_step5_without_assignments_redirects_to_step4(self):
        _seed_session(self.client, {"catalog_mode": "create", "feed_name": "X"})
        self.assertRedirects(self.client.get(STEP5_URL), STEP4_URL, fetch_redirect_response=False)


# =============================================================================
# Provision
# =============================================================================


class ProvisionTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin6", "g@h.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        make_organisation()
        self.client.defaults["HTTP_HOST"] = org_host()

    def test_provision_creates_collection_push_feed_and_variables(self):
        _seed_session(self.client, _full_session())
        self.client.post(PROVISION_URL)

        collection = Collection.objects.get(slug="weather-models-collection-1")
        self.assertEqual(collection.name, "Weather Models Collection 1")

        feed = PushFeed.objects.get()
        self.assertEqual(feed.name, "Surface variables")
        self.assertEqual(feed.valid_time_format, "CONTENT")
        self.assertFalse(feed.is_forecast)
        self.assertEqual(feed.files_per_run, 1)
        self.assertIsNone(feed.expected_interval)

        var = Variable.objects.get()
        self.assertEqual(var.slug, "2t")
        self.assertEqual(var.collection, collection)
        self.assertEqual(var.name, "2m temperature")

    def test_provision_records_the_incoming_unit_beside_the_output_unit(self):
        celsius = Unit.objects.get_or_create(symbol="degC", defaults={"name": "Celsius"})[0]
        session = _full_session()
        session["assignments"] = [
            _assignment(source_symbol="K", unit_id=celsius.pk, unit_create="", value_min=-40.0, value_max=50.0)
        ]
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)

        var = Variable.objects.get()
        self.assertEqual(var.source_unit.symbol, "K")
        self.assertEqual(var.unit, celsius)

    def test_provision_keeps_the_files_own_name_when_the_display_name_is_edited(self):
        session = _full_session()
        session["assignments"] = [_assignment(long_name="Air temp", source_long_name="2 metre temperature")]
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)

        var = Variable.objects.get()
        self.assertEqual(var.name, "Air temp")
        self.assertEqual(var.source_long_name, "2 metre temperature")

    def test_provision_stores_files_per_run_and_expected_interval(self):
        from datetime import timedelta

        session = _full_session()
        session["files_per_run"] = 3
        session["expected_interval_hours"] = 6.0
        _seed_session(self.client, session)
        self.client.post(PROVISION_URL)

        feed = PushFeed.objects.get()
        self.assertEqual(feed.files_per_run, 3)
        self.assertEqual(feed.expected_interval, timedelta(hours=6))

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
        self.assertEqual(PushFeed.objects.get().catalog, catalog)
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
        self.assertRedirects(self.client.get(PROVISION_URL), STEP5_URL, fetch_redirect_response=False)

    def test_provision_redirects_to_config_list(self):
        _seed_session(self.client, _full_session())
        response = self.client.post(PROVISION_URL)
        self.assertRedirects(response, FEED_LIST_URL, fetch_redirect_response=False)

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
            catalog=catalog,
            name="Tuned",
            slug="weather-models-collection-1",
        )
        unit = Unit.objects.get(symbol="°C")
        existing = Variable.objects.create(
            collection=collection,
            slug="2t",
            name="Hand-tuned temperature",
            transform_type=Variable.TransformType.PASSTHROUGH,
            unit=unit,
            value_min=-10.0,
            value_max=10.0,
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

    def test_provision_catalog_that_already_has_a_push_feed_errors_without_partial_state(self):
        catalog = _make_catalog("existing-cat")
        PushFeed.objects.create(
            catalog=catalog,
            name="Surface variables",
            valid_time_format="CONTENT",
        )
        session = _full_session()
        session["catalog_mode"] = "select"
        session["catalog_id"] = catalog.pk
        _seed_session(self.client, session)
        response = self.client.post(PROVISION_URL)

        self.assertRedirects(response, STEP1_URL, fetch_redirect_response=False)
        self.assertEqual(PushFeed.objects.count(), 1)
        self.assertFalse(Collection.objects.filter(slug="weather-models-collection-1").exists())
        self.assertFalse(Variable.objects.exists())

    def test_provision_assignment_without_unit_rolls_back_everything(self):
        session = _full_session()
        session["assignments"] = [_assignment(unit_id=None, unit_create="")]
        _seed_session(self.client, session)
        response = self.client.post(PROVISION_URL)

        self.assertRedirects(response, STEP5_URL, fetch_redirect_response=False)
        self.assertFalse(Catalog.objects.filter(slug="weather-models").exists())
        self.assertFalse(PushFeed.objects.exists())
        self.assertFalse(Variable.objects.exists())


class Step1SlugScopingTests(TestCase):
    """Catalog slugs are unique per organisation, so the wizard's
    "slug already taken" check must be too — otherwise one institution's
    catalog names quietly become unavailable to (and visible to) every other."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin_slug", "s@s.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.kenya = make_organisation("kenya")

    def _post(self, **headers):
        return self.client.post(
            STEP1_URL,
            {
                "catalog_mode": "create",
                "new_catalog_name": "Forecast",
                "new_catalog_slug": "forecast",
                "new_catalog_format": "grib2",
            },
            **headers,
        )

    def test_a_slug_taken_by_another_org_does_not_block_this_one(self):
        Catalog.objects.create(
            organisation=self.kenya,
            name="Forecast",
            slug="forecast",
            file_format="grib2",
        )

        # Posted on the default host — a different organisation's admin.
        response = self._post()

        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)

    def test_a_slug_taken_within_this_org_is_still_rejected(self):
        response = self._post(headers={"host": "kenya.testserver"})
        self.assertRedirects(response, STEP2_URL, fetch_redirect_response=False)

        Catalog.objects.create(
            organisation=self.kenya,
            name="Forecast",
            slug="forecast",
            file_format="grib2",
        )

        response = self._post(headers={"host": "kenya.testserver"})
        self.assertEqual(response.status_code, 200)  # re-rendered with the error
        self.assertContains(response, "already exists")


class CatalogOwnershipTests(TestCase):
    """A Catalog is either feed-managed or manually-managed, never both.

    Manual uploads must not target a feed-claimed Catalog: DataFeed deletion
    cascades to its Catalog, which would silently destroy manual Collections.
    """

    def setUp(self):
        self.user = User.objects.create_superuser("admin7", "o@o.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.client.defaults["HTTP_HOST"] = org_host()
        from georiva.sources.models import DataFeed

        self.claimed = _make_catalog(slug="chirps")
        self.unclaimed = _make_catalog(slug="local-models")
        DataFeed.objects.create(name="CHIRPS", catalog=self.claimed)

    def test_step1_dropdown_excludes_feed_claimed_catalogs(self):
        html = self.client.get(STEP1_URL).content.decode()
        self.assertIn("local-models", html)
        self.assertNotIn("chirps", html)

    def test_step1_select_post_rejects_feed_claimed_catalog(self):
        response = self.client.post(
            STEP1_URL,
            {
                "catalog_mode": "select",
                "catalog_id": str(self.claimed.pk),
            },
        )
        self.assertEqual(response.status_code, 200)  # re-rendered with error
        self.assertNotIn(SESSION_KEY, self.client.session)

    def test_step1_refusal_names_the_pull_feed(self):
        response = self.client.post(STEP1_URL, {"catalog_mode": "select", "catalog_id": str(self.claimed.pk)})
        self.assertContains(response, "already has a pull feed")
        self.assertContains(response, "CHIRPS")

    def test_provision_rejects_feed_claimed_catalog(self):
        session = _full_session()
        session["catalog_mode"] = "select"
        session["catalog_id"] = self.claimed.pk
        _seed_session(self.client, session)

        response = self.client.post(PROVISION_URL)

        self.assertRedirects(response, STEP1_URL, fetch_redirect_response=False)
        self.assertFalse(PushFeed.objects.exists())
        self.assertFalse(Variable.objects.exists())
        self.assertFalse(self.claimed.collections.exists())

    def test_provision_rejects_a_catalog_that_already_has_a_push_feed(self):
        PushFeed.objects.create(catalog=self.unclaimed, name="WRF", valid_time_format="YYYYMMDD")
        session = _full_session()
        session["catalog_mode"] = "select"
        session["catalog_id"] = self.unclaimed.pk
        _seed_session(self.client, session)

        response = self.client.post(PROVISION_URL)

        self.assertRedirects(response, STEP1_URL, fetch_redirect_response=False)
        self.assertEqual(PushFeed.objects.count(), 1)

    def test_provision_still_works_against_unclaimed_catalog(self):
        session = _full_session()
        session["catalog_mode"] = "select"
        session["catalog_id"] = self.unclaimed.pk
        _seed_session(self.client, session)

        self.client.post(PROVISION_URL)

        self.assertTrue(PushFeed.objects.filter(catalog=self.unclaimed).exists())


class DataManagerWizardAccessTests(TestCase):
    """The guided flows must work without raw model add/change permissions."""

    def setUp(self):
        from django.contrib.auth.models import Group

        self.user = User.objects.create_user("dm2", "dm2@x.com", "pw")
        self.user.groups.add(Group.objects.get(name="Data Managers"))
        join_org(self.user)
        dial_org(self.client)
        self.client.force_login(self.user)

    def test_data_manager_can_provision_via_the_wizard(self):
        _seed_session(self.client, _full_session())
        self.client.post(PROVISION_URL)
        self.assertTrue(Catalog.objects.filter(slug="weather-models").exists())
        self.assertTrue(PushFeed.objects.exists())

    def test_data_manager_can_reach_wizard_step1(self):
        self.assertEqual(self.client.get(STEP1_URL).status_code, 200)
