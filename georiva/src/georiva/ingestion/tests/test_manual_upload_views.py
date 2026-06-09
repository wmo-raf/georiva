from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog, Collection
from georiva.ingestion.models import ManualUploadConfig, ManualUploadConfigVariable

User = get_user_model()

LIST_URL = "/admin/manual-uploads/"
EDIT_URL = "/admin/manual-uploads/{}/edit/"
DELETE_URL = "/admin/manual-uploads/{}/delete/"


def _make_catalog(slug="cat"):
    return Catalog.objects.create(name=slug, slug=slug, file_format="grib2")


def _make_collection(catalog, slug="col"):
    return Collection.objects.create(name=slug, slug=slug, catalog=catalog)


def _make_config(catalog, name="Surface variables", fmt="YYYYMMDD"):
    return ManualUploadConfig.objects.create(
        catalog=catalog, name=name, valid_time_format=fmt,
    )


class ManualUploadConfigListTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@b.com", "pw")
        self.client.force_login(self.user)

    def test_list_renders(self):
        response = self.client.get(LIST_URL)
        self.assertEqual(response.status_code, 200)

    def test_list_shows_config_name_and_catalog(self):
        catalog = _make_catalog("weather-models")
        _make_config(catalog, name="Surface variables")
        response = self.client.get(LIST_URL)
        self.assertContains(response, "Surface variables")
        self.assertContains(response, "weather-models")

    def test_list_shows_variable_count(self):
        catalog = _make_catalog()
        col = _make_collection(catalog)
        config = _make_config(catalog)
        ManualUploadConfigVariable.objects.create(
            config=config, collection=col, variable_name="2t"
        )
        ManualUploadConfigVariable.objects.create(
            config=config, collection=col, variable_name="tp"
        )
        response = self.client.get(LIST_URL)
        self.assertContains(response, "2")

    def test_list_empty_state_renders_without_error(self):
        response = self.client.get(LIST_URL)
        self.assertEqual(response.status_code, 200)

    def test_list_contains_new_config_link_to_wizard(self):
        response = self.client.get(LIST_URL)
        self.assertContains(response, "/admin/manual-uploads/wizard/step1/")


class ManualUploadConfigEditTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin2", "b@c.com", "pw")
        self.client.force_login(self.user)
        self.catalog = _make_catalog()
        self.config = _make_config(self.catalog)

    def test_edit_renders(self):
        response = self.client.get(EDIT_URL.format(self.config.pk))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Surface variables")

    def test_edit_valid_post_saves_and_redirects_to_list(self):
        response = self.client.post(EDIT_URL.format(self.config.pk), {
            "name": "Updated name",
            "is_forecast": False,
            "valid_time_format": "DDMMYYYY",
        })
        self.assertRedirects(response, LIST_URL, fetch_redirect_response=False)
        self.config.refresh_from_db()
        self.assertEqual(self.config.name, "Updated name")
        self.assertEqual(self.config.valid_time_format, "DDMMYYYY")

    def test_edit_invalid_post_rerenders_with_errors(self):
        response = self.client.post(EDIT_URL.format(self.config.pk), {
            "name": "",
            "valid_time_format": "YYYYMMDD",
        })
        self.assertEqual(response.status_code, 200)

    def test_edit_unknown_pk_returns_404(self):
        response = self.client.get(EDIT_URL.format(99999))
        self.assertEqual(response.status_code, 404)

    def test_edit_lists_variables_with_collection_link(self):
        col = _make_collection(self.catalog, slug="surface")
        ManualUploadConfigVariable.objects.create(
            config=self.config, collection=col, variable_name="2t",
            long_name="2m temperature", units="K",
        )
        response = self.client.get(EDIT_URL.format(self.config.pk))
        self.assertContains(response, "2t")
        self.assertContains(response, "2m temperature")
        self.assertContains(response, f"/admin/collection/edit/{col.pk}/")


class ManualUploadConfigDeleteTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin3", "c@d.com", "pw")
        self.client.force_login(self.user)
        self.catalog = _make_catalog()
        self.config = _make_config(self.catalog)

    def test_delete_get_shows_confirmation(self):
        response = self.client.get(DELETE_URL.format(self.config.pk))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Surface variables")

    def test_delete_post_removes_config_and_redirects(self):
        pk = self.config.pk
        response = self.client.post(DELETE_URL.format(pk))
        self.assertRedirects(response, LIST_URL, fetch_redirect_response=False)
        self.assertFalse(ManualUploadConfig.objects.filter(pk=pk).exists())

    def test_delete_cascades_to_variables(self):
        col = _make_collection(self.catalog)
        ManualUploadConfigVariable.objects.create(
            config=self.config, collection=col, variable_name="2t"
        )
        self.client.post(DELETE_URL.format(self.config.pk))
        self.assertEqual(ManualUploadConfigVariable.objects.count(), 0)

    def test_delete_unknown_pk_returns_404(self):
        response = self.client.post(DELETE_URL.format(99999))
        self.assertEqual(response.status_code, 404)
