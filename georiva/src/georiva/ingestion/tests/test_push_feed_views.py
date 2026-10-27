from django.contrib.auth import get_user_model
from django.test import TestCase

from georiva.core.models import Catalog
from georiva.ingestion.models import PushFeed
from georiva.organisations.testing import dial_org, make_organisation

User = get_user_model()

LIST_URL = "/admin/push-feeds/"
DELETE_URL = "/admin/push-feeds/{}/delete/"


def _make_catalog(slug="cat"):
    return Catalog.objects.create(organisation=make_organisation(), name=slug, slug=slug, file_format="grib2")


def _make_feed(catalog, name="Surface variables", fmt="YYYYMMDD"):
    return PushFeed.objects.create(catalog=catalog, name=name, valid_time_format=fmt)


class PushFeedListTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@b.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)

    def test_list_renders_empty_state(self):
        response = self.client.get(LIST_URL)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No push feeds yet.")

    def test_list_shows_feed_name_and_catalog_and_links_to_its_page(self):
        feed = _make_feed(_make_catalog("weather-models"))
        response = self.client.get(LIST_URL)
        self.assertContains(response, "Surface variables")
        self.assertContains(response, "weather-models")
        self.assertContains(response, f"/admin/push-feeds/{feed.pk}/")

    def test_list_links_to_the_new_push_feed_wizard(self):
        response = self.client.get(LIST_URL)
        self.assertContains(response, "/admin/push-feeds/wizard/step1/")


class PushFeedDeleteTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin3", "c@d.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.catalog = _make_catalog()
        self.feed = _make_feed(self.catalog)

    def test_delete_get_shows_confirmation(self):
        response = self.client.get(DELETE_URL.format(self.feed.pk))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Surface variables")

    def test_delete_post_removes_feed_and_keeps_the_catalog(self):
        pk = self.feed.pk
        response = self.client.post(DELETE_URL.format(pk))
        self.assertRedirects(response, LIST_URL, fetch_redirect_response=False)
        self.assertFalse(PushFeed.objects.filter(pk=pk).exists())
        self.assertTrue(Catalog.objects.filter(pk=self.catalog.pk).exists())

    def test_delete_unknown_pk_returns_404(self):
        response = self.client.post(DELETE_URL.format(99999))
        self.assertEqual(response.status_code, 404)
