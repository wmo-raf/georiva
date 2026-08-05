from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.organisations.testing import dial_org

User = get_user_model()


class PluginListTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_plugins", "pl@test.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)
        self.url = reverse("plugin_list")

    def test_page_renders(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Plugins")

    def test_metadata_for_installed_package(self):
        # Use a package guaranteed to be installed to exercise the helper
        # independently of which plugins happen to be loaded.
        from georiva.core.plugins import get_plugin_metadata

        meta = get_plugin_metadata("wagtail")
        self.assertTrue(meta["available"])
        self.assertTrue(meta["name"])
        self.assertTrue(meta["version"])

    def test_metadata_for_missing_package(self):
        from georiva.core.plugins import get_plugin_metadata

        meta = get_plugin_metadata("definitely_not_a_real_package_xyz")
        self.assertFalse(meta["available"])
        self.assertEqual(meta["name"], "definitely_not_a_real_package_xyz")
