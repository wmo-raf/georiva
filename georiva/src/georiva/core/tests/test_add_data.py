from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.organisations.testing import dial_org

User = get_user_model()


class AddDataFrontDoorTests(TestCase):
    """The Add Data front door routes data managers to the right setup wizard."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@a.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)

    def test_front_door_offers_both_arrival_scenarios(self):
        response = self.client.get(reverse("add_data"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode()
        # Routes to the DataFeed setup wizard and the Manual Upload Setup Wizard
        self.assertIn(reverse("data_feed_add_select"), html)
        self.assertIn(reverse("upload_wizard_step1"), html)

    def test_front_door_speaks_data_manager_language(self):
        html = self.client.get(reverse("add_data")).content.decode()
        self.assertIn("automatically", html.lower())
        self.assertIn("upload", html.lower())

    def test_front_door_requires_admin_access(self):
        self.client.logout()
        response = self.client.get(reverse("add_data"))
        # Wagtail admin auth redirects anonymous users to login
        self.assertEqual(response.status_code, 302)
