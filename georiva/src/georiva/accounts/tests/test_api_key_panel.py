"""The panel where a person manages their own keys, and nobody else's.

The one thing this surface must never do is let a key be listed, revoked or
reasoned about by anyone other than its holder. Keys are not organisation data
— an org admin has no business over their members' personal credentials, and a
superuser has no more claim on them than anyone else — so every lookup here is
scoped by ``request.user`` rather than by the active organisation.
"""
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.accounts.models import ApiKey
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ApiKeyPanelTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.owner = make_user("owner")
        add_member(cls.owner, cls.kenya)
        cls.colleague = make_user("colleague")
        add_member(cls.colleague, cls.kenya)
        cls.admin = make_user("instance-admin", superuser=True)

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"
        self.client.login(username=self.owner.username, password=PASSWORD)

    def test_creating_a_key_shows_the_secret_exactly_once(self):
        response = self.client.post(
            reverse("api_keys"), {"name": "QGIS laptop"}, follow=True,
        )
        self.assertEqual(response.status_code, 200)
        key = ApiKey.objects.get(user=self.owner)
        secret = response.context["new_secret"]
        self.assertTrue(secret.startswith(key.prefix))

        # The secret is gone from the page the moment it is reloaded.
        self.assertIsNone(self.client.get(reverse("api_keys")).context["new_secret"])

    def test_the_listing_shows_only_the_signed_in_users_keys(self):
        ApiKey.objects.mint(user=self.owner, name="mine")
        ApiKey.objects.mint(user=self.colleague, name="theirs")

        response = self.client.get(reverse("api_keys"))

        self.assertEqual([k.name for k in response.context["keys"]], ["mine"])

    def test_an_instance_admin_does_not_see_other_peoples_keys(self):
        ApiKey.objects.mint(user=self.owner, name="mine")
        self.client.login(username=self.admin.username, password=PASSWORD)

        response = self.client.get(reverse("api_keys"))

        self.assertEqual(list(response.context["keys"]), [])

    def test_revoking_a_key_retires_it(self):
        key, _ = ApiKey.objects.mint(user=self.owner, name="lost phone")

        self.client.post(reverse("api_key_revoke", args=[key.pk]))

        key.refresh_from_db()
        self.assertIsNotNone(key.revoked_at)

    def test_revoking_somebody_elses_key_is_not_found(self):
        key, _ = ApiKey.objects.mint(user=self.colleague, name="theirs")

        response = self.client.post(reverse("api_key_revoke", args=[key.pk]))

        self.assertEqual(response.status_code, 404)
        key.refresh_from_db()
        self.assertIsNone(key.revoked_at)

    def test_an_instance_admin_cannot_revoke_somebody_elses_key_either(self):
        key, _ = ApiKey.objects.mint(user=self.owner, name="mine")
        self.client.login(username=self.admin.username, password=PASSWORD)

        response = self.client.post(reverse("api_key_revoke", args=[key.pk]))

        self.assertEqual(response.status_code, 404)
        key.refresh_from_db()
        self.assertIsNone(key.revoked_at)

    def test_a_get_never_revokes(self):
        key, _ = ApiKey.objects.mint(user=self.owner, name="mine")

        self.client.get(reverse("api_key_revoke", args=[key.pk]))

        key.refresh_from_db()
        self.assertIsNone(key.revoked_at)

    def test_an_anonymous_visitor_is_sent_to_the_login_page(self):
        self.client.logout()

        response = self.client.get(reverse("api_keys"))

        self.assertEqual(response.status_code, 302)
        self.assertIn("login", response["Location"])
