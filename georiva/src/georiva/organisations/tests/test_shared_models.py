"""The models that are not simply one organisation's, and how each behaves.

Three verdicts are under test here (decision #269), and they differ:

* ``Topic`` — instance-global and instance-curated. Every organisation reads the
  same taxonomy; only the instance admin may change it.
* ``ColorRamp`` — global *with* org overrides; its admin-surface tier tests
  live beside the ramp catalog in ``core/tests/test_color_ramps.py``.
* ``AdminBoundary`` — shared reference data whose chooser is deliberately
  unscoped, because a regional centre clips against several countries.

What is asserted is external behavior on the real admin URLs: which rows a
listing shows, which edits are refused, which options a form offers.
"""

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.models.catalog import Topic
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.ownership import is_shared_reference
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, grant_everything, make_user

KENYA_HOST = "kenya.georiva.test"


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class TopicIsInstanceAdminOnlyTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def _login(self, username, *, superuser=False):
        user = grant_everything(make_user(username, superuser=superuser))
        if not superuser:
            add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        self.client.login(username=username, password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = KENYA_HOST
        return user

    def test_superuser_can_reach_the_topic_admin(self):
        self._login("root", superuser=True)
        self.assertEqual(self.client.get(reverse("topic:index")).status_code, 200)

    def test_org_admin_cannot_reach_the_topic_index(self):
        self._login("amina")
        self.assertEqual(self.client.get(reverse("topic:index")).status_code, 302)

    def test_org_admin_cannot_create_a_topic(self):
        self._login("amina")
        response = self.client.post(reverse("topic:add"), {"name": "Smuggled", "description": "", "sort_order": 0})
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Topic.objects.filter(name="Smuggled").exists())

    def test_the_topic_menu_item_is_hidden_from_org_admins(self):
        self._login("amina")
        response = self.client.get(reverse("wagtailadmin_home"))
        self.assertNotContains(response, reverse("topic:index"))

    def test_topics_are_still_offered_on_the_catalog_form(self):
        """The other half: members meet topics as options, and must keep doing so."""
        Topic.objects.create(name="Rainfall")
        self._login("amina")
        response = self.client.get(reverse("catalog:add"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Rainfall")


class SharedReferenceDataTests(TestCase):
    """The exemption, asserted rather than left to a comment."""

    def test_admin_boundaries_read_as_shared_reference_data(self):
        from adminboundarymanager.models import AdminBoundary

        self.assertTrue(is_shared_reference(AdminBoundary))
