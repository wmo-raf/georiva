"""The two launch roles, and what joining an organisation grants.

Member and Org Admin differ in exactly one place: org-management surfaces. Data
work is identical for both, which is the point of a two-role launch — the split
is about who runs the institution's account, not who may touch its data.
"""
from django.contrib.auth.models import Group
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.core.groups import DATA_MANAGERS_GROUP
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, make_user

HOST = {"host": "kenya.georiva.test"}


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class MembershipGrantsCapabilitiesTests(TestCase):

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def test_joining_an_organisation_grants_the_data_manager_group(self):
        user = make_user("amina")
        self.assertFalse(user.groups.filter(name=DATA_MANAGERS_GROUP).exists())

        add_member(user, self.kenya)

        self.assertTrue(user.groups.filter(name=DATA_MANAGERS_GROUP).exists())

    def test_a_new_member_can_reach_the_data_surfaces_immediately(self):
        user = make_user("amina")
        add_member(user, self.kenya)
        self.client.login(username="amina", password=PASSWORD)

        response = self.client.get(reverse("catalog:index"), headers=HOST)

        self.assertEqual(response.status_code, 200)

    def test_leaving_does_not_strip_capabilities_earned_elsewhere(self):
        # Groups are instance-wide and a user may belong to several
        # organisations; the host-level membership check is what closes the door.
        user = make_user("amina")
        membership = add_member(user, self.kenya)
        membership.delete()

        self.assertTrue(user.groups.filter(name=DATA_MANAGERS_GROUP).exists())

    def test_a_second_membership_does_not_duplicate_the_group(self):
        uganda = provision_organisation(name="Uganda Met", slug="uganda")
        user = make_user("amina")
        add_member(user, self.kenya)
        add_member(user, uganda)

        self.assertEqual(user.groups.filter(name=DATA_MANAGERS_GROUP).count(), 1)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class OrganisationSettingsRoleTests(TestCase):
    """The one surface the roles differ on."""

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.url = reverse("organisation_settings")

    def _login(self, username, role=None, superuser=False):
        user = make_user(username, superuser=superuser)
        if role is not None:
            add_member(user, self.kenya, role=role)
        if superuser:
            # Superusers hold no membership; they need admin access like anyone.
            group = Group.objects.create(name=f"{username} access")
            group.permissions.add(
                *Group.objects.get(name=DATA_MANAGERS_GROUP).permissions.all()
            )
            user.groups.add(group)
        self.client.login(username=username, password=PASSWORD)
        return user

    def test_an_org_admin_can_edit_their_own_organisation(self):
        self._login("amina", role=OrganisationMembership.Role.ADMIN)

        response = self.client.post(
            self.url, {"name": "Kenya Meteorological Department"}, headers=HOST
        )

        self.assertEqual(response.status_code, 302)
        self.kenya.refresh_from_db()
        self.assertEqual(self.kenya.name, "Kenya Meteorological Department")

    def test_a_member_is_refused(self):
        self._login("juma", role=OrganisationMembership.Role.MEMBER)

        response = self.client.get(self.url, headers=HOST)

        # Wagtail's admin turns a PermissionDenied into a redirect to the admin
        # home carrying a message, rather than a bare 403 page.
        self.assertRedirects(
            response, reverse("wagtailadmin_home"), fetch_redirect_response=False
        )

    def test_a_member_cannot_post_past_the_gate_either(self):
        self._login("juma", role=OrganisationMembership.Role.MEMBER)

        self.client.post(self.url, {"name": "Hijacked"}, headers=HOST)

        self.kenya.refresh_from_db()
        self.assertEqual(self.kenya.name, "Kenya Met")

    def test_the_menu_entry_is_hidden_from_members(self):
        self._login("juma", role=OrganisationMembership.Role.MEMBER)

        response = self.client.get(reverse("wagtailadmin_home"), headers=HOST)

        self.assertNotContains(response, self.url)

    def test_the_menu_entry_is_shown_to_org_admins(self):
        self._login("amina", role=OrganisationMembership.Role.ADMIN)

        response = self.client.get(reverse("wagtailadmin_home"), headers=HOST)

        self.assertContains(response, self.url)

    def test_a_superuser_administers_any_organisation_they_visit(self):
        self._login("root", superuser=True)

        response = self.client.get(self.url, headers=HOST)

        self.assertEqual(response.status_code, 200)

    def test_the_slug_is_not_editable_here(self):
        self._login("amina", role=OrganisationMembership.Role.ADMIN)

        self.client.post(self.url, {"name": "Kenya Met", "slug": "hijacked"}, headers=HOST)

        self.kenya.refresh_from_db()
        self.assertEqual(self.kenya.slug, "kenya")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class MemberManagementTests(TestCase):
    """An org admin runs their own institution's roster, and only theirs."""

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        self.admin = make_user("amina")
        add_member(self.admin, self.kenya, role=OrganisationMembership.Role.ADMIN)
        self.client.login(username="amina", password=PASSWORD)

        self.colleague = make_user("juma")
        self.kenyan_membership = add_member(self.colleague, self.kenya)
        self.outsider = make_user("okot")
        self.ugandan_membership = add_member(self.outsider, self.uganda)

    def test_the_roster_lists_only_this_organisations_people(self):
        response = self.client.get(reverse("organisation_members"), headers=HOST)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "juma")
        self.assertNotContains(response, "okot")

    def test_another_organisations_membership_cannot_be_edited(self):
        url = reverse("organisation_member_edit", args=[self.ugandan_membership.pk])

        response = self.client.get(url, headers=HOST)

        self.assertEqual(response.status_code, 404)

    def test_another_organisations_membership_cannot_be_removed(self):
        url = reverse("organisation_member_remove", args=[self.ugandan_membership.pk])

        response = self.client.post(url, headers=HOST)

        self.assertEqual(response.status_code, 404)
        self.assertTrue(
            OrganisationMembership.objects.filter(pk=self.ugandan_membership.pk).exists()
        )

    def test_adding_a_member_creates_an_account_that_can_work_immediately(self):
        response = self.client.post(
            reverse("organisation_member_add"),
            {
                "username": "wanjiru",
                "email": "wanjiru@kenyamet.test",
                "first_name": "Wanjiru",
                "last_name": "N",
                "password1": "a-long-enough-passphrase",
                "password2": "a-long-enough-passphrase",
                "role": OrganisationMembership.Role.MEMBER,
            },
            headers=HOST,
        )

        self.assertEqual(response.status_code, 302)
        membership = OrganisationMembership.objects.get(user__username="wanjiru")
        self.assertEqual(membership.organisation, self.kenya)
        self.assertTrue(
            membership.user.groups.filter(name=DATA_MANAGERS_GROUP).exists()
        )

    def test_a_role_can_be_promoted(self):
        self.client.post(
            reverse("organisation_member_edit", args=[self.kenyan_membership.pk]),
            {"role": OrganisationMembership.Role.ADMIN},
            headers=HOST,
        )

        self.kenyan_membership.refresh_from_db()
        self.assertEqual(self.kenyan_membership.role, OrganisationMembership.Role.ADMIN)

    def test_removing_a_member_keeps_their_account(self):
        self.client.post(
            reverse("organisation_member_remove", args=[self.kenyan_membership.pk]),
            headers=HOST,
        )

        self.assertFalse(
            OrganisationMembership.objects.filter(pk=self.kenyan_membership.pk).exists()
        )
        self.colleague.refresh_from_db()
        self.assertTrue(self.colleague.is_active)

    def test_a_plain_member_cannot_reach_the_roster(self):
        self.client.login(username="juma", password=PASSWORD)

        response = self.client.get(reverse("organisation_members"), headers=HOST)

        self.assertRedirects(
            response, reverse("wagtailadmin_home"), fetch_redirect_response=False
        )

    def test_a_plain_member_cannot_add_one(self):
        self.client.login(username="juma", password=PASSWORD)

        self.client.post(
            reverse("organisation_member_add"),
            {
                "username": "smuggled",
                "email": "s@x.test",
                "password1": "a-long-enough-passphrase",
                "password2": "a-long-enough-passphrase",
                "role": OrganisationMembership.Role.ADMIN,
            },
            headers=HOST,
        )

        self.assertFalse(
            OrganisationMembership.objects.filter(user__username="smuggled").exists()
        )
