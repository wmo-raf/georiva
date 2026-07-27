from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.organisations.models import Organisation, OrganisationMembership
from georiva.organisations.provisioning import org_page_group_name, provision_organisation

from .factories import PASSWORD, add_member, make_user

HOST = {"host": "kenya.georiva.test"}


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class OrganisationAdminAccessTests(TestCase):

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def _login(self, username, **kwargs):
        user = make_user(username, **kwargs)
        # Wagtail's own admin gate: everyone who reaches /admin/ needs this.
        admin_access = Group.objects.create(name=f"{username} admin access")
        admin_access.permissions.add(Permission.objects.get(codename="access_admin"))
        user.groups.add(admin_access)
        self.client.login(username=username, password=PASSWORD)
        return user

    def test_superuser_can_reach_the_organisation_index(self):
        self._login("root", superuser=True)
        response = self.client.get(reverse("organisation:index"), headers=HOST)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Kenya Met")

    def test_org_admin_cannot_reach_the_organisation_index(self):
        user = self._login("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        response = self.client.get(reverse("organisation:index"), headers=HOST)
        self.assertEqual(response.status_code, 302)

    def test_org_admin_cannot_reach_the_organisation_add_view(self):
        user = self._login("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        response = self.client.get(reverse("organisation:add"), headers=HOST)
        self.assertEqual(response.status_code, 302)

    def test_org_admin_cannot_edit_an_organisation(self):
        user = self._login("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        response = self.client.post(
            reverse("organisation:edit", args=[self.kenya.pk]),
            {"name": "Hijacked"},
            headers=HOST,
        )
        self.assertEqual(response.status_code, 302)
        self.kenya.refresh_from_db()
        self.assertEqual(self.kenya.name, "Kenya Met")

    def test_menu_item_is_hidden_from_non_superusers(self):
        user = self._login("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        response = self.client.get(reverse("wagtailadmin_home"), headers=HOST)
        self.assertNotContains(response, reverse("organisation:index"))

    def test_org_admin_cannot_reach_the_membership_admin(self):
        user = self._login("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        response = self.client.get(reverse("organisation_membership:index"), headers=HOST)
        self.assertEqual(response.status_code, 302)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class FirstOrgAdminTests(TestCase):
    """The superuser's path from "new institution" to "someone who can run it"."""

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        make_user("root", superuser=True)
        self.client.login(username="root", password=PASSWORD)

    def test_superuser_can_make_a_users_first_org_admin_membership(self):
        amina = make_user("amina")

        response = self.client.post(
            reverse("organisation_membership:add"),
            {
                "user": amina.pk,
                "organisation": self.kenya.pk,
                "role": OrganisationMembership.Role.ADMIN,
            },
            headers=HOST,
        )
        self.assertEqual(response.status_code, 302)

        membership = OrganisationMembership.objects.get(user=amina, organisation=self.kenya)
        self.assertEqual(membership.role, OrganisationMembership.Role.ADMIN)

    def test_the_new_org_admin_holds_that_role_on_their_host(self):
        amina = make_user("amina")
        add_member(amina, self.kenya, role=OrganisationMembership.Role.ADMIN)

        self.client.login(username="amina", password=PASSWORD)
        request = self.client.get("/admin/login/", headers=HOST).wsgi_request
        self.assertEqual(request.active_org_role, OrganisationMembership.Role.ADMIN)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class OrganisationDeletionTests(TestCase):

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        make_user("root", superuser=True)
        self.client.login(username="root", password=PASSWORD)

    def test_even_a_superuser_cannot_delete_an_organisation_from_the_admin(self):
        response = self.client.post(reverse("organisation:delete", args=[self.kenya.pk]), headers=HOST)
        self.assertEqual(response.status_code, 302)
        self.assertTrue(Organisation.objects.filter(pk=self.kenya.pk).exists())


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class OrganisationAdminProvisioningTests(TestCase):

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        make_user("root", superuser=True)
        self.client.login(username="root", password=PASSWORD)

    def test_creating_from_the_admin_provisions_site_page_and_group(self):
        response = self.client.post(
            reverse("organisation:add"),
            {
                "name": "ICPAC",
                "slug": "icpac",
                "description": "",
                "contact_email": "",
                "website": "",
                "country": "",
                "default_provider": "",
                "default_provider_url": "",
            },
            headers=HOST,
        )
        self.assertEqual(response.status_code, 302)

        icpac = Organisation.objects.get(slug="icpac")
        self.assertEqual(icpac.site.hostname, "icpac.georiva.test")
        self.assertIsNotNone(icpac.site.root_page_id)
        self.assertTrue(Group.objects.filter(name=org_page_group_name("icpac")).exists())

    def test_reserved_slug_is_rejected_by_the_form(self):
        response = self.client.post(
            reverse("organisation:add"),
            {
                "name": "Admin",
                "slug": "admin",
                "description": "",
                "contact_email": "",
                "website": "",
                "country": "",
                "default_provider": "",
                "default_provider_url": "",
            },
            headers=HOST,
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(Organisation.objects.filter(slug="admin").exists())

    def test_edit_form_cannot_change_the_slug(self):
        response = self.client.post(
            reverse("organisation:edit", args=[self.kenya.pk]),
            {
                "name": "Kenya Meteorological Department",
                "slug": "hijacked",
                "description": "",
                "contact_email": "",
                "website": "",
                "country": "",
                "default_provider": "",
                "default_provider_url": "",
            },
            headers=HOST,
        )
        self.assertEqual(response.status_code, 302)

        self.kenya.refresh_from_db()
        self.assertEqual(self.kenya.name, "Kenya Meteorological Department")
        self.assertEqual(self.kenya.slug, "kenya")
