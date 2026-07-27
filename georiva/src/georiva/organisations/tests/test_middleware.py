from django.test import TestCase, override_settings

from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, make_user

# A URL that exists on every host, is cheap to serve, and needs no page tree.
PROBE_URL = "/admin/login/"


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class HostResolutionTests(TestCase):

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.icpac = provision_organisation(name="ICPAC", slug="icpac")

    def test_known_host_resolves_to_its_organisation(self):
        response = self.client.get(PROBE_URL, headers={"host": "kenya.georiva.test"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.wsgi_request.active_org, self.kenya)

    def test_each_host_resolves_to_its_own_organisation(self):
        response = self.client.get(PROBE_URL, headers={"host": "icpac.georiva.test"})
        self.assertEqual(response.wsgi_request.active_org, self.icpac)

    def test_unknown_host_fails_closed_with_404(self):
        response = self.client.get(PROBE_URL, headers={"host": "nobody.georiva.test"})
        self.assertEqual(response.status_code, 404)

    def test_unknown_host_does_not_fall_back_to_the_default_site(self):
        # Wagtail's own Site.find_for_request would hand back the default site
        # here; tenancy resolution must not.
        default_site = self.kenya.site
        default_site.is_default_site = True
        default_site.save(update_fields=["is_default_site"])

        response = self.client.get(PROBE_URL, headers={"host": "somewhere-else.georiva.test"})
        self.assertEqual(response.status_code, 404)

    def test_health_endpoint_answers_on_an_unknown_host(self):
        response = self.client.get("/health/", headers={"host": "georiva"})
        self.assertEqual(response.status_code, 200)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class ActiveRoleTests(TestCase):

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.icpac = provision_organisation(name="ICPAC", slug="icpac")
        self.member = make_user("amina")
        add_member(self.member, self.kenya)

    def _get(self, host="kenya.georiva.test"):
        return self.client.get(PROBE_URL, headers={"host": host})

    def test_anonymous_request_has_no_role(self):
        request = self._get().wsgi_request
        self.assertEqual(request.active_org, self.kenya)
        self.assertIsNone(request.active_org_role)

    def test_member_gets_their_role_on_their_own_host(self):
        self.client.login(username="amina", password=PASSWORD)
        request = self._get().wsgi_request
        self.assertEqual(request.active_org_role, OrganisationMembership.Role.MEMBER)

    def test_member_has_no_role_on_another_organisations_host(self):
        self.client.login(username="amina", password=PASSWORD)
        request = self._get(host="icpac.georiva.test").wsgi_request
        self.assertEqual(request.active_org, self.icpac)
        self.assertIsNone(request.active_org_role)

    def test_revoked_membership_takes_effect_on_the_next_request(self):
        self.client.login(username="amina", password=PASSWORD)
        self.assertEqual(self._get().wsgi_request.active_org_role, OrganisationMembership.Role.MEMBER)

        OrganisationMembership.objects.filter(user=self.member, organisation=self.kenya).delete()

        self.assertIsNone(self._get().wsgi_request.active_org_role)

    def test_org_admin_role_is_reported(self):
        add_member(self.member, self.icpac, role=OrganisationMembership.Role.ADMIN)
        self.client.login(username="amina", password=PASSWORD)
        request = self._get(host="icpac.georiva.test").wsgi_request
        self.assertEqual(request.active_org_role, OrganisationMembership.Role.ADMIN)

    def test_superuser_enters_any_host_as_admin_without_membership(self):
        make_user("root", superuser=True)
        self.client.login(username="root", password=PASSWORD)
        for host, organisation in [("kenya.georiva.test", self.kenya), ("icpac.georiva.test", self.icpac)]:
            with self.subTest(host=host):
                request = self._get(host=host).wsgi_request
                self.assertEqual(request.active_org, organisation)
                self.assertEqual(request.active_org_role, OrganisationMembership.Role.ADMIN)

    def test_inactive_user_has_no_role(self):
        self.member.is_active = False
        self.member.save(update_fields=["is_active"])
        request = self._get().wsgi_request
        self.assertIsNone(request.active_org_role)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class SingleLoginAcrossOrgHostsTests(TestCase):

    @override_settings(SESSION_COOKIE_DOMAIN=".georiva.test")
    def test_session_cookie_is_issued_for_the_parent_domain(self):
        provision_organisation(name="Kenya Met", slug="kenya")
        make_user("root", superuser=True)

        self.client.post(
            PROBE_URL,
            {"username": "root", "password": PASSWORD},
            headers={"host": "kenya.georiva.test"},
        )
        self.assertEqual(self.client.cookies["sessionid"]["domain"], ".georiva.test")

    def test_one_login_is_honoured_on_every_org_host(self):
        kenya = provision_organisation(name="Kenya Met", slug="kenya")
        icpac = provision_organisation(name="ICPAC", slug="icpac")
        user = make_user("amina")
        add_member(user, kenya)
        add_member(user, icpac)

        self.client.login(username="amina", password=PASSWORD)

        for host in ["kenya.georiva.test", "icpac.georiva.test"]:
            with self.subTest(host=host):
                request = self.client.get(PROBE_URL, headers={"host": host}).wsgi_request
                self.assertTrue(request.user.is_authenticated)
