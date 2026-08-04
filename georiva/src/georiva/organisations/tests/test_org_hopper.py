"""What the org-hopper shows, and — more to the point — whom it shows it to.

The hopper is the one admin surface that names organisations other than the
host's, so the tests that matter most here are the ones about the list's edges: a
member sees the institutions they hold a membership row in and nobody else's, and
the instance admin sees all of them.
"""
from django.contrib.auth.models import Group, Permission
from django.test import RequestFactory, TestCase, override_settings
from django.urls import reverse

from georiva.organisations.hopper import (
    AVATAR_PALETTE_SIZE,
    SEARCH_THRESHOLD,
    avatar_index,
    avatar_letters,
    org_hopper_context,
)
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, make_user

KENYA_HOST = "kenya.georiva.test"


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class OrgHopperScriptTests(TestCase):
    """The per-request half, driven over its real URL."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.tanzania = provision_organisation(name="Tanzania Met", slug="tanzania")

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = KENYA_HOST
        self.url = reverse("organisation_hopper_script")

    def _login(self, username, *, superuser=False):
        user = make_user(username, superuser=superuser)
        admin_access = Group.objects.create(name=f"{username} admin access")
        admin_access.permissions.add(Permission.objects.get(codename="access_admin"))
        user.groups.add(admin_access)
        self.client.login(username=username, password=PASSWORD)
        return user

    def _script(self, **kwargs):
        response = self.client.get(self.url, **kwargs)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response["Content-Type"].startswith("text/javascript"))
        return response.content.decode()

    # -- who is listed ------------------------------------------------------

    def test_a_multi_org_member_sees_every_organisation_they_belong_to(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)

        script = self._script()

        self.assertIn("Kenya Met", script)
        self.assertIn("Uganda Met", script)
        self.assertIn("uganda.georiva.test", script)

    def test_a_member_never_sees_an_organisation_they_do_not_belong_to(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)

        script = self._script()

        self.assertNotIn("Tanzania Met", script)
        self.assertNotIn("tanzania.georiva.test", script)

    def test_a_revoked_membership_leaves_the_list_at_once(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        membership = add_member(user, self.uganda)
        self.assertIn("Uganda Met", self._script())

        membership.delete()

        self.assertNotIn("Uganda Met", self._script())

    def test_the_instance_admin_sees_every_organisation(self):
        self._login("root", superuser=True)

        script = self._script()

        for name in ("Kenya Met", "Uganda Met", "Tanzania Met"):
            self.assertIn(name, script)

    def test_an_anonymous_request_is_told_about_no_organisation(self):
        """The login page loads this too, and there is nobody to answer for."""
        self.assertEqual(self._script().strip(), "")

    # -- what the block offers ---------------------------------------------

    def test_the_current_organisation_is_marked_and_the_popover_offered(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)

        script = self._script()

        self.assertIn("data-gr-orghop-trigger", script)
        self.assertIn("gr-orghop__item--current", script)
        # The tick, as the markup carries it into the script literal: JSON
        # encoding escapes every non-ASCII character on the way out.
        self.assertIn("\\u2713", script)

    def test_a_single_org_user_gets_a_static_badge_with_no_popover(self):
        user = self._login("joseph")
        add_member(user, self.kenya)

        script = self._script()

        self.assertIn("gr-orghop__badge", script)
        self.assertNotIn("data-gr-orghop-trigger", script)
        self.assertNotIn("data-gr-orghop-popover", script)

    def test_a_short_list_gets_no_search_box(self):
        self._login("root", superuser=True)
        self.assertNotIn("data-gr-orghop-search", self._script())

    def test_a_long_list_gets_a_search_box(self):
        self._login("root", superuser=True)
        for index in range(SEARCH_THRESHOLD):
            provision_organisation(name=f"Met {index}", slug=f"met-{index}")

        self.assertIn("data-gr-orghop-search", self._script())

    def test_the_search_box_follows_the_list_length_not_the_role(self):
        """A member of nine institutions has the same problem a superuser does."""
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)
        for index in range(SEARCH_THRESHOLD):
            add_member(user, provision_organisation(name=f"Met {index}", slug=f"met-{index}"))

        self.assertIn("data-gr-orghop-search", self._script())

    def test_an_entry_can_be_searched_by_name_slug_or_host(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)

        script = self._script()

        self.assertIn("uganda met uganda uganda.georiva.test", script)

    # -- the links themselves ----------------------------------------------

    def test_each_entry_is_a_plain_cross_host_link_to_that_orgs_admin(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)

        script = self._script()

        self.assertIn("//uganda.georiva.test/admin/", script)
        self.assertIn("//kenya.georiva.test/admin/", script)

    def test_a_link_names_no_scheme_so_the_browser_keeps_the_one_it_is_on(self):
        """Django cannot see through the TLS terminator; the browser can."""
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)

        script = self._script()

        self.assertNotIn("http://uganda.georiva.test", script)
        self.assertNotIn("https://uganda.georiva.test", script)

    def test_links_keep_the_port_the_request_arrived_on(self):
        """A dev instance on :8000 must not hand out links to the default port."""
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)
        self.client.defaults["HTTP_HOST"] = f"{KENYA_HOST}:8000"

        script = self._script()

        self.assertIn("//uganda.georiva.test:8000/admin/", script)

    # -- delivery -----------------------------------------------------------

    def test_the_markup_is_never_held_in_a_shared_cache(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        response = self.client.get(self.url)

        self.assertIn("private", response["Cache-Control"])
        self.assertIn("no-store", response["Cache-Control"])
        self.assertIn("Cookie", response["Vary"])

    def test_closing_script_tags_cannot_escape_the_script_element(self):
        user = self._login("amina")
        add_member(user, self.kenya)
        add_member(user, self.uganda)
        self.uganda.name = "</script><script>alert(1)</script>"
        self.uganda.save()

        script = self._script()

        self.assertNotIn("</script>", script)
        self.assertNotIn("<script>", script)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class OrgHopperAdminChromeTests(TestCase):
    """That the block is actually offered by the admin, not merely reachable."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def test_every_admin_page_loads_both_halves_of_the_hopper(self):
        user = make_user("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        admin_access = Group.objects.create(name="admin access")
        admin_access.permissions.add(Permission.objects.get(codename="access_admin"))
        user.groups.add(admin_access)
        self.client.login(username="amina", password=PASSWORD)

        response = self.client.get(reverse("wagtailadmin_home"), headers={"host": KENYA_HOST})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "organisations/org_hopper.js")
        self.assertContains(response, "organisations/org_hopper.css")
        self.assertContains(response, reverse("organisation_hopper_script"))


class OrgHopperContextTests(TestCase):
    """The parts that answer without a request cycle."""

    def test_there_is_nothing_to_show_without_an_organisation(self):
        class Request:
            active_org = None
            user = None

        self.assertIsNone(org_hopper_context(Request()))

    def test_avatar_letters_come_from_the_first_word_of_the_slug(self):
        self.assertEqual(avatar_letters("kenya"), "KEN")
        self.assertEqual(avatar_letters("meteo-rwanda"), "MET")
        self.assertEqual(avatar_letters("tma"), "TMA")

    def test_an_organisations_colour_is_stable_and_in_range(self):
        self.assertEqual(avatar_index("kenya"), avatar_index("kenya"))
        for slug in ("kenya", "uganda", "tanzania", "icpac", "acmad"):
            with self.subTest(slug=slug):
                self.assertIn(avatar_index(slug), range(AVATAR_PALETTE_SIZE))


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class OrgHopperNonMemberTests(TestCase):
    """A signed-in non-member of the host organisation gets no block at all.

    This case used to be unreachable — ``_guard_admin`` turned a signed-in
    non-member away before any admin URL was served — and the hopper handled it
    by naming the host organisation anyway, on the grounds that a block naming
    nobody was worse than a block naming the host.

    It is reachable now. ``/admin/org-hopper.js`` is in the admin's open set,
    because the sign-in page carries a script tag for it and a refusal there
    hands that tag an HTML document. So the question stopped being hypothetical,
    and the answer changed with it: a stranger's sidebar must not be captioned
    with the name of an institution they have nothing to do with.
    """

    def test_a_non_member_of_the_host_organisation_is_shown_no_block(self):
        kenya = provision_organisation(name="Kenya Met", slug="kenya")
        uganda = provision_organisation(name="Uganda Met", slug="uganda")
        stranger = make_user("stranger")
        add_member(stranger, uganda)

        request = RequestFactory().get("/admin/org-hopper.js", headers={"host": KENYA_HOST})
        request.active_org = kenya
        request.user = stranger

        self.assertIsNone(org_hopper_context(request))
