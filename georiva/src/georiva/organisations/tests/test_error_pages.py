"""What a refused or missing request actually shows, and the way out of a refusal.

The membership guard turns a signed-in non-member away from an organisation's
admin. That is correct, and it used to be a dead end: Django's stock 403 is bare
text, and the sign-out URL that would resolve it sits under the same ``/admin/``
prefix the guard had just closed. These tests pin both halves — the page, and the
narrowness of the exemption that makes its button work.

Everything here runs with ``DEBUG=False``. The handlers under test are the ones
Django only consults in production; with debug on it answers with its own
traceback and technical 404 page instead, and none of this would be exercised.
"""

from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, make_user

KENYA = {"host": "kenya.georiva.test"}
UGANDA = {"host": "uganda.georiva.test"}


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"], DEBUG=False)
class NonMemberRefusalTests(TestCase):
    """A member of one organisation, knocking on another's admin."""

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        self.user = make_user("amina")
        add_member(self.user, self.kenya)
        self.client.login(username="amina", password=PASSWORD)

    def test_the_refusal_is_a_rendered_page_and_not_bare_text(self):
        response = self.client.get(reverse("wagtailadmin_home"), headers=UGANDA)

        self.assertEqual(response.status_code, 403)
        self.assertTemplateUsed(response, "georiva/errors/403.html")

    def test_the_page_names_the_organisation_and_the_account(self):
        response = self.client.get(reverse("wagtailadmin_home"), headers=UGANDA)

        self.assertContains(response, "Uganda Met", status_code=403)
        self.assertContains(response, "amina", status_code=403)

    def test_the_page_asks_the_admin_for_nothing(self):
        """The refusal page fetches no subresources of its own.

        Wagtail's admin shell fetches an icon sprite from ``/admin/sprite/``, a
        translation catalog from ``/admin/jsi18n/`` and whatever is registered on
        ``insert_global_admin_js`` — the org-hopper, here. This page has no use
        for any of the three: it draws no icons, translates no strings, and
        carries no sidebar to mount a hopper into.

        Those URLs are all answerable for a non-member now (``AdminOpenPathTests``
        holds that), so this no longer guards against the sprite fetch returning
        the refusal page and ``icons.js`` injecting it into itself. It records a
        page that stays static instead.
        """
        response = self.client.get(reverse("wagtailadmin_home"), headers=UGANDA)
        html = response.content.decode()

        self.assertNotIn("data-sprite", html)
        self.assertNotIn("<script", html)
        self.assertEqual(html.count("<h1"), 1)

    def test_the_page_offers_a_way_out(self):
        response = self.client.get(reverse("wagtailadmin_home"), headers=UGANDA)

        self.assertContains(response, reverse("wagtailadmin_logout"), status_code=403)

    def test_signing_out_works_from_the_organisation_that_refused_them(self):
        """The regression the auth-path exemption exists for.

        Without it the sign-out button on the refusal page is itself under
        ``/admin/`` and is refused identically, leaving no way out but clearing a
        cookie by hand.
        """
        response = self.client.post(reverse("wagtailadmin_logout"), headers=UGANDA)

        self.assertEqual(response.status_code, 302)
        self.assertNotIn("_auth_user_id", self.client.session)

    def test_the_login_page_is_reachable_on_a_host_the_user_is_no_member_of(self):
        """Reachable meaning not refused. Wagtail then bounces a signed-in caller
        to the admin home — where the guard refuses them, correctly. What matters
        is that the guard is not what answered the login URL itself, since after
        signing out the same URL has to serve the form."""
        response = self.client.get(reverse("wagtailadmin_login"), headers=UGANDA)

        self.assertEqual(response.status_code, 302)

    def test_the_login_form_is_served_once_the_session_is_gone(self):
        self.client.post(reverse("wagtailadmin_logout"), headers=UGANDA)

        response = self.client.get(reverse("wagtailadmin_login"), headers=UGANDA)

        self.assertEqual(response.status_code, 200)

    def test_the_exemption_opens_nothing_else_under_the_admin(self):
        """The open set is what ``admin_open_paths`` names, and nothing beyond it.

        The counterweight to ``AdminOpenPathTests``: every test there asserts
        that some URL *answers* a non-member, and all of them would keep passing
        if somebody widened the exemption to ``/admin/`` wholesale. Only a test
        asserting something is still shut can fail that way.
        """
        for name in (
            "wagtailadmin_home",
            "wagtailadmin_account",
            "georiva_org_scoped_page_search",
            "organisation_settings",
        ):
            with self.subTest(url=name):
                response = self.client.get(reverse(name), headers=UGANDA)

                self.assertEqual(response.status_code, 403)

    def test_the_user_still_reaches_their_own_organisation(self):
        response = self.client.get(reverse("wagtailadmin_home"), headers=KENYA)

        self.assertEqual(response.status_code, 200)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"], DEBUG=False)
class AdminOpenPathTests(TestCase):
    """The admin URLs a signed-in non-member still gets an answer from.

    Wagtail applies ``require_admin_access`` to its admin urlconf and *then*
    appends four URLs deliberately left open, under the comment "these url
    patterns do not require an authenticated admin user" — because its own login
    page needs them. The guard used to refuse three of the four, which broke the
    login page for the very user it was refusing: ``icons.js`` fetches
    ``/admin/sprite/`` and assigns the response body straight into a
    ``<div data-sprite>`` with no check on status or content type, so the 403
    page was injected above the sign-in form and the page grew a second screen.

    The doubling itself cannot be tested here — it happens in a browser running
    ``icons.js``, which the test client never does. What is testable is the
    ingredient: these URLs must hand a non-member the thing they are for, not a
    document. Hence the assertions on the body rather than on the status alone;
    a status check would have passed throughout, since a 403 HTML page is a
    perfectly good string as far as ``fetch().then(r => r.text())`` cares.
    """

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        self.user = make_user("amina")
        add_member(self.user, self.kenya)
        self.client.login(username="amina", password=PASSWORD)

    def test_the_icon_sprite_is_icons_and_not_the_refusal_page(self):
        """The regression itself. The body assertions are what matter.

        ``icons.js`` reads this with ``fetch(u).then(r => r.text())``, so a 403
        HTML page satisfies it just as well as a sprite does — which is exactly
        how this shipped. Both a symbol definition and the absence of the
        refusal's own heading are asserted, because either alone would pass on a
        response that had gone wrong in the other direction.
        """
        response = self.client.get(reverse("wagtailadmin_sprite"), headers=UGANDA)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "image/svg+xml; charset=utf-8")
        self.assertContains(response, '<symbol id="icon-')
        self.assertNotContains(response, "Uganda Met")

    def test_the_translation_catalog_is_javascript(self):
        """Served to a ``<script src>`` on the login page. HTML there is a parse
        error that leaves ``django.gettext`` undefined for everything after it.

        The body is asserted for the same reason the sprite's is: the header
        alone would let through anything Django happened to label as script.
        """
        response = self.client.get(reverse("wagtailadmin_javascript_catalog"), headers=UGANDA)

        self.assertEqual(response.status_code, 200)
        self.assertIn("javascript", response["Content-Type"])
        self.assertContains(response, "gettext")
        self.assertNotContains(response, "Uganda Met")

    def test_a_password_reset_can_be_started(self):
        """The dead end one link over from the one the logout exemption fixed.

        The login page renders "Forgotten password?", and the signed-in
        non-member reading that page is exactly who needs it.
        """
        response = self.client.get(reverse("wagtailadmin_password_reset"), headers=UGANDA)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "<form")

    def test_the_org_hopper_answers_with_an_empty_script(self):
        """Open, and empty — the two halves have to arrive together.

        ``config/urls.py`` keeps this URL outside ``require_admin_access``
        precisely so the login page's copy of its script tag cannot follow a
        redirect into HTML; the guard was reimposing that. It answers rather
        than refuses because a non-member has no block to be shown, and
        ``org_hopper_context`` says so by returning ``None``.
        """
        response = self.client.get(reverse("organisation_hopper_script"), headers=UGANDA)

        self.assertEqual(response.status_code, 200)
        self.assertIn("javascript", response["Content-Type"])
        self.assertEqual(response.content, b"")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"], DEBUG=False)
class MemberWithoutAdminRightsTests(TestCase):
    """A member who is not an organisation admin does not get the sign-out page.

    Wagtail's ``require_admin_access`` catches a ``PermissionDenied`` raised
    inside an admin view and redirects to the admin home with a message, so
    ``require_org_admin``'s refusal never reaches ``handler403``. Pinned here
    because the 403 page's copy is written on the strength of it: if this ever
    starts returning 403, a legitimate member is being shown a sign-out button.
    """

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        user = make_user("juma")
        add_member(user, self.kenya, role=OrganisationMembership.Role.MEMBER)
        self.client.login(username="juma", password=PASSWORD)

    def test_a_restricted_surface_redirects_rather_than_refusing(self):
        response = self.client.get(reverse("organisation_settings"), headers=KENYA)

        self.assertRedirects(response, reverse("wagtailadmin_home"), fetch_redirect_response=False)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"], DEBUG=False)
class NotFoundPageTests(TestCase):
    """The 404 rendered, rather than failing to render.

    It used to extend a ``base.html`` that does not exist on this template path,
    so every production 404 raised ``TemplateDoesNotExist`` on its way out.
    """

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def test_a_missing_public_page_renders_the_public_shell(self):
        response = self.client.get("/no-such-page/", headers=KENYA)

        self.assertEqual(response.status_code, 404)
        self.assertTemplateUsed(response, "georiva/errors/404.html")
        self.assertTemplateUsed(response, "georiva/errors/base_public.html")

    def test_a_missing_admin_page_keeps_wagtails_own_not_found(self):
        """Wagtail answers 404s under its own prefix before `handler404` sees
        them, and its page already wears this project's branded `admin_base`
        override. Pinned so nobody adds an admin arm to the 404 template
        expecting it to be reached."""
        user = make_user("amina")
        add_member(user, self.kenya)
        self.client.login(username="amina", password=PASSWORD)

        response = self.client.get("/admin/no-such-page/", headers=KENYA)

        self.assertEqual(response.status_code, 404)
        self.assertTemplateUsed(response, "wagtailadmin/404.html")

    def test_an_unknown_host_renders_rather_than_crashing(self):
        response = self.client.get("/", headers={"host": "nobody.georiva.test"})

        self.assertEqual(response.status_code, 404)
        self.assertTemplateUsed(response, "georiva/errors/404.html")


class ServerErrorPageTests(TestCase):
    """The 500 has to render with nothing — no request, no context, no settings.

    Django's ``server_error`` hands it an empty context, so it is rendered here
    the same way rather than through the client. The file was previously empty,
    which made every production 500 a blank page.
    """

    def test_it_renders_from_an_empty_context(self):
        from django.template.loader import render_to_string

        html = render_to_string("500.html", {})

        self.assertIn("Something went wrong", html)
        self.assertNotIn("{{", html)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class PerOrganisationBrandingTests(TestCase):
    """Public chrome must come from the host's organisation, not the default Site.

    ``GeoRivaSettings`` is a ``BaseSiteSetting`` and the Site is the organisation,
    so ``use_default_site=True`` in the base templates dressed every tenant's
    pages in whichever organisation happened to hold the default Site row.
    """

    def setUp(self):
        from georiva.pages.home.models import GeoRivaSettings

        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        for org, name in ((self.kenya, "Kenya Portal"), (self.uganda, "Uganda Portal")):
            settings_record = GeoRivaSettings.for_site(org.site)
            settings_record.site_name = name
            settings_record.save()

    def test_each_host_is_branded_as_its_own_organisation(self):
        kenya = self.client.get("/no-such-page/", headers=KENYA)
        uganda = self.client.get("/no-such-page/", headers=UGANDA)

        self.assertContains(kenya, "Kenya Portal", status_code=404)
        self.assertNotContains(kenya, "Uganda Portal", status_code=404)
        self.assertContains(uganda, "Uganda Portal", status_code=404)
        self.assertNotContains(uganda, "Kenya Portal", status_code=404)
