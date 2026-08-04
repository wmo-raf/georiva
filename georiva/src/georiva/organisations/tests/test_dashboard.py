"""The admin dashboard tells an operator about one organisation: the host's.

Same fixture as the page-tree tests, and for the same reason: a user who belongs
to *both* institutions and holds every capability Wagtail knows. Wagtail's own
page permissions would already keep an ordinary single-org member out of another
tree, so a dashboard test built on one would pass without any of this code.

The assertions are on page titles in the rendered response — what the operator
sees — never on panel markup, which is Wagtail's to restyle.
"""
from django.contrib.auth.models import Group, Permission
from django.core.exceptions import ImproperlyConfigured
from django.test import RequestFactory, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from wagtail.log_actions import log
from wagtail.models import GroupApprovalTask, Workflow, WorkflowTask

from georiva.organisations import dashboard
from georiva.organisations.ownership import belongs_to_active_org
from georiva.organisations.provisioning import org_page_group_name, provision_organisation

from .factories import PASSWORD, add_member, make_user
from .test_page_trees import add_child_page


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class DashboardScopingTests(TestCase):

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_root = cls.kenya.site.root_page
        cls.uganda_root = cls.uganda.site.root_page
        cls.kenya_page = add_child_page(cls.kenya_root, "Nairobi Bulletin", "bulletin")
        cls.uganda_page = add_child_page(cls.uganda_root, "Kampala Bulletin", "bulletin")

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya)
        add_member(self.user, self.uganda)
        self.everything = Group.objects.create(name="everything")
        self.everything.permissions.add(*Permission.objects.all())
        self.user.groups.add(self.everything)
        for slug in ("kenya", "uganda"):
            self.user.groups.add(Group.objects.get(name=org_page_group_name(slug)))
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def dashboard(self):
        response = self.client.get(reverse("wagtailadmin_home"))
        self.assertEqual(response.status_code, 200)
        return response

    def record_edit(self, page, **kwargs):
        return log(instance=page, action="wagtail.edit", user=self.user, **kwargs)

    def start_workflow(self, page):
        """Put ``page`` into moderation, requested by and reviewable by our user.

        The approval group is the one the user is in, so the same page lands in
        both workflow panels — the queue they are waiting on and the queue they
        are reviewing.
        """
        task = GroupApprovalTask.objects.create(name="Moderation")
        task.groups.set([self.everything])
        workflow = Workflow.objects.create(name="Moderation")
        WorkflowTask.objects.create(workflow=workflow, task=task, sort_order=0)
        page.save_revision(user=self.user)
        return workflow.start(page, self.user)

    # -- recent edits ------------------------------------------------------

    def test_an_edit_in_another_organisation_is_not_listed(self):
        self.record_edit(self.uganda_page)
        self.assertNotContains(self.dashboard(), "Kampala Bulletin")

    def test_an_edit_in_this_organisation_is_listed(self):
        """The mirror: a panel narrowed to nothing would pass the test above."""
        self.record_edit(self.kenya_page)
        self.assertContains(self.dashboard(), "Nairobi Bulletin")

    def test_another_organisations_edits_cannot_crowd_out_this_ones(self):
        """The truncation case, and the one that separates a fix from a fiction.

        The panel takes its handful of rows in the database. Narrowing after the
        fact passes every other test here and fails this one: the other
        institution's edits consume the limit and this organisation's page never
        reaches the template.
        """
        self.record_edit(self.kenya_page, timestamp=timezone.now())
        for index in range(5):
            page = add_child_page(self.uganda_root, f"Kampala Note {index}", f"note-{index}")
            self.record_edit(page, timestamp=timezone.now())
        self.assertContains(self.dashboard(), "Nairobi Bulletin")

    # -- locked pages ------------------------------------------------------

    def test_a_page_locked_in_another_organisation_is_not_listed(self):
        self.lock(self.uganda_page)
        self.assertNotContains(self.dashboard(), "Kampala Bulletin")

    def test_a_page_locked_in_this_organisation_is_listed(self):
        self.lock(self.kenya_page)
        self.assertContains(self.dashboard(), "Nairobi Bulletin")

    def lock(self, page):
        page.locked = True
        page.locked_by = self.user
        page.locked_at = timezone.now()
        page.save()
        return page

    # -- workflow moderation ------------------------------------------------

    def test_neither_workflow_panel_shows_another_organisations_page(self):
        self.start_workflow(self.uganda_page)
        self.assertNotContains(self.dashboard(), "Kampala Bulletin")

    def test_the_workflow_panels_show_this_organisations_page(self):
        self.start_workflow(self.kenya_page)
        self.assertContains(self.dashboard(), "Nairobi Bulletin")

    # -- the upgrade canary -------------------------------------------------

    def test_every_panel_on_the_dashboard_is_one_we_have_scoped(self):
        """Fails on the Wagtail release that adds or renames a dashboard panel.

        A panel nobody has decided how to scope must not reach production by
        appearing quietly on the dashboard; it fails here and forces triage. The
        same assertion catches the other regression — a replacement strategy that
        rebuilt the list would drop the panels other GeoRiva modules registered,
        and their absence fails it too.
        """
        from georiva.ingestion.wagtail_hooks import IngestionActivityPanel

        allowed = set(dashboard.SCOPED_DASHBOARD_PANELS.values()) | {IngestionActivityPanel}
        panels = self.dashboard().context["panels"]
        self.assertEqual({type(panel) for panel in panels}, allowed)

    # -- the ownership predicate, by direct call ----------------------------

    def request_for(self, organisation):
        request = RequestFactory().get("/admin/")
        request.active_org = organisation
        request.user = self.user
        return request

    def test_a_model_that_declares_no_ownership_rule_is_refused(self):
        """Silence is refused rather than guessed, as it is for a queryset.

        By direct call because no dashboard request can reach it: the panels only
        ever hold pages and snippets, and the fail-closed sweep sees to it that
        every model declares something. ``ApiKey`` is what "no route to an
        organisation" looks like in this codebase — it belongs to a person, who
        may be a member of several institutions — and it lands on the same
        refusal an undeclared model would.
        """
        from georiva.accounts.models import ApiKey

        with self.assertRaises(ImproperlyConfigured):
            belongs_to_active_org(self.request_for(self.kenya), ApiKey())

    def test_a_page_is_judged_by_the_tree_it_sits_in(self):
        request = self.request_for(self.kenya)
        self.assertTrue(belongs_to_active_org(request, self.kenya_page))
        self.assertFalse(belongs_to_active_org(request, self.uganda_page))

    def test_an_org_owned_object_is_judged_by_the_route_it_declares(self):
        from georiva.organisations.testing import make_org_tree

        request = self.request_for(self.kenya)
        self.assertTrue(belongs_to_active_org(request, make_org_tree(self.kenya)["catalog"]))
        self.assertFalse(belongs_to_active_org(request, make_org_tree(self.uganda)["catalog"]))

    def test_shared_reference_data_belongs_everywhere(self):
        from georiva.core.models import Topic

        topic = Topic.objects.create(name="Rainfall", slug="rainfall")
        self.assertTrue(belongs_to_active_org(self.request_for(self.kenya), topic))

    def test_the_global_tier_of_a_nullable_owner_belongs_everywhere(self):
        """A shipped palette is owned by nobody and read by everybody."""
        from georiva.core.models import ColorPalette

        shipped = ColorPalette.objects.create(name="Shipped rainfall")
        theirs = ColorPalette.objects.create(name="Theirs", organisation=self.uganda)
        request = self.request_for(self.kenya)
        self.assertTrue(belongs_to_active_org(request, shipped))
        self.assertFalse(belongs_to_active_org(request, theirs))
