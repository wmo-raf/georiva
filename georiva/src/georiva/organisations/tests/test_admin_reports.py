"""Wagtail's admin reports list one organisation's rows: the host's (#297).

Same fixture as the dashboard tests, for the same reason: a user who belongs to
*both* institutions and holds every capability Wagtail knows, so Wagtail's own
permissions cannot be what keeps the other tree out.

Each report is read twice — the full page and the results-only twin the listing
refreshes itself from — because they are separate URLs and either could be left
open. Assertions are on page titles in the response, never on markup.
"""

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from wagtail.log_actions import log
from wagtail.models import GroupApprovalTask, GroupPagePermission, Workflow, WorkflowTask

from georiva.organisations.provisioning import org_page_group_name, provision_organisation

from .factories import PASSWORD, add_member, make_user
from .test_page_trees import add_child_page

PAGE_REPORTS = ["locked_pages", "workflow", "workflow_tasks", "site_history", "aging_pages"]


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class AdminReportScopingTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_page = add_child_page(cls.kenya.site.root_page, "Nairobi Bulletin", "bulletin")
        cls.uganda_page = add_child_page(cls.uganda.site.root_page, "Kampala Bulletin", "bulletin")

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya)
        add_member(self.user, self.uganda)
        everything = Group.objects.create(name="everything")
        everything.permissions.add(*Permission.objects.all())
        self.user.groups.add(everything)
        for slug in ("kenya", "uganda"):
            self.user.groups.add(Group.objects.get(name=org_page_group_name(slug)))
        self.everything = everything
        # The locked-pages report asks for page-level `unlock`, which an organisation's
        # own group is not provisioned with.
        unlock = Permission.objects.get(content_type__app_label="wagtailcore", codename="unlock_page")
        for organisation in (self.kenya, self.uganda):
            GroupPagePermission.objects.create(group=everything, page=organisation.site.root_page, permission=unlock)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"
        for page in (self.kenya_page, self.uganda_page):
            self.touch(page)

    def touch(self, page):
        """Put ``page`` on every page-shaped report at once."""
        page.locked, page.locked_by, page.locked_at = True, self.user, timezone.now()
        page.save()
        page.save_revision(user=self.user).publish()
        page.refresh_from_db()
        type(page).objects.filter(pk=page.pk).update(last_published_at=timezone.now() - timezone.timedelta(days=400))
        log(instance=page, action="wagtail.edit", user=self.user)
        task = GroupApprovalTask.objects.create(name=f"Moderation {page.pk}")
        task.groups.set([self.everything])
        workflow = Workflow.objects.create(name=f"Moderation {page.pk}")
        WorkflowTask.objects.create(workflow=workflow, task=task, sort_order=0)
        page.save_revision(user=self.user)
        workflow.start(page, self.user)

    def read(self, name, results_only):
        url = reverse(f"wagtailadmin_reports:{name}{'_results' if results_only else ''}")
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, url)
        return response

    def test_no_page_report_lists_another_organisations_page(self):
        for name in PAGE_REPORTS:
            for results_only in (False, True):
                with self.subTest(report=name, results_only=results_only):
                    self.assertNotContains(self.read(name, results_only), "Kampala Bulletin")

    def test_each_page_report_still_lists_this_organisations_page(self):
        """The mirror: a report narrowed to nothing would pass the test above."""
        for name in PAGE_REPORTS:
            for results_only in (False, True):
                with self.subTest(report=name, results_only=results_only):
                    self.assertContains(self.read(name, results_only), "Nairobi Bulletin")

    def test_page_types_usage_counts_only_this_organisations_pages(self):
        for results_only in (False, True):
            with self.subTest(results_only=results_only):
                response = self.read("page_types_usage", results_only)
                self.assertNotContains(response, "Kampala Bulletin")
                self.assertContains(response, "Nairobi Bulletin")

    def test_page_types_usage_does_not_offer_another_organisations_site(self):
        self.assertNotContains(self.read("page_types_usage", False), "Uganda Met")

    def test_the_host_decides_not_the_user(self):
        """Same user, other host: the other organisation's rows, and only those."""
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        for name in PAGE_REPORTS:
            with self.subTest(report=name):
                response = self.read(name, False)
                self.assertContains(response, "Kampala Bulletin")
                self.assertNotContains(response, "Nairobi Bulletin")

    def test_the_instance_admin_is_scoped_by_host_too(self):
        admin = make_user("root", superuser=True)
        add_member(admin, self.kenya)
        self.client.logout()
        self.client.login(username="root", password=PASSWORD)
        for name in PAGE_REPORTS:
            with self.subTest(report=name):
                self.assertNotContains(self.read(name, False), "Kampala Bulletin")

    def test_the_query_string_cannot_widen_page_types_usage_to_another_site(self):
        response = self.client.get(
            reverse("wagtailadmin_reports:page_types_usage"), {"site": self.uganda.site.root_page.path}
        )
        self.assertNotContains(response, "Kampala Bulletin")
        self.assertContains(response, "Nairobi Bulletin")

    def test_the_instance_admin_is_scoped_on_the_results_twins_and_usage_report_too(self):
        admin = make_user("root", superuser=True)
        add_member(admin, self.kenya)
        self.client.logout()
        self.client.login(username="root", password=PASSWORD)
        for name in (*PAGE_REPORTS, "page_types_usage"):
            with self.subTest(report=name):
                self.assertNotContains(self.read(name, True), "Kampala Bulletin")

    def test_a_csv_export_is_scoped_like_the_listing(self):
        response = self.client.get(reverse("wagtailadmin_reports:aging_pages"), {"export": "csv"})
        self.assertEqual(response.status_code, 200)
        body = b"".join(response.streaming_content).decode() if response.streaming else response.content.decode()
        self.assertIn("Nairobi Bulletin", body)
        self.assertNotIn("Kampala Bulletin", body)

    def test_every_report_wagtail_registers_is_claimed_by_a_scoped_view(self):
        """Fails on the Wagtail release that adds a report nobody has scoped.

        The names come from Wagtail's own report routes, not from a list kept here,
        so a report added upstream appears in this set before anyone has decided
        how to scope it.
        """
        from django.urls import resolve
        from wagtail.admin.urls import reports as wagtail_reports

        from georiva.organisations.reports import ORG_SCOPED_REPORTS

        wagtail_names = {pattern.name for pattern in wagtail_reports.urlpatterns}
        self.assertEqual(set(ORG_SCOPED_REPORTS), wagtail_names)
        for name in wagtail_names:
            url_name = resolve(reverse(f"wagtailadmin_reports:{name}")).url_name
            self.assertEqual(url_name, f"georiva_org_scoped_report_{name}")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class AdminReportFilterDropdownTests(TestCase):
    """The filter dropdowns name users, workflows and tasks: only the host's may appear (#452).

    Uganda's people and configuration are given names no Kenyan row shares, and
    the reports are read on Kenya's host by someone who belongs to both.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_page = add_child_page(cls.kenya.site.root_page, "Nairobi Bulletin", "bulletin")
        cls.uganda_page = add_child_page(cls.uganda.site.root_page, "Kampala Bulletin", "bulletin")
        cls.kenya_catalog = cls.make_catalog(cls.kenya, "Nairobi Feed")
        cls.uganda_catalog = cls.make_catalog(cls.uganda, "Kampala Feed")

    @staticmethod
    def make_catalog(organisation, name):
        from georiva.core.models import Catalog

        return Catalog.objects.create(organisation=organisation, name=name, slug=name.lower().replace(" ", "-"))

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya)
        add_member(self.user, self.uganda)
        self.kenyan = make_user("wanjiru")
        add_member(self.kenyan, self.kenya)
        self.ugandan = make_user("baraka")
        add_member(self.ugandan, self.uganda)
        everything = Group.objects.create(name="everything")
        everything.permissions.add(*Permission.objects.all())
        self.user.groups.add(everything)
        for slug in ("kenya", "uganda"):
            self.user.groups.add(Group.objects.get(name=org_page_group_name(slug)))
        unlock = Permission.objects.get(content_type__app_label="wagtailcore", codename="unlock_page")
        for organisation in (self.kenya, self.uganda):
            GroupPagePermission.objects.create(group=everything, page=organisation.site.root_page, permission=unlock)
        self.everything = everything
        for page, who, prefix in (
            (self.kenya_page, self.kenyan, "Nairobi"),
            (self.uganda_page, self.ugandan, "Kampala"),
        ):
            self.touch(page, who, prefix)
        log(instance=self.kenya_catalog, action="wagtail.create", user=self.kenyan)
        log(instance=self.uganda_catalog, action="wagtail.delete", user=self.ugandan)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def touch(self, page, who, prefix):
        page.save_revision(user=who).publish()
        log(instance=page, action="wagtail.edit", user=who)
        task = GroupApprovalTask.objects.create(name=f"{prefix} Review")
        task.groups.set([self.everything])
        workflow = Workflow.objects.create(name=f"{prefix} Sign-off")
        WorkflowTask.objects.create(workflow=workflow, task=task, sort_order=0)
        page.save_revision(user=who)
        workflow.start(page, who)
        # Last, because publishing and starting a workflow both reset the lock.
        type(page).objects.filter(pk=page.pk).update(locked=True, locked_by=who, locked_at=timezone.now())

    def read(self, name):
        url = reverse(f"wagtailadmin_reports:{name}")
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, url)
        return response

    def choices(self, response, field):
        return list(response.context["filters"].form.fields[field].queryset)

    def test_locked_pages_offers_only_this_organisations_users(self):
        names = [u.username for u in self.choices(self.read("locked_pages"), "locked_by")]
        self.assertIn("wanjiru", names)
        self.assertNotIn("baraka", names)

    def test_workflow_requested_by_offers_only_this_organisations_users(self):
        names = [u.username for u in self.choices(self.read("workflow"), "requested_by")]
        self.assertIn("wanjiru", names)
        self.assertNotIn("baraka", names)

    def test_workflow_report_offers_only_this_organisations_workflows(self):
        workflows = [w.name for w in self.choices(self.read("workflow"), "workflow")]
        self.assertEqual(workflows, ["Nairobi Sign-off"])

    def test_workflow_tasks_offers_only_this_organisations_workflows_and_tasks(self):
        response = self.read("workflow_tasks")
        workflows = [w.name for w in self.choices(response, "workflow")]
        tasks = [t.name for t in self.choices(response, "task")]
        self.assertEqual(workflows, ["Nairobi Sign-off"])
        self.assertEqual(tasks, ["Nairobi Review"])

    def test_site_history_offers_only_this_organisations_users_types_and_actions(self):
        response = self.read("site_history")
        form = response.context["filters"].form
        users = [u.username for u in self.choices(response, "user")]
        self.assertIn("wanjiru", users)
        self.assertNotIn("baraka", users)
        actions = [value for value, _ in form.fields["action"].choices]
        self.assertIn("wagtail.create", actions)
        self.assertNotIn("wagtail.delete", actions)
        self.assertIn("catalog", [ct.model for ct in self.choices(response, "object_type")])

    def test_site_history_does_not_offer_a_type_only_another_organisation_logged(self):
        from wagtail.models import ModelLogEntry

        ModelLogEntry.objects.filter(object_id=str(self.kenya_catalog.pk)).delete()
        types = self.choices(self.read("site_history"), "object_type")
        self.assertNotIn("catalog", [ct.model for ct in types])
