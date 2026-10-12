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

    def test_every_report_url_is_claimed_by_a_scoped_view(self):
        """Fails on the Wagtail release that adds a report nobody has scoped."""
        from django.urls import resolve

        from georiva.organisations.reports import ORG_SCOPED_REPORTS

        wagtail_names = {
            f"wagtailadmin_reports:{name}{suffix}"
            for name in (*PAGE_REPORTS, "page_types_usage")
            for suffix in ("", "_results")
        }
        self.assertEqual({f"wagtailadmin_reports:{n}" for n in ORG_SCOPED_REPORTS}, wagtail_names)
        for name in wagtail_names:
            self.assertTrue(resolve(reverse(name)).url_name.startswith("georiva_org_scoped_report_"), name)
