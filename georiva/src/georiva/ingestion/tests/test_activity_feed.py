from django.contrib.auth import get_user_model
from django.test import TestCase

User = get_user_model()

ACTIVITY_URL = "/admin/ingestion/activity/"


# =============================================================================
# Cycle 1: Activity page renders for authenticated users
# =============================================================================

class ActivityPageRenderTests(TestCase):

    def setUp(self):
        self.user = User.objects.create_superuser("admin_af", "af@test.com", "pw")
        self.client.force_login(self.user)

    def test_page_returns_200(self):
        response = self.client.get(ACTIVITY_URL)
        self.assertEqual(response.status_code, 200)

    def test_page_contains_sse_url(self):
        response = self.client.get(ACTIVITY_URL)
        self.assertContains(response, "/admin/api/ingestion/events/")

    def test_page_has_feed_container(self):
        response = self.client.get(ACTIVITY_URL)
        self.assertContains(response, 'id="activity-feed"')

    def test_unauthenticated_is_rejected(self):
        self.client.logout()
        response = self.client.get(ACTIVITY_URL, HTTP_X_REQUESTED_WITH="XMLHttpRequest")
        self.assertEqual(response.status_code, 403)


# =============================================================================
# Cycle 3: Dashboard panel has "View all" link to the activity feed
# =============================================================================

# =============================================================================
# Cycle 1 (issue #55): Cancel wiring present in activity feed template
# =============================================================================

class ActivityFeedCancelWiringTests(TestCase):

    def setUp(self):
        self.user = User.objects.create_superuser("admin_cw", "cw@test.com", "pw")
        self.client.force_login(self.user)

    def test_page_contains_cancel_jobs_url_prefix(self):
        response = self.client.get(ACTIVITY_URL)
        self.assertContains(response, "/api/jobs/")

    def test_page_contains_csrf_token_for_cancel_post(self):
        # The template inlines the CSRF token via {{ csrf_token }} into a JS
        # constant; verify the constant is declared (token value will differ per request).
        response = self.client.get(ACTIVITY_URL)
        self.assertContains(response, "CSRF_TOKEN")


class DashboardPanelViewAllTests(TestCase):

    def setUp(self):
        self.user = User.objects.create_superuser("admin_dp", "dp@test.com", "pw")
        self.client.force_login(self.user)

    def test_dashboard_panel_has_view_all_link(self):
        # The "View all →" link must be in the panel template itself,
        # not just in the sidebar menu.
        from django.template.loader import render_to_string
        from django.test import RequestFactory
        from georiva.ingestion.panels import IngestionActivityPanel

        request = RequestFactory().get("/admin/")
        request.user = self.user
        panel = IngestionActivityPanel()
        ctx = panel.get_context_data({"request": request})
        html = render_to_string(panel.template_name, ctx)
        self.assertIn("/admin/ingestion/activity/", html)
