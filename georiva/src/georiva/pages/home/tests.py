from unittest.mock import patch

from django.test import RequestFactory, TestCase
from wagtail.admin.panels import get_edit_handler
from wagtail.models import Page, Site
from wagtail.test.utils import WagtailPageTestCase

from georiva.core.web_apps import DEFAULT_ICON, WebAppMount
from georiva.pages.home.models import GeoRivaSettings, HomePage, PortalAppEntry
from georiva.pages.home.portal_apps import portal_apps
from georiva.pages.home.widgets import PortalAppSelect


class HomeSetUpTests(WagtailPageTestCase):
    """
    Tests for basic page structure setup and HomePage creation.
    """

    def test_root_create(self):
        root_page = Page.objects.get(pk=1)
        self.assertIsNotNone(root_page)

    def test_homepage_create(self):
        root_page = Page.objects.get(pk=1)
        homepage = HomePage(title="Home")
        root_page.add_child(instance=homepage)
        self.assertTrue(HomePage.objects.filter(title="Home").exists())


class HomeTests(WagtailPageTestCase):
    """
    Tests for homepage functionality and rendering.
    """

    def setUp(self):
        """
        Use the HomePage the initial migration creates — the default Site
        already roots "/" at it (see pages/home/migrations/0002_create_homepage).
        """
        self.homepage = HomePage.objects.get(slug="home")

    def test_homepage_is_renderable(self):
        self.assertPageIsRenderable(self.homepage)

    def test_homepage_template_used(self):
        response = self.client.get(self.homepage.url)
        self.assertTemplateUsed(response, "home/home_page.html")


class PortalAppsTests(TestCase):
    """The apps a portal links to: every app the instance serves, as its plugin
    describes it, unless this site's editors said otherwise."""

    FORECAST = WebAppMount(
        plugin="forecast_plugin",
        slug="forecast",
        index="forecast/index.html",
        name="Forecast Viewer",
        path="/app/forecast/",
        icon="cloud-sun",
        description="Step through the latest run.",
    )
    DROUGHT = WebAppMount(
        plugin="drought_plugin",
        slug="drought",
        index="drought/index.html",
        name="Drought Monitor",
        path="/app/drought/",
    )

    def setUp(self):
        self.site = Site.objects.get(is_default_site=True)
        self.settings = GeoRivaSettings.for_site(self.site)

    def serving(self, *mounts):
        return patch("georiva.pages.home.portal_apps.served_web_apps", return_value=list(mounts))

    def request(self):
        request = RequestFactory().get("/")
        request._wagtail_site = self.site
        return request

    def entry(self, plugin, **fields):
        return PortalAppEntry.objects.create(settings=self.settings, plugin=plugin, **fields)

    def apps(self, *mounts):
        with self.serving(*mounts):
            return portal_apps(self.request())

    def test_every_served_app_is_listed_without_any_editor_work(self):
        apps = self.apps(self.FORECAST, self.DROUGHT)

        self.assertEqual(
            [(a.title, a.description, a.icon, a.url) for a in apps],
            [
                ("Forecast Viewer", "Step through the latest run.", "cloud-sun", "/app/forecast/"),
                ("Drought Monitor", "", DEFAULT_ICON, "/app/drought/"),
            ],
        )

    def test_nothing_is_listed_when_nothing_is_served(self):
        self.assertEqual(self.apps(), [])

    def test_an_editor_may_hide_an_app(self):
        self.entry("forecast_plugin", hidden=True)

        self.assertEqual([a.plugin for a in self.apps(self.FORECAST, self.DROUGHT)], ["drought_plugin"])

    def test_an_editor_may_restate_an_app(self):
        self.entry("forecast_plugin", title="Forecasts", description="Ours.", icon="bi-sun")

        (app,) = self.apps(self.FORECAST)

        self.assertEqual((app.title, app.description, app.icon), ("Forecasts", "Ours.", "sun"))

    def test_a_blank_override_falls_back_to_the_plugins_own(self):
        self.entry("forecast_plugin", title="Forecasts")

        (app,) = self.apps(self.FORECAST)

        self.assertEqual((app.description, app.icon), ("Step through the latest run.", "cloud-sun"))

    def test_apps_with_a_row_come_first_in_row_order(self):
        self.entry("drought_plugin", sort_order=0)

        apps = self.apps(self.FORECAST, self.DROUGHT)

        self.assertEqual([a.plugin for a in apps], ["drought_plugin", "forecast_plugin"])

    def test_a_row_for_an_app_that_is_not_served_is_skipped(self):
        self.entry("uninstalled_plugin", title="Gone")

        self.assertEqual([a.plugin for a in self.apps(self.FORECAST)], ["forecast_plugin"])

    def test_the_first_of_two_rows_for_one_app_decides(self):
        self.entry("forecast_plugin", sort_order=0, hidden=True)
        self.entry("forecast_plugin", sort_order=1, title="Shown after all")

        self.assertEqual(self.apps(self.FORECAST), [])

    def test_one_sites_rows_do_not_reach_another_site(self):
        other_root = Page.objects.get(depth=1).add_child(instance=HomePage(title="Other", slug="other-home"))
        other_site = Site.objects.create(hostname="other.example", root_page=other_root)
        PortalAppEntry.objects.create(
            settings=GeoRivaSettings.for_site(other_site), plugin="forecast_plugin", hidden=True
        )

        self.assertEqual([a.plugin for a in self.apps(self.FORECAST)], ["forecast_plugin"])


class PortalAppsRenderingTests(WagtailPageTestCase):
    """The navbar's Apps menu and the home page's Apps section are one list."""

    FORECAST = PortalAppsTests.FORECAST

    def setUp(self):
        self.homepage = HomePage.objects.get(slug="home")

    def get_home(self, *mounts):
        with patch("georiva.pages.home.portal_apps.served_web_apps", return_value=list(mounts)):
            return self.client.get(self.homepage.url)

    def test_a_served_app_is_in_the_navbar_and_on_the_home_page(self):
        response = self.get_home(self.FORECAST)

        self.assertContains(response, 'href="/app/forecast/"', count=2)
        self.assertContains(response, "gr-navbar-dropdown")
        self.assertContains(response, "gr-apps-section")
        self.assertContains(response, "bi-cloud-sun", count=2)
        self.assertContains(response, "Step through the latest run.", count=2)

    def test_without_apps_there_is_neither_a_menu_nor_a_section(self):
        response = self.get_home()

        self.assertNotContains(response, "gr-navbar-dropdown")
        self.assertNotContains(response, "gr-apps-section")

    def test_the_section_heading_is_the_editors(self):
        self.homepage.apps_heading = "Tools"
        self.homepage.save_revision().publish()

        response = self.get_home(self.FORECAST)

        self.assertContains(response, '<h2 class="gr-section-title">Tools</h2>', html=True)


class PortalAppAdminTests(TestCase):
    """What an editor sees and may save on the settings' Apps panel."""

    def test_the_choice_is_of_served_apps(self):
        with patch("georiva.core.web_apps.served_web_apps", return_value=[PortalAppsTests.FORECAST]):
            html = PortalAppSelect().render("plugin", "")

        self.assertIn('value="forecast_plugin"', html)
        self.assertIn("Forecast Viewer", html)

    def test_a_row_for_an_uninstalled_app_says_so_rather_than_changing_app(self):
        with patch("georiva.core.web_apps.served_web_apps", return_value=[PortalAppsTests.FORECAST]):
            html = PortalAppSelect().render("plugin", "uninstalled_plugin")

        self.assertIn("uninstalled_plugin (not installed)", html)
        self.assertRegex(html, r'value="uninstalled_plugin"\s+selected')

    def test_two_rows_for_one_app_are_refused(self):
        settings = GeoRivaSettings.for_site(Site.objects.get(is_default_site=True))
        form_class = get_edit_handler(GeoRivaSettings).get_form_class()
        data = {
            "site_name": "GeoRiva",
            "accent_color": "#00c9b1",
            "map_viewer_url": "/map/",
            "stac_browser_url": "/stac-browser/",
            "app_entries-TOTAL_FORMS": "2",
            "app_entries-INITIAL_FORMS": "0",
            "app_entries-MIN_NUM_FORMS": "0",
            "app_entries-MAX_NUM_FORMS": "1000",
        }
        for i in range(2):
            data.update({f"app_entries-{i}-plugin": "forecast_plugin", f"app_entries-{i}-ORDER": str(i + 1)})

        form = form_class(data, instance=settings)

        self.assertFalse(form.is_valid())
        self.assertIn("plugin", form.formsets["app_entries"].forms[1].errors)
