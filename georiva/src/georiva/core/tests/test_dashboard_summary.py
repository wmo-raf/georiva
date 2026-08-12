from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.organisations.testing import dial_org, make_organisation

User = get_user_model()


class DashboardSummaryTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_dash", "dash@test.com", "pw")
        dial_org(self.client)
        self.client.force_login(self.user)

    def _request(self, organisation=None):
        """A request as OrganisationMiddleware would have left it.

        The tiles count rows, and counting rows means asking which organisation
        is asking — so a bare RequestFactory request, which never passes through
        the middleware, has to be given the same ``active_org`` the host would
        have resolved to.
        """
        from django.test import RequestFactory

        request = RequestFactory().get("/admin/")
        request.user = self.user
        request.active_org = organisation or make_organisation()
        return request

    def test_summary_item_counts(self):
        from django.conf import settings
        from georiva.core.views.summary_items import (
            CatalogSummaryItem,
            CollectionSummaryItem,
            PluginSummaryItem,
        )

        cat = Catalog.objects.create(organisation=make_organisation(), name="A", slug="a", file_format="grib2")
        Catalog.objects.create(organisation=make_organisation(), name="B", slug="b", file_format="grib2")
        Collection.objects.create(catalog=cat, name="c1", slug="c1")

        request = self._request()
        self.assertEqual(CatalogSummaryItem(request).get_count(), 2)
        self.assertEqual(CollectionSummaryItem(request).get_count(), 1)
        self.assertEqual(PluginSummaryItem(request).get_count(), len(settings.GEORIVA_PLUGIN_NAMES))

    def test_the_tiles_do_not_count_another_organisations_holdings(self):
        """A dashboard that boasts the instance's totals is quoting its
        neighbours' numbers."""
        from georiva.core.views.summary_items import CatalogSummaryItem, CollectionSummaryItem
        from georiva.organisations.testing import make_org_tree

        ours = make_organisation()
        make_org_tree(ours)
        make_org_tree(make_organisation("neighbour-org"))

        request = self._request(ours)
        self.assertEqual(CatalogSummaryItem(request).get_count(), 1)
        self.assertEqual(CollectionSummaryItem(request).get_count(), 1)

    def test_each_organisation_is_told_its_own_totals(self):
        """The same instance, two hosts, two answers — the tiles are not merely
        smaller than the global number, they track whoever is asking."""
        from georiva.core.views.summary_items import CatalogSummaryItem
        from georiva.organisations.testing import make_org_tree

        ours = make_organisation()
        neighbour = make_organisation("neighbour-org")
        make_org_tree(ours)
        make_org_tree(neighbour, slug="second")
        make_org_tree(neighbour)

        self.assertEqual(CatalogSummaryItem(self._request(ours)).get_count(), 1)
        self.assertEqual(CatalogSummaryItem(self._request(neighbour)).get_count(), 2)

    def test_the_plugin_tile_stays_instance_wide(self):
        """Plugins are installed into the instance, not owned by an organisation,
        and ``plugin_list`` lists all of them — so the count already matches the
        page it links to and must not be narrowed."""
        from django.conf import settings
        from georiva.core.views.summary_items import PluginSummaryItem
        from georiva.organisations.testing import make_org_tree

        make_org_tree(make_organisation("neighbour-org"))

        self.assertEqual(
            PluginSummaryItem(self._request()).get_count(),
            len(settings.GEORIVA_PLUGIN_NAMES),
        )

    def test_dashboard_renders_three_cards(self):
        response = self.client.get(reverse("wagtailadmin_home"))
        self.assertEqual(response.status_code, 200)
        # Substrings match both singular/plural label forms (the count drives
        # which is shown, and the test environment may have exactly 1 plugin).
        self.assertContains(response, "Catalog")
        self.assertContains(response, "Collection")
        self.assertContains(response, "Plugin")
        # Catalog/Collection cards link to the accordion; Plugins card to its page.
        self.assertContains(response, reverse("catalog:index"))
        self.assertContains(response, reverse("plugin_list"))
