from datetime import datetime, timezone

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.ingestion.models import FileIngestion

User = get_user_model()


def _setup():
    catalog = Catalog.objects.create(name="Models", slug="models", file_format="grib2")
    collection = Collection.objects.create(catalog=catalog, name="Surface", slug="surface")
    return catalog, collection


def _make_item(collection, source_file, t=None):
    if t is None:
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return Item.objects.create(collection=collection, time=t, source_file=source_file)


def _make_fi(bucket, file_path, status, error=""):
    return FileIngestion.objects.create(
        bucket=bucket,
        file_path=file_path,
        status=status,
        error=error,
    )


class CollectionItemsIngestionBadgeTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_ci", "ci@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog, self.collection = _setup()
        self.url = reverse("collection_items_list", args=[self.collection.pk])
    
    def test_completed_ingestion_shows_completed_badge(self):
        _make_item(self.collection, "mybucket:models/surface/file.grib")
        _make_fi("mybucket", "models/surface/file.grib", FileIngestion.Status.COMPLETED)
        
        response = self.client.get(self.url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "w-status-tag--primary")
    
    def test_failed_ingestion_shows_failed_badge_with_error(self):
        _make_item(self.collection, "mybucket:models/surface/failed.grib")
        _make_fi(
            "mybucket", "models/surface/failed.grib",
            FileIngestion.Status.FAILED, error="Decoding error",
        )
        
        response = self.client.get(self.url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "w-status-tag--critical")
        self.assertContains(response, "Decoding error")
    
    def test_item_with_no_ingestion_shows_dash(self):
        _make_item(self.collection, "")
        
        response = self.client.get(self.url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "w-text-grey-400")
    
    def test_multiple_items_from_same_source_file_all_show_status(self):
        _make_item(self.collection, "mybucket:models/surface/multi.grib", t=datetime(2024, 1, 1, tzinfo=timezone.utc))
        _make_item(self.collection, "mybucket:models/surface/multi.grib", t=datetime(2024, 1, 2, tzinfo=timezone.utc))
        _make_fi("mybucket", "models/surface/multi.grib", FileIngestion.Status.COMPLETED)
        
        response = self.client.get(self.url)
        
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content.decode().count("w-status-tag--primary ci-log-tag"), 2)


class CatalogIndexTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_cat", "cat@test.com", "pw")
        self.client.force_login(self.user)
        self.url = reverse("catalog:index")
        self.results_url = reverse("catalog:index_results")
    
    def _catalog(self, name, slug):
        return Catalog.objects.create(name=name, slug=slug, file_format="grib2")
    
    def test_renders_for_admin(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
    
    def test_empty_state_when_no_catalogs(self):
        response = self.client.get(self.url)
        self.assertContains(response, "There are no Catalogs to display")
    
    def test_all_catalogs_and_collections_present_in_markup(self):
        # Collapsed catalogs still render their collection rows server-side
        # (hidden via CSS), so every collection on the page ships in the HTML.
        cat_a = self._catalog("Alpha", "alpha")
        cat_b = self._catalog("Bravo", "bravo")
        Collection.objects.create(catalog=cat_a, name="Surface Temp", slug="surface-temp")
        Collection.objects.create(catalog=cat_a, name="Precip", slug="precip")
        Collection.objects.create(catalog=cat_b, name="Wind", slug="wind")
        
        response = self.client.get(self.url)
        html = response.content.decode()
        
        self.assertContains(response, "Alpha")
        self.assertContains(response, "Bravo")
        self.assertIn("Surface Temp", html)
        self.assertIn("Precip", html)
        self.assertIn("Wind", html)
    
    def test_badge_counts_collections_and_active(self):
        cat = self._catalog("Counts", "counts")
        Collection.objects.create(catalog=cat, name="C1", slug="c1", is_active=True)
        Collection.objects.create(catalog=cat, name="C2", slug="c2", is_active=True)
        Collection.objects.create(catalog=cat, name="C3", slug="c3", is_active=False)
        
        response = self.client.get(self.url)
        html = response.content.decode()
        
        # 3 collections total, 2 active
        self.assertIn("3 collections", html)
        self.assertIn("2", html)
        self.assertIn("active", html)
    
    def test_singular_collection_label(self):
        cat = self._catalog("Solo", "solo")
        Collection.objects.create(catalog=cat, name="Only", slug="only")
        
        response = self.client.get(self.url)
        self.assertContains(response, "1 collection")
    
    def test_catalog_with_no_collections_shows_empty_state(self):
        self._catalog("Barren", "barren")
        response = self.client.get(self.url)
        self.assertContains(response, "There are no collections to display for this catalog")
    
    def test_catalog_action_urls_present(self):
        from georiva.core.viewsets import CatalogViewSet, CollectionViewSet
        
        cat = self._catalog("Acts", "acts")
        Collection.objects.create(catalog=cat, name="Coll", slug="coll")
        
        response = self.client.get(self.url)
        html = response.content.decode()
        
        edit_url = reverse(CatalogViewSet().get_url_name("edit"), kwargs={"pk": cat.pk})
        delete_url = reverse(CatalogViewSet().get_url_name("delete"), kwargs={"pk": cat.pk})
        add_url = reverse(CollectionViewSet().get_url_name("add"))
        
        self.assertIn(edit_url, html)
        self.assertIn(delete_url, html)
        self.assertIn(add_url, html)
    
    def test_collection_action_urls_present(self):
        from georiva.core.viewsets import CollectionViewSet
        
        cat = self._catalog("CollActs", "collacts")
        coll = Collection.objects.create(catalog=cat, name="Coll", slug="coll")
        
        response = self.client.get(self.url)
        html = response.content.decode()
        
        coll_edit = reverse(CollectionViewSet().get_url_name("edit"), kwargs={"pk": coll.pk})
        items_url = reverse("collection_items_list", args=[coll.pk])
        
        self.assertIn(coll_edit, html)
        self.assertIn(items_url, html)
    
    def test_query_count_independent_of_catalog_count(self):
        # Lock in the prefetch fix: the page must issue a flat number of
        # queries regardless of how many catalogs exist (no N+1).
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        
        def query_count_for(n_catalogs):
            Catalog.objects.all().delete()
            for i in range(n_catalogs):
                c = self._catalog(f"Cat{i}", f"cat{i}")
                Collection.objects.create(catalog=c, name=f"col{i}", slug=f"col{i}")
            # Warm up (content types, permissions, etc.) so the measured
            # request reflects only steady-state query behaviour.
            self.client.get(self.url)
            with CaptureQueriesContext(connection) as ctx:
                self.client.get(self.url)
            return len(ctx.captured_queries)
        
        self.assertEqual(query_count_for(1), query_count_for(5))
    
    def test_header_search_matches_catalog_name(self):
        self._catalog("Alpha", "alpha")
        self._catalog("Bravo", "bravo")
        
        response = self.client.get(self.results_url, {"q": "alph"})
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Alpha")
        self.assertNotContains(response, "Bravo")
    
    def test_header_search_matches_collection_name(self):
        # Searching a collection's name should surface its parent catalog,
        # even though the term doesn't appear in the catalog's own name.
        gfs = self._catalog("GFS", "gfs")
        ecmwf = self._catalog("ECMWF", "ecmwf")
        Collection.objects.create(catalog=gfs, name="temperature-2m", slug="t2m")
        Collection.objects.create(catalog=ecmwf, name="precipitation", slug="precip")
        
        # Partial term — exercises the as-you-type autocomplete path.
        response = self.client.get(self.results_url, {"q": "temp"})
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "GFS")
        self.assertNotContains(response, "ECMWF")
    
    def test_pagination(self):
        # paginate_by = 20, so 25 catalogs span two pages.
        for i in range(25):
            self._catalog(f"Cat{i:02d}", f"cat{i:02d}")
        
        page1 = self.client.get(self.url)
        self.assertEqual(page1.status_code, 200)
        self.assertEqual(page1.context["page_obj"].paginator.num_pages, 2)
        self.assertEqual(len(page1.context["catalog_panels"]), 20)
        
        page2 = self.client.get(self.url, {"p": 2})
        self.assertEqual(len(page2.context["catalog_panels"]), 5)


class DashboardSummaryTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_dash", "dash@test.com", "pw")
        self.client.force_login(self.user)

    def _request(self):
        from django.test import RequestFactory
        request = RequestFactory().get("/admin/")
        request.user = self.user
        return request

    def test_summary_item_counts(self):
        from django.conf import settings
        from georiva.core.summary_items import (
            CatalogSummaryItem, CollectionSummaryItem, PluginSummaryItem,
        )

        cat = Catalog.objects.create(name="A", slug="a", file_format="grib2")
        Catalog.objects.create(name="B", slug="b", file_format="grib2")
        Collection.objects.create(catalog=cat, name="c1", slug="c1")

        request = self._request()
        self.assertEqual(CatalogSummaryItem(request).get_count(), 2)
        self.assertEqual(CollectionSummaryItem(request).get_count(), 1)
        self.assertEqual(
            PluginSummaryItem(request).get_count(), len(settings.GEORIVA_PLUGIN_NAMES)
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


class PluginListTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("admin_plugins", "pl@test.com", "pw")
        self.client.force_login(self.user)
        self.url = reverse("plugin_list")

    def test_page_renders(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Plugins")

    def test_metadata_for_installed_package(self):
        # Use a package guaranteed to be installed to exercise the helper
        # independently of which plugins happen to be loaded.
        from georiva.core.plugins import get_plugin_metadata

        meta = get_plugin_metadata("wagtail")
        self.assertTrue(meta["available"])
        self.assertTrue(meta["name"])
        self.assertTrue(meta["version"])

    def test_metadata_for_missing_package(self):
        from georiva.core.plugins import get_plugin_metadata

        meta = get_plugin_metadata("definitely_not_a_real_package_xyz")
        self.assertFalse(meta["available"])
        self.assertEqual(meta["name"], "definitely_not_a_real_package_xyz")


class CollectionVisibilityTests(TestCase):
    """A Collection is `public` (served) by default; `internal` intermediates
    are never served but read freely by the derivation engine."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="Models", slug="models", file_format="grib2"
        )

    def test_defaults_to_public(self):
        coll = Collection.objects.create(
            catalog=self.catalog, name="Surface", slug="surface"
        )
        self.assertEqual(coll.visibility, Collection.Visibility.PUBLIC)
        self.assertEqual(coll.visibility, "public")

    def test_can_be_marked_internal(self):
        coll = Collection.objects.create(
            catalog=self.catalog, name="Anomaly", slug="anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )
        coll.refresh_from_db()
        self.assertEqual(coll.visibility, "internal")


class TileConfigVisibilityTests(TestCase):
    """The internal tile-config endpoint must not serve internal collections."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.unit = Unit.objects.create(name="Celsius", symbol="C")

    def _variable(self, collection_slug, visibility):
        coll = Collection.objects.create(
            catalog=self.catalog, name=collection_slug, slug=collection_slug,
            visibility=visibility,
        )
        return Variable.objects.create(
            collection=coll, slug="tas", name="tas",
            unit=self.unit, value_min=0, value_max=50,
        )

    def test_public_collection_served(self):
        self._variable("tas", Collection.Visibility.PUBLIC)
        url = reverse("tile_config", args=["cmip6", "tas", "tas"])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_internal_collection_404(self):
        self._variable("tas-anomaly", Collection.Visibility.INTERNAL)
        url = reverse("tile_config", args=["cmip6", "tas-anomaly", "tas"])
        self.assertEqual(self.client.get(url).status_code, 404)


class AddDataFrontDoorTests(TestCase):
    """The Add Data front door routes data managers to the right setup wizard."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@a.com", "pw")
        self.client.force_login(self.user)

    def test_front_door_offers_both_arrival_scenarios(self):
        response = self.client.get(reverse("add_data"))
        self.assertEqual(response.status_code, 200)
        html = response.content.decode()
        # Routes to the DataFeed setup wizard and the Manual Upload Setup Wizard
        self.assertIn(reverse("data_feed_add_select"), html)
        self.assertIn(reverse("upload_wizard_step1"), html)

    def test_front_door_speaks_data_manager_language(self):
        html = self.client.get(reverse("add_data")).content.decode()
        self.assertIn("automatically", html.lower())
        self.assertIn("upload", html.lower())

    def test_front_door_requires_admin_access(self):
        self.client.logout()
        response = self.client.get(reverse("add_data"))
        # Wagtail admin auth redirects anonymous users to login
        self.assertEqual(response.status_code, 302)


class DataMenuTests(TestCase):
    """Acquisition surfaces live under the "Data" menu group; Catalogs sits
    just above it as a top-level entry; derived products have no menu item at
    all — they are reached from each feed's dashboard."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@a.com", "pw")

    def _request(self):
        from django.test import RequestFactory
        request = RequestFactory().get("/admin/")
        request.user = self.user
        return request

    def test_data_group_contains_the_acquisition_surfaces_in_order(self):
        from wagtail.admin.menu import admin_menu
        request = self._request()
        items = admin_menu.menu_items_for_request(request)
        data_item = next((i for i in items if str(i.label) == "Data"), None)
        self.assertIsNotNone(data_item, "No top-level 'Data' menu group found")
        sub_labels = [str(i.label) for i in data_item.menu.menu_items_for_request(request)]
        self.assertEqual(sub_labels, [
            "Add Data", "Automated Sources", "Manual Uploads",
        ])

    def test_catalogs_is_a_top_level_item_sorted_above_the_data_group(self):
        from wagtail.admin.menu import admin_menu
        items = {str(i.label): i for i in admin_menu.menu_items_for_request(self._request())}
        self.assertIn("Catalogs", items)
        # menu_items_for_request does not sort; the rendered menu orders by
        # `order`, so compare that.
        self.assertLess(items["Catalogs"].order, items["Data"].order)

    def test_acquisition_surfaces_and_derived_products_are_not_top_level_items(self):
        from wagtail.admin.menu import admin_menu
        labels = [str(i.label) for i in admin_menu.menu_items_for_request(self._request())]
        for old in ("Automated Sources", "Manual Uploads", "Derived Products"):
            self.assertNotIn(old, labels)


class DataManagersGatingTests(TestCase):
    """Data managers use the guided flows; raw model editing is for advanced users."""

    def _data_manager(self):
        from django.contrib.auth.models import Group
        user = User.objects.create_user("dm", "dm@x.com", "pw")
        user.groups.add(Group.objects.get(name="Data Managers"))
        return user

    def test_group_provisioned_with_browse_but_not_edit_permissions(self):
        from django.contrib.auth.models import Group
        group = Group.objects.get(name="Data Managers")
        codenames = set(group.permissions.values_list("codename", flat=True))
        self.assertIn("access_admin", codenames)
        self.assertIn("view_catalog", codenames)
        self.assertIn("view_collection", codenames)
        for forbidden in ("add_catalog", "change_catalog", "delete_catalog",
                          "add_collection", "change_collection", "delete_collection"):
            self.assertNotIn(forbidden, codenames)

    def test_data_manager_can_browse_but_not_edit_catalogs(self):
        self.client.force_login(self._data_manager())
        self.assertEqual(self.client.get(reverse("catalog:index")).status_code, 200)
        # Raw add/edit forms are denied server-side (redirect with permission error)
        self.assertEqual(self.client.get(reverse("catalog:add")).status_code, 302)

    def test_data_manager_cannot_edit_collections_raw(self):
        catalog = Catalog.objects.create(name="C", slug="c", file_format="grib2")
        collection = Collection.objects.create(catalog=catalog, name="S", slug="s")
        self.client.force_login(self._data_manager())
        self.assertEqual(
            self.client.get(reverse("collection:edit", args=[collection.pk])).status_code, 302
        )

    def test_advanced_user_keeps_the_raw_escape_hatch(self):
        admin = User.objects.create_superuser("root", "r@x.com", "pw")
        self.client.force_login(admin)
        self.assertEqual(self.client.get(reverse("catalog:add")).status_code, 200)

    def test_data_manager_sees_the_data_menu(self):
        from django.test import RequestFactory
        from wagtail.admin.menu import admin_menu
        request = RequestFactory().get("/admin/")
        request.user = self._data_manager()
        labels = [str(i.label) for i in admin_menu.menu_items_for_request(request)]
        self.assertIn("Data", labels)


class CatalogIndexAffordanceTests(TestCase):
    """The catalog accordion only shows affordances the user can actually use."""

    def setUp(self):
        self.catalog = Catalog.objects.create(name="Models", slug="models-idx", file_format="grib2")
        self.collection = Collection.objects.create(
            catalog=self.catalog, name="Surface", slug="surface-idx"
        )
        self.empty_catalog = Catalog.objects.create(name="Empty", slug="empty-idx", file_format="grib2")

    def _get_index_as(self, user):
        self.client.force_login(user)
        return self.client.get(reverse("catalog:index")).content.decode()

    def test_data_manager_sees_a_read_only_accordion(self):
        from django.contrib.auth.models import Group
        user = User.objects.create_user("dm-idx", "dmi@x.com", "pw")
        user.groups.add(Group.objects.get(name="Data Managers"))
        html = self._get_index_as(user)

        self.assertNotIn(reverse("catalog:edit", args=[self.catalog.pk]), html)
        self.assertNotIn(reverse("catalog:delete", args=[self.catalog.pk]), html)
        self.assertNotIn(reverse("collection:add"), html)
        self.assertNotIn(reverse("collection:edit", args=[self.collection.pk]), html)
        self.assertNotIn(reverse("collection:delete", args=[self.collection.pk]), html)
        # The collection name stays useful: it links to the items list instead
        self.assertIn(reverse("collection_items_list", args=[self.collection.pk]), html)

    def test_advanced_user_sees_all_affordances(self):
        user = User.objects.create_superuser("root-idx", "ri@x.com", "pw")
        html = self._get_index_as(user)

        self.assertIn(reverse("catalog:edit", args=[self.catalog.pk]), html)
        self.assertIn(reverse("catalog:delete", args=[self.catalog.pk]), html)
        self.assertIn(reverse("collection:add"), html)
        self.assertIn(reverse("collection:edit", args=[self.collection.pk]), html)
        self.assertIn(reverse("collection:delete", args=[self.collection.pk]), html)
