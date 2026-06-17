from datetime import datetime, timezone

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item
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
