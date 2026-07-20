"""
Admin HTTP-seam tests for the DataFeed listing.

The listing was a plain function view rendering an unpaginated two-column
table. It is now a Wagtail generic IndexView, which brings server-side
pagination and the admin search box. The URL name is deliberately unchanged:
DataFeedSuccessUrlMixin, DataFeed.delete_url, the admin menu and breadcrumbs
across several templates all reverse "data_feed_list", so the swap must be
invisible to them.
"""
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.sources.health import Health
from georiva.sources.models import DataFeed
from georiva.sources.views import DataFeedIndexView

User = get_user_model()

PAGE_SIZE = DataFeedIndexView.paginate_by


def _ago(**kwargs):
    return timezone.now() - timedelta(**kwargs)


def _make_feed_named(name, **kwargs):
    """Create a feed whose name may collide with others (catalog slugs stay unique)."""
    _make_feed_named.counter += 1
    catalog = Catalog.objects.create(
        name=f"{name} {_make_feed_named.counter}",
        slug=f"cat-{_make_feed_named.counter}",
        file_format="geotiff",
    )
    return DataFeed.objects.create(name=name, catalog=catalog, **kwargs)


_make_feed_named.counter = 0


def _make_feed(name, **kwargs):
    slug = name.lower().replace(" ", "-")
    catalog = Catalog.objects.create(name=name, slug=slug, file_format="geotiff")
    return DataFeed.objects.create(name=name, catalog=catalog, **kwargs)


class DataFeedIndexBase(TestCase):
    def setUp(self):
        from georiva.sources.tests.support import ensure_base_datafeed_viewset

        ensure_base_datafeed_viewset()
        self.user = User.objects.create_superuser("op", "op@test.com", "pw")
        self.client.force_login(self.user)
        self.url = reverse("data_feed_list")


class ListingRenderTests(DataFeedIndexBase):
    def test_listing_renders(self):
        _make_feed("CHIRPS Daily", last_run_status="success", last_run_at=_ago(minutes=5))
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "CHIRPS Daily")

    def test_listing_renders_with_no_feeds(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)

    def test_listing_uses_the_project_templates(self):
        _make_feed("CHIRPS Daily")
        response = self.client.get(self.url)
        used = [t.name for t in response.templates]
        self.assertIn("georivasources/data_feed_list.html", used)
        self.assertIn("georivasources/data_feed_list_results.html", used)

    def test_results_endpoint_renders_rows_without_page_chrome(self):
        """The search box swaps this fragment in; it must not re-render the shell."""
        _make_feed("CHIRPS Daily")
        response = self.client.get(reverse("data_feed_list_results"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "CHIRPS Daily")
        used = [t.name for t in response.templates]
        self.assertIn("georivasources/data_feed_list_results.html", used)
        self.assertNotIn("georivasources/data_feed_list.html", used)


class PaginationTests(DataFeedIndexBase):
    def _make_many(self, count):
        for n in range(count):
            _make_feed(f"Feed {n:03d}", last_run_status="success", last_run_at=_ago(minutes=5))

    def test_first_page_is_capped(self):
        self._make_many(PAGE_SIZE + 5)
        response = self.client.get(self.url)
        self.assertEqual(len(response.context["object_list"]), PAGE_SIZE)

    def test_second_page_returns_the_remainder(self):
        # Wagtail's listing views paginate on "p", not Django's default "page"
        # (BaseListingView.page_kwarg). Anything building listing URLs -- filter
        # chips, sort links -- must reset that same param.
        self._make_many(PAGE_SIZE + 5)
        response = self.client.get(self.url, {"p": 2})
        self.assertEqual(len(response.context["object_list"]), 5)

    def test_a_single_page_is_not_truncated(self):
        self._make_many(3)
        response = self.client.get(self.url)
        self.assertEqual(len(response.context["object_list"]), 3)


class SearchTests(DataFeedIndexBase):
    def setUp(self):
        super().setUp()
        _make_feed("CHIRPS Daily", last_run_status="success", last_run_at=_ago(minutes=5))
        _make_feed("ECMWF HRES", last_run_status="success", last_run_at=_ago(minutes=5))

    def test_search_narrows_to_matching_feeds(self):
        response = self.client.get(self.url, {"q": "chirps"})
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names, ["CHIRPS Daily"])

    def test_search_with_no_matches_returns_empty(self):
        response = self.client.get(self.url, {"q": "nothing-matches-this"})
        self.assertEqual(list(response.context["object_list"]), [])


class HealthOrderingTests(DataFeedIndexBase):
    def test_unhealthy_feeds_come_first_regardless_of_name(self):
        """The reason health is computed in SQL -- a broken feed must not sort onto page 2."""
        _make_feed("Aaa Healthy", last_run_status="success", last_run_at=_ago(minutes=5))
        _make_feed("Zzz Failed", last_run_status="failed", last_run_at=_ago(minutes=5))

        response = self.client.get(self.url)
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names, ["Zzz Failed", "Aaa Healthy"])

    def test_broken_feed_on_a_later_alphabetical_page_lands_on_page_one(self):
        for n in range(PAGE_SIZE + 5):
            _make_feed(f"Feed {n:03d}", last_run_status="success", last_run_at=_ago(minutes=5))
        _make_feed("Zzz Broken", last_run_status="failed", last_run_at=_ago(minutes=5))

        response = self.client.get(self.url)
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names[0], "Zzz Broken")

    def test_pagination_is_stable_when_names_collide(self):
        """Names are not unique; without a pk tiebreak rows repeat or vanish between pages."""
        for _n in range(PAGE_SIZE + 5):
            _make_feed_named("Same Name", last_run_status="success", last_run_at=_ago(minutes=5))

        page1 = list(self.client.get(self.url).context["object_list"])

        # Touch a row between the two page fetches. An UPDATE writes a new tuple
        # at the end of the heap, changing scan order -- which is precisely when
        # an ORDER BY with no unique tiebreak reshuffles equal keys.
        DataFeed.objects.filter(pk=page1[0].pk).update(total_runs=99)

        page2 = list(self.client.get(self.url, {"p": 2}).context["object_list"])

        seen = [f.pk for f in page1] + [f.pk for f in page2]
        self.assertEqual(len(seen), PAGE_SIZE + 5)
        self.assertEqual(len(set(seen)), PAGE_SIZE + 5, "a feed appeared on both pages")

    def test_health_rank_is_available_for_rendering(self):
        _make_feed("Broken", last_run_status="failed", last_run_at=_ago(minutes=5))
        response = self.client.get(self.url)
        feed = response.context["object_list"][0]
        self.assertEqual(feed.health_rank, Health.FAILED.rank)


class UrlIntegrityTests(DataFeedIndexBase):
    """The swap must not orphan the redirects that reverse this URL name."""

    def test_delete_url_still_points_at_the_cascade_page(self):
        feed = _make_feed("CHIRPS Daily")
        self.assertEqual(
            feed.delete_url, reverse("data_feed_delete", kwargs={"pk": feed.pk})
        )

    def test_success_url_mixin_still_targets_the_listing(self):
        from georiva.sources.viewsets import DataFeedSuccessUrlMixin

        self.assertEqual(str(DataFeedSuccessUrlMixin().get_success_url()), self.url)

    def test_add_feed_affordance_is_present(self):
        response = self.client.get(self.url)
        self.assertContains(response, reverse("data_feed_add_select"))
