"""
Rendering seam for the DataFeed status rows.

The listing is a health board: each feed is a full-width row with a status
rail, so an operator scanning one edge sees the broken feed. These tests
assert what a user actually sees -- chip labels, failure reasons, counts --
rather than CSS classes or DOM shape, which we expect to restyle.
"""
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog, Collection
from georiva.sources.health import Health
from georiva.sources.models import DataFeed

User = get_user_model()


def _ago(**kwargs):
    return timezone.now() - timedelta(**kwargs)


def _make_feed(name="CHIRPS Daily", **kwargs):
    slug = name.lower().replace(" ", "-")
    catalog = Catalog.objects.create(name=name, slug=slug, file_format="geotiff")
    return DataFeed.objects.create(name=name, catalog=catalog, **kwargs)


class RowRenderBase(TestCase):
    def setUp(self):
        from georiva.sources.tests.support import ensure_base_datafeed_viewset

        ensure_base_datafeed_viewset()
        self.user = User.objects.create_superuser("op", "op@test.com", "pw")
        self.client.force_login(self.user)
        self.url = reverse("data_feed_list")


class HealthChipTests(RowRenderBase):
    def test_overdue_feed_shows_the_overdue_chip(self):
        _make_feed(last_run_status="success", last_run_at=_ago(days=5), interval_minutes=360)
        response = self.client.get(self.url)
        self.assertContains(response, "Overdue")

    def test_healthy_feed_resolves_to_the_ok_state(self):
        # "OK" is too generic a string to assert in HTML -- it matches page
        # chrome. The state itself is structure, so assert it in context.
        _make_feed(last_run_status="success", last_run_at=_ago(minutes=5), interval_minutes=360)
        response = self.client.get(self.url)
        self.assertEqual(response.context["feed_rows"][0]["health"], Health.OK)

    def test_never_run_feed_shows_never_run(self):
        _make_feed(last_run_at=None)
        response = self.client.get(self.url)
        self.assertContains(response, "Never run")


class FailureReasonTests(RowRenderBase):
    def test_failed_feed_shows_its_error_inline(self):
        _make_feed(
            last_run_status="failed",
            last_run_at=_ago(minutes=5),
            last_run_message="HTTP 403 fetching chirps-v2.0.2026",
        )
        response = self.client.get(self.url)
        self.assertContains(response, "HTTP 403 fetching chirps-v2.0.2026")

    def test_healthy_feed_does_not_show_a_stale_error(self):
        """last_run_message can survive a later success -- it must not read as current."""
        _make_feed(
            last_run_status="success",
            last_run_at=_ago(minutes=5),
            last_run_message="HTTP 403 from an older run",
        )
        response = self.client.get(self.url)
        self.assertNotContains(response, "HTTP 403 from an older run")


class IdentityLineTests(RowRenderBase):
    def test_row_shows_catalog_and_scope_counts(self):
        feed = _make_feed(last_run_status="success", last_run_at=_ago(minutes=5))
        Collection.objects.create(catalog=feed.catalog, name="Rainfall", slug="rainfall")
        Collection.objects.create(catalog=feed.catalog, name="Anomaly", slug="anomaly")
        from georiva.sources.models import DataFeedCollectionLink

        for col in Collection.objects.all():
            DataFeedCollectionLink.objects.create(
                data_feed=feed, collection=col, definition_key=col.slug
            )

        response = self.client.get(self.url)
        self.assertContains(response, "chirps-daily")  # catalog slug or name
        self.assertContains(response, "2 collections")


class LifetimeTotalsTests(RowRenderBase):
    def test_row_shows_lifetime_run_count(self):
        _make_feed(
            last_run_status="success",
            last_run_at=_ago(minutes=5),
            total_runs=342,
        )
        response = self.client.get(self.url)
        self.assertContains(response, "342 runs")


class HealthFilterTests(RowRenderBase):
    def setUp(self):
        super().setUp()
        _make_feed("Broken One", last_run_status="failed", last_run_at=_ago(minutes=5))
        _make_feed("Broken Two", last_run_status="failed", last_run_at=_ago(minutes=5))
        _make_feed("Fine One", last_run_status="success", last_run_at=_ago(minutes=5))
        _make_feed("Muted One", is_active=False)

    def test_unfiltered_shows_everything(self):
        response = self.client.get(self.url)
        self.assertEqual(len(response.context["object_list"]), 4)

    def test_filtering_narrows_to_one_state(self):
        response = self.client.get(self.url, {"health": Health.FAILED.rank})
        names = sorted(f.name for f in response.context["object_list"])
        self.assertEqual(names, ["Broken One", "Broken Two"])

    def test_counts_are_computed_over_the_unfiltered_set(self):
        """Chips must keep showing the whole picture once a filter is applied."""
        response = self.client.get(self.url, {"health": Health.FAILED.rank})
        counts = {c["state"]: c["count"] for c in response.context["health_chips"]}
        self.assertEqual(counts[Health.FAILED], 2)
        self.assertEqual(counts[Health.OK], 1)
        self.assertEqual(counts[Health.INACTIVE], 1)

    def test_active_filter_is_echoed_back(self):
        response = self.client.get(self.url, {"health": Health.FAILED.rank})
        self.assertEqual(response.context["active_health"], Health.FAILED.rank)

    def test_an_unknown_health_value_is_ignored(self):
        response = self.client.get(self.url, {"health": "not-a-rank"})
        self.assertEqual(len(response.context["object_list"]), 4)

    def test_chips_cover_every_state_present(self):
        response = self.client.get(self.url)
        counts = {c["state"]: c["count"] for c in response.context["health_chips"]}
        self.assertEqual(counts, {Health.FAILED: 2, Health.OK: 1, Health.INACTIVE: 1})

    def test_chip_links_reset_pagination_and_keep_the_search(self):
        """Filtering must reset pagination -- Wagtail paginates on 'p'."""
        response = self.client.get(self.url, {"p": 1, "q": "Broken", "sort": "name"})
        urls = [c["url"] for c in response.context["health_chips"]]
        urls.append(response.context["all_chip_url"])
        self.assertTrue(urls)
        for url in urls:
            self.assertNotIn("p=", url)
            self.assertIn("q=Broken", url)
            self.assertIn("sort=name", url)

    def test_filter_survives_alongside_search(self):
        response = self.client.get(self.url, {"health": Health.FAILED.rank, "q": "Two"})
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names, ["Broken Two"])


class SortSelectorTests(RowRenderBase):
    def setUp(self):
        super().setUp()
        _make_feed("Aaa Healthy", last_run_status="success", last_run_at=_ago(days=2))
        _make_feed("Zzz Failed", last_run_status="failed", last_run_at=_ago(minutes=1))

    def test_default_sort_is_health_first(self):
        response = self.client.get(self.url)
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names, ["Zzz Failed", "Aaa Healthy"])

    def test_sorting_by_name_overrides_health(self):
        response = self.client.get(self.url, {"sort": "name"})
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names, ["Aaa Healthy", "Zzz Failed"])

    def test_sorting_by_last_run_puts_most_recent_first(self):
        response = self.client.get(self.url, {"sort": "last_run"})
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names, ["Zzz Failed", "Aaa Healthy"])

    def test_sort_selector_offers_every_option(self):
        response = self.client.get(self.url)
        values = [o["value"] for o in response.context["sort_options"]]
        self.assertEqual(values, ["health", "name", "last_run"])

    def test_an_unknown_sort_falls_back_to_health(self):
        response = self.client.get(self.url, {"sort": "bogus"})
        names = [f.name for f in response.context["object_list"]]
        self.assertEqual(names, ["Zzz Failed", "Aaa Healthy"])


class QueryBudgetTests(RowRenderBase):
    """A full page must cost a bounded number of queries, not one per row."""

    def _make_page(self, count):
        for n in range(count):
            feed = _make_feed(f"Feed {n:03d}", last_run_status="success", last_run_at=_ago(minutes=5))
            col = Collection.objects.create(
                catalog=feed.catalog, name=f"Col {n}", slug=f"col-{n}"
            )
            from georiva.sources.models import DataFeedCollectionLink

            DataFeedCollectionLink.objects.create(
                data_feed=feed, collection=col, definition_key=col.slug
            )

    def test_a_page_of_feeds_costs_the_same_as_a_few(self):
        """The real guard: cost must not scale with rows on the page."""
        self._make_page(3)
        with CaptureQueriesContext(connection) as few:
            self.client.get(self.url)

        DataFeed.objects.all().delete()
        Catalog.objects.all().delete()

        self._make_page(20)
        with CaptureQueriesContext(connection) as many:
            self.client.get(self.url)

        self.assertEqual(
            len(many.captured_queries),
            len(few.captured_queries),
            "query count grew with the number of rows -- an N+1 crept in",
        )


class TemplateHygieneTests(RowRenderBase):
    def test_no_template_comment_leaks_into_the_page(self):
        """Django only supports SINGLE-line {# #}; a multi-line one renders as text."""
        _make_feed(last_run_status="failed", last_run_at=_ago(minutes=5))
        response = self.client.get(self.url)
        content = response.content.decode()
        self.assertNotIn("{#", content)
        self.assertNotIn("#}", content)


class ChipCountAccuracyTests(RowRenderBase):
    def test_counts_are_not_multiplied_by_related_rows(self):
        """Joins from the row annotations must not inflate the GROUP BY."""
        from georiva.sources.models import DerivedProduct

        feed = _make_feed("Busy Feed", last_run_status="failed", last_run_at=_ago(minutes=5))
        for n in range(2):
            col = Collection.objects.create(
                catalog=feed.catalog, name=f"Col {n}", slug=f"col-{n}"
            )
            from georiva.sources.models import DataFeedCollectionLink

            DataFeedCollectionLink.objects.create(
                data_feed=feed, collection=col, definition_key=col.slug
            )
        for n in range(3):
            DerivedProduct.objects.create(
                data_feed=feed, definition_key=f"prod-{n}", recipe_type="promotion"
            )

        response = self.client.get(self.url)
        counts = {c["state"]: c["count"] for c in response.context["health_chips"]}
        self.assertEqual(counts[Health.FAILED], 1, "one feed must count once")

    def test_all_count_stays_total_when_filtered(self):
        """The All chip shows the whole picture, not the filtered subset."""
        _make_feed("Broken", last_run_status="failed", last_run_at=_ago(minutes=5))
        _make_feed("Fine", last_run_status="success", last_run_at=_ago(minutes=5))

        response = self.client.get(self.url, {"health": Health.FAILED.rank})
        self.assertEqual(response.context["total_count"], 2)


class ControlsSurviveEmptyResultsTests(RowRenderBase):
    """Wagtail wraps {% block results %} in {% if object_list %}. Controls must
    live outside it, or filtering to zero strands the user with no way back."""

    def setUp(self):
        super().setUp()
        _make_feed("Broken", last_run_status="failed", last_run_at=_ago(minutes=5))
        _make_feed("Fine", last_run_status="success", last_run_at=_ago(minutes=5))

    def test_chips_still_render_when_the_filter_matches_nothing(self):
        response = self.client.get(
            self.url, {"health": Health.FAILED.rank, "q": "matches-nothing"}
        )
        self.assertEqual(len(response.context["object_list"]), 0)
        self.assertContains(response, "Failed")  # a chip label, not chrome

    def test_all_chip_is_reachable_when_the_filter_matches_nothing(self):
        response = self.client.get(
            self.url, {"health": Health.FAILED.rank, "q": "matches-nothing"}
        )
        # The escape hatch: a URL with no health param must be offered.
        self.assertNotIn("health=", response.context["all_chip_url"])
        self.assertContains(response, response.context["all_chip_url"])

    def test_sort_selector_still_renders_when_empty(self):
        response = self.client.get(
            self.url, {"health": Health.FAILED.rank, "q": "matches-nothing"}
        )
        self.assertContains(response, 'name="sort"')

    def test_active_chip_is_shown_even_at_zero_count(self):
        """You must be able to see -- and leave -- the state you filtered to."""
        _make_feed("Idle", last_run_status="empty", last_run_at=_ago(minutes=5))
        response = self.client.get(self.url, {"health": Health.PARTIAL.rank})
        states = [c["state"] for c in response.context["health_chips"]]
        self.assertIn(Health.PARTIAL, states)


class EmptyStateTests(RowRenderBase):
    def test_no_feeds_at_all_says_so(self):
        response = self.client.get(self.url)
        self.assertContains(response, "No data feeds")

    def test_no_search_matches_names_the_query(self):
        _make_feed("CHIRPS Daily", last_run_status="success", last_run_at=_ago(minutes=5))
        response = self.client.get(self.url, {"q": "zzz-no-such-feed"})
        self.assertContains(response, "zzz-no-such-feed")


class TotalsCompletenessTests(RowRenderBase):
    def test_totals_include_files_fetched(self):
        """Spec asks for runs/files/bytes; files was silently dropped."""
        _make_feed(
            last_run_status="success",
            last_run_at=_ago(minutes=5),
            total_runs=342,
            total_files_fetched=18204,
            total_bytes_transferred=1_200_000_000,
        )
        response = self.client.get(self.url)
        self.assertContains(response, "18,204")


class HumanizeMinutesTests(TestCase):
    """Unit seam: the interval label an operator reads on every row."""

    def test_intervals_read_the_way_operators_say_them(self):
        from georiva.sources.views import _humanize_minutes

        cases = [(5, "5 min"), (59, "59 min"), (60, "1 hour"), (90, "90 min"),
                 (360, "6 hours"), (1440, "1 day"), (43200, "30 days")]
        for minutes, expected in cases:
            with self.subTest(minutes=minutes):
                self.assertEqual(_humanize_minutes(minutes), expected)
