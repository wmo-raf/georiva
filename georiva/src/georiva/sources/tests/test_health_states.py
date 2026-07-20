from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from django.contrib.contenttypes.models import ContentType
from django.db import connection, models
from django.db.models import Count

from georiva.core.models import Catalog
from georiva.sources.health import Health
from georiva.sources.models import DataFeed


def _make_feed(name="Test Feed", **kwargs):
    catalog = Catalog.objects.create(name=name, slug=name.lower().replace(" ", "-"), file_format="grib2")
    return DataFeed.objects.create(name=name, catalog=catalog, **kwargs)


def _ago(**kwargs):
    return timezone.now() - timedelta(**kwargs)


class HealthAnnotationTests(TestCase):
    """Seam A: DataFeed.objects.with_health() — the queryset the listing consumes."""

    def test_failed_feed_ranks_failed(self):
        _make_feed(last_run_status="failed", last_run_at=_ago(minutes=5))
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.FAILED.rank)

    def test_successful_but_long_past_run_is_stale(self):
        """The reason STALE is derived: a stored 'success' would read green forever."""
        _make_feed(
            last_run_status="success",
            last_run_at=_ago(days=5),
            interval_minutes=360,  # 6h → threshold 12h
        )
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.STALE.rank)

    def test_recent_successful_run_is_ok(self):
        _make_feed(last_run_status="success", last_run_at=_ago(minutes=10), interval_minutes=360)
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.OK.rank)

    def test_frequent_feed_is_not_stale_below_the_floor(self):
        """A naive 2x would call this stale at 10 min; the 30-min floor absorbs jitter."""
        _make_feed(last_run_status="success", last_run_at=_ago(minutes=20), interval_minutes=5)
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.OK.rank)

    def test_frequent_feed_is_stale_above_the_floor(self):
        _make_feed(last_run_status="success", last_run_at=_ago(minutes=45), interval_minutes=5)
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.STALE.rank)

    def test_infrequent_feed_is_stale_at_the_ceiling(self):
        """A naive 2x would hide this for 60 days; the 48h ceiling catches it."""
        _make_feed(
            last_run_status="success",
            last_run_at=_ago(days=3),
            interval_minutes=30 * 24 * 60,  # monthly
        )
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.STALE.rank)

    def test_inactive_feed_is_never_stale_however_old(self):
        _make_feed(is_active=False, last_run_status="success", last_run_at=_ago(days=90))
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.INACTIVE.rank)

    def test_inactive_feed_outranks_a_failed_run(self):
        _make_feed(is_active=False, last_run_status="failed", last_run_at=_ago(minutes=5))
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.INACTIVE.rank)

    def test_never_run_feed_is_new_not_stale(self):
        _make_feed(last_run_at=None, last_run_status="")
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.NEW.rank)

    def test_queued_and_running_collapse_to_one_state(self):
        _make_feed(name="Queued Feed", last_run_status="queued", last_run_at=_ago(minutes=1))
        _make_feed(name="Running Feed", last_run_status="running", last_run_at=_ago(minutes=1))
        ranks = set(DataFeed.objects.with_health().values_list("health_rank", flat=True))
        self.assertEqual(ranks, {Health.RUNNING.rank})

    def test_running_outranks_overdue(self):
        """An overdue feed that has been picked up reports recovery, not waiting."""
        _make_feed(last_run_status="running", last_run_at=_ago(days=5), interval_minutes=360)
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.RUNNING.rank)

    def test_empty_result_is_ok_when_fresh(self):
        """'empty' is a real status choice -- a run that found no data still ran."""
        _make_feed(last_run_status="empty", last_run_at=_ago(minutes=10), interval_minutes=360)
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.OK.rank)

    def test_partial_run_ranks_partial(self):
        _make_feed(last_run_status="partial", last_run_at=_ago(minutes=5))
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.PARTIAL.rank)

    def test_infrequent_feed_is_ok_below_the_ceiling(self):
        _make_feed(
            last_run_status="success",
            last_run_at=_ago(hours=36),
            interval_minutes=30 * 24 * 60,  # monthly
        )
        feed = DataFeed.objects.with_health().get()
        self.assertEqual(feed.health_rank, Health.OK.rank)


class HealthLookupTests(TestCase):
    """Seam B: the rank -> display contract the template depends on."""

    def test_from_rank_resolves_the_state(self):
        self.assertIs(Health.from_rank(Health.STALE.rank), Health.STALE)
        self.assertIs(Health.from_rank(Health.INACTIVE.rank), Health.INACTIVE)

    def test_from_rank_rejects_an_unknown_rank(self):
        with self.assertRaises(KeyError):
            Health.from_rank(999)

    def test_ranks_are_unique(self):
        """A duplicate rank silently makes from_rank ambiguous and drops a state."""
        ranks = [state.rank for state in Health]
        self.assertEqual(len(ranks), len(set(ranks)))

    def test_exactly_one_default_state(self):
        """Zero defaults breaks as_case; two makes which one wins arbitrary."""
        defaults = [state for state in Health if state.value.condition is None]
        self.assertEqual(len(defaults), 1)


class HealthOrderingTests(TestCase):
    """Seam C: the promise the whole design rests on -- broken feeds land on page 1."""

    def test_unhealthy_feeds_sort_above_healthy_ones(self):
        _make_feed(name="Zulu Healthy", last_run_status="success", last_run_at=_ago(minutes=1))
        _make_feed(name="Alpha Failed", last_run_status="failed", last_run_at=_ago(minutes=1))
        _make_feed(name="Mike Inactive", is_active=False)

        names = list(
            DataFeed.objects.with_health()
            .order_by("health_rank", "name")
            .values_list("name", flat=True)
        )
        self.assertEqual(names, ["Alpha Failed", "Zulu Healthy", "Mike Inactive"])

    def test_ordering_beats_alphabetical_default(self):
        """Meta.ordering is ['name']; health ordering must override it, not tie-break it."""
        _make_feed(name="Aaa Healthy", last_run_status="success", last_run_at=_ago(minutes=1))
        _make_feed(name="Zzz Failed", last_run_status="failed", last_run_at=_ago(minutes=1))

        first = DataFeed.objects.with_health().order_by("health_rank", "name").first()
        self.assertEqual(first.name, "Zzz Failed")


class HealthCountTests(TestCase):
    """Seam D: per-state counts for the filter chips."""

    def test_counts_group_by_health_state(self):
        _make_feed(name="F1", last_run_status="failed", last_run_at=_ago(minutes=1))
        _make_feed(name="F2", last_run_status="failed", last_run_at=_ago(minutes=1))
        _make_feed(name="Ok1", last_run_status="success", last_run_at=_ago(minutes=1))
        _make_feed(name="Off1", is_active=False)

        counts = dict(
            DataFeed.objects.with_health()
            .values_list("health_rank")
            .annotate(n=Count("pk"))
            .values_list("health_rank", "n")
        )
        self.assertEqual(counts[Health.FAILED.rank], 2)
        self.assertEqual(counts[Health.OK.rank], 1)
        self.assertEqual(counts[Health.INACTIVE.rank], 1)


class StubPluginFeed(DataFeed):
    """A stand-in for a plugin feed (CHIRPSDataFeed et al).

    Declared here rather than importing a real plugin: plugins only load when
    dev-plugins is bind-mounted, so importing one would make this suite fail
    wherever it is not.
    """

    note = models.CharField(max_length=32, blank=True, default="")

    class Meta:
        app_label = "georivasources"


class PolymorphicHealthTests(TestCase):
    """Production feeds are always subclasses -- annotations must survive the downcast."""

    @classmethod
    def setUpClass(cls):
        # Both the table and the ContentType row must exist *outside* the class
        # atomic: polymorphic resolves the type per insert, and a row created
        # inside a test would be rolled back while staying in ContentType's cache.
        #
        # The table is deliberately never dropped. StubPluginFeed is declared at
        # module level, so it stays in the app registry for the whole run; every
        # later DataFeed deletion has its cascade collector walk this subclass.
        # Dropping the table would leave the registry pointing at nothing and
        # break unrelated tests. Creation is idempotent for --keepdb.
        if StubPluginFeed._meta.db_table not in connection.introspection.table_names():
            with connection.schema_editor() as editor:
                editor.create_model(StubPluginFeed)
        ContentType.objects.get_for_model(StubPluginFeed)
        super().setUpClass()

    def test_annotation_survives_the_polymorphic_downcast(self):
        catalog = Catalog.objects.create(name="Sub", slug="sub", file_format="grib2")
        StubPluginFeed.objects.create(
            name="Sub Feed", catalog=catalog, last_run_status="failed", last_run_at=_ago(minutes=1)
        )

        feed = DataFeed.objects.with_health().get()

        self.assertIsInstance(feed, StubPluginFeed)  # the re-fetch really happened
        self.assertEqual(feed.health_rank, Health.FAILED.rank)  # and kept the annotation

    def test_ordering_holds_across_mixed_subclasses(self):
        """The grouped re-fetch issues one query per content type -- ordering must survive it."""
        _make_feed(name="Base Healthy", last_run_status="success", last_run_at=_ago(minutes=1))
        catalog = Catalog.objects.create(name="Sub", slug="sub", file_format="grib2")
        StubPluginFeed.objects.create(
            name="Sub Failed", catalog=catalog, last_run_status="failed", last_run_at=_ago(minutes=1)
        )

        names = list(
            DataFeed.objects.with_health()
            .order_by("health_rank", "name")
            .values_list("name", flat=True)
        )
        self.assertEqual(names, ["Sub Failed", "Base Healthy"])
