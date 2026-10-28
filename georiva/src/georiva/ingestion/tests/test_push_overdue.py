from datetime import timedelta

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.ingestion.models import Delivery, PushFeed, PushFeedOverdue
from georiva.ingestion.overdue import record_overdue_feeds
from georiva.organisations.testing import make_organisation

INTERVAL = timedelta(hours=6)


def _feed(slug="cat", interval=INTERVAL, since=None):
    catalog = Catalog.objects.create(organisation=make_organisation(), name=slug, slug=slug, file_format="netcdf")
    feed = PushFeed.objects.create(catalog=catalog, name=f"Feed {slug}", valid_time_format="YYYYMMDD")
    if interval is not None:
        feed.expected_interval = interval
        feed.save()
    if since is not None:
        PushFeed.objects.filter(pk=feed.pk).update(expected_since=since)
        feed.refresh_from_db()
    return feed


def _accept(feed, at):
    delivery = Delivery.objects.create(
        push_feed=feed, original_filename="f.nc", file_path="f.nc", correlation_id=None, status="accepted"
    )
    Delivery.objects.filter(pk=delivery.pk).update(accepted_at=at)
    return delivery


class IsOverdueTests(TestCase):
    def setUp(self):
        self.now = timezone.now()

    def test_no_interval_is_never_overdue(self):
        feed = _feed(interval=None)
        self.assertFalse(feed.is_overdue(self.now + timedelta(days=400)))

    def test_no_deliveries_counts_from_when_the_interval_was_set(self):
        feed = _feed(since=self.now - timedelta(hours=7))
        self.assertTrue(feed.is_overdue(self.now))
        fresh = _feed("other", since=self.now - timedelta(hours=1))
        self.assertFalse(fresh.is_overdue(self.now))

    def test_exactly_at_the_interval_is_not_overdue(self):
        feed = _feed()
        _accept(feed, self.now - INTERVAL)
        self.assertFalse(feed.is_overdue(self.now))
        self.assertTrue(feed.is_overdue(self.now + timedelta(seconds=1)))

    def test_a_fresh_delivery_clears_it(self):
        feed = _feed(since=self.now - timedelta(days=2))
        self.assertTrue(feed.is_overdue(self.now))
        _accept(feed, self.now)
        self.assertFalse(feed.is_overdue(self.now))

    def test_received_but_not_accepted_does_not_count(self):
        feed = _feed(since=self.now - timedelta(days=2))
        Delivery.objects.create(push_feed=feed, original_filename="f", correlation_id=None)
        self.assertTrue(feed.is_overdue(self.now))

    def test_setting_the_interval_starts_the_clock(self):
        feed = _feed()
        self.assertIsNotNone(feed.expected_since)
        feed.expected_interval = None
        feed.save()
        self.assertIsNone(feed.expected_since)

    def test_chip_text_names_the_last_delivery(self):
        feed = _feed()
        _accept(feed, self.now - timedelta(hours=31))
        self.assertEqual(feed.overdue_text(self.now), "Overdue · last delivery 31 h ago")
        empty = _feed("e", since=self.now - timedelta(hours=9))
        self.assertEqual(empty.overdue_text(self.now), "Overdue · no deliveries yet")
        self.assertIsNone(_feed("n", interval=None).overdue_text(self.now))
        fresh = _feed("ok")
        _accept(fresh, self.now)
        self.assertIsNone(fresh.overdue_text(self.now))


class RecordOverdueTests(TestCase):
    def setUp(self):
        self.now = timezone.now()

    def test_one_event_per_spell(self):
        feed = _feed(since=self.now - timedelta(hours=7))
        for _ in range(3):
            record_overdue_feeds(self.now)
        self.assertEqual(PushFeedOverdue.objects.filter(push_feed=feed).count(), 1)

    def test_a_later_spell_records_again(self):
        feed = _feed(since=self.now - timedelta(hours=7))
        record_overdue_feeds(self.now)
        _accept(feed, self.now + timedelta(hours=1))
        record_overdue_feeds(self.now + timedelta(hours=2))  # cleared: nothing
        self.assertEqual(PushFeedOverdue.objects.count(), 1)
        record_overdue_feeds(self.now + timedelta(hours=8))
        record_overdue_feeds(self.now + timedelta(hours=9))
        self.assertEqual(PushFeedOverdue.objects.count(), 2)

    def test_feeds_that_are_not_overdue_record_nothing(self):
        _feed(interval=None)
        _feed("fresh", since=self.now)
        record_overdue_feeds(self.now)
        self.assertEqual(PushFeedOverdue.objects.count(), 0)

    def test_event_is_failed_and_grouped_under_the_feed(self):
        feed = _feed(since=self.now - timedelta(hours=7))
        record_overdue_feeds(self.now)
        event = PushFeedOverdue.objects.get()
        self.assertEqual(event.outcome_class, "failed")
        self.assertEqual(event.organisation_id, feed.catalog.organisation_id)
        group = event.failure_group()
        self.assertEqual(group.key, event.failure_group_key())
        self.assertEqual(group.label, feed.name)
        self.assertEqual(group.url, reverse("push_feed_page", args=[feed.pk]))

    def test_two_feeds_are_two_groups(self):
        _feed("a", since=self.now - timedelta(hours=7))
        _feed("b", since=self.now - timedelta(hours=7))
        record_overdue_feeds(self.now)
        keys = {e.failure_group_key() for e in PushFeedOverdue.objects.all()}
        self.assertEqual(len(keys), 2)


class OverdueTaskTests(TestCase):
    def test_the_beat_task_records_through_the_sweep_wrapper(self):
        from georiva.ingestion.tasks import record_overdue_push_feeds

        _feed(since=timezone.now() - timedelta(hours=7))
        self.assertEqual(record_overdue_push_feeds(), 1)
        self.assertEqual(record_overdue_push_feeds(), 0)
