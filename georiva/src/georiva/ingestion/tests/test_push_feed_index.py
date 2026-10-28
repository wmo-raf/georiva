"""The push-feed index: the pull index's status rows, with health resolved in Python."""

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from georiva.core.models import Catalog
from georiva.ingestion.models import Delivery, PushFeed
from georiva.ingestion.push_health import PushHealth, annotate_push_feeds, push_health
from georiva.organisations.testing import dial_org, make_organisation

User = get_user_model()

LIST_URL = "/admin/push-feeds/"
INTERVAL = timedelta(hours=6)


def _feed(slug="cat", name=None, interval=None, since=None, organisation=None):
    catalog = Catalog.objects.create(
        organisation=organisation or make_organisation(), name=slug, slug=slug, file_format="netcdf"
    )
    feed = PushFeed.objects.create(catalog=catalog, name=name or f"Feed {slug}", valid_time_format="YYYYMMDD")
    if interval is not None:
        feed.expected_interval = interval
        feed.save()
    if since is not None:
        PushFeed.objects.filter(pk=feed.pk).update(expected_since=since)
    return feed


def _delivery(feed, status, at, size=0):
    delivery = Delivery.objects.create(
        push_feed=feed, original_filename="f.nc", file_path="f.nc", correlation_id=None, status=status, bytes=size
    )
    Delivery.objects.filter(pk=delivery.pk).update(received_at=at, accepted_at=at if status == "accepted" else None)
    return delivery


def _state(feed, now):
    return push_health(annotate_push_feeds(PushFeed.objects.filter(pk=feed.pk), now).get(), now)


class PushHealthTests(TestCase):
    def setUp(self):
        self.now = timezone.now()

    def test_nothing_ever_delivered_and_no_interval_is_new(self):
        self.assertIs(_state(_feed(), self.now), PushHealth.NEW)

    def test_an_accepted_delivery_makes_it_ok(self):
        feed = _feed()
        _delivery(feed, "accepted", self.now)
        self.assertIs(_state(feed, self.now), PushHealth.OK)

    def test_a_recent_rejection_outranks_ok(self):
        feed = _feed()
        _delivery(feed, "accepted", self.now - timedelta(hours=1))
        _delivery(feed, "rejected", self.now - timedelta(days=2))
        self.assertIs(_state(feed, self.now), PushHealth.REJECTIONS)

    def test_a_rejection_older_than_the_window_is_forgotten(self):
        feed = _feed()
        _delivery(feed, "accepted", self.now)
        _delivery(feed, "rejected", self.now - timedelta(days=8))
        self.assertIs(_state(feed, self.now), PushHealth.OK)

    def test_overdue_outranks_rejections(self):
        feed = _feed(interval=INTERVAL, since=self.now - timedelta(days=1))
        _delivery(feed, "rejected", self.now - timedelta(hours=1))
        self.assertIs(_state(feed, self.now), PushHealth.OVERDUE)

    def test_the_annotations_total_accepted_files_and_bytes_only(self):
        feed = _feed()
        _delivery(feed, "accepted", self.now, size=100)
        _delivery(feed, "accepted", self.now, size=50)
        _delivery(feed, "rejected", self.now, size=999)
        annotated = annotate_push_feeds(PushFeed.objects.filter(pk=feed.pk), self.now).get()
        self.assertEqual((annotated.accepted_files, annotated.accepted_bytes, annotated.recent_rejections), (2, 150, 1))


class PushFeedIndexTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("idx", "i@j.com", "pw")
        self.org = make_organisation()
        dial_org(self.client)
        self.client.force_login(self.user)
        self.now = timezone.now()

    def test_rows_show_identity_health_and_totals(self):
        feed = _feed("surface", name="Surface variables", interval=INTERVAL, organisation=self.org)
        _delivery(feed, "accepted", self.now - timedelta(hours=2), size=2048)
        response = self.client.get(LIST_URL)
        self.assertContains(response, "gfeed-row--positive")
        self.assertContains(response, "Surface variables")
        self.assertContains(response, "expected every 6 hours")
        self.assertContains(response, "last delivery 2\xa0hours ago")
        self.assertContains(response, "1 file")
        self.assertContains(response, "2.0\xa0KB")

    def test_an_overdue_feed_gets_the_warning_rail(self):
        _feed("late", interval=INTERVAL, since=self.now - timedelta(days=1), organisation=self.org)
        self.assertContains(self.client.get(LIST_URL), "gfeed-row--warning")

    def test_rejections_are_counted_on_the_row(self):
        feed = _feed(organisation=self.org)
        _delivery(feed, "rejected", self.now - timedelta(days=1))
        _delivery(feed, "rejected", self.now - timedelta(days=2))
        response = self.client.get(LIST_URL)
        self.assertContains(response, "gfeed-row--critical")
        self.assertContains(response, "2 rejected in the last 7 days")

    def test_the_health_filter_narrows_rows_but_not_the_chips(self):
        ok = _feed("ok", name="Fine feed", organisation=self.org)
        _delivery(ok, "accepted", self.now)
        _feed("new", name="Empty feed", organisation=self.org)
        response = self.client.get(f"{LIST_URL}?health={PushHealth.NEW.rank}")
        self.assertContains(response, "Empty feed")
        self.assertNotContains(response, "Fine feed")
        self.assertContains(response, "No deliveries")
        self.assertEqual(response.context["total_count"], 2)

    def test_unfit_health_values_are_ignored(self):
        _feed(name="Any feed", organisation=self.org)
        self.assertContains(self.client.get(f"{LIST_URL}?health=abc"), "Any feed")

    def test_unhealthy_feeds_sort_first(self):
        ok = _feed("ok", name="Aaa fine", organisation=self.org)
        _delivery(ok, "accepted", self.now)
        bad = _feed("bad", name="Zzz broken", organisation=self.org)
        _delivery(bad, "rejected", self.now)
        names = [row["feed"].name for row in self.client.get(LIST_URL).context["feed_rows"]]
        self.assertEqual(names, ["Zzz broken", "Aaa fine"])

    def test_search_matches_the_feed_name(self):
        _feed("one", name="Rainfall", organisation=self.org)
        _feed("two", name="Winds", organisation=self.org)
        response = self.client.get(f"{LIST_URL}results/?q=rain")
        self.assertContains(response, "Rainfall")
        self.assertNotContains(response, "Winds")

    def test_no_match_message_names_the_query(self):
        _feed(organisation=self.org)
        self.assertContains(self.client.get(f"{LIST_URL}results/?q=zzz"), "No push feeds match")

    def test_another_organisations_feeds_are_not_listed(self):
        _feed("theirs", name="Their feed", organisation=make_organisation("other"))
        response = self.client.get(LIST_URL)
        self.assertNotContains(response, "Their feed")
        self.assertEqual(response.context["total_count"], 0)

    def test_the_menu_carries_delete(self):
        feed = _feed(organisation=self.org)
        self.assertContains(self.client.get(LIST_URL), f"/admin/push-feeds/{feed.pk}/delete/")
