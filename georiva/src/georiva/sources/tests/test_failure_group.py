"""What a fetch run's failure rolls up under on the dashboard (ADR 0033)."""

from django.test import SimpleTestCase

from georiva.sources.models import FetchRunFinished


class FetchFailureGroupTests(SimpleTestCase):
    def test_it_groups_under_its_feed(self):
        event = FetchRunFinished(data_feed_id=12, feed_name="ECMWF IFS")
        self.assertEqual(event.failure_group_key(), "feed:12")
        self.assertEqual(event.failure_group().label, "ECMWF IFS")

    def test_once_the_feed_is_gone_its_name_stands_in_and_there_is_no_link(self):
        event = FetchRunFinished(data_feed_id=None, feed_name="ECMWF IFS")
        self.assertEqual(event.failure_group_key(), "feed-name:ECMWF IFS")
        self.assertIsNone(event.failure_group().url)
