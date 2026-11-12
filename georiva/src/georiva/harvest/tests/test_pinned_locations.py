"""
An Upstream may name its files by `s3://bucket/key`. A harvest stores such an
href only as the public `https` address a person pinned for that bucket, and
that address is the one it judges (ADR 0042 §9).

Inside our deployment `s3://` means our own object store, so no `s3://` href is
ever stored. The only fake is the Upstream catalog client.
"""

from datetime import timedelta

from django.test import TestCase

from georiva.core.models import Asset, Item
from georiva.sources.models import FetchRun

from .support import HarvestFeedMixin, day, rain_href

BUCKET = "deafrica-input-datasets"
BUCKET_HOST = "deafrica-input-datasets.s3.af-south-1.amazonaws.com"


def s3_href(n: int, bucket: str = BUCKET) -> str:
    return f"s3://{bucket}/rainfall_chirps_monthly/chirps-v2.0_2026.{n:02d}.tif"


def https_href(n: int) -> str:
    return f"https://{BUCKET_HOST}/rainfall_chirps_monthly/chirps-v2.0_2026.{n:02d}.tif"


class PinnedLocationTests(HarvestFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        self.variable = self.map_variable("rainfall")
        self.feed.pinned_buckets = {BUCKET: BUCKET_HOST}
        self.feed.save()

    def hrefs(self):
        return sorted(Asset.objects.values_list("href", flat=True))

    def test_an_href_in_a_pinned_bucket_is_stored_as_its_https_address(self):
        self.upstream.add_item(day(1), {"rainfall": s3_href(1)})

        result = self.harvest()

        self.assertEqual((result.indexed, result.rejected), (1, 0))
        self.assertEqual(self.hrefs(), [https_href(1)])

    def test_the_pinned_host_is_allowed_without_being_added_a_second_time(self):
        self.assertEqual(self.feed.asset_hosts, [])
        self.assertIn(BUCKET_HOST, self.feed.allowed_asset_hosts)

    def test_an_href_in_a_bucket_nobody_pinned_is_rejected_and_the_harvest_goes_on(self):
        self.upstream.add_item(day(1), {"rainfall": s3_href(1)})
        self.upstream.add_item(day(2), {"rainfall": s3_href(2, bucket="georiva-assets")})

        result = self.harvest()

        run = FetchRun.objects.get()
        self.assertEqual((run.status, run.harvest_indexed, run.harvest_rejected), (FetchRun.Status.COMPLETED, 1, 1))
        self.assertEqual(result.rejected, 1)
        self.assertEqual(self.hrefs(), [https_href(1)])
        self.assertEqual(Item.objects.count(), 1)

    def test_the_translated_address_is_the_one_judged(self):
        # Pinned, and still held to the rule every asset host is: public addresses only.
        self.upstream.addresses[BUCKET_HOST] = ["10.0.0.7"]
        self.upstream.add_item(day(1), {"rainfall": s3_href(1)})

        result = self.harvest()

        self.assertEqual((result.indexed, result.rejected), (0, 1))
        self.assertEqual(self.upstream.hosts_resolved, [BUCKET_HOST])
        self.assertFalse(Asset.objects.exists())

    def test_a_key_is_spelled_as_a_url_spells_it(self):
        self.upstream.add_item(day(1), {"rainfall": f"s3://{BUCKET}/monthly rain/year=2026/a#1?.tif"})

        self.harvest()

        self.assertEqual(self.hrefs(), [f"https://{BUCKET_HOST}/monthly%20rain/year=2026/a%231%3F.tif"])

    def test_a_bucket_named_with_no_key_is_rejected(self):
        for href in (f"s3://{BUCKET}", f"s3://{BUCKET}/"):
            with self.subTest(href=href):
                FetchRun.objects.all().delete()
                self.upstream.items = []
                self.upstream.add_item(day(1), {"rainfall": href})

                result = self.harvest()

                self.assertEqual((result.indexed, result.rejected), (0, 1))

    def test_a_resync_rewrites_the_href_through_the_same_translation(self):
        item = self.upstream.add_item(day(1), {"rainfall": s3_href(1)}, updated=day(2))
        self.harvest()
        self.upstream.drop_item(day(1))
        self.upstream.add_item(day(1), {"rainfall": s3_href(7)}, updated=item.updated + timedelta(days=1))

        result = self.harvest()

        self.assertEqual((result.updated, result.rejected), (1, 0))
        self.assertEqual(self.hrefs(), [https_href(7)])

    def test_harvesting_twice_changes_nothing_the_second_time(self):
        self.upstream.add_item(day(1), {"rainfall": s3_href(1)}, updated=day(2))
        self.harvest()

        result = self.harvest()

        self.assertEqual((result.indexed, result.updated, result.removed, result.rejected), (0, 0, 0, 0))

    def test_an_https_href_is_stored_as_the_upstream_spelled_it(self):
        self.upstream.add_item(day(1), {"rainfall": rain_href(1)})

        self.harvest()

        self.assertEqual(self.hrefs(), [rain_href(1)])

    def test_no_s3_href_is_ever_stored(self):
        self.upstream.add_item(day(1), {"rainfall": s3_href(1)})
        self.upstream.add_item(day(2), {"rainfall": s3_href(2, bucket="elsewhere")})
        self.upstream.add_item(day(3), {"rainfall": f"S3://{BUCKET}/upper.tif"})

        self.harvest()

        self.assertFalse(Asset.objects.filter(href__istartswith="s3:").exists())
