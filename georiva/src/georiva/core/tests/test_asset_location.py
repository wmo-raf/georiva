"""Where an asset's bytes are — the one seam every reader of ``href`` asks (ADR 0043 §1).

An ``Asset.href`` is a key in our assets bucket, or — for a Remote asset
(ADR 0042) — an Upstream URL. Each test here builds a Remote asset directly
(no harvester exists yet) and follows it down one path that used to treat every
href as a key: the public URL, the STAC API, the delete that follows a row out,
and the orphan sweep. The local asset beside it is the control: its behaviour
must not move.
"""

import os
from datetime import UTC, datetime, timedelta
from io import StringIO
from unittest import mock

import numpy as np
import rasterio
from django.core.management import call_command
from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone
from rasterio.transform import from_origin

from georiva.core.models import Asset, Collection, Item
from georiva.core.storage.asset_location import AssetLocation, local_q, remote_q
from georiva.core.tests.test_asset_deletion import DELAY, AssetsBucketTestCase
from georiva.organisations.testing import dial_org, make_org_tree, make_organisation

UPSTREAM = "https://data.upstream.example/chirps/2026/03/01/chirps-v2.0.2026.03.01.tif"


def add_remote_asset(tree, href=UPSTREAM):
    """A Remote asset on its own item beside the tree's local one, as a harvest would leave it."""
    item = Item.objects.create(collection=tree["collection"], time=datetime(2026, 3, 2, tzinfo=UTC))
    return Asset.objects.create(item=item, variable=tree["variable"], href=href)


class AssetLocationTests(SimpleTestCase):
    def test_a_bucket_key_is_local(self):
        location = AssetLocation("kenya/chirps/daily/rain/2026/03/01/a.tif")

        self.assertFalse(location.is_remote)
        self.assertEqual(location.key, "kenya/chirps/daily/rain/2026/03/01/a.tif")

    def test_an_upstream_url_is_remote(self):
        location = AssetLocation(UPSTREAM)

        self.assertTrue(location.is_remote)

    def test_any_url_scheme_is_remote_not_only_https(self):
        for href in ("http://host/a.tif", "s3://bucket/a.tif", "HTTPS://HOST/A.TIF"):
            with self.subTest(href=href):
                self.assertTrue(AssetLocation(href).is_remote)

    def test_a_key_that_merely_mentions_http_is_local(self):
        self.assertFalse(AssetLocation("kenya/http-feed/https/a.tif").is_remote)

    def test_a_remote_location_has_no_key(self):
        with self.assertRaises(ValueError):
            _ = AssetLocation(UPSTREAM).key

    def test_a_remote_public_url_is_the_href_untouched(self):
        self.assertEqual(AssetLocation(UPSTREAM).public_url(), UPSTREAM)

    def test_a_remote_raster_is_opened_at_its_href(self):
        with mock.patch("rasterio.open") as opened:
            AssetLocation(UPSTREAM).open_raster()

        opened.assert_called_once_with(UPSTREAM)


class AssetLocationQueryTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"))
        cls.local = cls.tree["asset"]
        cls.remote = add_remote_asset(cls.tree)

    def test_the_database_agrees_with_the_seam(self):
        self.assertEqual(list(Asset.objects.filter(remote_q())), [self.remote])
        self.assertEqual(list(Asset.objects.filter(local_q())), [self.local])

    def test_a_related_path_can_be_prefixed(self):
        self.assertEqual(list(Item.objects.filter(remote_q("assets__"))), [self.remote.item])

    def test_an_asset_answers_through_the_seam(self):
        self.assertTrue(self.remote.location.is_remote)
        self.assertFalse(self.local.location.is_remote)
        self.assertEqual(self.remote.url, UPSTREAM)


class LocalRasterRangeReadTests(AssetsBucketTestCase):
    """A local asset opens through the bucket for range reads, not as bytes in memory."""

    def test_a_local_raster_opens_from_the_bucket_without_reading_it_whole(self):
        key = "kenya/c/c/v/2026/03/01/a.tif"
        path = self.bucket.storage.path(key)

        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = np.arange(16, dtype="float32").reshape(4, 4)
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=4,
            height=4,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(0, 4, 1, 1),
        ) as dst:
            dst.write(data, 1)

        manager = mock.Mock(assets=self.bucket)
        with (
            mock.patch("georiva.core.storage.asset_location.storage", manager),
            mock.patch.object(self.bucket, "read_bytes", side_effect=AssertionError("read whole")),
            AssetLocation(key).open_raster() as src,
        ):
            np.testing.assert_array_equal(src.read(1), data)


class STACRemoteHrefTests(TestCase):
    def setUp(self):
        dial_org(self.client)
        self.tree = make_org_tree(make_organisation(), slug="chirps")
        Collection.objects.filter(pk=self.tree["collection"].pk).update(visibility=Collection.Visibility.PUBLIC)
        self.local = self.tree["asset"]
        Asset.objects.filter(pk=self.local.pk).update(format=Asset.Format.COG)

    def _hrefs(self):
        response = self.client.get(reverse("stac:item-detail", args=["chirps", "chirps", "chirps", "20260301T120000Z"]))
        self.assertEqual(response.status_code, 200)
        return [asset["href"] for asset in response.json()["assets"].values()]

    def test_a_remote_asset_is_served_with_its_upstream_href_exactly(self):
        Asset.objects.filter(pk=self.local.pk).update(href=UPSTREAM)

        self.assertIn(UPSTREAM, self._hrefs())

    def test_a_local_asset_href_is_still_the_absolute_storage_url(self):
        hrefs = self._hrefs()

        self.assertTrue(any(h.startswith("http") and h.endswith("/x.tif") for h in hrefs), hrefs)


class RemoteAssetDeletionTests(TestCase):
    """Our cleanup never touches upstream (ADR 0043 §8)."""

    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"))

    def test_deleting_a_remote_assets_row_deletes_no_object(self):
        remote = add_remote_asset(self.tree)

        with mock.patch(DELAY) as delay, self.captureOnCommitCallbacks(execute=True):
            remote.delete()

        delay.assert_not_called()

    def test_a_local_asset_deleted_beside_it_still_follows_its_row_out(self):
        add_remote_asset(self.tree)

        with mock.patch(DELAY) as delay, self.captureOnCommitCallbacks(execute=True):
            self.tree["collection"].delete()

        delay.assert_called_once()
        self.assertEqual(delay.call_args.args[0], ["x.tif"])


class OrphanSweepIgnoresRemoteTests(AssetsBucketTestCase):
    def setUp(self):
        super().setUp()
        self.tree = make_org_tree(make_organisation("kenya"))
        self.prefix = f"{self.tree['catalog'].storage_prefix}/{self.tree['collection'].slug}"

    def sweep(self):
        manager = mock.Mock(assets=self.bucket)
        with (
            mock.patch("georiva.core.management.commands.cleanup_asset_orphans.storage", manager),
            mock.patch.object(self.bucket, "delete", wraps=self.bucket.delete) as delete,
        ):
            call_command(
                "cleanup_asset_orphans",
                "--org",
                "kenya",
                "--catalog",
                self.tree["catalog"].slug,
                "--apply",
                stdout=StringIO(),
            )
        return [c.args[0] for c in delete.call_args_list]

    def test_a_remote_href_is_never_a_live_key_nor_an_orphan(self):
        orphan = self.put(f"{self.prefix}/v/2026/03/01/old.tif", modified=timezone.now() - timedelta(hours=1))
        # An upstream URL whose path happens to spell our key is still not our key.
        add_remote_asset(self.tree, href=f"https://upstream.example/{orphan}")

        deleted = self.sweep()

        self.assertEqual(deleted, [orphan])
        self.assertFalse(self.exists(orphan))
