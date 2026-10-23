"""Published asset objects follow their rows out, on commit (ADR 0032).

Three properties carry the design, and none is visible in a happy-path delete:

**Nothing is deleted for a delete that did not happen.** The object goes only
once the row's delete commits; a rolled-back delete leaves both.

**A deterministic key can come back to life.** Re-ingesting a timestamp writes
the same key again, object first and row second. So a deletion re-checks at run
time and spares a key a live Asset names, and an object written since the
commit (within a skew margin) — whichever half of the rewrite has landed.

**A cascade is one batch, not one task per row.** Deleting a collection fires
``post_delete`` for every asset; they are buffered per transaction and
dispatched in chunks.
"""

import os
import shutil
import tempfile
from datetime import UTC, datetime, timedelta
from io import StringIO
from unittest import mock

from django.conf import settings
from django.core.management import call_command
from django.db import transaction
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from georiva.core.models import Asset, Collection, Item
from georiva.core.storage import Bucket, BucketType, asset_deletion
from georiva.core.storage.asset_cleanup import (
    DELETE,
    DELETION_MARGIN,
    MISSING,
    SKIP_LIVE,
    SKIP_RECENT,
    deletion_verdict,
)
from georiva.organisations.testing import make_org_tree, make_organisation

DELAY = "georiva.core.tasks.delete_asset_objects.delay"
COMMIT = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)


class DeletionVerdictTests(SimpleTestCase):
    def test_an_old_unreferenced_object_is_deleted(self):
        old = COMMIT - DELETION_MARGIN - timedelta(seconds=1)
        self.assertEqual(deletion_verdict("k.tif", set(), old, COMMIT), DELETE)

    def test_a_key_a_live_asset_names_is_spared_however_old(self):
        old = COMMIT - timedelta(days=30)
        self.assertEqual(deletion_verdict("k.tif", {"k.tif"}, old, COMMIT), SKIP_LIVE)

    def test_an_object_rewritten_after_the_commit_is_spared(self):
        # Re-ingest wrote new bytes before its row exists: no live href yet.
        self.assertEqual(deletion_verdict("k.tif", set(), COMMIT + timedelta(seconds=5), COMMIT), SKIP_RECENT)

    def test_an_object_modified_within_the_margin_is_spared(self):
        # The app's clock and the object store's may disagree by a little.
        within = COMMIT - DELETION_MARGIN + timedelta(seconds=1)
        self.assertEqual(deletion_verdict("k.tif", set(), within, COMMIT), SKIP_RECENT)

    def test_a_missing_object_is_not_an_error(self):
        self.assertEqual(deletion_verdict("k.tif", set(), None, COMMIT), MISSING)


class AssetsBucketTestCase(TestCase):
    """An assets bucket on a real filesystem in a temporary directory."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        override = override_settings(
            STORAGES={
                **settings.STORAGES,
                "georiva-assets": {
                    "BACKEND": "django.core.files.storage.FileSystemStorage",
                    "OPTIONS": {"location": self.tmp, "base_url": "/assets/"},
                },
            }
        )
        override.enable()
        self.addCleanup(override.disable)
        self.bucket = Bucket(BucketType.ASSETS, "georiva-assets")

    def put(self, key, *, modified):
        path = os.path.join(self.tmp, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(b"cog")
        os.utime(path, (modified.timestamp(), modified.timestamp()))
        return key

    def exists(self, key):
        return os.path.exists(os.path.join(self.tmp, key))


class DeleteObjectsTests(AssetsBucketTestCase):
    def setUp(self):
        super().setUp()
        self.tree = make_org_tree(make_organisation("kenya"))
        self.old = COMMIT - timedelta(hours=1)

    def test_deletes_the_objects_of_deleted_rows(self):
        key = self.put("kenya/c/c/v/2026/03/01/a.tif", modified=self.old)

        summary = asset_deletion.delete_objects([key], COMMIT, bucket=self.bucket)

        self.assertFalse(self.exists(key))
        self.assertEqual(summary["deleted"], 1)

    def test_spares_an_object_rewritten_since_the_commit(self):
        key = self.put("kenya/c/c/v/2026/03/01/a.tif", modified=COMMIT + timedelta(seconds=10))

        summary = asset_deletion.delete_objects([key], COMMIT, bucket=self.bucket)

        self.assertTrue(self.exists(key))
        self.assertEqual(summary[SKIP_RECENT], 1)

    def test_spares_a_key_a_new_asset_names_again(self):
        key = self.put("kenya/c/c/v/2026/03/01/a.tif", modified=self.old)
        Asset.objects.filter(pk=self.tree["asset"].pk).update(href=key)

        summary = asset_deletion.delete_objects([key], COMMIT, bucket=self.bucket)

        self.assertTrue(self.exists(key))
        self.assertEqual(summary[SKIP_LIVE], 1)

    def test_a_missing_object_counts_as_done(self):
        summary = asset_deletion.delete_objects(["kenya/gone.tif"], COMMIT, bucket=self.bucket)

        self.assertEqual(summary[MISSING], 1)
        self.assertEqual(summary["failed"], 0)

    def test_one_failing_key_does_not_strand_the_rest(self):
        bad = self.put("kenya/c/c/v/2026/03/01/bad.tif", modified=self.old)
        good = self.put("kenya/c/c/v/2026/03/01/good.tif", modified=self.old)
        real_delete = self.bucket.delete

        def delete(path):
            if path == bad:
                raise OSError("permission denied")
            return real_delete(path)

        with mock.patch.object(self.bucket, "delete", side_effect=delete):
            summary = asset_deletion.delete_objects([bad, good], COMMIT, bucket=self.bucket)

        self.assertFalse(self.exists(good))
        self.assertEqual((summary["deleted"], summary["failed"]), (1, 1))


class DeleteAssetObjectsTaskTests(SimpleTestCase):
    def call(self, summary=None, error=None, retries=0):
        from celery.exceptions import Retry

        from georiva.core.tasks import delete_asset_objects

        patch = mock.patch(
            "georiva.core.storage.asset_deletion.delete_objects",
            return_value=summary,
            side_effect=error,
        )
        delete_asset_objects.push_request(retries=retries)
        self.addCleanup(delete_asset_objects.pop_request)
        with patch, mock.patch.object(delete_asset_objects, "retry", side_effect=Retry()) as retry:
            try:
                result = delete_asset_objects.run(["a.tif", "b.tif"], COMMIT.isoformat())
            except Retry:
                result = "retried"
        return result, retry

    def test_is_deferrable_work_on_the_default_queue(self):
        from georiva.core.tasks import delete_asset_objects

        self.assertEqual(delete_asset_objects.queue, "georiva-default")

    def test_returns_the_summary(self):
        summary = {"deleted": 2, SKIP_LIVE: 0, SKIP_RECENT: 0, MISSING: 0, "failed": 0}
        result, retry = self.call(summary=summary)

        self.assertEqual(result, summary)
        retry.assert_not_called()

    def test_storage_unreachable_retries_the_chunk(self):
        result, retry = self.call(error=ConnectionError("minio down"))

        self.assertEqual(result, "retried")
        retry.assert_called_once()

    def test_every_key_failing_is_storage_unreachable_too(self):
        summary = {"deleted": 0, SKIP_LIVE: 0, SKIP_RECENT: 0, MISSING: 0, "failed": 2}
        result, retry = self.call(summary=summary)

        self.assertEqual(result, "retried")

    def test_gives_up_loudly_once_retries_are_spent(self):
        with self.assertLogs("georiva.core.tasks", "ERROR") as logs, self.assertRaises(ConnectionError):
            self.call(error=ConnectionError("minio down"), retries=3)

        self.assertIn("cleanup_asset_orphans", logs.output[0])


class DispatchOnCommitTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"))

    def add_assets(self, count):
        """``count`` more items, one asset each, on the tree's variable."""
        hrefs = []
        for day in range(2, 2 + count):
            item = Item.objects.create(collection=self.tree["collection"], time=datetime(2026, 3, day, tzinfo=UTC))
            href = f"kenya/{day}.tif"
            Asset.objects.create(item=item, variable=self.tree["variable"], href=href)
            hrefs.append(href)
        return hrefs

    def test_deleting_an_item_dispatches_its_asset_hrefs_after_commit(self):
        with mock.patch(DELAY) as delay:
            with self.captureOnCommitCallbacks(execute=False) as callbacks:
                self.tree["item"].delete()
            delay.assert_not_called()  # not before the commit

            for callback in callbacks:
                callback()

        delay.assert_called_once()
        hrefs, deleted_at = delay.call_args.args
        self.assertEqual(hrefs, ["x.tif"])
        datetime.fromisoformat(deleted_at)

    def test_a_rolled_back_delete_dispatches_nothing(self):
        with mock.patch(DELAY) as delay, self.captureOnCommitCallbacks(execute=True):
            try:
                with transaction.atomic():
                    self.tree["item"].delete()
                    raise RuntimeError("abort")
            except RuntimeError:
                pass

        delay.assert_not_called()
        self.assertTrue(Asset.objects.filter(pk=self.tree["asset"].pk).exists())

    def test_a_rolled_back_delete_leaves_nothing_behind_for_the_next(self):
        (other,) = self.add_assets(1)

        with mock.patch(DELAY) as delay, self.captureOnCommitCallbacks(execute=True):
            try:
                with transaction.atomic():
                    self.tree["item"].delete()
                    raise RuntimeError("abort")
            except RuntimeError:
                pass
            Asset.objects.get(href=other).item.delete()

        delay.assert_called_once()
        self.assertEqual(delay.call_args.args[0], [other])

    def test_a_cascade_is_batched_into_chunks(self):
        self.add_assets(4)  # five assets in all

        with (
            mock.patch.object(asset_deletion, "CHUNK_SIZE", 2),
            mock.patch(DELAY) as delay,
            self.captureOnCommitCallbacks(execute=True),
        ):
            self.tree["collection"].delete()

        self.assertEqual([len(call.args[0]) for call in delay.call_args_list], [2, 2, 1])
        self.assertEqual(len({call.args[1] for call in delay.call_args_list}), 1)

    def test_every_format_is_covered_not_only_cogs(self):
        Asset.objects.create(
            item=self.tree["item"], variable=self.tree["variable"], href="x.webp", format=Asset.Format.WEBP
        )

        with mock.patch(DELAY) as delay, self.captureOnCommitCallbacks(execute=True):
            self.tree["item"].delete()

        self.assertEqual(sorted(delay.call_args.args[0]), ["x.tif", "x.webp"])


class ItemCountTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.tree = make_org_tree(make_organisation("kenya"))

    def count(self):
        return Collection.objects.get(pk=self.tree["collection"].pk).item_count

    def test_deleting_an_item_counts_it_out(self):
        Collection.objects.filter(pk=self.tree["collection"].pk).update(item_count=1)

        self.tree["item"].delete()

        self.assertEqual(self.count(), 0)

    def test_a_count_that_already_drifted_low_clamps_at_zero(self):
        Collection.objects.filter(pk=self.tree["collection"].pk).update(item_count=0)

        self.tree["item"].delete()

        self.assertEqual(self.count(), 0)

    def test_deleting_the_collection_itself_is_not_an_error(self):
        self.tree["collection"].delete()

        self.assertFalse(Collection.objects.filter(pk=self.tree["collection"].pk).exists())

    def test_recount_repairs_a_drifted_count(self):
        Collection.objects.filter(pk=self.tree["collection"].pk).update(item_count=40)

        call_command("recount_items", stdout=StringIO())

        self.assertEqual(self.count(), 1)


class SweepMarginTests(AssetsBucketTestCase):
    """The sweep reads live hrefs before listing, so an object written in
    between looks orphaned for a moment; the shared guard spares it."""

    def setUp(self):
        super().setUp()
        self.tree = make_org_tree(make_organisation("kenya"))
        self.prefix = f"{self.tree['catalog'].storage_prefix}/{self.tree['collection'].slug}"

    def sweep(self):
        manager = mock.Mock(assets=self.bucket)
        with mock.patch("georiva.core.management.commands.cleanup_asset_orphans.storage", manager):
            call_command(
                "cleanup_asset_orphans",
                "--org",
                "kenya",
                "--catalog",
                self.tree["catalog"].slug,
                "--apply",
                stdout=StringIO(),
            )

    def test_an_old_orphan_is_swept(self):
        key = self.put(f"{self.prefix}/v/2026/03/01/old.tif", modified=timezone.now() - timedelta(hours=1))

        self.sweep()

        self.assertFalse(self.exists(key))

    def test_an_object_written_moments_ago_is_spared(self):
        key = self.put(f"{self.prefix}/v/2026/03/01/new.tif", modified=timezone.now())

        self.sweep()

        self.assertTrue(self.exists(key))
