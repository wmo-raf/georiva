"""
Unit tests for the append/rebuild classifier (design decisions 1 + 2).

Watermark = arrival order (max Asset.modified at last commit).  New items
strictly after the committed time_end append; anything earlier/overlapping
(backfill, out-of-order, overwrite) falls back to a full rebuild.
"""

from datetime import datetime, timedelta, timezone

from django.test import SimpleTestCase

from georiva.virtual_zarr.classify import (
    BuildMode,
    CommittedState,
    SourceRow,
    classify,
)

UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _row(hours: int, modified_hours: int, url: str | None = None) -> SourceRow:
    return SourceRow(
        time=T0 + timedelta(hours=hours),
        url=url or f"http://minio/assets/cog-{hours}.tif",
        modified=T0 + timedelta(hours=modified_hours),
    )


class ClassifyTests(SimpleTestCase):
    def test_no_committed_state_rebuilds_everything(self):
        rows = [_row(0, 100), _row(1, 100)]
        plan = classify(None, rows)
        self.assertEqual(plan.mode, BuildMode.REBUILD)
        self.assertEqual(list(plan.rows), rows)

    def test_empty_rows_raises(self):
        with self.assertRaises(ValueError):
            classify(None, [])

    def test_rows_sorted_by_time(self):
        rows = [_row(3, 100), _row(1, 100), _row(2, 100)]
        plan = classify(None, rows)
        self.assertEqual(
            [r.time for r in plan.rows],
            sorted(r.time for r in rows),
        )

    def test_new_items_strictly_after_time_end_append(self):
        committed = CommittedState(
            watermark=T0 + timedelta(hours=50),
            time_end=T0 + timedelta(hours=2),
            item_count=3,
        )
        rows = [_row(0, 10), _row(1, 10), _row(2, 10), _row(3, 60), _row(4, 61)]
        plan = classify(committed, rows)
        self.assertEqual(plan.mode, BuildMode.APPEND)
        self.assertEqual([r.time.hour for r in plan.rows], [3, 4])

    def test_nothing_new_is_up_to_date(self):
        committed = CommittedState(
            watermark=T0 + timedelta(hours=50),
            time_end=T0 + timedelta(hours=2),
            item_count=3,
        )
        rows = [_row(0, 10), _row(1, 10), _row(2, 10)]
        plan = classify(committed, rows)
        self.assertEqual(plan.mode, BuildMode.UP_TO_DATE)
        self.assertEqual(plan.rows, ())
        self.assertIsNone(plan.watermark)

    def test_modified_asset_inside_committed_range_rebuilds(self):
        # Same-pk COG overwrite (update_or_create) bumps Asset.modified past
        # the committed watermark → rebuild fallback, never a blind append.
        committed = CommittedState(
            watermark=T0 + timedelta(hours=50),
            time_end=T0 + timedelta(hours=2),
            item_count=3,
        )
        rows = [_row(0, 10), _row(1, 99), _row(2, 10), _row(3, 99)]
        plan = classify(committed, rows)
        self.assertEqual(plan.mode, BuildMode.REBUILD)
        self.assertEqual(len(plan.rows), 4)

    def test_backfill_item_before_time_end_rebuilds(self):
        # A brand-new item landing inside the committed range changes the
        # in-range item count → rebuild.
        committed = CommittedState(
            watermark=T0 + timedelta(hours=50),
            time_end=T0 + timedelta(hours=4),
            item_count=2,
        )
        rows = [_row(0, 10), _row(2, 99), _row(4, 10)]
        plan = classify(committed, rows)
        self.assertEqual(plan.mode, BuildMode.REBUILD)

    def test_deleted_item_inside_committed_range_rebuilds(self):
        committed = CommittedState(
            watermark=T0 + timedelta(hours=50),
            time_end=T0 + timedelta(hours=4),
            item_count=3,
        )
        rows = [_row(0, 10), _row(4, 10)]  # one in-range item gone
        plan = classify(committed, rows)
        self.assertEqual(plan.mode, BuildMode.REBUILD)

    def test_item_at_exactly_time_end_is_not_appended(self):
        committed = CommittedState(
            watermark=T0 + timedelta(hours=50),
            time_end=T0 + timedelta(hours=2),
            item_count=3,
        )
        # item at hour 2 == time_end is part of the committed range
        rows = [_row(0, 10), _row(1, 10), _row(2, 10), _row(3, 60)]
        plan = classify(committed, rows)
        self.assertEqual(plan.mode, BuildMode.APPEND)
        self.assertEqual([r.time.hour for r in plan.rows], [3])

    def test_watermark_is_max_modified_over_all_rows(self):
        rows = [_row(0, 10), _row(1, 70), _row(2, 30)]
        plan = classify(None, rows)
        self.assertEqual(plan.watermark, T0 + timedelta(hours=70))

    def test_append_watermark_covers_all_rows_not_just_appended(self):
        committed = CommittedState(
            watermark=T0 + timedelta(hours=50),
            time_end=T0 + timedelta(hours=1),
            item_count=2,
        )
        rows = [_row(0, 10), _row(1, 20), _row(2, 60)]
        plan = classify(committed, rows)
        self.assertEqual(plan.mode, BuildMode.APPEND)
        self.assertEqual(plan.watermark, T0 + timedelta(hours=60))
