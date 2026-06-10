from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytz
from django.test import TestCase
from task_ferry.progress import Progress

from georiva.core.models import Catalog, Collection
from georiva.ingestion.handlers.ingestion_handler import IngestionHandler
from georiva.ingestion.progress import PublishingProgress
from georiva.ingestion.service import IngestionService


# =============================================================================
# PublishingProgress unit tests
# =============================================================================

class PublishingProgressTests(TestCase):

    def test_is_progress_subclass(self):
        pub = PublishingProgress(total=10)
        self.assertIsInstance(pub, Progress)

    def test_increment_advances_percentage(self):
        pub = PublishingProgress(total=10)
        pub.increment(5, state="testing")
        self.assertEqual(pub.percentage, 50)


# =============================================================================
# IngestionService.process_file() progress checkpoints
# =============================================================================

class ProcessFileProgressTests(TestCase):

    def setUp(self):
        catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff",
            is_active=True, clip_mode="none",
        )
        Collection.objects.create(
            catalog=catalog, name="Rainfall", slug="rainfall", is_active=True,
        )

    def _run(self):
        mock_progress = MagicMock(spec=Progress)
        loop_progress = MagicMock(spec=Progress)
        ts_slot = MagicMock(spec=Progress)
        loop_progress.create_child.return_value = ts_slot
        mock_progress.create_child.return_value = loop_progress

        mock_plugin = MagicMock()
        mock_plugin.get_timestamps.return_value = [datetime(2024, 1, 15, tzinfo=pytz.utc)]

        mock_item = MagicMock()
        mock_item.pk = "item-1"

        with (
            patch("georiva.ingestion.service.format_registry") as mock_registry,
            patch("georiva.ingestion.service.storage") as mock_storage,
            patch("georiva.ingestion.service.IngestionHandler") as mock_handler_cls,
            patch.object(IngestionService, "_get_first_variable_name", return_value="temperature"),
        ):
            mock_registry.get.return_value = mock_plugin
            mock_storage.assets = MagicMock()
            mock_storage.bucket.return_value = MagicMock()

            mock_handler = MagicMock()
            mock_handler.process_timestamp.return_value = (mock_item, [MagicMock()], {}, [])
            mock_handler_cls.return_value = mock_handler

            service = IngestionService()
            mock_sfm = MagicMock()
            ctx_mgr = MagicMock()
            ctx_mgr.__enter__ = MagicMock(return_value="/tmp/chirps.tif")
            ctx_mgr.__exit__ = MagicMock(return_value=False)
            mock_sfm.download_to_temp.return_value = ctx_mgr
            service._source_file_manager = mock_sfm

            service.process_file(
                "chirps/rainfall/2024/01/15/file.tif",
                progress=mock_progress,
            )

        return mock_progress

    def _states(self, mock_progress):
        return [
            c.kwargs.get("state", c.args[1] if len(c.args) > 1 else "")
            for c in mock_progress.increment.call_args_list
        ]

    def test_emits_file_opened_checkpoint(self):
        states = self._states(self._run())
        self.assertTrue(
            any("open" in s.lower() or "variable" in s.lower() for s in states),
            f"Expected file-opened checkpoint in: {states}",
        )

    def test_emits_timestamps_checkpoint(self):
        states = self._states(self._run())
        self.assertTrue(
            any("timestamp" in s.lower() for s in states),
            f"Expected timestamps checkpoint in: {states}",
        )

    def test_creates_child_for_timestamp_loop(self):
        mock_progress = self._run()
        mock_progress.create_child.assert_called()

    def test_emits_archiving_and_done_checkpoints(self):
        states = self._states(self._run())
        self.assertTrue(
            any("archiv" in s.lower() for s in states),
            f"Expected archiving checkpoint in: {states}",
        )
        self.assertTrue(
            any("done" in s.lower() for s in states),
            f"Expected done checkpoint in: {states}",
        )


# =============================================================================
# IngestionHandler.process_timestamp() progress checkpoints
# =============================================================================

class ProcessTimestampProgressTests(TestCase):

    def _make_handler(self):
        ctx = MagicMock()
        ctx.clipper.is_active = False
        ctx.reference_time = None
        ctx.ingestion_log = None
        ctx.extractor.get_metadata.return_value = {
            "width": 720, "height": 360,
            "bounds": (-180.0, -90.0, 180.0, 90.0),
            "crs": "EPSG:4326",
        }
        handler = IngestionHandler(ctx)
        mock_item = MagicMock()
        mock_item.pk = "item-1"
        handler.item_handler = MagicMock()
        handler.item_handler.get_or_create.return_value = (mock_item, True)
        handler.asset_handler = MagicMock()
        handler.asset_handler.process_variable.return_value = [MagicMock()]
        handler.extent_handler = MagicMock()
        return handler

    def _make_collection(self, *var_slugs):
        mock_col = MagicMock()
        mock_col.slug = "test-collection"
        mock_col.crs = "EPSG:4326"
        vars_ = []
        for slug in var_slugs:
            v = MagicMock()
            v.is_active = True
            v.slug = slug
            vars_.append(v)
        mock_col.variables.all.return_value = vars_
        return mock_col, vars_

    def test_one_increment_per_variable_at_outcome(self):
        handler = self._make_handler()
        collection, variables = self._make_collection("temperature", "rainfall")
        mock_progress = MagicMock(spec=Progress)

        handler.process_timestamp(
            collection=collection,
            local_path=Path("/tmp/file.tif"),
            timestamp=datetime(2024, 1, 15, tzinfo=pytz.utc),
            source_file="sources:chirps/file.tif",
            progress=mock_progress,
        )

        self.assertEqual(mock_progress.increment.call_count, len(variables))

    def test_successful_variable_state_contains_slug(self):
        handler = self._make_handler()
        collection, variables = self._make_collection("temperature")
        mock_progress = MagicMock(spec=Progress)

        handler.process_timestamp(
            collection=collection,
            local_path=Path("/tmp/file.tif"),
            timestamp=datetime(2024, 1, 15, tzinfo=pytz.utc),
            source_file="sources:chirps/file.tif",
            progress=mock_progress,
        )

        state = mock_progress.increment.call_args.kwargs.get("state", "")
        self.assertIn("temperature", state)

    def test_failed_variable_state_contains_slug_and_reason(self):
        handler = self._make_handler()
        handler.asset_handler.process_variable.side_effect = RuntimeError("band missing")
        collection, variables = self._make_collection("temperature")
        mock_progress = MagicMock(spec=Progress)

        handler.process_timestamp(
            collection=collection,
            local_path=Path("/tmp/file.tif"),
            timestamp=datetime(2024, 1, 15, tzinfo=pytz.utc),
            source_file="sources:chirps/file.tif",
            progress=mock_progress,
        )

        state = mock_progress.increment.call_args.kwargs.get("state", "")
        self.assertIn("temperature", state)
        self.assertIn("band missing", state)
