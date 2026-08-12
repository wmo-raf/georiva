"""
Integration tests for the Icechunk swap — the spike's four go/no-go checks
promoted to tests, plus an end-to-end run of the classifier-driven build.

These run against the dev stack's MinIO (the same instance the app uses) and
are skipped when the S3 storage backend is not configured.  Every test works
under a unique scratch prefix and purges it afterwards.
"""

import time
import unittest
import uuid
from datetime import datetime
from datetime import timezone as dt_timezone

import numpy as np
import pandas as pd
from django.conf import settings
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

S3_READY = bool(
    getattr(settings, "AWS_S3_ENDPOINT_URL", None) and getattr(settings, "GEORIVA_STORAGE_BACKEND", "") == "s3"
)

BOUNDS = (30.0, -10.0, 40.0, 0.0)  # west, south, east, north


def _purge_prefix(bucket, prefix: str) -> None:
    for entry in bucket.list_files(prefix, recursive=True):
        try:
            bucket.storage.delete(entry["path"])
        except Exception:
            pass


def _write_cog(key: str, data: np.ndarray) -> str:
    from georiva.core.storage import storage
    from georiva.ingestion.asset_writer import AssetWriter

    return AssetWriter(storage.assets).write_cog(data, key, BOUNDS)


def _read_source(url: str) -> np.ndarray:
    import rasterio

    with rasterio.open(url) as src:
        return src.read(1)


@unittest.skipUnless(S3_READY, "requires the dev MinIO (s3 storage backend)")
class IcechunkStorageChecks(SimpleTestCase):
    """Spike checks 1–4: conflict, overwrite safety, predictors, append."""

    def setUp(self):
        from georiva.core.storage import storage
        from georiva.virtual_zarr.virtual_zarr import MinioStoreConfig

        self.scratch = f"itest-icechunk/{uuid.uuid4().hex[:12]}"
        self.repo_path = f"{self.scratch}/repo/"
        self.config = MinioStoreConfig.from_django_settings()
        self.addCleanup(_purge_prefix, storage.assets, self.scratch)
        self.addCleanup(_purge_prefix, storage.zarr, self.scratch)

    # -- helpers -------------------------------------------------------------

    def _cog(self, name: str, data: np.ndarray) -> str:
        key = f"{self.scratch}/{name}"
        _write_cog(key, data)
        return key

    def _build_vds(self, dated_keys, variable="precip", per_cog_check=None):
        from georiva.virtual_zarr.virtual_zarr import VirtualZarrBuilder

        url_df = pd.DataFrame(
            [{"date": pd.Timestamp(date), "url": self.config.url_for(key)} for date, key in dated_keys]
        )
        return VirtualZarrBuilder(self.config).build(url_df, variable_name=variable, per_cog_check=per_cog_check)

    def _commit_rebuild(self, repo, vds, item_count):
        from georiva.virtual_zarr.repo import commit_metadata
        from georiva.virtual_zarr.virtual_zarr import write_rebuild

        now = timezone.now()
        return write_rebuild(
            repo,
            vds,
            last_updated_at=now,
            metadata=commit_metadata(now, item_count, now, now),
            message=f"rebuild: {item_count}",
        )

    def _open_tip(self, repo, **kwargs):
        import xarray as xr

        session = repo.readonly_session(branch="main")
        return xr.open_zarr(session.store, consolidated=False, chunks=None, **kwargs)

    # -- check 1: conditional writes on MinIO --------------------------------

    def test_concurrent_commit_conflicts_cleanly(self):
        import icechunk
        import zarr

        from georiva.virtual_zarr.repo import open_repo

        repo = open_repo(self.repo_path, create=True)
        s1 = repo.writable_session("main")
        s2 = repo.writable_session("main")

        for session, value in ((s1, 1), (s2, 2)):
            group = zarr.open_group(session.store, mode="a")
            arr = group.create_array("x", shape=(4,), dtype="int32")
            arr[:] = value

        s1.commit("first writer")
        with self.assertRaises(icechunk.ConflictError):
            s2.commit("second writer")

        # Tip stays consistent: the first writer's values won.
        reopened = open_repo(self.repo_path)
        group = zarr.open_group(reopened.readonly_session(branch="main").store, mode="r")
        np.testing.assert_array_equal(group["x"][:], np.full(4, 1, dtype="int32"))

    # -- check 2: overwrite safety -------------------------------------------

    def test_cog_overwrite_errors_at_read_never_wrong_bytes(self):
        from georiva.virtual_zarr.repo import open_repo

        rng = np.random.default_rng(0)
        data0 = rng.random((64, 64), dtype="float32") * 100
        key = self._cog("t0.tif", data0)
        source = _read_source(self.config.url_for(key))

        repo = open_repo(self.repo_path, create=True)
        vds = self._build_vds([("2026-01-01", key)])
        self._commit_rebuild(repo, vds, 1)

        ds = self._open_tip(repo)
        np.testing.assert_array_equal(ds["precip"].isel(time=0).values, source)

        # Rewrite different data at the same key.  Last-Modified has
        # 1-second granularity, so step past the recorded checksum second.
        time.sleep(1.2)
        _write_cog(key, data0 + 1.0)

        fresh = open_repo(self.repo_path)
        with self.assertRaises(Exception) as ctx:
            self._open_tip(fresh)["precip"].isel(time=0).values
        self.assertIn("checksum", str(ctx.exception).lower())

    # -- check 3: predictor round-trip ---------------------------------------

    def test_predictor_roundtrip_bit_equal_to_rasterio(self):

        rng = np.random.default_rng(1)
        cases = {
            # predictor=2 (int16) and predictor=3 (float32), both derived by
            # AssetWriter exactly as in production.
            "int16": (rng.integers(-500, 4000, (64, 64)).astype("int16")),
            "float32": (rng.random((64, 64), dtype="float32") * 40 - 10),
        }
        for dtype, data in cases.items():
            with self.subTest(dtype=dtype):
                key = self._cog(f"pred-{dtype}.tif", data)
                source = _read_source(self.config.url_for(key))

                repo_path = f"{self.scratch}/repo-{dtype}/"
                from georiva.virtual_zarr.repo import open_repo as _open

                repo = _open(repo_path, create=True)
                vds = self._build_vds([("2026-01-01", key)], variable="v")
                self._commit_rebuild(repo, vds, 1)

                # mask_and_scale=False: xarray otherwise promotes ints with a
                # _FillValue to NaN-masked floats (same as the kerchunk path)
                ds = self._open_tip(repo, mask_and_scale=False)
                np.testing.assert_array_equal(ds["v"].isel(time=0).values, source)

    # -- check 4: virtual append + pinned snapshot ----------------------------

    def test_append_extends_time_axis_and_pins_survive(self):
        from georiva.virtual_zarr.repo import commit_metadata, open_repo
        from georiva.virtual_zarr.virtual_zarr import (
            last_committed_time,
            write_append,
        )

        rng = np.random.default_rng(2)
        stack = [rng.random((64, 64), dtype="float32") for _ in range(4)]
        dates = pd.date_range("2026-01-01", periods=4, freq="D")
        keys = [self._cog(f"step-{i}.tif", d) for i, d in enumerate(stack)]

        repo = open_repo(self.repo_path, create=True)
        vds = self._build_vds(list(zip(dates[:2], keys[:2])))
        c1 = self._commit_rebuild(repo, vds, 2)

        self.assertEqual(last_committed_time(repo), dates[1])

        now = timezone.now()
        vds_new = self._build_vds(list(zip(dates[2:], keys[2:])))
        c2 = write_append(
            repo,
            vds_new,
            last_updated_at=now,
            metadata=commit_metadata(now, 4, now, now),
            message="append: 2",
        )
        self.assertNotEqual(c1, c2)

        ds = self._open_tip(repo)
        np.testing.assert_array_equal(ds["time"].values, dates.values)
        for i, key in enumerate(keys):
            np.testing.assert_array_equal(
                ds["precip"].isel(time=i).values,
                _read_source(self.config.url_for(key)),
            )

        # Readers pinned to the pre-append snapshot keep their 2-step view.
        import xarray as xr

        pinned = repo.readonly_session(snapshot_id=c1)
        ds_pinned = xr.open_zarr(pinned.store, consolidated=False, chunks=None)
        self.assertEqual(ds_pinned.sizes["time"], 2)


@unittest.skipUnless(S3_READY, "requires the dev MinIO (s3 storage backend)")
class BuildTaskEndToEndTests(TestCase):
    """The classifier-driven build against real Items/Assets and MinIO."""

    def setUp(self):
        from georiva.core.models import Catalog, Collection, Unit, Variable
        from georiva.core.storage import storage
        from georiva.organisations.testing import make_organisation

        self.organisation = make_organisation()
        self.catalog = Catalog.objects.create(
            organisation=self.organisation,
            name="Itest",
            slug=f"itest-{uuid.uuid4().hex[:8]}",
            file_format="geotiff",
        )
        self.collection = Collection.objects.create(
            catalog=self.catalog,
            name="Daily",
            slug="daily",
        )
        self.unit, _ = Unit.objects.get_or_create(name="Millimetre", defaults={"symbol": "mm"})
        self.variable = Variable.objects.create(
            collection=self.collection,
            slug="precip",
            name="Precip",
            unit=self.unit,
            value_min=0,
            value_max=500,
        )
        self.rng = np.random.default_rng(3)
        self.addCleanup(_purge_prefix, storage.assets, self.catalog.storage_prefix)
        self.addCleanup(_purge_prefix, storage.zarr, self.catalog.storage_prefix)

    def _add_item(self, day: int):
        from georiva.core.models import Asset, Item

        ts = datetime(2026, 3, day, tzinfo=dt_timezone.utc)
        item = Item.objects.create(
            collection=self.collection,
            time=ts,
            bounds=list(BOUNDS),
            crs="EPSG:4326",
            width=64,
            height=64,
        )
        key = f"{self.catalog.storage_prefix}/daily/precip/2026/03/{day:02d}/precip.tif"
        _write_cog(key, self.rng.random((64, 64), dtype="float32"))
        asset = Asset.objects.create(
            item=item,
            variable=self.variable,
            format=Asset.Format.COG,
            href=key,
        )
        return item, asset

    def _manifest(self):
        from georiva.virtual_zarr.models import VirtualZarrManifest

        manifest, _ = VirtualZarrManifest.objects.get_or_create(
            variable=self.variable,
            defaults={"repo_path": VirtualZarrManifest.make_repo_path(self.variable)},
        )
        return manifest

    def _ancestry_len(self, manifest) -> int:
        from georiva.virtual_zarr.repo import open_repo

        repo = open_repo(manifest.get_repo_path())
        return sum(1 for _ in repo.ancestry(branch="main"))

    def test_full_cycle_rebuild_append_and_overwrite_fallback(self):
        from georiva.virtual_zarr.models import VirtualZarrManifest
        from georiva.virtual_zarr.tasks import _run_build

        _, asset0 = self._add_item(1)
        self._add_item(2)
        manifest = self._manifest()

        # --- first build: full rebuild into a fresh repo ---
        _run_build(manifest)
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)
        self.assertEqual(manifest.item_count, 2)
        self.assertTrue(manifest.snapshot_id)
        self.assertIsNotNone(manifest.watermark)
        first_snapshot = manifest.snapshot_id
        # initial commit + rebuild commit
        commits_after_first = self._ancestry_len(manifest)

        # --- new item strictly after time_end: appends, does not rebuild ---
        self._add_item(3)
        _run_build(manifest)
        manifest.refresh_from_db()
        self.assertEqual(manifest.item_count, 3)
        self.assertNotEqual(manifest.snapshot_id, first_snapshot)
        self.assertEqual(self._ancestry_len(manifest), commits_after_first + 1)

        ds = manifest.open_dataset(chunks=None)
        self.assertEqual(ds.sizes["time"], 3)
        self.assertIn("precip", ds)

        # --- no changes: up-to-date, no new commit ---
        snapshot_before = manifest.snapshot_id
        _run_build(manifest)
        manifest.refresh_from_db()
        self.assertEqual(manifest.snapshot_id, snapshot_before)
        self.assertEqual(self._ancestry_len(manifest), commits_after_first + 1)

        # --- overwrite inside the committed range: rebuild fallback ---
        asset0.save()  # update_or_create semantics: same pk, modified bumps
        _run_build(manifest)
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)
        self.assertEqual(manifest.item_count, 3)
        self.assertEqual(self._ancestry_len(manifest), commits_after_first + 2)

        ds = manifest.open_dataset(chunks=None)
        self.assertEqual(ds.sizes["time"], 3)

    def test_build_and_gc_tasks_record_build_logs_and_size_caches(self):
        from georiva.core.models import Item
        from georiva.virtual_zarr.models import (
            VirtualZarrBuildLog,
            VirtualZarrManifest,
        )
        from georiva.virtual_zarr.tasks import (
            build_virtual_zarr_manifest,
            gc_virtual_zarr_repos,
        )

        self._add_item(1)
        self._add_item(2)
        # An Item with no COG asset for this variable: skipped during builds.
        Item.objects.create(
            collection=self.collection,
            time=datetime(2026, 3, 5, tzinfo=dt_timezone.utc),
            bounds=list(BOUNDS),
            crs="EPSG:4326",
            width=64,
            height=64,
        )
        manifest = self._manifest()

        # --- first build: rebuild, with the skip counted -------------------
        build_virtual_zarr_manifest.apply(args=[manifest.pk])
        manifest.refresh_from_db()
        self.assertEqual(manifest.status, VirtualZarrManifest.Status.READY)

        log = manifest.build_logs.get()
        self.assertEqual(log.kind, VirtualZarrBuildLog.Kind.BUILD)
        self.assertEqual(log.outcome, VirtualZarrBuildLog.Outcome.SUCCESS)
        self.assertEqual(log.mode, VirtualZarrBuildLog.Mode.REBUILD)
        self.assertEqual(log.items_written, 2)
        self.assertEqual(log.items_skipped, 1)
        self.assertEqual(log.snapshot_id, manifest.snapshot_id)
        self.assertEqual(log.error, "")
        self.assertLessEqual(log.started_at, log.finished_at)

        self.assertGreater(manifest.repo_size_bytes, 0)
        self.assertGreater(manifest.repo_object_count, 0)

        # --- append cycle: its own row, tail count only --------------------
        self._add_item(3)
        build_virtual_zarr_manifest.apply(args=[manifest.pk])
        manifest.refresh_from_db()

        append_log = manifest.build_logs.order_by("-started_at").first()
        self.assertEqual(append_log.mode, VirtualZarrBuildLog.Mode.APPEND)
        self.assertEqual(append_log.items_written, 1)
        self.assertEqual(append_log.snapshot_id, manifest.snapshot_id)

        # --- up-to-date cycle: recorded, nothing written -------------------
        build_virtual_zarr_manifest.apply(args=[manifest.pk])
        uptodate_log = manifest.build_logs.order_by("-started_at").first()
        self.assertEqual(uptodate_log.mode, VirtualZarrBuildLog.Mode.UP_TO_DATE)
        self.assertEqual(uptodate_log.items_written, 0)

        # --- GC leaves a per-repo record and refreshes the caches ----------
        VirtualZarrManifest.objects.filter(pk=manifest.pk).update(repo_size_bytes=0, repo_object_count=0)
        gc_virtual_zarr_repos.apply()

        gc_log = manifest.build_logs.get(kind=VirtualZarrBuildLog.Kind.GC)
        self.assertEqual(gc_log.outcome, VirtualZarrBuildLog.Outcome.SUCCESS)

        manifest.refresh_from_db()
        self.assertGreater(manifest.repo_size_bytes, 0)
        self.assertGreater(manifest.repo_object_count, 0)
