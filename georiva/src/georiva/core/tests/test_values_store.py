"""Reading a Store asset through the values module (ADR 0046 §10, §12; ADR 0044 §9, §11).

A Store asset's href is a whole Icechunk repository; which field it means is
its item's reference time and valid time and an array's path in the store.
These tests build a small forecast-shaped repository on local disk and read it
through a session, as a batch reads a harvested store.

The one thing stood in for is where a store's address is opened: an Upstream
store is reached over HTTP, and here the address leads to the repository on
disk instead. Nothing reaches a network.
"""

import os
import tempfile
from datetime import UTC, datetime, timedelta
from unittest import mock

import icechunk
import numpy as np
import zarr
from affine import Affine
from django.test import SimpleTestCase, TestCase

from georiva.core import values
from georiva.core.models import Asset, Catalog, Item, Unit, Variable
from georiva.harvest import upstream
from georiva.harvest.models import HarvestFeed, UpstreamShape
from georiva.organisations.testing import make_organisation

STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
ARRAY = "temperature_2m"

RUNS = [datetime(2026, 3, 22, 12, tzinfo=UTC), datetime(2026, 3, 23, 0, tzinfo=UTC)]
LEADS = [timedelta(hours=0), timedelta(hours=6), timedelta(hours=12)]
#: Cell centres on a 10° grid: 0…80°E, 20°S…20°N. Latitudes are written south
#: first, as many stores keep them; a read is north-up all the same.
LATITUDES = [-15.0, -5.0, 5.0, 15.0]
LONGITUDES = [5.0, 15.0, 25.0, 35.0, 45.0, 55.0, 65.0, 75.0]
NORTH_UP = Affine(10.0, 0.0, 0.0, 0.0, -10.0, 20.0)


def expected(run: int, step: int, *, edition: float = 0.0) -> np.ndarray:
    """The field of one run and step, row 0 north: every cell a different value."""
    return (1000.0 * edition + 100.0 * run + 10.0 * step + np.arange(32.0).reshape(4, 8)).astype(np.float32)


class LocalStore:
    """A forecast store on local disk, opened at :data:`STORE`."""

    def setUp(self):
        super().setUp()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = folder.name
        self.storage = icechunk.local_filesystem_storage(os.path.join(folder.name, "repo"))
        self.config = icechunk.RepositoryConfig.default()
        self.repository = None
        opening = mock.patch("georiva.core.values.store.repository_storage", side_effect=self.storage_at)
        opening.start()
        self.addCleanup(opening.stop)

    def storage_at(self, href):
        self.assertEqual(href, STORE)
        return self.storage

    def commit(self, build):
        if self.repository is None:
            self.repository = icechunk.Repository.create(self.storage, config=self.config)
            self.repository.save_config()
        session = self.repository.writable_session("main")
        build(session.store)
        session.commit("build")

    def build_forecast(self, store):
        seconds = [int(run.timestamp()) for run in RUNS]
        init_time = zarr.create_array(store, name="init_time", shape=(2,), dtype="i8", dimension_names=["init_time"])
        init_time[:] = seconds
        init_time.attrs.update({"units": "seconds since 1970-01-01", "calendar": "proleptic_gregorian"})
        lead_time = zarr.create_array(store, name="lead_time", shape=(3,), dtype="i8", dimension_names=["lead_time"])
        lead_time[:] = [int(lead.total_seconds()) for lead in LEADS]
        lead_time.attrs.update({"units": "seconds"})
        zarr.create_array(store, name="latitude", shape=(4,), dtype="f8", dimension_names=["latitude"])[:] = LATITUDES
        zarr.create_array(store, name="longitude", shape=(8,), dtype="f8", dimension_names=["longitude"])[:] = (
            LONGITUDES
        )
        array = zarr.create_array(
            store,
            name=ARRAY,
            shape=(2, 3, 4, 8),
            chunks=(1, 1, 4, 8),
            dtype="f8",
            fill_value=np.nan,
            dimension_names=["init_time", "lead_time", "latitude", "longitude"],
        )
        for run in range(2):
            for step in range(3):
                array[run, step] = np.flipud(expected(run, step))  # south first, as written

    def at(self, run: int, step: int, *, places=None) -> values.StoreSlice:
        return values.StoreSlice(
            store=STORE,
            array=ARRAY,
            time=RUNS[run] + LEADS[step],
            reference_time=RUNS[run],
            chunk_locations=places or {},
        )


class StoreReadTests(LocalStore, SimpleTestCase):
    def setUp(self):
        super().setUp()
        self.commit(self.build_forecast)

    def test_a_whole_field_is_read_north_up_float32_and_placed_by_the_stores_own_grid(self):
        with values.session() as s:
            field = s.field(self.at(1, 2))

        self.assertEqual(field.data.dtype, np.float32)
        np.testing.assert_array_equal(field.data, expected(1, 2))
        self.assertEqual(field.transform, NORTH_UP)
        self.assertEqual(field.crs, "EPSG:4326")

    def test_a_window_is_the_cells_under_a_wgs84_box(self):
        with values.session() as s:
            part = s.window(self.at(0, 1), (10.0, -10.0, 30.0, 10.0))

        np.testing.assert_array_equal(part.data, expected(0, 1)[1:3, 1:3])
        self.assertEqual(part.bounds, (10.0, -10.0, 30.0, 10.0))

    def test_a_point_series_reads_each_step_in_the_order_given(self):
        steps = [self.at(1, 2), self.at(1, 0), self.at(1, 1)]

        with values.session() as s:
            series = s.point_series(steps, 25.0, 5.0)  # north-up row 1, column 2

        self.assertEqual(series, [float(expected(1, step)[1, 2]) for step in (2, 0, 1)])

    def test_a_store_is_opened_once_per_session_however_many_fields_are_read(self):
        with mock.patch.object(upstream, "open_repository", wraps=upstream.open_repository) as opening:
            with values.session() as s:
                s.field(self.at(0, 0))
                s.window(self.at(0, 0), (0.0, 0.0, 40.0, 20.0))
                s.point_series([self.at(run, step) for run in range(2) for step in range(3)], 5.0, -15.0)
                self.assertEqual(s.open_stores, 1)

        self.assertEqual(opening.call_count, 1)

    def test_each_session_opens_the_store_again(self):
        with mock.patch.object(upstream, "open_repository", wraps=upstream.open_repository) as opening:
            values.field(self.at(0, 0))
            values.field(self.at(0, 0))

        self.assertEqual(opening.call_count, 2)

    def test_a_commit_during_a_session_is_not_seen_by_it_and_is_seen_by_the_next(self):
        def rewrite(store):
            zarr.open_array(store, path=ARRAY, mode="r+")[1, 1] = np.flipud(expected(1, 1, edition=1))

        with values.session() as s:
            s.field(self.at(1, 0))  # the store is open, at the snapshot before the commit
            self.commit(rewrite)
            during = s.field(self.at(1, 1))

        np.testing.assert_array_equal(during.data, expected(1, 1))
        np.testing.assert_array_equal(values.field(self.at(1, 1)).data, expected(1, 1, edition=1))

    def test_nothing_stays_open_after_the_session_ends(self):
        with values.session() as s:
            s.field(self.at(0, 0))

        self.assertEqual(s.open_stores, 0)
        with self.assertRaises(values.SessionClosed):
            s.field(self.at(0, 0))

    def test_a_step_the_store_does_not_hold_is_an_error_not_a_field_of_gaps(self):
        def allocate_a_run_ahead(store):
            zarr.open_array(store, path=ARRAY, mode="r+").resize((3, 3, 4, 8))
            init_time = zarr.open_array(store, path="init_time", mode="r+")
            init_time.resize((3,))
            init_time[2] = int((RUNS[1] + timedelta(hours=12)).timestamp())

        self.commit(allocate_a_run_ahead)
        run = RUNS[1] + timedelta(hours=12)

        with self.assertRaises(values.StepNotHeld):
            values.field(values.StoreSlice(STORE, ARRAY, run, run))


class OnlyPinnedPlacesAreFollowedTests(LocalStore, SimpleTestCase):
    """A virtual store keeps a step's data in its producer's files, and its own
    configuration says where those are and how to reach them (ADR 0044 §11)."""

    def setUp(self):
        super().setUp()
        self.data_prefix = f"file://{self.folder}/producer/"
        os.makedirs(os.path.join(self.folder, "producer"))
        raw = os.path.join(self.folder, "producer", "field.bin")
        np.flipud(expected(0, 2)).astype("f8").tofile(raw)
        self.config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(self.data_prefix, icechunk.local_filesystem_store(self.data_prefix[7:]))
        )

        def build(store):
            self.build_forecast(store)
            store.set_virtual_ref(f"{ARRAY}/c/0/2/0/0", f"file://{raw}", offset=0, length=256)

        self.commit(build)

    def test_data_in_a_place_the_feed_did_not_pin_fails_to_read_and_the_error_names_it(self):
        with self.assertRaises(values.StoreDataNotPinned) as refused:
            values.field(self.at(0, 2, places={"s3://noaa-gfs-bdp-pds/": "us-east-1"}))

        self.assertTrue(refused.exception.place.startswith(self.data_prefix), refused.exception.place)
        self.assertIn(self.data_prefix, str(refused.exception))

    def test_with_nothing_pinned_the_stores_own_places_are_not_followed(self):
        with self.assertRaises(values.StoreDataNotPinned):
            values.field(self.at(0, 2))

    def test_the_fields_kept_in_the_store_itself_still_read(self):
        np.testing.assert_array_equal(values.field(self.at(0, 1)).data, expected(0, 1))


class StoreAssetRowTests(LocalStore, TestCase):
    """A Store asset row is read as the field its item and array name, with its feed's pins."""

    PINS = {"s3://noaa-gfs-bdp-pds/": "us-east-1"}

    def setUp(self):
        super().setUp()
        self.commit(self.build_forecast)
        catalog = Catalog.objects.create(
            organisation=make_organisation(), name="GFS", slug="gfs", file_format=Catalog.FileFormat.ZARR
        )
        self.feed = HarvestFeed.objects.create(
            name="GFS upstream",
            catalog=catalog,
            upstream_url="https://stac.upstream.example/catalog.json",
            upstream_collection_id="noaa-gfs-forecast-virtual",
            upstream_shape=UpstreamShape.STORE,
            store_href=STORE,
            asset_hosts=["dynamical-noaa-gfs.s3.us-west-2.amazonaws.com"],
            pinned_chunk_locations=self.PINS,
        )
        unit, _ = Unit.objects.get_or_create(name="Kelvin", symbol="K")
        collection = self.feed.ensure_collection()
        variable = Variable.objects.create(collection=collection, slug="temperature-2m", name="T2m", unit=unit)
        item = Item.objects.create(collection=collection, time=RUNS[1] + LEADS[1], reference_time=RUNS[1])
        self.asset = Asset.objects.create(
            item=item,
            variable=variable,
            format=Asset.Format.ZARR,
            href=STORE,
            store_array=ARRAY,
            harvested_by=self.feed,
        )

    def test_the_rows_field_is_read_following_only_its_feeds_pins(self):
        with mock.patch.object(upstream, "open_repository", wraps=upstream.open_repository) as opening:
            field = values.field(Asset.objects.get(pk=self.asset.pk))

        np.testing.assert_array_equal(field.data, expected(1, 1))
        self.assertEqual(opening.call_args.args[1], self.PINS)

    def test_a_row_whose_feed_is_gone_follows_its_data_nowhere(self):
        # Provenance outlives its feed, and then names one that is not there.
        Asset.objects.filter(pk=self.asset.pk).update(harvested_by_id=self.feed.pk + 1000)

        with mock.patch.object(upstream, "open_repository", wraps=upstream.open_repository) as opening:
            values.field(Asset.objects.get(pk=self.asset.pk))

        self.assertEqual(opening.call_args.args[1], {})
