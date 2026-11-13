"""
A harvest of an Upstream store: one Item per Held step of the runs inside the
feed's Rolling window, one Store asset per mapped Variable (ADR 0044 §2, §3, §6, §7).

The only fake is the Upstream — here one that is a store, told which steps it holds.
"""

import json
from datetime import UTC, datetime, timedelta
from unittest import mock
from xml.etree import ElementTree as ET

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from django_redis import get_redis_connection

from georiva.core.machine_plane.palette_cache import warm_all
from georiva.core.machine_plane.remote_assets import (
    PUBLISHED_FIELD,
    REMOTE_ASSETS_KEY_PREFIX,
    timestep_field,
    variable_key,
)
from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.harvest.harvester import harvest
from georiva.harvest.models import HarvestFeed, UpstreamShape, VariableMapping
from georiva.harvest.tasks import run_harvest
from georiva.harvest.testing import FakeUpstream
from georiva.organisations.testing import dial_org, make_organisation
from georiva.sources.models import FetchRun
from georiva.virtual_zarr.models import VirtualZarrManifest
from georiva.wmts.testing import NS, IsolatedCapabilitiesCache

from .support import TITILER_DIALS

CATALOG = "https://stac.upstream.example/catalog.json"
STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"
STORE_HOST = "dynamical-noaa-gfs.s3.us-west-2.amazonaws.com"
STORE_TYPE = "application/x-icechunk"
FORECAST = {"init_time": None, "lead_time": 209, "latitude": 721, "longitude": 1440}
RUN_STEP_GRID = ("init_time", "lead_time", "latitude", "longitude")
WHOLE_FIELD = (1, 1, 721, 1440)
ANALYSIS = {"time": None, "latitude": 721, "longitude": 1440}
TIME_GRID = ("time", "latitude", "longitude")
GLOBE = [-180.0, -90.0, 180.0, 90.0]


def hours(n: float) -> timedelta:
    return timedelta(hours=n)


class StoreFeedMixin:
    """A feed over a fake forecast store: two fields a feed may map and one it
    does not, runs every six hours, a Rolling window of three days."""

    arrays = {
        "temperature_2m": (RUN_STEP_GRID, WHOLE_FIELD),
        "wind_u_10m": (RUN_STEP_GRID, WHOLE_FIELD),
        "pressure_surface": (RUN_STEP_GRID, WHOLE_FIELD),
    }
    dimensions = FORECAST
    feed_fields = {"rolling_window_days": 3}

    def setUp(self):
        super().setUp()
        self.organisation = make_organisation()
        catalog = Catalog.objects.create(
            organisation=self.organisation, name="GFS", slug="gfs", file_format=Catalog.FileFormat.ZARR
        )
        self.feed = HarvestFeed.objects.create(
            name="GFS upstream",
            catalog=catalog,
            upstream_url=CATALOG,
            upstream_collection_id="noaa-gfs-forecast-virtual",
            upstream_shape=UpstreamShape.STORE,
            store_href=STORE,
            asset_hosts=[STORE_HOST],
            **self.feed_fields,
        )
        self.upstream = FakeUpstream("noaa-gfs-forecast-virtual", title="NOAA GFS forecast")
        self.upstream.is_api = False
        self.upstream.become_store(STORE, self.dimensions, self.arrays, media_type=STORE_TYPE)
        self.temperature = self.map_variable("temperature_2m", "temperature-2m")
        #: A run that began a day ago, on the hour.
        self.run = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=1)

    def map_variable(self, path, slug):
        unit, _ = Unit.objects.get_or_create(name="Degree Celsius", symbol="°C")
        variable = Variable.objects.create(
            collection=self.feed.ensure_collection(),
            slug=slug,
            name=slug.title(),
            unit=unit,
            value_min=-40,
            value_max=45,
        )
        VariableMapping.objects.create(feed=self.feed, asset_key=path, variable=variable)
        return variable

    def harvest(self, **kwargs):
        return harvest(self.feed, upstream=self.upstream, **kwargs)

    def hold(self, path, *leads, run=None):
        """The store holds `path` at each of `leads` hours into `run`."""
        run = run or self.run
        for lead in leads:
            self.upstream.hold(path, run + hours(lead), run=run)

    def steps(self, variable=None):
        """`(run, hours into it)` of every Store asset of `variable`, in order."""
        rows = (
            Asset.objects.filter(variable=variable or self.temperature, format=Asset.Format.ZARR)
            .order_by("item__reference_time", "item__time")
            .values_list("item__reference_time", "item__time")
        )
        return [(run, (when - run) / hours(1)) for run, when in rows]


class StoreHarvestTests(StoreFeedMixin, TestCase):
    def test_it_writes_one_item_per_held_step_and_one_store_asset_per_mapped_variable(self):
        wind = self.map_variable("wind_u_10m", "wind-u-10m")
        self.hold("temperature_2m", 0, 3, 6)
        self.hold("wind_u_10m", 0, 3)

        result = self.harvest()

        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.COMPLETED, result.errors)
        self.assertEqual(self.steps(), [(self.run, 0), (self.run, 3), (self.run, 6)])
        self.assertEqual(self.steps(wind), [(self.run, 0), (self.run, 3)])
        self.assertEqual(Item.objects.count(), 3)
        self.assertEqual(result.indexed, 5)

    def test_a_step_no_mapped_array_holds_gets_no_row(self):
        # Step 6 is in the store's coordinates like every other; nothing is held there yet.
        self.hold("temperature_2m", 0, 3, 9)

        self.harvest()

        self.assertEqual(self.steps(), [(self.run, 0), (self.run, 3), (self.run, 9)])
        self.assertFalse(Item.objects.filter(time=self.run + hours(6)).exists())

    def test_every_store_asset_points_at_the_store_and_names_its_array(self):
        self.hold("temperature_2m", 0, 3)

        self.harvest()

        for asset in Asset.objects.all():
            self.assertEqual(asset.href, STORE)
            self.assertEqual(asset.format, Asset.Format.ZARR)
            self.assertEqual(asset.media_type, STORE_TYPE)
            self.assertEqual(asset.store_array, "temperature_2m")
            self.assertEqual(asset.harvested_by_id, self.feed.pk)
            self.assertEqual(asset.roles, [Asset.Role.DATA])
            self.assertIsNone(asset.stats_min)
            self.assertIsNone(asset.stats_max)
            self.assertTrue(asset.location.is_remote)
            self.assertTrue(asset.location.is_store)

    def test_an_item_carries_its_run_its_valid_time_and_the_feeds_provenance(self):
        self.hold("temperature_2m", 3)

        self.harvest()

        item = Item.objects.get()
        self.assertEqual((item.reference_time, item.time), (self.run, self.run + hours(3)))
        self.assertEqual(item.harvested_by_id, self.feed.pk)
        collection = self.feed.collection
        self.assertEqual(collection.item_count, 1)
        self.assertEqual(collection.name, "NOAA GFS forecast")

    def test_an_unmapped_array_is_not_indexed_and_not_asked_about(self):
        self.hold("temperature_2m", 0)
        self.hold("pressure_surface", 0, 3)

        self.harvest()

        self.assertEqual(Asset.objects.count(), 1)
        self.assertEqual(Item.objects.count(), 1)
        (asked,) = self.upstream.steps_asked
        self.assertEqual(asked[:2], (STORE, ["temperature_2m"]))

    def test_the_store_is_asked_for_the_rolling_window_on_run_time(self):
        before = timezone.now()

        self.harvest()

        start, end = self.upstream.steps_asked[0][2]
        self.assertIsNone(end)
        self.assertAlmostEqual(start, before - timedelta(days=3), delta=timedelta(minutes=1))

    def test_a_run_inside_the_window_keeps_steps_however_far_ahead_they_reach(self):
        self.hold("temperature_2m", 0, 240, 384)

        self.harvest()

        self.assertEqual(self.steps(), [(self.run, 0), (self.run, 240), (self.run, 384)])

    def test_a_run_older_than_the_window_is_not_indexed_whatever_the_upstream_answers(self):
        old = self.run - timedelta(days=10)
        self.hold("temperature_2m", 0, run=old)
        self.hold("temperature_2m", 0)
        self.upstream.ignores_window = True

        self.harvest()

        self.assertEqual(self.steps(), [(self.run, 0)])

    def test_a_store_on_a_host_that_is_not_allowed_is_rejected(self):
        HarvestFeed.objects.filter(pk=self.feed.pk).update(asset_hosts=[])
        self.feed.refresh_from_db()
        self.hold("temperature_2m", 0, 3)

        result = self.harvest()

        self.assertFalse(Asset.objects.exists())
        self.assertFalse(Item.objects.exists())
        self.assertEqual((result.indexed, result.rejected), (0, 2))

    def test_no_file_is_recorded_and_the_run_has_the_harvests_own_counts(self):
        self.hold("temperature_2m", 0, 3)

        self.harvest()

        run = FetchRun.objects.get()
        self.assertEqual(run.status, FetchRun.Status.COMPLETED)
        self.assertEqual((run.harvest_indexed, run.harvest_removed), (2, 0))
        self.assertFalse(run.fetched_files.exists())


class StoreMirrorTests(StoreFeedMixin, TestCase):
    def test_a_later_harvest_adds_the_steps_that_have_arrived_since(self):
        self.hold("temperature_2m", 0, 6)
        self.harvest()

        self.hold("temperature_2m", 3, 9)  # out of order, as they land
        result = self.harvest()

        self.assertEqual(self.steps(), [(self.run, 0), (self.run, 3), (self.run, 6), (self.run, 9)])
        self.assertEqual((result.indexed, result.updated, result.removed), (2, 0, 0))
        self.assertEqual(self.feed.collection.item_count, 4)

    def test_a_harvest_of_the_same_window_twice_changes_nothing(self):
        self.hold("temperature_2m", 0, 3)
        self.harvest()
        before = list(Asset.objects.order_by("pk").values_list("pk", "modified"))

        result = self.harvest()

        self.assertEqual((result.indexed, result.updated, result.removed), (0, 0, 0))
        self.assertEqual(list(Asset.objects.order_by("pk").values_list("pk", "modified")), before)
        self.assertEqual(Item.objects.count(), 2)

    def test_a_step_no_longer_held_loses_its_row(self):
        self.hold("temperature_2m", 0, 3)
        self.harvest()

        self.upstream.release("temperature_2m", self.run + hours(3), run=self.run)
        result = self.harvest()

        self.assertEqual(self.steps(), [(self.run, 0)])
        self.assertEqual(result.removed, 1)
        self.assertEqual(Item.objects.count(), 1)
        self.assertEqual(self.feed.collection.item_count, 1)

    def test_a_run_that_has_aged_out_of_the_rolling_window_loses_its_rows(self):
        newer = self.run + hours(12)
        self.hold("temperature_2m", 0, 3)
        self.hold("temperature_2m", 0, run=newer)
        self.harvest()

        # The store still holds the old run; only the time has moved.
        with mock.patch("django.utils.timezone.now", return_value=self.run + timedelta(days=3, hours=1)):
            result = self.harvest()

        self.assertEqual(self.steps(), [(newer, 0)])
        self.assertEqual(result.removed, 2)
        self.assertFalse(Item.objects.filter(reference_time=self.run).exists())

    def test_rows_outside_the_jurisdiction_are_left_when_their_run_ages_out(self):
        self.hold("temperature_2m", 0)
        self.harvest()
        mine = Item.objects.get()
        # Somebody else's rows of the same old run: no Provenance, so not the Mirror's.
        theirs = Item.objects.create(
            collection=self.feed.collection, time=self.run + hours(3), reference_time=self.run, bounds=GLOBE
        )
        Asset.objects.create(item=theirs, variable=self.temperature, format=Asset.Format.ZARR, href=STORE)
        beside = Asset.objects.create(
            item=mine, variable=self.temperature, format=Asset.Format.COG, href="org/gfs/x/t/2026/01/01/t.tif"
        )

        with mock.patch("django.utils.timezone.now", return_value=self.run + timedelta(days=4)):
            result = self.harvest()

        self.assertEqual(result.removed, 1)
        left = sorted([Asset.objects.get(item=theirs).pk, beside.pk])
        self.assertEqual(list(Asset.objects.order_by("pk").values_list("pk", flat=True)), left)
        # An item of the feed's goes only once nothing at all is left on it.
        self.assertTrue(Item.objects.filter(pk=mine.pk).exists())

    def test_a_mapped_array_the_collection_no_longer_describes_stops_the_run(self):
        self.map_variable("wind_u_10m", "wind-u-10m")
        self.hold("temperature_2m", 0)
        self.hold("wind_u_10m", 0)
        self.harvest()
        arrays = {path: said for path, said in self.arrays.items() if path != "wind_u_10m"}
        self.upstream.become_store(STORE, self.dimensions, arrays, media_type=STORE_TYPE)

        self.harvest()

        # Believed, it would cost the Variable every row it has.
        self.assertEqual(FetchRun.objects.latest("pk").status, FetchRun.Status.FAILED)
        self.assertEqual(Asset.objects.count(), 2)

    def test_a_variable_no_longer_mapped_keeps_its_rows_until_their_run_ages_out(self):
        wind = self.map_variable("wind_u_10m", "wind-u-10m")
        self.hold("temperature_2m", 0)
        self.hold("wind_u_10m", 0, 3)
        self.harvest()
        VariableMapping.objects.filter(variable=wind).delete()

        result = self.harvest()

        self.assertEqual(result.removed, 0)
        self.assertEqual(self.steps(wind), [(self.run, 0), (self.run, 3)])

        with mock.patch("django.utils.timezone.now", return_value=self.run + timedelta(days=4)):
            self.harvest()

        self.assertEqual(self.steps(wind), [])

    def test_a_store_published_at_a_new_address_is_followed_once_the_feed_names_it(self):
        self.hold("temperature_2m", 0, 3)
        self.harvest()
        moved = STORE.replace("v0.1.0", "v0.2.0")
        HarvestFeed.objects.filter(pk=self.feed.pk).update(store_href=moved)
        self.feed.refresh_from_db()
        self.upstream.become_store(moved, self.dimensions, self.arrays, media_type=STORE_TYPE)

        result = self.harvest()

        self.assertEqual(set(Asset.objects.values_list("href", flat=True)), {moved})
        self.assertEqual((result.indexed, result.updated, result.removed), (0, 2, 0))

    def test_an_unreachable_store_is_a_failed_run_and_changes_no_row(self):
        self.hold("temperature_2m", 0, 3)
        self.harvest()
        before = list(Asset.objects.order_by("pk").values_list("pk", "href", "modified"))

        self.upstream.store_unreachable = "it did not answer within 300 seconds"
        self.harvest()

        self.assertEqual(list(Asset.objects.order_by("pk").values_list("pk", "href", "modified")), before)
        self.assertEqual(Item.objects.count(), 2)
        failed = FetchRun.objects.latest("pk")
        self.assertEqual(failed.status, FetchRun.Status.FAILED)
        self.assertIsNotNone(failed.retry_due_at)

    def test_a_collection_that_no_longer_describes_its_store_stops_the_run(self):
        self.hold("temperature_2m", 0)
        self.harvest()
        self.upstream.become_store(STORE, self.dimensions, self.arrays, described=False)

        self.harvest()

        self.assertEqual(FetchRun.objects.latest("pk").status, FetchRun.Status.FAILED)
        self.assertEqual(Asset.objects.count(), 1)


class AnalysisStoreHarvestTests(StoreFeedMixin, TestCase):
    """A store with no runs: one time axis, and the fixed window."""

    arrays = {"temperature_2m": (TIME_GRID, (1, 721, 1440))}
    dimensions = ANALYSIS
    feed_fields = {
        "window_start": datetime(2026, 3, 2, tzinfo=UTC),
        "window_end": datetime(2026, 3, 4, tzinfo=UTC),
    }

    def test_it_harvests_with_no_reference_time_inside_the_fixed_window(self):
        for n in (1, 2, 3, 5):
            self.upstream.hold("temperature_2m", datetime(2026, 3, n, tzinfo=UTC))

        result = self.harvest()

        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.COMPLETED, result.errors)
        self.assertEqual(
            list(Item.objects.order_by("time").values_list("time", "reference_time")),
            [(datetime(2026, 3, 2, tzinfo=UTC), None), (datetime(2026, 3, 3, tzinfo=UTC), None)],
        )
        self.assertEqual(self.upstream.steps_asked[0][2], (self.feed.window_start, self.feed.window_end))
        asset = Asset.objects.first()
        self.assertEqual((asset.href, asset.format, asset.store_array), (STORE, Asset.Format.ZARR, "temperature_2m"))

    def test_a_time_no_longer_held_loses_its_row_and_one_outside_the_window_is_left(self):
        for n in (2, 3):
            self.upstream.hold("temperature_2m", datetime(2026, 3, n, tzinfo=UTC))
        self.harvest()
        HarvestFeed.objects.filter(pk=self.feed.pk).update(window_start=datetime(2026, 3, 3, tzinfo=UTC))
        self.feed.refresh_from_db()
        self.upstream.release("temperature_2m", datetime(2026, 3, 3, tzinfo=UTC))

        result = self.harvest()

        # The 3rd is gone upstream; the 2nd is outside a fixed window, and so not the Mirror's to judge.
        self.assertEqual(result.removed, 1)
        self.assertEqual(list(Item.objects.values_list("time", flat=True)), [datetime(2026, 3, 2, tzinfo=UTC)])


class StoreHarvestTaskTests(StoreFeedMixin, TestCase):
    def test_the_harvest_task_harvests_a_store_feed(self):
        self.hold("temperature_2m", 0)

        with mock.patch.object(HarvestFeed, "upstream_client", return_value=self.upstream):
            outcome = run_harvest(self.feed.pk)

        self.assertEqual(FetchRun.objects.get().status, FetchRun.Status.COMPLETED)
        self.assertEqual(outcome["indexed"], 1)

    def test_it_runs_on_the_default_queue(self):
        self.assertEqual(run_harvest.queue, "georiva-default")

    def test_run_now_enqueues_the_harvest(self):
        with mock.patch.object(run_harvest, "delay") as delay:
            self.feed.run_now()

        delay.assert_called_once_with(self.feed.pk, scheduled=False)


class StoreReadPlaneTests(StoreFeedMixin, TestCase):
    def setUp(self):
        super().setUp()
        dial_org(self.client, self.organisation.slug)
        self.hold("temperature_2m", 0, 3)
        with mock.patch("celery.app.task.Task.apply_async") as self.dispatched:
            self.harvest()
        self.collection = self.feed.collection
        Collection.objects.filter(pk=self.collection.pk).update(visibility=Collection.Visibility.PUBLIC)

    def stac_items(self):
        url = reverse("stac:items", args=["gfs", self.collection.slug, "temperature-2m"])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()["features"]

    def test_stac_gives_the_store_its_media_type_and_the_array_on_each_asset(self):
        features = self.stac_items()

        self.assertEqual(len(features), 2)
        for feature in features:
            asset = feature["assets"]["temperature-2m_zarr"]
            self.assertEqual(asset["href"], STORE)
            self.assertEqual(asset["type"], STORE_TYPE)
            self.assertEqual(asset["georiva:array"], "temperature_2m")

    def test_stac_gives_each_item_its_valid_time_and_its_run(self):
        found = {
            (
                datetime.fromisoformat(feature["properties"]["forecast:reference_datetime"]),
                datetime.fromisoformat(feature["properties"]["datetime"].replace("Z", "+00:00")),
            )
            for feature in self.stac_items()
        }

        self.assertEqual(found, {(self.run, self.run), (self.run, self.run + hours(3))})

    def test_edr_lists_the_collection(self):
        response = self.client.get(reverse("edr:collection-list"))

        self.assertEqual(response.status_code, 200)
        self.assertIn(self.collection.slug, response.content.decode())

    def test_no_zonal_statistics_and_no_virtual_zarr_manifest_follow_a_harvest(self):
        from adminboundarymanager.models import AdminBoundary
        from django.contrib.gis.geos import MultiPolygon, Polygon

        from georiva.analysis.zonal_stats.tasks import sweep_missing_boundary_stats

        # Everything the backfill asks of an asset but its kind: levels to compute, and age enough.
        AdminBoundary.objects.create(
            name_0="Testland", gid_0="TST", level=1, geom=MultiPolygon(Polygon.from_bbox((-10, -10, 10, 10)))
        )
        Collection.objects.filter(pk=self.collection.pk).update(boundary_stats_levels=[1])
        Asset.objects.update(created=timezone.now() - timedelta(hours=1))

        with mock.patch("celery.app.task.Task.apply_async") as swept:
            sweep_missing_boundary_stats()

        self.assertFalse(self.dispatched.called)
        self.assertFalse(swept.called)
        self.assertFalse(VirtualZarrManifest.objects.exists())

    def test_a_time_series_request_says_it_is_not_available_yet(self):
        variable = f"gfs/{self.collection.slug}/temperature-2m"
        point = self.client.get(reverse("timeseries:point"), {"variable": variable, "lat": 0.5, "lon": 36.5})
        area = self.client.post(
            reverse("timeseries:area"),
            {
                "variable": variable,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[34.0, -1.0], [36.0, -1.0], [36.0, 1.0], [34.0, 1.0], [34.0, -1.0]]],
                },
            },
            content_type="application/json",
        )

        for response in (point, area):
            self.assertEqual(response.status_code, 501, response.content)
            self.assertIn("not available for this collection yet", response.json()["detail"])
            self.assertNotIn("error", response.json())

    def test_tile_config_answers_with_the_store_to_read_and_no_file_address(self):
        url = reverse("tile_config", args=[self.organisation.slug, "gfs", self.collection.slug, "temperature-2m"])
        stamp = self.run.strftime("%Y-%m-%dT%H:%M:%SZ")

        response = self.client.get(f"{url}?time={stamp}&reftime={stamp}", HTTP_HOST=TITILER_DIALS)

        self.assertEqual(response.status_code, 200, response.content)
        self.assertNotIn("remote_href", response.json())
        self.assertEqual(
            response.json()["remote_store"], {"store": STORE, "array": "temperature_2m", "chunk_locations": {}}
        )


class StoreCollectionInWMTSCapabilitiesTests(IsolatedCapabilitiesCache, StoreFeedMixin, TestCase):
    """A store-backed collection is a layer like any other: its rows decide its times."""

    def setUp(self):
        super().setUp()
        dial_org(self.client, self.organisation.slug)
        self.hold("temperature_2m", 0, 3)
        self.harvest()
        self.collection = self.feed.collection

    def layer(self):
        response = self.client.get(reverse("wmts:capabilities", args=[self.organisation.slug]))
        self.assertEqual(response.status_code, 200)
        layers = {
            layer.findtext("ows:Identifier", namespaces=NS): layer
            for layer in ET.fromstring(response.content).findall("wmts:Contents/wmts:Layer", NS)
        }
        name = f"gfs:{self.collection.slug}:temperature-2m"
        self.assertIn(name, layers)
        return layers[name]

    def test_the_collection_is_listed_and_its_tiles_are_addressed_at_the_collection(self):
        template = self.layer().find("wmts:ResourceURL", NS).get("template")

        self.assertIn(f"/titiler/{self.organisation.slug}/gfs/{self.collection.slug}/temperature-2m/", template)
        self.assertNotIn("dynamical", template)


class StoreAssetsArePublishedForTitilerTests(StoreFeedMixin, TestCase):
    """What Titiler reads to draw a Store asset (ADR 0044 §9): the store, the
    array, and the feed's Pinned locations for its data, per timestep, in the
    same hash a Remote COG's href is published in, for a collection of any tier."""

    PINS = {"s3://noaa-gfs-bdp-pds/": "us-east-1", "gs://ecmwf-open-data/": ""}

    def setUp(self):
        super().setUp()
        self.redis = get_redis_connection("default")
        self._sweep_redis()
        self.addCleanup(self._sweep_redis)
        HarvestFeed.objects.filter(pk=self.feed.pk).update(pinned_chunk_locations=self.PINS)
        self.feed.refresh_from_db()
        self.hold("temperature_2m", 0, 3)
        self.harvest()
        self.collection = self.feed.collection

    def _sweep_redis(self):
        keys = list(self.redis.scan_iter(match=f"{REMOTE_ASSETS_KEY_PREFIX}:*"))
        if keys:
            self.redis.delete(*keys)

    def published(self, variable=None) -> dict:
        fields = {
            k.decode(): v.decode() for k, v in self.redis.hgetall(variable_key(variable or self.temperature)).items()
        }
        self.assertEqual(fields.pop(PUBLISHED_FIELD, None), "1", "the hash is Django's whole word")
        return {field: json.loads(value) for field, value in fields.items()}

    def field(self, lead) -> str:
        return timestep_field(self.run + hours(lead), self.run)

    def entry(self, array="temperature_2m", pins=None):
        return {"store": STORE, "array": array, "chunk_locations": self.PINS if pins is None else pins}

    def tile_config(self, lead, variable="temperature-2m"):
        url = reverse("tile_config", args=[self.organisation.slug, "gfs", self.collection.slug, variable])
        stamp = (self.run + hours(lead)).strftime("%Y-%m-%dT%H:%M:%SZ")
        reftime = self.run.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self.client.get(f"{url}?time={stamp}&reftime={reftime}", HTTP_HOST=TITILER_DIALS)

    def test_a_harvest_publishes_each_held_step_as_the_store_its_array_and_the_pins(self):
        self.assertEqual(self.published(), {self.field(0): self.entry(), self.field(3): self.entry()})

    def test_each_variable_names_its_own_array(self):
        wind = self.map_variable("wind_u_10m", "wind-u-10m")
        self.hold("wind_u_10m", 3)

        self.harvest()

        self.assertEqual(self.published(wind), {self.field(3): self.entry("wind_u_10m")})

    def test_a_private_collection_is_published_too(self):
        Collection.objects.filter(pk=self.collection.pk).update(visibility=Collection.Visibility.PRIVATE)
        self._sweep_redis()

        warm_all()

        self.assertEqual(self.published(), {self.field(0): self.entry(), self.field(3): self.entry()})

    def test_a_step_no_longer_held_is_withdrawn(self):
        self.upstream.release("temperature_2m", self.run + hours(3), run=self.run)

        self.harvest()

        self.assertEqual(set(self.published()), {self.field(0)})

    def test_pins_confirmed_later_reach_the_tile_server_with_the_next_harvest(self):
        HarvestFeed.objects.filter(pk=self.feed.pk).update(pinned_chunk_locations={"gs://ecmwf-open-data/": ""})

        self.harvest()

        self.assertEqual(self.published()[self.field(0)]["chunk_locations"], {"gs://ecmwf-open-data/": ""})

    def test_a_store_asset_saved_alone_is_published_at_once(self):
        Asset.objects.filter(variable=self.temperature).delete()
        self._sweep_redis()
        self.redis.hset(variable_key(self.temperature), PUBLISHED_FIELD, "1")
        item = Item.objects.get(collection=self.collection, time=self.run)

        Asset.objects.create(
            item=item,
            variable=self.temperature,
            format=Asset.Format.ZARR,
            href=STORE,
            store_array="temperature_2m",
            harvested_by=self.feed,
            roles=[Asset.Role.DATA],
        )

        self.assertEqual(self.published(), {self.field(0): self.entry()})

    def test_the_rows_of_a_deleted_feed_are_published_with_nowhere_to_follow_their_data(self):
        self._sweep_redis()
        HarvestFeed.objects.filter(pk=self.feed.pk).delete()

        warm_all()

        self.assertEqual(self.published()[self.field(0)], self.entry(pins={}))

    def test_tile_config_answers_a_public_collections_store_on_a_miss(self):
        Collection.objects.filter(pk=self.collection.pk).update(visibility=Collection.Visibility.PUBLIC)

        response = self.tile_config(3)

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["remote_store"], self.entry())
        self.assertNotIn("remote_href", response.json())

    def test_tile_config_answers_no_store_at_a_step_with_no_row(self):
        Collection.objects.filter(pk=self.collection.pk).update(visibility=Collection.Visibility.PUBLIC)

        self.assertNotIn("remote_store", self.tile_config(6).json())

    def test_tile_config_still_answers_nothing_for_a_private_collection(self):
        Collection.objects.filter(pk=self.collection.pk).update(visibility=Collection.Visibility.PRIVATE)

        self.assertEqual(self.tile_config(3).status_code, 404)
