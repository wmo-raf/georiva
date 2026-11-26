"""
The real Upstream catalog client reads an anonymous STAC API, or a catalog of
plain files, and nothing else.

The HTTP session is canned: these tests are about what the client asks for,
what it makes of the answers, and where it refuses to go. No test reaches a
network.
"""

import threading
import time
from datetime import UTC, datetime

import requests
import urllib3
from django.test import SimpleTestCase

from georiva.harvest.upstream import (
    CubeArray,
    CubeDimension,
    StacApiClient,
    UpstreamStatistics,
    UpstreamTimedOut,
    UpstreamUnreachable,
    parse_asset,
    parse_collection,
    parse_item,
)

API = "https://stac.upstream.example/api"


class _Response:
    def __init__(self, payload, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _Session:
    """Answers each URL from a table, and remembers what it was asked."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        answer = self.pages[url]
        if isinstance(answer, Exception):
            raise answer
        return answer if isinstance(answer, _Response) else _Response(answer)


def redirect(to, status_code=302):
    return _Response({}, status_code=status_code, headers={"Location": to})


def feature(item_id, when, **assets):
    return {"id": item_id, "bbox": [33, -5, 42, 5], "properties": {"datetime": when}, "assets": assets}


class ParseTests(SimpleTestCase):
    def test_an_item_is_read_with_its_time_extent_and_assets(self):
        item = parse_item(
            {
                "id": "chirps-2026-03-01",
                "bbox": [33, -5, 42, 5],
                "geometry": {"type": "Point", "coordinates": [36, 0]},
                "properties": {"datetime": "2026-03-01T00:00:00Z", "updated": "2026-03-02T06:00:00+00:00"},
                "assets": {"rainfall": {"href": "https://data.example/a.tif", "type": "image/tiff"}},
            }
        )

        self.assertEqual(item.id, "chirps-2026-03-01")
        self.assertEqual(item.datetime, datetime(2026, 3, 1, tzinfo=UTC))
        self.assertEqual(item.updated, datetime(2026, 3, 2, 6, tzinfo=UTC))
        self.assertEqual(item.bbox, [33.0, -5.0, 42.0, 5.0])
        self.assertEqual(item.geometry["type"], "Point")
        self.assertEqual(item.assets["rainfall"].href, "https://data.example/a.tif")

    def test_an_item_covering_a_period_takes_its_start(self):
        item = parse_item({"id": "x", "properties": {"datetime": None, "start_datetime": "2026-03-01T00:00:00Z"}})

        self.assertEqual(item.datetime, datetime(2026, 3, 1, tzinfo=UTC))

    def test_an_item_with_no_usable_time_has_none(self):
        self.assertIsNone(parse_item({"id": "x", "properties": {"datetime": "yesterday"}}).datetime)

    def test_a_3d_bbox_loses_its_elevation(self):
        self.assertEqual(parse_item({"id": "x", "bbox": [33, -5, 0, 42, 5, 100]}).bbox, [33.0, -5.0, 42.0, 5.0])

    def test_statistics_are_lifted_from_raster_bands(self):
        asset = parse_asset(
            {
                "href": "https://data.example/a.tif",
                "file:size": 2048,
                "file:checksum": "1220abcd",
                "raster:bands": [
                    {"nodata": -9999, "statistics": {"minimum": 0, "maximum": 87.5, "mean": 3.2, "stddev": 6.1}}
                ],
            }
        )

        self.assertEqual(asset.statistics, UpstreamStatistics(minimum=0.0, maximum=87.5, mean=3.2, stddev=6.1))
        self.assertEqual((asset.nodata, asset.bands, asset.file_size, asset.checksum), (-9999.0, 1, 2048, "1220abcd"))

    def test_statistics_are_lifted_from_the_common_bands_field_too(self):
        asset = parse_asset({"href": "a.tif", "bands": [{"statistics": {"minimum": 1, "maximum": 2}}]})

        self.assertEqual(asset.statistics, UpstreamStatistics(minimum=1.0, maximum=2.0))

    def test_an_asset_without_band_metadata_has_no_statistics(self):
        asset = parse_asset({"href": "https://data.example/a.tif"})

        self.assertIsNone(asset.statistics)
        self.assertIsNone(asset.nodata)

    def test_statistics_that_are_not_numbers_are_left_out(self):
        asset = parse_asset({"href": "a.tif", "raster:bands": [{"nodata": "nan", "statistics": {"minimum": "low"}}]})

        self.assertEqual(asset.statistics, UpstreamStatistics())
        self.assertIsNone(asset.nodata)

    def test_an_asset_with_no_href_is_not_given_the_pages(self):
        self.assertEqual(parse_asset({"type": "image/tiff"}, base_url=f"{API}/collections/c/items").href, "")

    def test_a_relative_href_is_resolved_against_the_page_it_came_from(self):
        asset = parse_asset({"href": "../data/a.tif"}, base_url=f"{API}/collections/c/items")

        self.assertEqual(asset.href, f"{API}/collections/data/a.tif")


class StacApiClientTests(SimpleTestCase):
    def client_over(self, pages):
        self.session = _Session(pages)
        return StacApiClient(f"{API}/", session=self.session, page_size=2)

    def test_collections_are_listed_across_pages(self):
        client = self.client_over(
            {
                f"{API}/collections": {
                    "collections": [{"id": "a", "title": "A", "extent": {"spatial": {"bbox": [[1, 2, 3, 4]]}}}],
                    "links": [{"rel": "next", "href": f"{API}/collections?page=2"}],
                },
                f"{API}/collections?page=2": {"collections": [{"id": "b", "description": "Second"}]},
            }
        )

        collections = list(client.list_collections())

        self.assertEqual([(c.id, c.title, c.description) for c in collections], [("a", "A", ""), ("b", "", "Second")])
        self.assertEqual(collections[0].bbox, [1.0, 2.0, 3.0, 4.0])

    def test_items_are_asked_for_by_window_and_followed_across_pages(self):
        items_url = f"{API}/collections/chirps/items"
        client = self.client_over(
            {
                items_url: {
                    "features": [feature("one", "2026-03-01T00:00:00Z")],
                    "links": [{"rel": "next", "href": "items?token=abc"}],
                },
                f"{items_url}?token=abc": {"features": [feature("two", "2026-03-02T00:00:00Z")]},
            }
        )

        items = list(client.list_items("chirps", (datetime(2026, 3, 1, tzinfo=UTC), None)))

        self.assertEqual([item.id for item in items], ["one", "two"])
        first_url, first = self.session.calls[0]
        self.assertEqual(first_url, items_url)
        self.assertEqual(first["params"], {"limit": 2, "datetime": "2026-03-01T00:00:00Z/.."})
        # The next link carries its own query; ours is not sent again.
        self.assertIsNone(self.session.calls[1][1]["params"])

    def test_an_unbounded_window_asks_for_no_datetime(self):
        client = self.client_over({f"{API}/collections/chirps/items": {"features": []}})

        list(client.list_items("chirps", (None, None)))

        self.assertEqual(self.session.calls[0][1]["params"], {"limit": 2})

    def test_the_session_is_never_left_to_follow_a_redirect_itself(self):
        client = self.client_over({f"{API}/collections": {"collections": []}})

        list(client.list_collections())

        self.assertIs(self.session.calls[0][1]["allow_redirects"], False)

    def test_a_redirect_to_the_catalogs_own_host_is_followed(self):
        # Digital Earth Africa answers an items listing with a redirect to its search.
        search = f"{API}/search?collection=chirps&limit=2&datetime=2026-03-01T00:00:00Z/.."
        client = self.client_over(
            {
                f"{API}/collections/chirps/items": redirect(search),
                search: {"features": [feature("a", "2026-03-01T00:00:00Z")]},
            }
        )

        items = list(client.list_items("chirps", (datetime(2026, 3, 1, tzinfo=UTC), None)))

        self.assertEqual([item.id for item in items], ["a"])
        # The redirect's own address carries the window; ours is not sent a second time.
        self.assertIsNone(self.session.calls[1][1]["params"])

    def test_a_listing_pages_on_from_where_a_redirect_landed(self):
        search = f"{API}/search?collection=chirps"
        client = self.client_over(
            {
                f"{API}/collections/chirps/items": redirect("/api/search?collection=chirps", status_code=307),
                search: {
                    "features": [feature("a", "2026-03-01T00:00:00Z", rainfall={"href": "a.tif"})],
                    "links": [{"rel": "next", "href": "?collection=chirps&page=2"}],
                },
                f"{API}/search?collection=chirps&page=2": {"features": [feature("b", "2026-03-02T00:00:00Z")]},
            }
        )

        items = list(client.list_items("chirps", (None, None)))

        self.assertEqual([item.id for item in items], ["a", "b"])
        self.assertEqual(items[0].assets["rainfall"].href, f"{API}/a.tif")

    def test_a_redirect_to_another_host_scheme_or_port_is_refused(self):
        elsewhere = (
            "http://169.254.169.254/latest/meta-data",
            "https://other.example/api/collections",
            "http://stac.upstream.example/api/collections",
            "https://stac.upstream.example:8443/api/collections",
        )
        for target in elsewhere:
            with self.subTest(target=target):
                client = self.client_over({f"{API}/collections": redirect(target)})

                with self.assertRaisesMessage(UpstreamUnreachable, target):
                    list(client.list_collections())
                self.assertEqual(len(self.session.calls), 1)

    def test_a_redirect_that_names_no_target_is_unreachable(self):
        client = self.client_over({f"{API}/collections": _Response({}, status_code=302)})

        with self.assertRaisesMessage(UpstreamUnreachable, "302"):
            list(client.list_collections())

    def test_a_chain_of_redirects_is_followed_only_so_far(self):
        hops = {f"{API}/hop/{n}": redirect(f"{API}/hop/{n + 1}") for n in range(20)}
        client = self.client_over({f"{API}/collections": redirect(f"{API}/hop/0"), **hops})

        with self.assertRaisesMessage(UpstreamUnreachable, "redirect"):
            list(client.list_collections())
        self.assertLessEqual(len(self.session.calls), 6)

    def test_a_redirect_back_to_where_it_came_from_is_refused(self):
        client = self.client_over({f"{API}/collections": redirect(f"{API}/collections")})

        with self.assertRaisesMessage(UpstreamUnreachable, "redirect"):
            list(client.list_collections())

    def test_a_next_page_on_another_host_is_refused(self):
        client = self.client_over(
            {
                f"{API}/collections": {
                    "collections": [],
                    "links": [{"rel": "next", "href": "http://169.254.169.254/latest/meta-data"}],
                }
            }
        )

        with self.assertRaisesMessage(UpstreamUnreachable, "another host"):
            list(client.list_collections())
        self.assertEqual(len(self.session.calls), 1)

    def test_a_next_page_that_loops_is_refused(self):
        client = self.client_over(
            {f"{API}/collections": {"collections": [], "links": [{"rel": "next", "href": f"{API}/collections"}]}}
        )

        with self.assertRaisesMessage(UpstreamUnreachable, "loop"):
            list(client.list_collections())

    def test_every_way_an_upstream_fails_to_answer_is_unreachable(self):
        failures = {
            "a connection error": requests.ConnectionError("refused"),
            "a timeout": requests.Timeout("timed out"),
            "a server error": _Response({}, status_code=503),
            "a credential demand": _Response({}, status_code=401),
            "not JSON": _Response(ValueError("no json")),
            "JSON that is not an object": _Response(["nope"]),
        }
        for what, answer in failures.items():
            with self.subTest(what):
                client = self.client_over({f"{API}/collections": answer})
                with self.assertRaises(UpstreamUnreachable):
                    list(client.list_collections())

    def test_a_catalog_that_does_not_answer_in_time_says_so_apart_from_one_that_refuses(self):
        client = self.client_over({f"{API}/collections": requests.Timeout("read timed out")})

        with self.assertRaises(UpstreamTimedOut) as raised:
            list(client.list_collections())

        self.assertIn("did not answer within 30 seconds", str(raised.exception))
        for refusal in (requests.ConnectionError("refused"), _Response({}, status_code=401)):
            with self.subTest(refusal), self.assertRaises(UpstreamUnreachable) as raised:
                list(self.client_over({f"{API}/collections": refusal}).list_collections())
            self.assertNotIsInstance(raised.exception, UpstreamTimedOut)


STATIC = "https://stac.upstream.example/catalog.json"
GFS = "https://stac.upstream.example/noaa-gfs-forecast-virtual/collection.json"
CHIRPS = "https://stac.upstream.example/chirps.json"
STORE = "https://dynamical-noaa-gfs.s3.us-west-2.amazonaws.com/noaa-gfs-forecast-virtual/v0.1.0.icechunk"

#: A collection as dynamical.org publishes a virtual store (shortened): no items, one cube.
GFS_COLLECTION = {
    "type": "Collection",
    "stac_version": "1.1.0",
    "id": "noaa-gfs-forecast-virtual",
    "title": "NOAA GFS forecast (virtual)",
    "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}},
    "assets": {
        "icechunk": {
            "href": "s3://dynamical-noaa-gfs/noaa-gfs-forecast-virtual/v0.1.0.icechunk/",
            "type": "application/x-icechunk",
            "xarray:storage_options": {"anon": True},
            "icechunk:virtual_chunk_containers": [
                {"url_prefix": "s3://noaa-gfs-bdp-pds/", "credentials": {"type": "s3", "anonymous": True}}
            ],
        },
        "icechunk-https": {"href": STORE, "type": "application/x-icechunk"},
    },
    "cube:dimensions": {
        "init_time": {"type": "temporal", "extent": ["2021-05-01T00:00:00Z", None]},
        "lead_time": {"type": "temporal", "unit": "seconds", "size": 209},
        "latitude": {"type": "spatial", "axis": "y", "extent": [-90, 90], "size": 721},
        "longitude": {"type": "spatial", "axis": "x", "extent": [-180, 179.75], "size": 1440},
    },
    "cube:variables": {
        "temperature_2m": {
            "type": "data",
            "dimensions": ["init_time", "lead_time", "latitude", "longitude"],
            "chunks": [1, 1, 721, 1440],
            "unit": "degree_Celsius",
            "long_name": "2 metre temperature",
        },
        "valid_time": {"type": "auxiliary", "dimensions": ["init_time", "lead_time"]},
    },
}


class PlainFilesCatalogTests(SimpleTestCase):
    """A catalog with no API: its collections are what its root links to (ADR 0044 §1)."""

    def client_over(self, pages, url=STATIC):
        self.session = _Session(pages)
        return StacApiClient(url, session=self.session)

    def catalog(self, **more):
        return {
            STATIC: {
                "type": "Catalog",
                "id": "dynamical",
                "links": [
                    {"rel": "self", "href": STATIC},
                    {"rel": "child", "href": "./noaa-gfs-forecast-virtual/collection.json", "title": "GFS"},
                    {"rel": "child", "href": "./chirps.json"},
                ],
            },
            GFS: GFS_COLLECTION,
            CHIRPS: {"type": "Collection", "id": "chirps", "title": "CHIRPS"},
            **more,
        }

    def asked(self):
        return [url for url, _ in self.session.calls]

    def test_its_collections_are_listed_from_the_roots_links_each_described_by_its_own_document(self):
        client = self.client_over(self.catalog())

        collections = list(client.list_collections())

        self.assertEqual(
            [(c.id, c.title) for c in collections],
            [("noaa-gfs-forecast-virtual", "NOAA GFS forecast (virtual)"), ("chirps", "CHIRPS")],
        )
        self.assertEqual(collections[0].bbox, [-180.0, -90.0, 180.0, 90.0])
        asked = self.asked()
        self.assertEqual(asked[0], STATIC, "the root is read first, and once")
        self.assertEqual(sorted(asked[1:]), sorted([GFS, CHIRPS]), "no listing an API would have is asked for")

    def test_it_is_listed_the_same_once_its_root_has_been_read_whatever_its_address(self):
        # Served at a folder: nothing in the address says "file" until the root is read.
        folder = "https://stac.upstream.example/catalog"
        pages = self.catalog()
        pages[folder] = pages.pop(STATIC)
        client = self.client_over(pages, url=folder)

        self.assertFalse(client.describe_root().is_api)

        self.assertEqual([c.id for c in client.list_collections()], ["noaa-gfs-forecast-virtual", "chirps"])

    def test_a_collection_that_will_not_describe_itself_is_still_offered_by_its_link(self):
        client = self.client_over(self.catalog(**{GFS: _Response({}, status_code=404)}))

        collections = list(client.list_collections())

        offered = [(c.id, c.title) for c in collections]
        self.assertEqual(offered, [("noaa-gfs-forecast-virtual", "GFS"), ("chirps", "CHIRPS")])

    def test_a_collection_keeps_the_id_it_was_offered_by(self):
        pages = self.catalog()
        pages[CHIRPS] = {"type": "Collection", "id": "CHIRPS-v2", "title": "CHIRPS"}
        client = self.client_over(pages)

        self.assertEqual([c.id for c in client.list_collections()][1], "chirps")
        self.assertEqual(client.describe_collection("chirps").id, "chirps")

    def test_a_link_to_another_host_is_never_followed(self):
        pages = self.catalog()
        pages[STATIC]["links"].append({"rel": "child", "href": "http://169.254.169.254/latest/collection.json"})
        client = self.client_over(pages)

        collections = list(client.list_collections())

        self.assertEqual(collections[-1].id, "latest")
        self.assertNotIn("http://169.254.169.254/latest/collection.json", self.asked())

    def test_a_link_that_leads_to_another_catalog_is_not_taken_for_a_collection(self):
        pages = self.catalog()
        pages[CHIRPS] = {"type": "Catalog", "id": "chirps", "links": []}
        client = self.client_over(pages)

        self.assertIsNone(client.describe_collection("chirps"))

    def test_an_address_that_is_one_collections_own_document_lists_that_collection(self):
        client = self.client_over({GFS: GFS_COLLECTION}, url=GFS)

        self.assertEqual([c.id for c in client.list_collections()], ["noaa-gfs-forecast-virtual"])

    def test_one_collection_is_described_with_its_store_and_its_cube(self):
        client = self.client_over(self.catalog())

        described = client.describe_collection("noaa-gfs-forecast-virtual")

        self.assertEqual(described.assets["icechunk-https"].href, STORE)
        self.assertEqual(described.assets["icechunk"].chunk_locations, ("s3://noaa-gfs-bdp-pds/",))
        self.assertEqual(described.assets["icechunk-https"].chunk_locations, ())
        self.assertEqual(
            described.cube.dimensions["latitude"], CubeDimension("latitude", kind="spatial", axis="y", size=721)
        )
        self.assertIsNone(described.cube.dimensions["init_time"].size)
        self.assertEqual(
            described.cube.arrays["temperature_2m"],
            CubeArray(
                "temperature_2m",
                dimensions=("init_time", "lead_time", "latitude", "longitude"),
                chunks=(1, 1, 721, 1440),
                title="2 metre temperature",
                unit="degree_Celsius",
            ),
        )
        self.assertIsNone(described.cube.arrays["valid_time"].chunks)

    def test_a_stores_address_given_relative_to_its_collection_is_resolved(self):
        pages = self.catalog()
        store = {"href": "./v1.icechunk", "type": "application/x-icechunk"}
        pages[GFS] = {**GFS_COLLECTION, "assets": {"store": store}}
        client = self.client_over(pages)

        described = client.describe_collection("noaa-gfs-forecast-virtual")

        self.assertEqual(
            described.assets["store"].href, "https://stac.upstream.example/noaa-gfs-forecast-virtual/v1.icechunk"
        )

    def test_a_collection_that_describes_no_cube_has_none(self):
        self.assertIsNone(parse_collection({"id": "chirps"}).cube)

    def test_a_cube_described_badly_is_kept_as_far_as_it_goes(self):
        cube = parse_collection(
            {
                "id": "x",
                "cube:dimensions": {"time": {"type": "temporal", "values": ["a", "b", "c"]}, "bad": "nonsense"},
                "cube:variables": {
                    "t": {"dimensions": ["time"], "chunks": [1, "many"]},
                    "u": {"dimensions": "time", "chunks": [True]},
                    "bad": 7,
                },
            }
        ).cube

        self.assertEqual(cube.dimensions.keys(), {"time"})
        self.assertEqual(cube.dimensions["time"].size, 3)
        self.assertEqual((cube.arrays["t"].dimensions, cube.arrays["t"].chunks), (("time",), None))
        self.assertEqual((cube.arrays["u"].dimensions, cube.arrays["u"].chunks), ((), None))

    def test_an_extra_axis_lists_its_values_where_the_collection_gives_them(self):
        # As dynamical.org describes its ensembles (#830).
        cube = parse_collection(
            {
                "id": "x",
                "cube:dimensions": {
                    "statistic": {"type": "other", "values": ["mean", "p10"], "unit": "1", "size": 2},
                    "ensemble_member": {"type": "other", "extent": [0, 50], "unit": "1", "size": 51},
                    "pressure_level": {"type": "other", "extent": [10, 1000], "unit": "hPa", "size": 14},
                    "level": {"type": "other", "extent": [100, 1000], "step": 300, "unit": "hPa"},
                    "member": {"type": "other", "values": [True, {"a": 1}]},
                    "latitude": {"type": "spatial", "axis": "y", "extent": [-90, 90], "size": 181},
                },
            }
        ).cube

        self.assertEqual(cube.dimensions["statistic"].values, ("mean", "p10"))
        self.assertEqual(cube.dimensions["ensemble_member"].values, tuple(range(51)))
        self.assertIsNone(cube.dimensions["pressure_level"].values)
        self.assertEqual(cube.dimensions["pressure_level"].unit, "hPa")
        self.assertEqual(cube.dimensions["level"].values, (100, 400, 700, 1000))
        self.assertIsNone(cube.dimensions["member"].values)
        self.assertIsNone(cube.dimensions["latitude"].values)

    def test_a_grid_dimension_keeps_its_unit(self):
        cube = parse_collection(
            {"id": "x", "cube:dimensions": {"x": {"type": "spatial", "axis": "x", "unit": "degree_east"}}}
        ).cube

        self.assertEqual(cube.dimensions["x"].unit, "degree_east")

    def test_a_vertical_axis_typed_spatial_lists_its_values_too(self):
        # The datacube extension types a vertical axis "spatial", axis "z" (#830).
        cube = parse_collection(
            {
                "id": "x",
                "cube:dimensions": {
                    "pressure_level": {"type": "spatial", "axis": "z", "values": [500, 850], "unit": "hPa"},
                },
            }
        ).cube

        self.assertEqual(cube.dimensions["pressure_level"].values, (500, 850))
        self.assertEqual(cube.dimensions["pressure_level"].unit, "hPa")

    def many(self, count):
        """A catalog of plain files linking to `count` collections, each with its own document."""
        hrefs = [f"https://stac.upstream.example/c{n}/collection.json" for n in range(count)]
        pages = {
            STATIC: {"type": "Catalog", "links": [{"rel": "child", "href": href} for href in hrefs]},
            **{href: {"type": "Collection", "id": f"c{n}", "title": f"Collection {n}"} for n, href in enumerate(hrefs)},
        }
        return pages, hrefs

    def test_collection_documents_are_read_several_at_a_time_and_no_more_than_the_bound(self):
        pages, _hrefs = self.many(47)
        lock, in_flight, most = threading.Lock(), [0], [0]

        class Overlapping(_Session):
            def get(self, url, **kwargs):
                with lock:
                    in_flight[0] += 1
                    most[0] = max(most[0], in_flight[0])
                try:
                    if url != STATIC:
                        time.sleep(0.02)
                    return super().get(url, **kwargs)
                finally:
                    with lock:
                        in_flight[0] -= 1

        self.session = Overlapping(pages)
        collections = list(StacApiClient(STATIC, session=self.session).list_collections())

        self.assertEqual([c.title for c in collections], [f"Collection {n}" for n in range(47)], "in the root's order")
        self.assertGreater(most[0], 1, "not one after another")
        self.assertLessEqual(most[0], StacApiClient.LISTING_CONCURRENCY)

    def test_one_slow_collection_does_not_hold_up_the_listing_past_its_bound(self):
        pages, hrefs = self.many(5)
        release = threading.Event()
        self.addCleanup(release.set)

        class OneSlow(_Session):
            def get(self, url, **kwargs):
                if url == hrefs[2]:
                    release.wait(30)
                return super().get(url, **kwargs)

        client = StacApiClient(STATIC, session=OneSlow(pages), listing_timeout=0.3)
        started = time.monotonic()
        collections = list(client.list_collections())

        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual([c.id for c in collections], [f"c{n}" for n in range(5)], "the slow one is still offered")
        self.assertEqual(collections[2].title, "", "by its id alone")
        self.assertEqual(collections[3].title, "Collection 3", "the others are described")

    def test_a_collection_document_that_breaks_the_reader_leaves_the_others_listed(self):
        pages, hrefs = self.many(3)
        pages[hrefs[1]] = _Response(RuntimeError("the reader broke"))
        client = self.client_over(pages)

        collections = list(client.list_collections())

        offered = [(c.id, c.title) for c in collections]
        self.assertEqual(offered, [("c0", "Collection 0"), ("c1", ""), ("c2", "Collection 2")])

    def test_one_client_reads_the_root_once(self):
        client = self.client_over(self.catalog())

        client.describe_root()
        list(client.list_collections())
        client.describe_collection("chirps")

        self.assertEqual(self.asked().count(STATIC), 1)

    def test_an_api_is_asked_for_its_collections_directly_as_before(self):
        api = "https://stac.upstream.example/api"
        client = self.client_over({f"{api}/collections": {"collections": [{"id": "a"}]}}, url=api)

        self.assertEqual([c.id for c in client.list_collections()], ["a"])
        self.assertEqual(self.asked(), [f"{api}/collections"])


class _Streamed:
    """What the pinned opener answers with: a status, headers, and a body read in chunks."""

    def __init__(self, status=200, headers=None, body=b"GTIFF-bytes"):
        self.status = status
        self.headers = headers or {}
        self.body = body
        self.closed = False

    def stream(self, amount):
        yield self.body

    def close(self):
        self.closed = True


class FetchAssetConnectsOnlyToJudgedAddressesTests(SimpleTestCase):
    """A materialization's bytes are read from the address the Asset host
    allowlist judged, never from a second lookup of the host (#723): a host
    that answers a public address to the check and a private one to the
    connection must not reach inside our network."""

    HREF = "https://data.upstream.example/chirps/3.tif"
    PUBLIC = "93.184.216.34"

    def setUp(self):
        import tempfile
        from pathlib import Path

        self.dest = Path(tempfile.mkdtemp()) / "asset.tif"
        self.opened: list[tuple[str, str]] = []
        self.answers: dict[str, _Streamed] = {}

        #: Addresses that refuse every connection, as an unreachable network would.
        self.unreachable: set[str] = set()

        def opener(url, address, *, timeout):
            self.opened.append((url, address))
            if address in self.unreachable:
                raise urllib3.exceptions.NewConnectionError(None, f"cannot reach {address}")
            answer = self.answers.get(url) or _Streamed()
            self.given.append(answer)
            return answer

        self.given: list[_Streamed] = []

        self.client = StacApiClient(API, opener=opener)

    def allowlist(self, answers):
        """The feed's allowlist over a resolver that gives each answer in turn: DNS that changes its mind."""
        from georiva.harvest.asset_hosts import AssetHostAllowlist

        self.lookups = []

        def resolve(host):
            self.lookups.append(host)
            return answers[min(len(self.lookups), len(answers)) - 1]

        hosts = {"data.upstream.example", "mirror.upstream.example"}
        return AssetHostAllowlist(hosts, allow_http=False, resolve=resolve)

    def fetch(self, allowlist):
        return self.client.fetch_asset(self.HREF, self.dest, addresses=allowlist.addresses)

    def test_it_connects_to_the_address_the_allowlist_judged(self):
        allowlist = self.allowlist([[self.PUBLIC]])
        self.assertIsNone(allowlist.refusal(self.HREF))

        written = self.fetch(allowlist)

        self.assertEqual(self.opened, [(self.HREF, self.PUBLIC)])
        self.assertEqual(self.dest.read_bytes(), b"GTIFF-bytes")
        self.assertEqual(written, len(b"GTIFF-bytes"))

    def test_a_host_that_turns_private_after_it_was_judged_is_still_reached_at_the_judged_address(self):
        allowlist = self.allowlist([[self.PUBLIC], ["169.254.169.254"]])
        self.assertIsNone(allowlist.refusal(self.HREF))

        self.fetch(allowlist)

        self.assertEqual(self.opened, [(self.HREF, self.PUBLIC)])
        self.assertEqual(self.lookups, ["data.upstream.example"])

    def test_a_host_judged_private_is_never_connected_to(self):
        allowlist = self.allowlist([["10.0.0.7"]])

        with self.assertRaises(UpstreamUnreachable):
            self.fetch(allowlist)

        self.assertEqual(self.opened, [])
        self.assertFalse(self.dest.exists())

    def test_a_redirect_is_reached_at_its_own_judged_address(self):
        target = "https://mirror.upstream.example/3.tif"
        self.answers[self.HREF] = _Streamed(status=302, headers={"Location": target})
        allowlist = self.allowlist([[self.PUBLIC], ["93.184.216.35"]])

        self.fetch(allowlist)

        self.assertEqual(self.opened, [(self.HREF, self.PUBLIC), (target, "93.184.216.35")])

    def test_a_redirect_to_a_host_not_allowed_is_not_connected_to(self):
        self.answers[self.HREF] = _Streamed(status=302, headers={"Location": "http://169.254.169.254/latest/meta-data"})

        with self.assertRaises(UpstreamUnreachable):
            self.fetch(self.allowlist([[self.PUBLIC]]))

        self.assertEqual(self.opened, [(self.HREF, self.PUBLIC)])
        self.assertFalse(self.dest.exists())

    def test_a_redirect_to_a_host_that_resolves_privately_is_not_connected_to(self):
        target = "https://mirror.upstream.example/3.tif"
        self.answers[self.HREF] = _Streamed(status=302, headers={"Location": target})

        with self.assertRaises(UpstreamUnreachable):
            self.fetch(self.allowlist([[self.PUBLIC], ["127.0.0.1"]]))

        self.assertEqual(self.opened, [(self.HREF, self.PUBLIC)])

    def test_a_host_with_ipv6_and_ipv4_addresses_is_tried_on_ipv4_first(self):
        allowlist = self.allowlist([["2606:4700::6810:84e5", self.PUBLIC]])

        self.fetch(allowlist)

        self.assertEqual(self.opened, [(self.HREF, self.PUBLIC)])

    def test_each_judged_address_is_tried_in_turn_until_one_answers(self):
        self.unreachable.add(self.PUBLIC)
        allowlist = self.allowlist([["2606:4700::6810:84e5", self.PUBLIC]])

        self.fetch(allowlist)

        self.assertEqual(self.opened, [(self.HREF, self.PUBLIC), (self.HREF, "2606:4700::6810:84e5")])
        self.assertEqual(self.dest.read_bytes(), b"GTIFF-bytes")

    def test_a_host_none_of_whose_addresses_answers_is_unreachable(self):
        self.unreachable |= {self.PUBLIC, "93.184.216.35"}

        with self.assertRaises(UpstreamUnreachable):
            self.fetch(self.allowlist([[self.PUBLIC, "93.184.216.35"]]))

        self.assertFalse(self.dest.exists())

    def test_every_connection_is_closed_after_a_redirect_a_failure_or_a_copy(self):
        target = "https://mirror.upstream.example/3.tif"
        self.answers[self.HREF] = _Streamed(status=302, headers={"Location": target})
        self.fetch(self.allowlist([[self.PUBLIC], ["93.184.216.35"]]))

        self.answers[self.HREF] = _Streamed(status=503)
        with self.assertRaises(UpstreamUnreachable):
            self.fetch(self.allowlist([[self.PUBLIC]]))

        self.assertEqual([answer.closed for answer in self.given], [True, True, True])

    def test_an_href_whose_port_is_not_a_port_is_refused_and_never_opened(self):
        href = "https://data.upstream.example:99999/3.tif"
        allowlist = self.allowlist([[self.PUBLIC]])

        self.assertIn("not a URL", allowlist.refusal(href))
        with self.assertRaises(UpstreamUnreachable):
            self.client.fetch_asset(href, self.dest, addresses=allowlist.addresses)
        self.assertEqual(self.opened, [])

    def test_an_answer_other_than_ok_leaves_nothing_behind(self):
        self.answers[self.HREF] = _Streamed(status=503)

        with self.assertRaises(UpstreamUnreachable):
            self.fetch(self.allowlist([[self.PUBLIC]]))

        self.assertFalse(self.dest.exists())


class PinnedOpenerTests(SimpleTestCase):
    """The opener itself: it connects to the address it is given, and names
    the href's host in everything the server and TLS see."""

    def test_it_connects_to_the_given_address_and_sends_the_hosts_name(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        from georiva.harvest.upstream import pinned_open

        seen = {}

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                seen["host"] = self.headers.get("Host")
                seen["path"] = self.path
                self.send_response(200)
                self.send_header("Content-Length", "5")
                self.end_headers()
                self.wfile.write(b"bytes")

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        port = server.server_address[1]

        response = pinned_open(f"http://data.upstream.example:{port}/chirps/3.tif?v=2", "127.0.0.1", timeout=5)

        self.assertEqual(response.status, 200)
        self.assertEqual(b"".join(response.stream(1024)), b"bytes")
        self.assertEqual(seen, {"host": f"data.upstream.example:{port}", "path": "/chirps/3.tif?v=2"})

    def test_tls_is_verified_against_the_hrefs_host_not_the_address(self):
        from unittest import mock

        from georiva.harvest import upstream

        with mock.patch.object(upstream.urllib3, "HTTPSConnectionPool") as pool:
            upstream.pinned_open("https://data.upstream.example/3.tif", "93.184.216.34", timeout=5)

        args, kwargs = pool.call_args
        self.assertEqual(args[:2], ("93.184.216.34", 443))
        self.assertEqual(kwargs["server_hostname"], "data.upstream.example")
        self.assertEqual(kwargs["assert_hostname"], "data.upstream.example")
        self.assertEqual(kwargs["cert_reqs"], "CERT_REQUIRED")
        _, call = pool.return_value.urlopen.call_args
        self.assertEqual(call["headers"]["Host"], "data.upstream.example")
        self.assertFalse(call["redirect"])
