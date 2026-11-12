"""
The real Upstream catalog client reads an anonymous STAC API, or a catalog of
plain files, and nothing else.

The HTTP session is canned: these tests are about what the client asks for,
what it makes of the answers, and where it refuses to go. No test reaches a
network.
"""

from datetime import UTC, datetime

import requests
from django.test import SimpleTestCase

from georiva.harvest.upstream import (
    CubeArray,
    CubeDimension,
    StacApiClient,
    UpstreamStatistics,
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
        self.assertEqual(self.asked(), [STATIC, STATIC, GFS, CHIRPS], "no listing an API would have is asked for")

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

    def test_an_api_is_asked_for_its_collections_directly_as_before(self):
        api = "https://stac.upstream.example/api"
        client = self.client_over({f"{api}/collections": {"collections": [{"id": "a"}]}}, url=api)

        self.assertEqual([c.id for c in client.list_collections()], ["a"])
        self.assertEqual(self.asked(), [f"{api}/collections"])
