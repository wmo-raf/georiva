"""
The real Upstream catalog client reads an anonymous STAC API, and nothing else.

The HTTP session is canned: these tests are about what the client asks for,
what it makes of the answers, and where it refuses to go. No test reaches a
network.
"""

from datetime import UTC, datetime

import requests
from django.test import SimpleTestCase

from georiva.harvest.upstream import (
    StacApiClient,
    UpstreamStatistics,
    UpstreamUnreachable,
    parse_asset,
    parse_item,
)

API = "https://stac.upstream.example/api"


class _Response:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

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

    def test_redirects_are_never_followed(self):
        client = self.client_over({f"{API}/collections": _Response({}, status_code=302)})

        with self.assertRaises(UpstreamUnreachable):
            list(client.list_collections())
        self.assertIs(self.session.calls[0][1]["allow_redirects"], False)

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
