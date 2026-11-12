"""A failed storage read must not outlive the object it was about (#400).

Every other suite here reads through the local-filesystem redirect the harness
sets up, which is the right shape for testing addresses and rendering and the
wrong one for testing this: the whole defect lives in GDAL's ``/vsicurl/``
layer, which a filesystem path never enters. So these tests stand up a real
(tiny, in-process) HTTP object store and read over it, which is also the only
way to state the bug as a test — that GDAL, left alone, answers the second
request out of what it learned from the first.

That is also why this is the one suite here that does not keep to the harness
contract of asserting on responses alone: most of what follows drives
``ResilientReader`` directly, because a tile route can show that a read failed
but not *whether storage was asked*, and that is the entire distinction under
test. The last class comes back to the HTTP boundary for what is visible there.
"""

import json

import pytest
from app import config, vsicurl
from app.reader import ResilientReader, is_not_found
from rasterio.errors import RasterioIOError
from rio_tiler.io import Reader

from tests.conftest import (
    CATALOG,
    COG_SIZE,
    COLLECTION,
    ORG,
    TILE_CONFIG,
    VARIABLE,
)

TIME = "2026-03-23T12:00:00Z"


class TestWhatGdalRemembers:
    """The defect itself, stated as a test, so a GDAL that stops doing this says so."""

    def test_a_plain_reader_never_sees_a_cog_it_once_failed_to_read(self, store):
        path = store.unique_path()
        url = store.url_for(path)
        store.fail(path, 503)

        with pytest.raises(RasterioIOError):
            Reader(url)

        store.recovers(path)
        store.write(path)

        with pytest.raises(RasterioIOError):
            Reader(url)
        assert store.requests.count(path) == 1, "GDAL answered from its cache, never asking storage"


class TestResilientReader:
    def test_a_transient_failure_is_retried_within_the_same_request(self, store):
        path = store.unique_path()
        store.write(path)
        store.fail(path, 503, times=1)

        with ResilientReader(store.url_for(path)) as src:
            assert src.width == COG_SIZE

    def test_a_failure_never_outlives_the_write_that_ends_it(self, store):
        path = store.unique_path()
        url = store.url_for(path)
        store.fail(path, 503)

        with pytest.raises(RasterioIOError):
            ResilientReader(url)

        store.recovers(path)
        store.write(path)

        with ResilientReader(url) as src:
            assert src.width == COG_SIZE

    def test_an_absent_object_is_answered_at_once_and_asked_about_only_once(self, store):
        path = store.unique_path()
        url = store.url_for(path)

        with pytest.raises(RasterioIOError):
            ResilientReader(url)

        assert store.requests.count(path) == 1, "an absence is storage's answer, not a reason to ask twice"

    def test_an_absent_object_becomes_readable_the_moment_it_lands(self, store):
        path = store.unique_path()
        url = store.url_for(path)

        with pytest.raises(RasterioIOError):
            ResilientReader(url)

        store.write(path)

        with ResilientReader(url) as src:
            assert src.width == COG_SIZE


class TestTheDirectoryIsNeverListed:
    """The second cache #400 can be poisoned by, asserted through its symptom.

    A listing of the directory a COG sits in is cached like any other lookup,
    and a sibling written afterwards is missing from it. The reader disables
    the listing outright; what that looks like from storage is that opening one
    object asks about that object and nothing else.
    """

    def test_the_reader_depends_on_the_listing_being_disabled(self):
        """Stated directly, and first, because the test below cannot fail politely.

        A GDAL left to list the directory does not merely list it: against
        anything that is not a real object store it stalls, so a regression
        here would hang the run rather than report itself.
        """
        assert config.GDAL_DISABLE_READDIR_ON_OPEN == "EMPTY_DIR"

    def test_opening_a_cog_asks_storage_about_that_cog_alone(self, store):
        path = store.unique_path()
        store.write(path)

        with ResilientReader(store.url_for(path)):
            pass

        assert set(store.requests) == {path}


class TestForget:
    def test_gdal_cache_recovery_is_reachable_on_this_build(self):
        """The fix degrades silently if the C entry point cannot be found.

        Nothing else here would fail on a build where that happens — the
        object store is local and fast enough that a retry hides it — so this
        asserts the mechanism directly.
        """
        assert vsicurl.forget("http://georiva-minio:9000/georiva-assets/kenya/a/b/c.tif")

    def test_a_local_path_has_nothing_cached_to_drop(self, tmp_path):
        assert not vsicurl.forget(str(tmp_path / "temperature_120000.tif"))


class TestNotFoundClassification:
    def test_absence_is_recognised_over_http_and_on_a_filesystem(self):
        assert is_not_found(RasterioIOError("HTTP response code: 404"))
        assert is_not_found(RasterioIOError("temperature_120000.tif: No such file or directory"))
        assert is_not_found(
            RasterioIOError("'/vsicurl/http://minio/a/t.tif' does not exist in the file system, and is not recognized")
        )

    def test_a_transient_failure_is_not_read_as_absence(self):
        assert not is_not_found(RasterioIOError("HTTP response code: 503"))

    def test_a_path_that_merely_spells_404_is_not_an_absence(self):
        """``t_140400.tif`` contains "404" and says nothing about being missing."""
        error = RasterioIOError("'/vsicurl/http://minio/a/temperature_140400.tif': HTTP response code: 503")

        assert not is_not_found(error)


class TestFailureResponses:
    """What a client is told, and what it — or nginx — may keep."""

    def test_a_missing_cog_is_answered_404_and_never_cached(self, client, fake_redis):
        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(TILE_CONFIG)

        response = client.get(
            f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}/tiles/WebMercatorQuad/0/0/0.png",
            params={"time": TIME},
        )

        assert response.status_code == 404
        assert response.headers["cache-control"] == "no-store"

    def test_the_wmts_exception_report_is_never_cached_either(self, client, fake_redis):
        """The surface #400 was reported on speaks XML, not the JSON above.

        GetTile and GetFeatureInfo answer a missing COG as an ExceptionReport,
        which never reaches the JSON handler — and a 404 with no cache headers
        is one a browser may reuse of its own accord.
        """
        fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(TILE_CONFIG)

        response = client.get(
            f"/{ORG}/wmts",
            params={
                "SERVICE": "WMTS",
                "VERSION": "1.0.0",
                "REQUEST": "GetTile",
                "LAYER": f"{CATALOG}:{COLLECTION}:{VARIABLE}",
                "STYLE": "",
                "TILEMATRIXSET": "WebMercatorQuad",
                "TILEMATRIX": "0",
                "TILEROW": "0",
                "TILECOL": "0",
                "FORMAT": "image/png",
                "TIME": TIME,
            },
        )

        assert response.status_code == 404
        assert response.headers["content-type"].startswith("application/xml")
        assert response.headers["cache-control"] == "no-store"
