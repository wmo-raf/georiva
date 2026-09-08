"""Writing GeoRiva's data out for a reader that is not GeoRiva (ADR 0027).

Two properties are load-bearing and neither is visible in a happy-path write.

**The root is the tenancy boundary.** A foreign service is handed
``{org}/{slug}/`` and cannot resolve anything above it, so a path that escapes
the root is not a bug in one publication — it is one organisation's data
appearing under another's prefix.

**Completion markers go last, and the writer never writes them.** A reader
polls for a marker and loads whatever it names; a marker that lands before its
bytes is a reader loading half a dataset, with nothing anywhere reporting an
error, because from the reader's side the marker was a promise. Ordering is
therefore enforced by the sink rather than left to the writer's discipline:
``write`` refuses a marker path and ``publish_markers`` refuses everything else.
"""

import shutil
import tempfile

from django.conf import settings
from django.test import SimpleTestCase, override_settings

from georiva.core.publishing import (
    CompletionMarker,
    MarkerOrderingError,
    PublicationSink,
    PublicationSinkError,
)
from georiva.core.storage import Bucket, BucketType, StorageManager

#: What a Forti publication declares: a per-area pointer to the current version,
#: and a per-version manifest. Real patterns, so the tests exercise the shapes
#: the first publisher actually uses.
FORTI_MARKERS = ("latest/*", "*/complete.json")


class SinkTestCase(SimpleTestCase):
    """A publications bucket on a real filesystem in a temporary directory.

    Deliberately not a mock: half of what is being tested is how a Django
    storage backend behaves when asked to overwrite, and a mock would agree with
    whatever the test assumed.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

        override = override_settings(
            STORAGES={
                **settings.STORAGES,
                "georiva-publications": {
                    "BACKEND": "django.core.files.storage.FileSystemStorage",
                    "OPTIONS": {"location": self.tmp, "base_url": "/publications/"},
                },
            }
        )
        override.enable()
        self.addCleanup(override.disable)

        self.bucket = Bucket(BucketType.PUBLICATIONS, "georiva-publications")
        self.sink = self.make_sink()

    def make_sink(self, organisation="kenya", slug="forti", marker_patterns=FORTI_MARKERS):
        return PublicationSink(
            organisation,
            slug,
            marker_patterns=marker_patterns,
            bucket=self.bucket,
        )


class BucketRegistrationTests(SimpleTestCase):
    def test_publications_is_a_known_bucket(self):
        self.assertIn(BucketType.PUBLICATIONS, BucketType.ALL)

    def test_the_manager_hands_out_the_publications_bucket(self):
        self.assertEqual(StorageManager().publications.bucket_type, BucketType.PUBLICATIONS)

    def test_it_is_reachable_by_type_string(self):
        self.assertEqual(StorageManager().bucket("publications").bucket_type, BucketType.PUBLICATIONS)

    def test_it_is_configured_and_never_notifies(self):
        """A notification on this bucket would hand a publication's own output
        back to the ingestion consumer as a file to ingest."""
        from georiva.core.management.commands.setup_minio import BUCKET_CONFIGS

        self.assertIn("publications", settings.GEORIVA_BUCKETS)
        self.assertEqual(settings.GEORIVA_PUBLICATIONS_BUCKET, "georiva-publications")
        self.assertFalse(BUCKET_CONFIGS["publications"]["notify_on_create"])
        self.assertFalse(BUCKET_CONFIGS["publications"]["public_read"])


class RootTests(SinkTestCase):
    def test_the_root_is_org_then_publication(self):
        self.assertEqual(self.sink.root, "kenya/forti/")

    def test_every_key_opens_with_the_root(self):
        self.assertEqual(self.sink.key("nairobi/1/complete.json"), "kenya/forti/nairobi/1/complete.json")

    def test_a_path_that_climbs_out_of_the_root_is_refused(self):
        for escape in ("../uganda/forti/x", "nairobi/../../../etc/passwd", "/absolute", ""):
            with self.subTest(path=escape), self.assertRaises(ValueError):
                self.sink.key(escape)

    def test_two_organisations_cannot_collide(self):
        kenya = self.make_sink("kenya")
        uganda = self.make_sink("uganda")

        self.assertNotEqual(kenya.key("nairobi/1/x"), uganda.key("nairobi/1/x"))

    def test_a_slug_with_a_separator_in_it_is_not_a_slug(self):
        with self.assertRaises(ValueError):
            PublicationSink("kenya", "forti/nested", bucket=self.bucket)
        with self.assertRaises(ValueError):
            PublicationSink("kenya/uganda", "forti", bucket=self.bucket)


class WriteTests(SinkTestCase):
    def test_bytes_go_to_the_key_they_were_asked_for(self):
        key = self.sink.write("nairobi/100/grid/latitude", b"\x00\x01")

        self.assertEqual(key, "kenya/forti/nairobi/100/grid/latitude")
        self.assertEqual(self.sink.read_bytes("nairobi/100/grid/latitude"), b"\x00\x01")

    def test_rewriting_a_key_replaces_it_rather_than_renaming(self):
        """``latest/<area>`` is rewritten every run and polled by exact name, so
        ``FileSystemStorage``'s habit of appending a suffix would leave the
        reader following a name nothing writes to any more."""
        self.sink.write("nairobi/100/meta.json", b"first")
        key = self.sink.write("nairobi/100/meta.json", b"second")

        self.assertEqual(key, "kenya/forti/nairobi/100/meta.json")
        self.assertEqual(self.sink.read_bytes("nairobi/100/meta.json"), b"second")
        self.assertEqual(self.sink.list_keys("nairobi/100"), ["nairobi/100/meta.json"])

    def test_a_storage_that_moves_the_object_is_an_error_not_a_shrug(self):
        self.bucket.save = lambda key, content: key + "_moved"

        with self.assertRaises(PublicationSinkError):
            self.sink.write("nairobi/100/meta.json", b"x")

    def test_json_is_encoded_stably(self):
        self.sink.write_json("jsonformat.json", {"b": 1, "a": [2, 3]})
        first = self.sink.read_bytes("jsonformat.json")
        self.sink.write_json("jsonformat.json", {"a": [2, 3], "b": 1})

        self.assertEqual(self.sink.read_bytes("jsonformat.json"), first)
        self.assertEqual(self.sink.read_json("jsonformat.json"), {"a": [2, 3], "b": 1})


class MarkerOrderingTests(SinkTestCase):
    def test_the_writer_cannot_write_a_marker(self):
        for marker in ("latest/nairobi", "nairobi/100/complete.json"):
            with self.subTest(path=marker), self.assertRaises(MarkerOrderingError):
                self.sink.write(marker, b"100")

    def test_the_refusal_explains_which_half_of_the_system_writes_them(self):
        with self.assertRaises(MarkerOrderingError) as ctx:
            self.sink.write("latest/nairobi", b"100")

        self.assertIn("publish_markers", str(ctx.exception))

    def test_an_ordinary_path_is_not_mistaken_for_a_marker(self):
        self.assertFalse(self.sink.is_marker("nairobi/100/grid/latitude"))
        self.assertTrue(self.sink.is_marker("latest/nairobi"))
        self.assertTrue(self.sink.is_marker("nairobi/100/complete.json"))

    def test_markers_are_written_in_the_order_given(self):
        """The manifest before the pointer that names it: a reader that finds
        the pointer follows it immediately."""
        written = []
        self.bucket.save = lambda key, content: written.append(key) or key

        self.sink.publish_markers(
            [
                CompletionMarker("nairobi/100/complete.json", b"{}"),
                CompletionMarker("latest/nairobi", b"100"),
            ]
        )

        self.assertEqual(
            written,
            ["kenya/forti/nairobi/100/complete.json", "kenya/forti/latest/nairobi"],
        )

    def test_an_ordinary_object_cannot_be_smuggled_through_the_marker_door(self):
        with self.assertRaises(MarkerOrderingError):
            self.sink.publish_markers([CompletionMarker("nairobi/100/grid/latitude", b"x")])

    def test_a_publication_with_no_declared_markers_can_publish_none(self):
        sink = self.make_sink(marker_patterns=())

        with self.assertRaises(MarkerOrderingError):
            sink.publish_markers([CompletionMarker("latest/nairobi", b"100")])

    def test_markers_are_refused_when_the_bytes_they_promise_are_missing(self):
        with self.assertRaises(MarkerOrderingError) as ctx:
            self.sink.publish_markers(
                [CompletionMarker("latest/nairobi", b"100")],
                require_staged=["nairobi/100/grid/latitude"],
            )

        self.assertIn("not", str(ctx.exception))
        self.assertFalse(self.sink.exists("latest/nairobi"))

    def test_markers_land_once_their_bytes_are_staged(self):
        self.sink.write("nairobi/100/grid/latitude", b"\x00")

        published = self.sink.publish_markers(
            [CompletionMarker("latest/nairobi", b"100")],
            require_staged=["nairobi/100/grid/latitude"],
        )

        self.assertEqual(published, ["kenya/forti/latest/nairobi"])
        self.assertEqual(self.sink.read_bytes("latest/nairobi"), b"100")

    def test_a_marker_must_carry_bytes_and_a_relative_path(self):
        with self.assertRaises(MarkerOrderingError):
            CompletionMarker("latest/nairobi", "100")
        with self.assertRaises(MarkerOrderingError):
            CompletionMarker("/latest/nairobi", b"100")


class ListingAndPruningTests(SinkTestCase):
    def setUp(self):
        super().setUp()
        for version in (100, 200):
            self.sink.write(f"nairobi/{version}/grid/latitude", b"\x00")
            self.sink.write(f"nairobi/{version}/grid/data", b"\x00")
        self.sink.publish_markers([CompletionMarker("latest/nairobi", b"200")])

    def test_listing_is_relative_to_the_publication(self):
        self.assertEqual(
            sorted(self.sink.list_keys("nairobi/100")),
            ["nairobi/100/grid/data", "nairobi/100/grid/latitude"],
        )

    def test_the_versions_of_an_area_are_its_child_directories(self):
        self.assertEqual(sorted(self.sink.children("nairobi")), ["100", "200"])

    def test_an_emptied_version_stops_being_a_version(self):
        """Local storage leaves the directory behind when its last object goes;
        S3 has no directory to leave. A retention pass counting versions must
        agree with the object store, not with the filesystem."""
        self.sink.delete_prefix("nairobi/100", include_markers=True)

        self.assertEqual(self.sink.children("nairobi"), ["200"])

    def test_a_superseded_version_can_be_pruned_whole(self):
        removed = self.sink.delete_prefix("nairobi/100")

        self.assertEqual(removed, 2)
        self.assertEqual(self.sink.list_keys("nairobi/100"), [])
        self.assertEqual(
            sorted(self.sink.list_keys("nairobi/200")),
            ["nairobi/200/grid/data", "nairobi/200/grid/latitude"],
        )

    def test_pruning_never_removes_a_marker_a_reader_may_be_following(self):
        self.sink.delete_prefix("latest")

        self.assertTrue(self.sink.exists("latest/nairobi"))

    def test_a_superseded_version_including_its_own_manifest_can_be_dropped(self):
        """A version directory whose ``complete.json`` cannot be deleted never
        goes away, so retention would leave one orphan per run forever."""
        self.sink.publish_markers([CompletionMarker("nairobi/100/complete.json", b"{}")])

        removed = self.sink.delete_prefix("nairobi/100", include_markers=True)

        self.assertEqual(removed, 3)
        self.assertEqual(self.sink.list_keys("nairobi/100"), [])
        self.assertTrue(self.sink.exists("latest/nairobi"))

    def test_listing_a_prefix_that_does_not_exist_is_empty_not_an_error(self):
        self.assertEqual(self.sink.list_keys("mombasa"), [])
