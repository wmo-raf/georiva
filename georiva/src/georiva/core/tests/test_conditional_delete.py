import shutil
import tempfile
import uuid
from datetime import UTC, datetime
from unittest import skipUnless
from unittest.mock import PropertyMock, patch

import boto3
from botocore.stub import Stubber
from django.core.files.storage import FileSystemStorage
from django.test import SimpleTestCase, tag

from georiva.core.storage import storage
from georiva.core.storage.manager import Bucket, BucketType

KEY = "test-org/wrf/wrf_20260101T00Z.nc"


class LocalConditionalDeleteTests(SimpleTestCase):
    def setUp(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        self.bucket = Bucket(BucketType.INCOMING, "incoming")
        self.bucket._storage = FileSystemStorage(location=root)

    def test_deletes_the_version_it_was_given(self):
        self.bucket.save(KEY, b"first run")
        version = self.bucket.version_of(KEY)

        self.assertTrue(self.bucket.delete_if_version(KEY, version))
        self.assertFalse(self.bucket.exists(KEY))

    def test_keeps_a_newer_write(self):
        self.bucket.save(KEY, b"first run")
        version = self.bucket.version_of(KEY)
        self.bucket.delete(KEY)
        self.bucket.save(KEY, b"the corrected run")

        self.assertFalse(self.bucket.delete_if_version(KEY, version))
        self.assertEqual(self.bucket.read_bytes(KEY), b"the corrected run")

    def test_a_missing_key_has_no_version(self):
        self.assertIsNone(self.bucket.version_of(KEY))
        self.assertFalse(self.bucket.delete_if_version(KEY, "anything"))


def incoming_over(test, client, bucket_name):
    """The incoming Bucket, its S3 storage replaced by ``client`` on ``bucket_name`` for ``test``."""
    s3 = type("S3", (), {})()
    s3.bucket_name = bucket_name
    s3.bucket = object()
    s3.connection = type("Conn", (), {"meta": type("Meta", (), {"client": client})()})()
    patcher = patch.object(Bucket, "storage", new_callable=PropertyMock, return_value=s3)
    patcher.start()
    test.addCleanup(patcher.stop)
    return Bucket(BucketType.INCOMING, "incoming")


class S3StubMixin:
    """A Bucket over a stubbed S3 client."""

    def setUp(self):
        self.client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="AK", aws_secret_access_key="SK")
        self.stub = Stubber(self.client)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)

        self.bucket = incoming_over(self, self.client, "georiva-incoming")


class S3ConditionalDeleteTests(S3StubMixin, SimpleTestCase):
    def _head(self, etag, modified):
        self.stub.add_response(
            "head_object",
            {"ETag": etag, "LastModified": modified, "ContentLength": 3},
            {"Bucket": "georiva-incoming", "Key": KEY},
        )

    def test_deletes_only_if_the_etag_still_matches(self):
        read_at = datetime(2026, 1, 1, 6, tzinfo=UTC)
        self._head('"e1"', read_at)
        version = self.bucket.version_of(KEY)

        self._head('"e1"', read_at)
        self.stub.add_response(
            "delete_object", {}, {"Bucket": "georiva-incoming", "Key": KEY, "VersionId": "null", "IfMatch": '"e1"'}
        )

        self.assertTrue(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()

    def test_a_newer_write_is_not_deleted(self):
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC))
        version = self.bucket.version_of(KEY)

        self._head('"e2"', datetime(2026, 1, 1, 7, tzinfo=UTC))

        self.assertFalse(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()

    def test_the_same_content_written_again_is_not_deleted(self):
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC))
        version = self.bucket.version_of(KEY)

        self._head('"e1"', datetime(2026, 1, 1, 7, tzinfo=UTC))

        self.assertFalse(self.bucket.delete_if_version(KEY, version))

    def test_a_write_landing_between_the_check_and_the_delete_is_kept(self):
        read_at = datetime(2026, 1, 1, 6, tzinfo=UTC)
        self._head('"e1"', read_at)
        version = self.bucket.version_of(KEY)

        self._head('"e1"', read_at)
        self.stub.add_client_error(
            "delete_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params={"Bucket": "georiva-incoming", "Key": KEY, "VersionId": "null", "IfMatch": '"e1"'},
        )

        self.assertFalse(self.bucket.delete_if_version(KEY, version))

    def test_a_missing_key_has_no_version(self):
        self.stub.add_client_error("head_object", service_error_code="404", http_status_code=404)

        self.assertIsNone(self.bucket.version_of(KEY))


class S3VersionedDeleteTests(S3StubMixin, SimpleTestCase):
    """On a versioned bucket (``incoming``) a write is named by its VersionId, and
    the delete removes that version, and any older one behind it, but never a
    later one, whatever its bytes (#773)."""

    def _head(self, etag, modified, version_id=None):
        response = {"ETag": etag, "LastModified": modified, "ContentLength": 3}
        if version_id is not None:
            response["VersionId"] = version_id
        self.stub.add_response("head_object", response, {"Bucket": "georiva-incoming", "Key": KEY})

    def test_a_versioned_write_is_named_by_its_version_id(self):
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC), "v1")
        first = self.bucket.version_of(KEY)
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC), "v2")

        self.assertNotEqual(first, self.bucket.version_of(KEY))

    def _listing(self, *version_ids):
        """The key's versions, newest first, as S3 lists them."""
        versions = [
            {"Key": KEY, "VersionId": vid, "IsLatest": i == 0, "LastModified": datetime(2026, 1, 1, 6, tzinfo=UTC)}
            for i, vid in enumerate(version_ids)
        ]
        self.stub.add_response(
            "list_object_versions", {"Versions": versions}, {"Bucket": "georiva-incoming", "Prefix": KEY}
        )

    def _expect_delete(self, version_id):
        self.stub.add_response("delete_object", {}, {"Bucket": "georiva-incoming", "Key": KEY, "VersionId": version_id})

    def test_the_version_read_is_deleted_by_its_id_and_a_later_one_kept(self):
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC), "v1")
        version = self.bucket.version_of(KEY)

        # The same bytes delivered again are v2 by now; v1 alone is deleted.
        self._listing("v2", "v1")
        self._expect_delete("v1")

        self.assertTrue(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()

    def test_writes_older_than_the_one_read_go_with_it(self):
        # v0 kept by a partial ingest, then overwritten: deleting v1 alone would make v0 current again.
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC), "v1")
        version = self.bucket.version_of(KEY)

        self._listing("v2", "v1", "v0")
        self._expect_delete("v1")
        self._expect_delete("v0")

        self.assertTrue(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()

    def test_another_key_sharing_the_prefix_is_untouched(self):
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC), "v1")
        version = self.bucket.version_of(KEY)

        at = datetime(2026, 1, 1, tzinfo=UTC)
        this_key = {"Key": KEY, "VersionId": "v1", "IsLatest": True, "LastModified": at}
        longer_key = {"Key": f"{KEY}.idx", "VersionId": "x0", "IsLatest": True, "LastModified": at}
        self.stub.add_response(
            "list_object_versions",
            {"Versions": [this_key, longer_key]},
            {"Bucket": "georiva-incoming", "Prefix": KEY},
        )
        self._expect_delete("v1")

        self.assertTrue(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()

    def test_a_version_already_gone_deletes_nothing(self):
        self._head('"e1"', datetime(2026, 1, 1, 6, tzinfo=UTC), "v1")
        version = self.bucket.version_of(KEY)

        self._listing("v2")

        self.assertFalse(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()

    def test_a_write_from_before_versioning_is_deleted_as_the_null_version(self):
        # Read before versioning was turned on: no VersionId, the ETag@LastModified form.
        read_at = datetime(2026, 1, 1, 6, tzinfo=UTC)
        self._head('"e1"', read_at)
        version = self.bucket.version_of(KEY)

        self._head('"e1"', read_at)
        self.stub.add_response(
            "delete_object", {}, {"Bucket": "georiva-incoming", "Key": KEY, "VersionId": "null", "IfMatch": '"e1"'}
        )

        self.assertTrue(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()

    def test_a_write_from_before_versioning_is_not_deleted_once_a_versioned_write_is_current(self):
        read_at = datetime(2026, 1, 1, 6, tzinfo=UTC)
        self._head('"e1"', read_at)
        version = self.bucket.version_of(KEY)

        self._head('"e1"', read_at, "v2")

        self.assertFalse(self.bucket.delete_if_version(KEY, version))
        self.stub.assert_no_pending_responses()


class LocalDeleteIfWrittenTests(SimpleTestCase):
    """``Bucket.put`` says which write it made; ``delete_if_written`` removes
    that write and no other under the key (#771)."""

    def setUp(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        self.bucket = Bucket(BucketType.ASSETS, "assets")
        self.bucket._storage = FileSystemStorage(location=root)

    def test_its_own_write_is_deleted(self):
        _, receipt = self.bucket.put(KEY, b"ours")

        self.assertTrue(self.bucket.delete_if_written(KEY, receipt))
        self.assertFalse(self.bucket.exists(KEY))

    def test_a_later_write_is_kept(self):
        _, receipt = self.bucket.put(KEY, b"ours")
        self.bucket.delete(KEY)
        self.bucket.save(KEY, b"a re-ingest's")

        self.assertFalse(self.bucket.delete_if_written(KEY, receipt))
        self.assertEqual(self.bucket.read_bytes(KEY), b"a re-ingest's")

    def test_a_missing_key_deletes_nothing(self):
        _, receipt = self.bucket.put(KEY, b"ours")
        self.bucket.delete(KEY)

        self.assertFalse(self.bucket.delete_if_written(KEY, receipt))


class S3DeleteIfWrittenTests(S3StubMixin, SimpleTestCase):
    """On S3 the receipt is the ETag the PUT answered with, and the delete is
    one request conditional on it — nothing is asked between (#771)."""

    def test_the_put_answers_which_write_it_made(self):
        self.stub.add_response(
            "put_object",
            {"ETag": '"e1"'},
            {"Bucket": "georiva-incoming", "Key": KEY, "Body": b"ours", "ContentType": "application/x-netcdf"},
        )

        path, receipt = self.bucket.put(KEY, b"ours")

        self.assertEqual((path, receipt), (KEY, '"e1"'))
        self.stub.assert_no_pending_responses()

    def test_the_delete_is_conditional_on_the_receipt_alone(self):
        self.stub.add_response("delete_object", {}, {"Bucket": "georiva-incoming", "Key": KEY, "IfMatch": '"e1"'})

        self.assertTrue(self.bucket.delete_if_written(KEY, '"e1"'))
        self.stub.assert_no_pending_responses()

    def test_another_write_under_the_key_is_kept(self):
        self.stub.add_client_error(
            "delete_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params={"Bucket": "georiva-incoming", "Key": KEY, "IfMatch": '"e1"'},
        )

        self.assertFalse(self.bucket.delete_if_written(KEY, '"e1"'))


@skipUnless(storage.assets.is_s3, "needs the assets bucket on S3 (MinIO): CI runs on local storage")
class MinioDeleteIfWrittenTests(SimpleTestCase):
    """Against the real assets bucket: MinIO honours ``If-Match`` on a delete,
    which is what makes ``delete_if_written`` take one write and no other (#771)."""

    def setUp(self):
        self.bucket = storage.assets
        # Under a root no organisation slug can spell (as ADR 0027's `_forti`), and removed after.
        self.key = f"_test-771/{uuid.uuid4().hex}.bin"
        self.addCleanup(self.bucket.delete, self.key)

    def test_a_delete_for_an_earlier_write_is_refused_and_the_later_kept(self):
        _, first = self.bucket.put(self.key, b"first write")
        _, second = self.bucket.put(self.key, b"second write")

        self.assertFalse(self.bucket.delete_if_written(self.key, first))
        self.assertEqual(self.bucket.read_bytes(self.key), b"second write")

    def test_a_delete_for_the_write_held_removes_it(self):
        _, receipt = self.bucket.put(self.key, b"the only write")

        self.assertTrue(self.bucket.delete_if_written(self.key, receipt))
        self.assertFalse(self.bucket.exists(self.key))


@tag("integration")
class MinioVersionedDeleteTests(SimpleTestCase):
    """Against the dev silo, in a throwaway versioned bucket with no notifications:
    the write a cleanup read is deleted, and a byte-identical one sent after it is
    kept (#773). Skips when the silo isn't reachable."""

    def setUp(self):
        from botocore.exceptions import BotoCoreError, ClientError
        from django.conf import settings

        self.client = boto3.client(
            "s3",
            endpoint_url=settings.AWS_S3_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_S3_REGION_NAME,
        )
        self.bucket_name = f"test-773-{uuid.uuid4().hex[:12]}"
        try:
            self.client.create_bucket(Bucket=self.bucket_name)
        except (BotoCoreError, ClientError):
            self.skipTest("silo is not reachable")
        self.addCleanup(self._remove_bucket)

        self.bucket = incoming_over(self, self.client, self.bucket_name)

    def _remove_bucket(self):
        listing = self.client.list_object_versions(Bucket=self.bucket_name)
        for v in listing.get("Versions", []) + listing.get("DeleteMarkers", []):
            self.client.delete_object(Bucket=self.bucket_name, Key=v["Key"], VersionId=v["VersionId"])
        self.client.delete_bucket(Bucket=self.bucket_name)

    def _version(self):
        self.client.put_bucket_versioning(Bucket=self.bucket_name, VersioningConfiguration={"Status": "Enabled"})

    def _current(self):
        return self.client.get_object(Bucket=self.bucket_name, Key=KEY)["Body"].read()

    def _versions(self):
        return self.client.list_object_versions(Bucket=self.bucket_name, Prefix=KEY).get("Versions", [])

    def test_a_byte_identical_write_sent_after_the_read_is_kept(self):
        self._version()
        self.bucket.put(KEY, b"the run")
        read = self.bucket.version_of(KEY)
        self.bucket.put(KEY, b"the run")

        self.assertTrue(self.bucket.delete_if_version(KEY, read))

        self.assertEqual(self._current(), b"the run")
        self.assertEqual(len(self._versions()), 1)
        self.assertNotEqual(self.bucket.version_of(KEY), read)

    def test_a_write_older_than_the_one_read_does_not_come_back(self):
        # A partial ingest kept the first write; the sender overwrote it; the second is ingested.
        self._version()
        self.bucket.put(KEY, b"kept by a partial ingest")
        self.bucket.put(KEY, b"the run")
        read = self.bucket.version_of(KEY)
        self.bucket.put(KEY, b"the run")

        self.assertTrue(self.bucket.delete_if_version(KEY, read))

        self.assertEqual(self._current(), b"the run")
        self.assertEqual(len(self._versions()), 1)

    def test_the_write_read_is_deleted_outright(self):
        self._version()
        self.bucket.put(KEY, b"the run")

        self.assertTrue(self.bucket.delete_if_version(KEY, self.bucket.version_of(KEY)))

        listing = self.client.list_object_versions(Bucket=self.bucket_name, Prefix=KEY)
        self.assertEqual(listing.get("Versions", []) + listing.get("DeleteMarkers", []), [])

    def test_a_write_from_before_versioning_is_kept_once_a_byte_identical_one_is_current(self):
        self.bucket.put(KEY, b"the run")
        read = self.bucket.version_of(KEY)
        self._version()
        self.bucket.put(KEY, b"the run")

        # The re-delivery is current, so the earlier write is no longer the one stored.
        self.assertFalse(self.bucket.delete_if_version(KEY, read))
        self.assertEqual(self._current(), b"the run")

    def test_a_write_from_before_versioning_still_current_is_deleted(self):
        self.bucket.put(KEY, b"the run")
        read = self.bucket.version_of(KEY)
        self._version()

        self.assertTrue(self.bucket.delete_if_version(KEY, read))
        self.assertFalse(self._versions())
