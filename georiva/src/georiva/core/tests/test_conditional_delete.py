import shutil
import tempfile
import uuid
from datetime import UTC, datetime
from unittest import skipUnless
from unittest.mock import PropertyMock, patch

import boto3
from botocore.stub import Stubber
from django.core.files.storage import FileSystemStorage
from django.test import SimpleTestCase

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


class S3StubMixin:
    """A Bucket over a stubbed S3 client."""

    def setUp(self):
        self.client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="AK", aws_secret_access_key="SK")
        self.stub = Stubber(self.client)
        self.stub.activate()
        self.addCleanup(self.stub.deactivate)

        storage = type("S3", (), {})()
        storage.bucket_name = "georiva-incoming"
        storage.bucket = object()
        storage.connection = type("Conn", (), {"meta": type("Meta", (), {"client": self.client})()})()
        self.bucket = Bucket(BucketType.INCOMING, "incoming")
        patcher = patch.object(Bucket, "storage", new_callable=PropertyMock, return_value=storage)
        patcher.start()
        self.addCleanup(patcher.stop)


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
        self.stub.add_response("delete_object", {}, {"Bucket": "georiva-incoming", "Key": KEY, "IfMatch": '"e1"'})

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
            expected_params={"Bucket": "georiva-incoming", "Key": KEY, "IfMatch": '"e1"'},
        )

        self.assertFalse(self.bucket.delete_if_version(KEY, version))

    def test_a_missing_key_has_no_version(self):
        self.stub.add_client_error("head_object", service_error_code="404", http_status_code=404)

        self.assertIsNone(self.bucket.version_of(KEY))


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
