from unittest.mock import MagicMock, PropertyMock, patch
from urllib.parse import parse_qs, urlparse

from django.test import SimpleTestCase, override_settings

from georiva.core.storage.manager import Bucket, BucketType


def _bucket(is_s3=True):
    bucket = Bucket(BucketType.INCOMING, "incoming")
    storage = MagicMock()
    storage.bucket_name = "georiva-incoming"
    patches = [
        patch.object(Bucket, "storage", new_callable=PropertyMock, return_value=storage),
        patch.object(Bucket, "is_s3", new_callable=PropertyMock, return_value=is_s3),
    ]
    for p in patches:
        p.start()
    return bucket, patches


@override_settings(
    MINIO_PUBLIC_ENDPOINT="s3.example.org",
    MINIO_PUBLIC_ENDPOINT_USE_SSL=True,
    AWS_ACCESS_KEY_ID="AKTEST",
    AWS_SECRET_ACCESS_KEY="secret",
)
class UploadUrlTests(SimpleTestCase):
    def _url(self, **kwargs):
        bucket, patches = _bucket(**kwargs)
        self.addCleanup(lambda: [p.stop() for p in patches])
        return bucket.get_upload_url("test-org/wrf/file.nc", 900)

    def test_is_signed_against_the_public_endpoint(self):
        parsed = urlparse(self._url())

        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "s3.example.org")
        self.assertEqual(parsed.path, "/georiva-incoming/test-org/wrf/file.nc")

    def test_is_good_for_the_one_key_and_the_given_time(self):
        query = parse_qs(urlparse(self._url()).query)

        self.assertEqual(query["X-Amz-Expires"], ["900"])
        self.assertEqual(query["X-Amz-SignedHeaders"], ["host"])
        self.assertTrue(query["X-Amz-Credential"][0].startswith("AKTEST/"))

    @override_settings(MINIO_PUBLIC_ENDPOINT_USE_SSL=False)
    def test_follows_the_public_endpoints_scheme(self):
        self.assertTrue(self._url().startswith("http://s3.example.org/"))

    def test_there_is_nothing_to_sign_for_on_a_local_backend(self):
        self.assertIsNone(self._url(is_s3=False))
