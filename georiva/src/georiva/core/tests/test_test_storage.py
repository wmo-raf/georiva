"""The suite's buckets live in a directory of the run's own (#767).

A test database is a clone and a test worker has its own Redis database, but the
buckets came from ``.env``: on a dev machine that is the running stack's MinIO, so
every Collection a test saved wrote a ``.keep`` into the live ``incoming`` bucket,
and the live consumer saw it. The runner points every bucket at a temporary
directory instead, before any test runs. These tests hold that it did.
"""

import os
import tempfile

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from django.test import SimpleTestCase

from georiva.config.test_runner import TEST_STORAGE_ROOT_ENV, suite_storages, use_test_storage
from georiva.core.storage import storage
from georiva.core.storage.manager import BucketType


class SuiteStoragesFunctionTests(SimpleTestCase):
    def test_every_bucket_becomes_a_directory_under_the_root(self):
        configured = {
            "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
            "georiva-incoming": {"BACKEND": "storages.backends.s3boto3.S3Boto3Storage", "OPTIONS": {}},
        }
        buckets = {"incoming": {"name": "georiva-incoming"}, "assets": {"name": "georiva-assets"}}

        result = suite_storages(configured, buckets, "/run")

        self.assertEqual(result["default"], configured["default"])
        self.assertEqual(
            result["georiva-incoming"],
            {
                "BACKEND": "django.core.files.storage.FileSystemStorage",
                "OPTIONS": {"location": "/run/georiva-incoming", "base_url": "/storage/georiva-incoming/"},
            },
        )
        self.assertEqual(result["georiva-assets"]["OPTIONS"]["location"], "/run/georiva-assets")


class ThisProcessStorageTests(SimpleTestCase):
    """What the runner set up for this very process."""

    def test_the_backend_is_local(self):
        self.assertEqual(settings.GEORIVA_STORAGE_BACKEND, "local")

    def test_every_bucket_is_under_the_runs_own_root(self):
        root = os.environ[TEST_STORAGE_ROOT_ENV]
        self.assertTrue(root.startswith(tempfile.gettempdir()), root)
        for bucket_type in BucketType.ALL:
            backend = storage.bucket(bucket_type).storage
            self.assertIsInstance(backend, FileSystemStorage, bucket_type)
            self.assertTrue(backend.location.startswith(root + os.sep), (bucket_type, backend.location))

    def test_the_root_is_not_the_configured_one(self):
        root = os.environ[TEST_STORAGE_ROOT_ENV]
        self.assertNotEqual(os.path.realpath(root), os.path.realpath(_configured_storage_root()))


class UseTestStorageTests(SimpleTestCase):
    def setUp(self):
        original = settings.GEORIVA_STORAGE_ROOT
        self.addCleanup(use_test_storage, original)

    def test_a_write_lands_in_the_new_root_even_through_a_bucket_already_used(self):
        storage.incoming.storage  # noqa: B018 -- the singleton has resolved its backend
        with tempfile.TemporaryDirectory() as root:
            use_test_storage(root)
            storage.incoming.storage.save("org/cat/col/.keep", ContentFile(b""))
            self.assertTrue(os.path.exists(os.path.join(root, "georiva-incoming", "org/cat/col/.keep")))


def _configured_storage_root():
    """Where ``.env`` would have put a local backend: base.py's default or the env's."""
    return os.environ.get("GEORIVA_STORAGE_ROOT", os.path.join(settings.BASE_DIR, "georiva_data"))
