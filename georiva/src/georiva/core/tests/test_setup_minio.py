from io import StringIO
from unittest.mock import MagicMock, patch

from django.core.management import CommandError, call_command
from django.test import SimpleTestCase, override_settings

BUCKETS = {
    "incoming": {"name": "georiva-incoming"},
    "sources": {"name": "georiva-sources"},
    "quarantine": {"name": "georiva-quarantine"},
}


@override_settings(GEORIVA_STORAGE_BACKEND="s3", GEORIVA_BUCKETS=BUCKETS)
class SetupMinioVersioningTests(SimpleTestCase):
    """``incoming`` is versioned, so a source file's cleanup deletes the write it
    ingested and no other; a lifecycle rule keeps the old versions and delete
    markers every other delete leaves from piling up (#773)."""

    def setUp(self):
        self.s3 = MagicMock()
        self.s3.get_bucket_versioning.return_value = {"Status": "Enabled"}
        patcher = patch("georiva.core.management.commands.setup_minio.boto3.client", return_value=self.s3)
        patcher.start()
        self.addCleanup(patcher.stop)
        call_command("setup_minio", stdout=StringIO())

    def _versioned(self):
        return {
            c.kwargs["Bucket"]
            for c in self.s3.put_bucket_versioning.call_args_list
            if c.kwargs["VersioningConfiguration"] == {"Status": "Enabled"}
        }

    def _lifecycles(self, bucket_name):
        configured = self.s3.put_bucket_lifecycle_configuration.call_args_list
        return [c.kwargs["LifecycleConfiguration"] for c in configured if c.kwargs["Bucket"] == bucket_name]

    def _rules(self, bucket_name):
        (lifecycle,) = self._lifecycles(bucket_name)
        return lifecycle["Rules"]

    def test_incoming_alone_is_versioned(self):
        self.assertEqual(self._versioned(), {"georiva-incoming"})

    def test_incoming_expires_old_versions_and_bare_delete_markers(self):
        (rule,) = self._rules("georiva-incoming")

        self.assertEqual(rule["Status"], "Enabled")
        self.assertEqual(rule["NoncurrentVersionExpiration"], {"NoncurrentDays": 1})
        self.assertEqual(rule["Expiration"], {"ExpiredObjectDeleteMarker": True})

    def test_current_files_in_incoming_never_expire(self):
        (rule,) = self._rules("georiva-incoming")

        self.assertNotIn("Days", rule["Expiration"])

    def test_quarantine_still_expires_after_thirty_days(self):
        (rule,) = self._rules("georiva-quarantine")

        self.assertEqual(rule["Expiration"], {"Days": 30})

    def test_sources_has_no_lifecycle(self):
        self.assertEqual(self._lifecycles("georiva-sources"), [])


@override_settings(GEORIVA_STORAGE_BACKEND="s3", GEORIVA_BUCKETS=BUCKETS)
class SetupMinioFailsWithoutVersionedIncomingTests(SimpleTestCase):
    """A deployment whose ``incoming`` is not versioned is back to #773's race, so
    ``setup_minio`` fails rather than logging a line: the entrypoint runs it under
    ``set -e``, and the container stops (#776)."""

    def setUp(self):
        self.s3 = MagicMock()
        self.s3.get_bucket_versioning.return_value = {"Status": "Enabled"}
        patcher = patch("georiva.core.management.commands.setup_minio.boto3.client", return_value=self.s3)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _setup(self):
        out = StringIO()
        call_command("setup_minio", stdout=out)
        return out.getvalue()

    def test_versioning_refused_fails_the_setup(self):
        self.s3.put_bucket_versioning.side_effect = RuntimeError("AccessDenied")

        with self.assertRaisesMessage(CommandError, "georiva-incoming"):
            self._setup()

    def test_every_bucket_is_still_set_up_before_it_fails(self):
        self.s3.put_bucket_versioning.side_effect = RuntimeError("AccessDenied")

        with self.assertRaises(CommandError):
            self._setup()

        lifecycles = {c.kwargs["Bucket"] for c in self.s3.put_bucket_lifecycle_configuration.call_args_list}
        self.assertIn("georiva-quarantine", lifecycles)

    def test_versioning_read_back_as_suspended_fails_the_setup(self):
        self.s3.get_bucket_versioning.return_value = {"Status": "Suspended"}

        with self.assertRaisesMessage(CommandError, "georiva-incoming"):
            self._setup()

    def test_versioning_never_turned_on_fails_the_setup(self):
        # S3 answers a bucket never versioned with no Status at all.
        self.s3.get_bucket_versioning.return_value = {}

        with self.assertRaisesMessage(CommandError, "georiva-incoming"):
            self._setup()

    def _lifecycle_refused_on(self, refused):
        def put(Bucket, LifecycleConfiguration):
            if Bucket == refused:
                raise RuntimeError("NotImplemented")

        self.s3.put_bucket_lifecycle_configuration.side_effect = put

    def test_incoming_lifecycle_refused_fails_the_setup(self):
        # Without it every quarantine move leaves an old version behind, and incoming grows.
        self._lifecycle_refused_on("georiva-incoming")

        with self.assertRaisesMessage(CommandError, "georiva-incoming"):
            self._setup()

    def test_quarantine_lifecycle_refused_is_only_reported(self):
        self._lifecycle_refused_on("georiva-quarantine")

        self.assertIn("Failed to set lifecycle rules", self._setup())

    def test_a_versioned_incoming_with_its_lifecycle_completes(self):
        self.assertIn("MinIO setup complete", self._setup())
