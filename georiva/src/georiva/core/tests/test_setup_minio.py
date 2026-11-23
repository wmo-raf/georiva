from io import StringIO
from unittest.mock import MagicMock, patch

from django.core.management import call_command
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
