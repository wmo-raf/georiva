import ast
import fcntl
import hashlib
import io
import json
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from unittest import mock

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.test import SimpleTestCase, TestCase, override_settings, tag

from georiva.core.models import Catalog
from georiva.core.storage import BucketType, get_bucket_config
from georiva.ingestion import machine_senders
from georiva.ingestion.models import PushFeed
from georiva.ingestion.sender import georiva_push as gp
from georiva.organisations.testing import dial_org, join_org, make_organisation

SCRIPT = Path(gp.__file__)

# The examples in AWS's "Signature Version 4 for Amazon S3" header-auth guide.
AWS_KEY = "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class SignatureTests(SimpleTestCase):
    def test_get_object_example(self):
        headers = {
            "Host": "examplebucket.s3.amazonaws.com",
            "Range": "bytes=0-9",
            "x-amz-content-sha256": EMPTY_SHA256,
            "x-amz-date": "20130524T000000Z",
        }
        auth = gp.authorization(
            "GET", "/test.txt", headers, EMPTY_SHA256, AWS_KEY, AWS_SECRET, "us-east-1", "20130524T000000Z"
        )
        self.assertEqual(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        )

    def test_put_object_example(self):
        payload = hashlib.sha256(b"Welcome to Amazon S3.").hexdigest()
        headers = {
            "Date": "Fri, 24 May 2013 00:00:00 GMT",
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256": payload,
            "x-amz-date": "20130524T000000Z",
            "x-amz-storage-class": "REDUCED_REDUNDANCY",
        }
        auth = gp.authorization(
            "PUT", "/test%24file.text", headers, payload, AWS_KEY, AWS_SECRET, "us-east-1", "20130524T000000Z"
        )
        self.assertTrue(auth.endswith("Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"))

    def test_host_header_drops_only_a_default_port(self):
        self.assertEqual(gp.host_header("https", "s3.example.org:443"), "s3.example.org")
        self.assertEqual(gp.host_header("http", "10.0.0.5:80"), "10.0.0.5")
        self.assertEqual(gp.host_header("http", "10.0.0.5:9000"), "10.0.0.5:9000")
        self.assertEqual(gp.host_header("https", "s3.example.org"), "s3.example.org")


BLOCK = """[georiva]
type = s3
provider = Minio
access_key_id = AK
secret_access_key = SK
endpoint = {endpoint}
region = us-east-1
georiva_dest = georiva-incoming/kmd/wrf
{extra}
"""


class ConfigTests(SimpleTestCase):
    def _load(self, endpoint="https://s3.example.org", extra="", environ=None, remote="georiva"):
        with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as fh:
            fh.write(BLOCK.format(endpoint=endpoint, extra=extra))
        self.addCleanup(os.remove, fh.name)
        return gp.load_config(fh.name, remote, environ={} if environ is None else environ)

    def test_reads_the_block_the_feed_page_issues(self):
        cfg = self._load()
        self.assertEqual(cfg["dest"], "georiva-incoming/kmd/wrf")
        self.assertEqual((cfg["scheme"], cfg["netloc"]), ("https", "s3.example.org"))
        key = gp.object_key(cfg, "/data/out/wrf_20261027T06Z.nc")
        self.assertEqual(key, "georiva-incoming/kmd/wrf/wrf_20261027T06Z.nc")

    def test_reads_a_block_machine_senders_writes(self):
        feed = mock.Mock()
        feed.catalog.storage_prefix = "kmd/wrf"
        with override_settings(MINIO_PUBLIC_ENDPOINT="10.0.0.5:9000", MINIO_PUBLIC_ENDPOINT_USE_SSL=False):
            text = machine_senders.rclone_config(feed, "ops", "AK", "SK")
        with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as fh:
            fh.write(text)
        self.addCleanup(os.remove, fh.name)
        cfg = gp.load_config(fh.name, "georiva", environ={})
        self.assertEqual(cfg["netloc"], "10.0.0.5:9000")
        self.assertTrue(cfg["dest"].endswith("/kmd/wrf"))

    def test_plain_http_is_refused_without_the_opt_in(self):
        with self.assertRaisesMessage(gp.ConfigError, "refusing plain HTTP"):
            self._load(endpoint="http://10.0.0.5:9000")
        cfg = self._load(endpoint="http://10.0.0.5:9000", extra="georiva_allow_http = true")
        self.assertEqual(cfg["scheme"], "http")

    def test_environment_overrides_the_block(self):
        cfg = self._load(environ={"GEORIVA_PUSH_SECRET_ACCESS_KEY": "FROM-ENV"})
        self.assertEqual(cfg["secret_key"], "FROM-ENV")

    def test_missing_values_are_named(self):
        with self.assertRaisesMessage(gp.ConfigError, "access_key_id, secret_access_key"):
            gp.load_config(None, "georiva", environ={"GEORIVA_PUSH_ENDPOINT": "https://x"})

    def test_an_unknown_section_is_named(self):
        with self.assertRaisesMessage(gp.ConfigError, "no [other] section"):
            self._load(remote="other")


CFG = {
    "access_key": "AK",
    "secret_key": "SK",
    "scheme": "https",
    "netloc": "s3.example.org",
    "region": "us-east-1",
    "dest": "georiva-incoming/kmd/wrf",
}


class SendFileTests(SimpleTestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "wrf_20261027T06Z.nc")
        with open(self.path, "wb") as fh:
            fh.write(b"netcdf bytes")
        self.put = mock.Mock()
        self.sleep = mock.Mock()

    def tearDown(self):
        for name in os.listdir(self.dir):
            os.remove(os.path.join(self.dir, name))
        os.rmdir(self.dir)

    def _send(self, **kwargs):
        return gp.send_file(CFG, self.path, put=self.put, sleep=self.sleep, **kwargs)

    def test_sends_to_the_feeds_area_and_writes_a_marker(self):
        self.assertTrue(self._send())
        checksum = hashlib.sha256(b"netcdf bytes").hexdigest()
        self.put.assert_called_once_with(CFG, self.path, "georiva-incoming/kmd/wrf/wrf_20261027T06Z.nc", checksum)
        with open(self.path + ".sent") as fh:
            marker = json.load(fh)
        self.assertEqual(marker["sha256"], checksum)

    def test_a_matching_marker_skips_the_file(self):
        self._send()
        self.put.reset_mock()
        self.assertTrue(self._send())
        self.put.assert_not_called()

    def test_force_and_a_changed_file_send_again(self):
        self._send()
        self._send(force=True)
        with open(self.path, "wb") as fh:
            fh.write(b"corrected run")
        self._send()
        self.assertEqual(self.put.call_count, 3)

    def test_a_transient_failure_is_retried_then_sent(self):
        self.put.side_effect = [gp.SendError("timed out", retry=True), None]
        self.assertTrue(self._send())
        self.sleep.assert_called_once_with(gp.RETRY_DELAYS[0])
        self.assertTrue(os.path.exists(self.path + ".sent"))

    def test_gives_up_after_the_last_retry_and_leaves_no_marker(self):
        self.put.side_effect = gp.SendError("connection refused", retry=True)
        self.assertFalse(self._send())
        self.assertEqual(self.put.call_count, len(gp.RETRY_DELAYS) + 1)
        self.assertEqual([c.args[0] for c in self.sleep.call_args_list], gp.RETRY_DELAYS)
        self.assertFalse(os.path.exists(self.path + ".sent"))

    def test_a_refusal_is_not_retried(self):
        self.put.side_effect = gp.SendError(gp._explain(403, "AccessDenied"), retry=gp._retryable(403))
        self.assertFalse(self._send())
        self.put.assert_called_once()
        self.sleep.assert_not_called()

    def test_server_errors_and_throttling_are_retryable(self):
        self.assertTrue(gp._retryable(503))
        self.assertTrue(gp._retryable(429))
        self.assertFalse(gp._retryable(403))
        self.assertFalse(gp._retryable(400))

    def test_a_missing_file_fails_without_sending(self):
        self.assertFalse(gp.send_file(CFG, self.path + ".nope", put=self.put))
        self.put.assert_not_called()


class SendDirTests(SimpleTestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)
        self.put = mock.Mock()
        self.sleep = mock.Mock()

    def _write(self, name, data=b"netcdf bytes", age=600):
        """A file last modified ``age`` seconds ago; older files get larger ages."""
        path = os.path.join(self.dir, name)
        with open(path, "wb") as fh:
            fh.write(data)
        stamp = time.time() - age
        os.utime(path, (stamp, stamp))
        return path

    def _pass(self, **kwargs):
        return gp.send_dir(CFG, self.dir, put=self.put, sleep=self.sleep, **kwargs)

    def _sent(self):
        return sorted(os.path.basename(c.args[1]) for c in self.put.call_args_list)

    def test_a_pass_sends_unmarked_and_changed_files_and_skips_matching_ones(self):
        self._write("a.nc", age=900)
        changed = self._write("b.nc", age=800)
        self.assertTrue(self._pass())
        self.assertEqual(self._sent(), ["a.nc", "b.nc"])

        self.put.reset_mock()
        self._write("b.nc", b"corrected run", age=700)
        self._write("c.nc", age=600)
        self.assertTrue(self._pass())
        self.assertEqual(self._sent(), ["b.nc", "c.nc"])
        with open(changed + ".sent") as fh:
            self.assertEqual(json.load(fh)["sha256"], hashlib.sha256(b"corrected run").hexdigest())

    def test_files_are_sent_oldest_first(self):
        self._write("new.nc", age=300)
        self._write("old.nc", age=900)
        self._pass()
        self.assertEqual([os.path.basename(c.args[1]) for c in self.put.call_args_list], ["old.nc", "new.nc"])

    def test_partial_names_dot_files_and_markers_are_not_sent(self):
        for name in ("run.nc.part", "run.nc.tmp", ".run.nc", "run.nc.sent"):
            self._write(name)
        os.mkdir(os.path.join(self.dir, "sub"))
        self.assertTrue(self._pass())
        self.put.assert_not_called()

    def test_a_recently_modified_file_waits_for_the_settle_window(self):
        self._write("fresh.nc", age=30)
        self._write("settled.nc", age=200)
        self._pass(settle=120)
        self.assertEqual(self._sent(), ["settled.nc"])

    def test_a_file_still_growing_is_left_for_the_next_pass(self):
        growing = self._write("growing.nc")
        self._write("done.nc")

        def append(_seconds):
            with open(growing, "ab") as fh:
                fh.write(b"more")

        self.sleep.side_effect = append
        self._pass()
        self.assertEqual(self._sent(), ["done.nc"])
        self.sleep.assert_called_once_with(gp.SIZE_WAIT)

    def test_a_pass_with_nothing_to_send_does_not_wait(self):
        self._write("a.nc")
        self._pass()
        self.sleep.reset_mock()
        self._pass()
        self.sleep.assert_not_called()

    def test_an_unchanged_marked_file_is_not_hashed_again(self):
        self._write("a.nc")
        self._pass()
        with mock.patch.object(gp, "sha256_of") as sha256_of:
            self._pass()
        sha256_of.assert_not_called()

    def test_a_file_another_sender_is_sending_is_skipped(self):
        path = self._write("a.nc")
        with open(path, "rb") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            self.assertTrue(self._pass())
        self.put.assert_not_called()
        self.assertFalse(os.path.exists(path + ".sent"))

    def test_a_marker_without_a_stat_is_brought_up_to_date_instead_of_resending(self):
        path = self._write("a.nc")
        with open(path + ".sent", "w") as fh:  # as 1.0.0 wrote it
            json.dump({"sha256": hashlib.sha256(b"netcdf bytes").hexdigest(), "key": "k", "sent_at": "then"}, fh)
        self._pass()
        self.put.assert_not_called()
        self.assertEqual(gp.read_marker(path)["sent_at"], "then")
        self.sleep.reset_mock()
        with mock.patch.object(gp, "sha256_of") as sha256_of:
            self._pass()
        sha256_of.assert_not_called()
        self.sleep.assert_not_called()

    def test_a_file_that_cannot_be_read_does_not_stop_the_pass(self):
        bad = self._write("bad.nc", age=900)
        self._write("good.nc", age=800)
        real = gp.sha256_of

        def sha256_of(path):
            if path == bad:
                raise PermissionError("denied")
            return real(path)

        with mock.patch.object(gp, "sha256_of", side_effect=sha256_of):
            self.assertFalse(self._pass(keep=0))
        self.assertEqual(self._sent(), ["good.nc"])
        self.assertTrue(os.path.exists(bad))

    def test_retention_leaves_a_file_another_sender_holds(self):
        path = self._write("a.nc")
        self._pass()
        with open(path, "rb") as held:
            fcntl.flock(held, fcntl.LOCK_EX)
            self._pass(keep=0)
        self.assertTrue(os.path.exists(path))
        self._pass(keep=0)
        self.assertFalse(os.path.exists(path))

    def test_a_failed_send_fails_the_pass(self):
        self._write("a.nc")
        self.put.side_effect = gp.SendError("refused (403)", retry=False)
        self.assertFalse(self._pass())

    def test_retention_keeps_the_newest_sent_files(self):
        for i in range(5):
            self._write(f"run{i}.nc", age=1000 - i * 100)
        self._pass(keep=2)
        self.assertEqual(sorted(os.listdir(self.dir)), ["run3.nc", "run3.nc.sent", "run4.nc", "run4.nc.sent"])

    def test_retention_never_removes_a_file_without_a_matching_marker(self):
        self._write("old_unsent.nc", age=1000)
        # old_changed.nc was sent once and has changed since.
        changed = self._write("old_changed.nc", b"first run", age=950)
        gp.write_marker(changed, hashlib.sha256(b"first run").hexdigest(), "k", os.stat(changed))
        self._write("old_changed.nc", b"corrected run", age=900)
        self._write("new.nc", age=300)

        def put(cfg, path, key, checksum):
            if not path.endswith("new.nc"):
                raise gp.SendError("refused (403)", retry=False)

        self.put.side_effect = put
        self._pass(keep=0)
        self.assertEqual(sorted(os.listdir(self.dir)), ["old_changed.nc", "old_changed.nc.sent", "old_unsent.nc"])


class CommandLineTests(SimpleTestCase):
    def test_send_takes_files_or_a_folder_not_both(self):
        for argv in (
            ["send"],
            ["send", "a.nc", "--dir", "/data"],
            ["send", "a.nc", "--keep", "3"],
            ["send", "--dir", "/data", "--force"],
            ["send", "--dir", "/data", "--keep", "-1"],
        ):
            with self.subTest(argv=argv), mock.patch("sys.stderr", io.StringIO()):
                with self.assertRaises(SystemExit) as ctx:
                    gp.main(argv)
                self.assertEqual(ctx.exception.code, 2)

    def test_a_folder_that_does_not_exist_is_a_usage_error(self):
        with mock.patch.object(gp, "load_config", return_value=CFG), mock.patch("sys.stdout", io.StringIO()):
            self.assertEqual(gp.main(["send", "--dir", "/nonexistent/georiva"]), 2)


class StdlibPutTests(SimpleTestCase):
    def test_signs_streams_and_names_itself(self):
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(b"abc")
        self.addCleanup(os.remove, fh.name)
        checksum = hashlib.sha256(b"abc").hexdigest()
        response = mock.MagicMock()
        with mock.patch.object(gp.urllib.request, "urlopen", return_value=response) as urlopen:
            gp.put_stdlib(CFG, fh.name, "georiva-incoming/kmd/wrf/a b.nc", checksum)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://s3.example.org/georiva-incoming/kmd/wrf/a%20b.nc")
        self.assertEqual(request.get_method(), "PUT")
        self.assertEqual(request.headers["Content-length"], "3")
        self.assertEqual(request.headers["X-amz-content-sha256"], checksum)
        self.assertEqual(request.headers["User-agent"], gp.USER_AGENT)
        self.assertIn("SignedHeaders=host;x-amz-content-sha256;x-amz-date,", request.headers["Authorization"])

    def test_an_http_refusal_is_explained_and_final(self):
        error = gp.urllib.error.HTTPError("u", 403, "Forbidden", {}, io.BytesIO(b"<Code>AccessDenied</Code>"))
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(b"abc")
        self.addCleanup(os.remove, fh.name)
        with mock.patch.object(gp.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(gp.SendError) as ctx:
                gp.put_stdlib(CFG, fh.name, "georiva-incoming/kmd/wrf/a.nc", "0" * 64)
        self.assertFalse(ctx.exception.retry)
        self.assertIn("georiva_dest", str(ctx.exception))


class CompatibilityTests(SimpleTestCase):
    def test_parses_with_the_python_3_6_grammar(self):
        ast.parse(SCRIPT.read_text(), feature_version=(3, 6))

    def test_imports_nothing_from_georiva_or_django(self):
        tree = ast.parse(SCRIPT.read_text())
        names = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        names |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        self.assertFalse(names & {"georiva", "django", "minio"}, names)


class DownloadTests(TestCase):
    def test_a_signed_in_user_can_download_the_script(self):
        org = make_organisation("kmd")
        user = get_user_model().objects.create_user("ops", password="x")
        user.user_permissions.add(Permission.objects.get(codename="access_admin"))
        join_org(user, org)
        catalog = Catalog.objects.create(organisation=org, name="WRF", slug="wrf", file_format="netcdf")
        feed = PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="CONTENT")
        self.client.force_login(user)
        dial_org(self.client, org)

        page = self.client.get(f"/admin/push-feeds/{feed.pk}/")
        self.assertContains(page, "/admin/push-feeds/georiva_push.py")
        response = self.client.get("/admin/push-feeds/georiva_push.py")
        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment", response["Content-Disposition"])
        self.assertEqual(b"".join(response.streaming_content), SCRIPT.read_bytes())

    def test_anonymous_is_sent_to_log_in(self):
        response = self.client.get("/admin/push-feeds/georiva_push.py")
        self.assertEqual(response.status_code, 302)


@tag("integration")
class SendAgainstSiloTests(TestCase):
    """Both transports end to end against the dev silo; skips when it isn't reachable."""

    def test_sends_into_the_feeds_area_and_is_refused_elsewhere(self):
        from urllib.parse import urlparse

        from minio import Minio

        try:
            admin = machine_senders.get_admin()
            admin.user_list()
        except Exception:
            self.skipTest("silo is not reachable")

        suffix = uuid.uuid4().hex[:6]
        org = make_organisation(f"gp{suffix}")
        catalog = Catalog.objects.create(organisation=org, name="WRF", slug="wrf", file_format="netcdf")
        feed = PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="CONTENT")
        sender, secret = machine_senders.add_sender(feed, "ops", admin=admin)
        parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
        bucket = get_bucket_config()[BucketType.INCOMING]["name"]
        cfg = {
            "access_key": sender.access_key,
            "secret_key": secret,
            "scheme": parsed.scheme,
            "netloc": parsed.netloc,
            "region": machine_senders.SENDER_REGION,
            "dest": f"{bucket}/{catalog.storage_prefix}",
        }
        root = Minio(
            parsed.netloc,
            access_key=settings.AWS_ACCESS_KEY_ID,
            secret_key=settings.AWS_SECRET_ACCESS_KEY,
            secure=parsed.scheme == "https",
        )
        tmp = tempfile.mkdtemp()
        sent = []
        try:
            for put in (gp.put_stdlib, gp.put_boto3):
                path = os.path.join(tmp, f"{put.__name__} run.nc")
                with open(path, "wb") as fh:
                    fh.write(b"abc")
                key = gp.object_key(cfg, path)
                put(cfg, path, key, hashlib.sha256(b"abc").hexdigest())
                sent.append(key.partition("/")[2])
                self.assertEqual(root.stat_object(bucket, sent[-1]).size, 3)

                outside = dict(cfg, dest=f"{bucket}/gp{suffix}/other")
                with self.assertRaises(gp.SendError) as ctx:
                    put(outside, path, gp.object_key(outside, path), hashlib.sha256(b"abc").hexdigest())
                self.assertFalse(ctx.exception.retry)
        finally:
            feed.delete()
            for name in sent:
                try:
                    root.remove_object(bucket, name)
                except Exception:
                    pass
            for name in os.listdir(tmp):
                os.remove(os.path.join(tmp, name))
            os.rmdir(tmp)
