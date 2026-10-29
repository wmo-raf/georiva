import io
import uuid
from unittest import mock

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.test import TestCase, override_settings, tag
from minio.error import S3Error

from georiva.core.models import Catalog
from georiva.core.storage import BucketType, get_bucket_config
from georiva.ingestion import machine_senders
from georiva.ingestion.models import (
    PushFeed,
    PushSender,
    PushSenderAdded,
    PushSenderRevoked,
    PushSenderRotated,
)
from georiva.organisations.testing import dial_org, join_org, make_organisation

User = get_user_model()


class FakeAdmin:
    def __init__(self):
        self.users, self.policies, self.attached = {}, {}, {}

    def user_add(self, name, secret):
        self.users[name] = secret

    def user_remove(self, name):
        del self.users[name]

    def policy_add(self, name, path):
        import json

        with open(path) as f:
            self.policies[name] = json.load(f)

    def policy_set(self, name, user=None):
        self.attached[user] = name

    def policy_remove(self, name):
        self.policies.pop(name, None)


def _feed(org="test-org", slug="wrf"):
    catalog = Catalog.objects.create(organisation=make_organisation(org), name=slug, slug=slug, file_format="netcdf")
    return PushFeed.objects.create(catalog=catalog, name="WRF", valid_time_format="YYYYMMDD")


class SenderServiceTests(TestCase):
    def setUp(self):
        self.admin = FakeAdmin()
        self.feed = _feed()

    def test_policy_allows_only_put_object_under_the_feeds_own_prefix(self):
        sender, _ = machine_senders.add_sender(self.feed, "ops", admin=self.admin)
        policy = self.admin.policies[machine_senders.policy_name(self.feed)]
        (statement,) = policy["Statement"]
        self.assertEqual(statement["Action"], ["s3:PutObject"])
        bucket = get_bucket_config()[BucketType.INCOMING]["name"]
        self.assertEqual(statement["Resource"], [f"arn:aws:s3:::{bucket}/test-org/wrf/*"])
        self.assertEqual(self.admin.attached[sender.access_key], machine_senders.policy_name(self.feed))

    def test_add_returns_secret_once_and_stores_only_the_access_key(self):
        sender, secret = machine_senders.add_sender(self.feed, "ops", admin=self.admin)
        self.assertEqual(self.admin.users[sender.access_key], secret)
        stored = " ".join(str(v) for v in PushSender.objects.values().get().values())
        self.assertNotIn(secret, stored)
        self.assertEqual(PushSenderAdded.objects.get().access_key, sender.access_key)

    def test_rotate_replaces_the_secret(self):
        sender, old = machine_senders.add_sender(self.feed, "ops", admin=self.admin)
        new = machine_senders.rotate_sender(sender, admin=self.admin)
        self.assertNotEqual(old, new)
        self.assertEqual(self.admin.users[sender.access_key], new)
        self.assertEqual(PushSenderRotated.objects.count(), 1)

    def test_revoke_removes_user_and_row(self):
        sender, _ = machine_senders.add_sender(self.feed, "ops", admin=self.admin)
        machine_senders.revoke_sender(sender, admin=self.admin)
        self.assertEqual(self.admin.users, {})
        self.assertFalse(PushSender.objects.exists())
        self.assertEqual(PushSenderRevoked.objects.count(), 1)

    def test_failed_policy_leaves_no_user_or_row(self):
        self.admin.policy_set = mock.Mock(side_effect=RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            machine_senders.add_sender(self.feed, "ops", admin=self.admin)
        self.assertEqual(self.admin.users, {})
        self.assertFalse(PushSender.objects.exists())

    def test_deleting_the_feed_revokes_its_senders(self):
        admin = self.admin
        with mock.patch.object(machine_senders, "get_admin", return_value=admin):
            machine_senders.add_sender(self.feed, "a", admin=admin)
            machine_senders.add_sender(self.feed, "b", admin=admin)
            self.feed.delete()
        self.assertEqual(admin.users, {})
        self.assertEqual(PushSenderRevoked.objects.count(), 2)

    def test_rclone_config_uses_public_endpoint_and_region(self):
        with override_settings(MINIO_PUBLIC_ENDPOINT="s3.example.org", MINIO_PUBLIC_ENDPOINT_USE_SSL=True):
            text = machine_senders.rclone_config(self.feed, "ops", "AK", "SK")
        self.assertIn("endpoint = https://s3.example.org", text)
        self.assertIn("region = us-east-1", text)
        self.assertIn("incoming/test-org/wrf/", text)

    def test_rclone_config_names_the_senders_destination(self):
        import configparser

        with override_settings(MINIO_PUBLIC_ENDPOINT_USE_SSL=True):
            text = machine_senders.rclone_config(self.feed, "ops", "AK", "SK")
        section = configparser.ConfigParser()
        section.read_string(text)
        prefix = machine_senders.sender_policy(self.feed)["Statement"][0]["Resource"][0]
        self.assertEqual(f"arn:aws:s3:::{section['georiva']['georiva_dest']}/*", prefix)

    def test_rclone_config_allows_http_only_while_the_endpoint_is_plain_http(self):
        with override_settings(MINIO_PUBLIC_ENDPOINT="10.0.0.5:9000", MINIO_PUBLIC_ENDPOINT_USE_SSL=False):
            text = machine_senders.rclone_config(self.feed, "ops", "AK", "SK")
        self.assertIn("endpoint = http://10.0.0.5:9000", text)
        self.assertIn("georiva_allow_http = true", text)
        with override_settings(MINIO_PUBLIC_ENDPOINT_USE_SSL=True):
            text = machine_senders.rclone_config(self.feed, "ops", "AK", "SK")
        self.assertNotIn("georiva_allow_http", text)


class SenderViewTests(TestCase):
    def setUp(self):
        self.admin = FakeAdmin()
        patcher = mock.patch.object(machine_senders, "get_admin", return_value=self.admin)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.feed = _feed()
        self.manager = User.objects.create_user("mgr", password="pw", is_staff=True)
        self.manager.user_permissions.add(
            Permission.objects.get(codename="manage_push_senders"),
            Permission.objects.get(codename="access_admin"),
        )
        join_org(self.manager)
        self.viewer = User.objects.create_user("viewer", password="pw", is_staff=True)
        self.viewer.user_permissions.add(Permission.objects.get(codename="access_admin"))
        join_org(self.viewer)
        dial_org(self.client)

    def _add_url(self):
        return f"/admin/push-feeds/{self.feed.pk}/senders/add/"

    def test_add_shows_secret_once(self):
        self.client.force_login(self.manager)
        response = self.client.post(self._add_url(), {"label": "ops"})
        self.assertEqual(response.status_code, 200)
        (secret,) = self.admin.users.values()
        self.assertContains(response, secret)
        self.assertContains(response, "region = us-east-1")
        page = self.client.get(f"/admin/push-feeds/{self.feed.pk}/")
        self.assertNotContains(page, secret)
        self.assertContains(page, PushSender.objects.get().access_key)

    def test_add_says_the_sending_machine_must_reach_the_endpoint(self):
        self.client.force_login(self.manager)
        with override_settings(MINIO_PUBLIC_ENDPOINT="10.0.0.5:9000"):
            response = self.client.post(self._add_url(), {"label": "ops"})
        self.assertContains(response, "The sending machine must reach 10.0.0.5:9000")

    def test_rotate_and_revoke(self):
        self.client.force_login(self.manager)
        sender, old = machine_senders.add_sender(self.feed, "ops", admin=self.admin)
        base = f"/admin/push-feeds/{self.feed.pk}/senders/{sender.pk}"
        response = self.client.post(f"{base}/rotate/")
        self.assertContains(response, self.admin.users[sender.access_key])
        self.assertNotEqual(self.admin.users[sender.access_key], old)
        self.client.post(f"{base}/revoke/")
        self.assertEqual(self.admin.users, {})

    def test_user_without_permission_gets_403_and_no_buttons(self):
        sender, _ = machine_senders.add_sender(self.feed, "ops", admin=self.admin)
        self.client.force_login(self.viewer)
        page = self.client.get(f"/admin/push-feeds/{self.feed.pk}/")
        self.assertEqual(page.status_code, 200)
        self.assertNotContains(page, "Add sender")
        self.assertNotContains(page, "Rotate")
        self.assertNotContains(page, "Revoke")
        base = f"/admin/push-feeds/{self.feed.pk}/senders"
        self.assertEqual(self.client.post(f"{base}/add/", {"label": "x"}).status_code, 403)
        self.assertEqual(self.client.post(f"{base}/{sender.pk}/rotate/").status_code, 403)
        self.assertEqual(self.client.post(f"{base}/{sender.pk}/revoke/").status_code, 403)
        self.assertEqual(PushSender.objects.count(), 1)

    def test_other_organisations_feed_is_not_reachable(self):
        other = _feed(org="other-org", slug="gfs")
        self.client.force_login(self.manager)
        response = self.client.post(f"/admin/push-feeds/{other.pk}/senders/add/", {"label": "x"})
        self.assertEqual(response.status_code, 404)

    def test_warning_while_endpoint_is_plain_http(self):
        self.client.force_login(self.manager)
        with override_settings(MINIO_PUBLIC_ENDPOINT_USE_SSL=False):
            self.assertContains(self.client.get(f"/admin/push-feeds/{self.feed.pk}/"), "plain HTTP")
        with override_settings(MINIO_PUBLIC_ENDPOINT_USE_SSL=True):
            self.assertNotContains(self.client.get(f"/admin/push-feeds/{self.feed.pk}/"), "plain HTTP")


@tag("integration")
class SenderAgainstSiloTests(TestCase):
    """The service end to end against the dev silo; skips when it isn't reachable."""

    def test_sender_uploads_inside_its_area_only_and_is_cut_off_by_revoke(self):
        from urllib.parse import urlparse

        from minio import Minio

        try:
            admin = machine_senders.get_admin()
            urlparse(settings.AWS_S3_ENDPOINT_URL)
            admin.user_list()
        except Exception:
            self.skipTest("silo is not reachable")

        suffix = uuid.uuid4().hex[:6]
        feed = _feed(org=f"sp{suffix}", slug="wrf")
        sender, secret = machine_senders.add_sender(feed, "ops", admin=admin)
        parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
        client = Minio(
            parsed.netloc,
            access_key=sender.access_key,
            secret_key=secret,
            secure=parsed.scheme == "https",
            region=machine_senders.SENDER_REGION,
        )
        bucket = get_bucket_config()[BucketType.INCOMING]["name"]
        prefix = feed.catalog.storage_prefix
        try:
            client.put_object(bucket, f"{prefix}/a.nc", io.BytesIO(b"abc"), 3)
            with self.assertRaises(S3Error) as ctx:
                client.put_object(bucket, f"sp{suffix}/other/a.nc", io.BytesIO(b"abc"), 3)
            self.assertEqual(ctx.exception.code, "AccessDenied")

            new = machine_senders.rotate_sender(sender, admin=admin)
            with self.assertRaises(S3Error):
                client.put_object(bucket, f"{prefix}/b.nc", io.BytesIO(b"abc"), 3)
            client = Minio(
                parsed.netloc,
                access_key=sender.access_key,
                secret_key=new,
                secure=parsed.scheme == "https",
                region=machine_senders.SENDER_REGION,
            )
            client.put_object(bucket, f"{prefix}/c.nc", io.BytesIO(b"abc"), 3)

            feed.delete()
            with self.assertRaises(S3Error):
                client.put_object(bucket, f"{prefix}/d.nc", io.BytesIO(b"abc"), 3)
        finally:
            root = Minio(
                parsed.netloc,
                access_key=settings.AWS_ACCESS_KEY_ID,
                secret_key=settings.AWS_SECRET_ACCESS_KEY,
                secure=parsed.scheme == "https",
            )
            for key in ("a.nc", "c.nc"):
                try:
                    root.remove_object(bucket, f"{prefix}/{key}")
                except Exception:
                    pass
