"""Spike for ADR 0034: can silo's admin API manage push feed senders through
the ``minio`` client's ``MinioAdmin``? Skips when silo isn't reachable."""

import json
import threading
import uuid
from urllib.parse import urlparse

from django.conf import settings
from django.test import SimpleTestCase, tag
from minio import Minio
from minio.error import S3Error
from minio.minioadmin import MinioAdmin

from georiva.core.storage import BucketType, get_bucket_config


def _root_clients():
    endpoint = settings.AWS_S3_ENDPOINT_URL
    if not endpoint or not settings.AWS_ACCESS_KEY_ID:
        return None
    parsed = urlparse(endpoint)
    secure = parsed.scheme == "https"
    creds = dict(
        access_key=settings.AWS_ACCESS_KEY_ID,
        secret_key=settings.AWS_SECRET_ACCESS_KEY,
    )
    s3 = Minio(parsed.netloc, secure=secure, **creds)
    try:
        s3.list_buckets()
    except Exception:
        return None
    admin = MinioAdmin(
        endpoint=parsed.netloc,
        credentials=s3._provider,
        secure=secure,
    )
    return s3, admin, parsed.netloc, secure


@tag("integration")
class SiloMachineSenderTests(SimpleTestCase):
    """Steps 1-6 of the spike, end to end against the dev silo."""

    def test_sender_lifecycle(self):
        clients = _root_clients()
        if clients is None:
            self.skipTest("silo is not reachable")
        root, admin, netloc, secure = clients

        bucket = get_bucket_config()[BucketType.INCOMING]["name"]
        suffix = uuid.uuid4().hex[:8]
        org, catalog = f"spike{suffix}", "wrf"
        user, password = f"sender-{suffix}", uuid.uuid4().hex
        policy_name = f"push-{suffix}"
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:PutObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/{org}/{catalog}/*"],
                }
            ],
        }
        policy_file = self._write_policy(policy)

        events, got_event = [], threading.Event()

        def listen():
            with root.listen_bucket_notification(bucket, prefix=f"{org}/", events=["s3:ObjectCreated:*"]) as stream:
                for ev in stream:
                    events.extend(ev.get("Records", []))
                    got_event.set()
                    return

        listener = threading.Thread(target=listen, daemon=True)
        listener.start()

        try:
            # 1-3. user, policy, attach. The user's access key is its name.
            admin.user_add(user, password)
            admin.policy_add(policy_name, policy_file)
            admin.policy_set(policy_name, user=user)

            # Region is given so the client skips GetBucketLocation, which the policy denies.
            sender = Minio(
                netloc,
                access_key=user,
                secret_key=password,
                secure=secure,
                region=settings.AWS_S3_REGION_NAME,
            )

            # 4. inside the area works, outside is refused
            sender.put_object(bucket, f"{org}/{catalog}/a.nc", self._data(), 3)
            with self.assertRaises(S3Error) as ctx:
                sender.put_object(bucket, f"{org}/other/a.nc", self._data(), 3)
            self.assertEqual(ctx.exception.code, "AccessDenied")

            # 5. the notification names the sender
            self.assertTrue(got_event.wait(15), "no notification arrived")
            self.assertEqual(events[0]["userIdentity"]["principalId"], user)

            # 6. deleting the user stops the key working
            admin.user_remove(user)
            with self.assertRaises(S3Error):
                sender.put_object(bucket, f"{org}/{catalog}/b.nc", self._data(), 3)
        finally:
            for fn in (lambda: admin.user_remove(user), lambda: admin.policy_remove(policy_name)):
                try:
                    fn()
                except Exception:
                    pass
            for key in (f"{org}/{catalog}/a.nc", f"{org}/{catalog}/b.nc"):
                try:
                    root.remove_object(bucket, key)
                except Exception:
                    pass

    @staticmethod
    def _data():
        import io

        return io.BytesIO(b"abc")

    @staticmethod
    def _write_policy(policy):
        import tempfile

        f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump(policy, f)
        f.close()
        return f.name
