"""Machine senders of a push feed: MinIO users GeoRiva creates and removes (ADR 0034).

Each sender is a MinIO user whose policy allows only ``s3:PutObject`` on
``incoming/<org>/<catalog>/*``. The prefix is built from the feed's own catalog,
never from anything a person typed. One policy per feed is shared by its
senders. The approach was settled by the #504 spike
(``core/tests/test_silo_machine_senders.py``).
"""

import json
import logging
import os
import secrets
import tempfile
from urllib.parse import urlparse

from django.conf import settings
from django.db import transaction
from minio.minioadmin import MinioAdmin

from georiva.core.storage import BucketType, get_bucket_config
from georiva.events.context import new_context, use_context
from georiva.events.recording import record

from .models import PushSender, PushSenderAdded, PushSenderRevoked, PushSenderRotated

logger = logging.getLogger(__name__)

#: A sender must name a region, or its client first asks GetBucketLocation, which
#: the PutObject-only policy denies.
SENDER_REGION = "us-east-1"


def policy_name(push_feed) -> str:
    return f"push-feed-{push_feed.pk}"


def sender_policy(push_feed) -> dict:
    bucket = get_bucket_config()[BucketType.INCOMING]["name"]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject"],
                "Resource": [f"arn:aws:s3:::{bucket}/{push_feed.catalog.storage_prefix}/*"],
            }
        ],
    }


def get_admin() -> MinioAdmin:
    """The MinIO admin client, on the root credentials from settings."""
    from minio import Minio

    parsed = urlparse(settings.AWS_S3_ENDPOINT_URL)
    secure = parsed.scheme == "https"
    provider = Minio(
        parsed.netloc,
        access_key=settings.AWS_ACCESS_KEY_ID,
        secret_key=settings.AWS_SECRET_ACCESS_KEY,
        secure=secure,
    )._provider
    return MinioAdmin(endpoint=parsed.netloc, credentials=provider, secure=secure)


def _apply_policy(admin, push_feed, access_key):
    # ``policy_add`` reads a file, not a dict.
    handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    try:
        json.dump(sender_policy(push_feed), handle)
        handle.close()
        admin.policy_add(policy_name(push_feed), handle.name)
    finally:
        os.unlink(handle.name)
    admin.policy_set(policy_name(push_feed), user=access_key)


def _new_access_key() -> str:
    return f"snd-{secrets.token_hex(8)}"


def _new_secret() -> str:
    return secrets.token_urlsafe(30)


def _record(event_model, sender, push_feed, user):
    actor = f"user:{user.pk}" if user else "system:push-senders"
    context = new_context(actor=actor, organisation_id=push_feed.catalog.organisation_id)
    with use_context(context):
        record(
            event_model,
            push_feed=push_feed,
            feed_name=push_feed.name,
            sender_label=sender.label,
            access_key=sender.access_key,
        )


def add_sender(push_feed, label, user=None, admin=None):
    """Create a sender for ``push_feed``. Returns ``(sender, secret)``; the secret is not kept."""
    admin = admin or get_admin()
    access_key, secret = _new_access_key(), _new_secret()
    admin.user_add(access_key, secret)
    try:
        _apply_policy(admin, push_feed, access_key)
        with transaction.atomic():
            sender = PushSender.objects.create(push_feed=push_feed, label=label, access_key=access_key, created_by=user)
            _record(PushSenderAdded, sender, push_feed, user)
    except Exception:
        _remove_user(admin, access_key)
        raise
    return sender, secret


def rotate_sender(sender, user=None, admin=None):
    """Replace the sender's secret; the old one stops working. Returns the new secret."""
    admin = admin or get_admin()
    secret = _new_secret()
    # ``user_add`` on an existing user sets its secret.
    admin.user_add(sender.access_key, secret)
    with transaction.atomic():
        _record(PushSenderRotated, sender, sender.push_feed, user)
    return secret


def revoke_sender(sender, user=None, admin=None):
    """Remove the sender's MinIO user and its row."""
    admin = admin or get_admin()
    _remove_user(admin, sender.access_key, strict=True)
    with transaction.atomic():
        _record(PushSenderRevoked, sender, sender.push_feed, user)
        sender.delete()


def revoke_all(push_feed, user=None, admin=None):
    """Revoke every sender of ``push_feed`` and drop the feed's policy."""
    senders = list(push_feed.senders.select_related("push_feed__catalog"))
    if not senders:
        return
    admin = admin or get_admin()
    for sender in senders:
        revoke_sender(sender, user=user, admin=admin)
    try:
        admin.policy_remove(policy_name(push_feed))
    except Exception:
        logger.warning("Could not remove policy %s", policy_name(push_feed), exc_info=True)


def _remove_user(admin, access_key, strict=False):
    try:
        admin.user_remove(access_key)
    except Exception:
        if strict:
            raise
        logger.warning("Could not remove MinIO user %s", access_key, exc_info=True)


def rclone_config(push_feed, sender_label, access_key, secret) -> str:
    """A ready-to-paste rclone remote for a sender, and the target to copy to.

    ``georiva_dest`` and ``georiva_allow_http`` are read by ``georiva_push.py``; rclone
    ignores keys it does not know. The HTTP opt-in is written only while the endpoint is
    plain HTTP, so a block issued after the instance moves to TLS no longer carries it.
    """
    plain_http = endpoint_is_plain_http()
    scheme = "http" if plain_http else "https"
    dest = f"{get_bucket_config()[BucketType.INCOMING]['name']}/{push_feed.catalog.storage_prefix}"
    return (
        "[georiva]\n"
        "type = s3\n"
        "provider = Minio\n"
        f"access_key_id = {access_key}\n"
        f"secret_access_key = {secret}\n"
        f"endpoint = {scheme}://{settings.MINIO_PUBLIC_ENDPOINT}\n"
        f"region = {SENDER_REGION}\n"
        f"georiva_dest = {dest}\n" + ("georiva_allow_http = true\n" if plain_http else "") + "\n"
        f"# rclone copy <file> georiva:{dest}/\n"
    )


def endpoint_is_plain_http() -> bool:
    return not settings.MINIO_PUBLIC_ENDPOINT_USE_SSL
