"""Helpers for tests that touch the ingestion app's live updates."""

import uuid
from unittest import mock

from georiva.ingestion import live_updates


def use_own_channel(test):
    """Publish and listen on a channel only ``test`` uses, and return its name.

    Redis pub/sub ignores the database number: a channel is server-wide, so a
    parallel test worker on its own database still hears every other worker's
    events on the real channel.
    """
    channel = f"{live_updates.CHANNEL}:test:{uuid.uuid4().hex}"
    patcher = mock.patch.object(live_updates, "CHANNEL", channel)
    patcher.start()
    test.addCleanup(patcher.stop)
    return channel


#: How long a silo cleanup keeps looking for objects the live consumer may still
#: be moving, and how many empty looks in a row count as settled.
SILO_PURGE_DEADLINE = 15.0
SILO_PURGE_QUIET_PASSES = 3
SILO_PURGE_INTERVAL = 0.5


def purge_silo_prefix_on_cleanup(test, client, prefix):
    """Remove every object under ``prefix/`` from ``incoming`` and ``quarantine`` once ``test`` ends.

    For a test that puts objects into the real silo on purpose (#767). Registered
    with ``addCleanup``, so it runs whether the test passed or not. Both buckets,
    because the live consumer hears the put too: under an organisation the dev
    database has never heard of, it moves the object to ``quarantine``, and it
    may still be moving one while the cleanup runs. So the cleanup keeps looking
    until several looks in a row find nothing.
    """
    from georiva.core.storage import BucketType, get_bucket_config

    config = get_bucket_config()
    names = [config[bucket]["name"] for bucket in (BucketType.INCOMING, BucketType.QUARANTINE)]
    test.addCleanup(purge_silo_prefix, client, prefix, names)


def purge_silo_prefix(client, prefix, bucket_names):
    import time

    from minio.error import S3Error

    deadline = time.monotonic() + SILO_PURGE_DEADLINE
    quiet = 0
    while quiet < SILO_PURGE_QUIET_PASSES:
        removed = 0
        for name in bucket_names:
            try:
                for obj in client.list_objects(name, prefix=f"{prefix}/", recursive=True):
                    client.remove_object(name, obj.object_name)
                    removed += 1
            except S3Error as e:
                if e.code != "NoSuchBucket":
                    raise
        quiet = 0 if removed else quiet + 1
        if time.monotonic() > deadline:
            raise AssertionError(f"objects under {prefix}/ kept arriving for {SILO_PURGE_DEADLINE}s")
        if quiet < SILO_PURGE_QUIET_PASSES:
            time.sleep(SILO_PURGE_INTERVAL)
