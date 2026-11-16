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
