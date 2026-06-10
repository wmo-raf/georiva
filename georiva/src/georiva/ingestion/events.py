import json
import logging

import redis
from django.conf import settings

logger = logging.getLogger(__name__)

CHANNEL = "ingestion:events"


def _get_redis():
    return redis.from_url(settings.REDIS_URL)


def publish_event(event: dict) -> None:
    try:
        r = _get_redis()
        r.publish(CHANNEL, json.dumps(event))
    except Exception as e:
        logger.warning("Ingestion event publish failed: %s", e)
