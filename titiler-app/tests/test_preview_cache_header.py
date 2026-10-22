"""The colorized preview is cacheable only when its URL carries the version token."""

import json

from tests.conftest import CATALOG, COLLECTION, ORG, TILE_CONFIG, VARIABLE

TIME = "2026-03-23T12:00:00Z"
URL = f"/{ORG}/{CATALOG}/{COLLECTION}/{VARIABLE}/preview.webp"


def seed(fake_redis, seed_cog):
    fake_redis.store[f"georiva:palette:{ORG}:{CATALOG}:{COLLECTION}:{VARIABLE}"] = json.dumps(TILE_CONFIG)
    seed_cog()


class TestPreviewCacheHeader:
    def test_a_tokened_preview_is_immutable(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        response = client.get(URL, params={"time": TIME, "v": "abcd1234"})

        assert response.status_code == 200
        assert response.headers["cache-control"] == "public, max-age=31536000, immutable"

    def test_an_untokened_preview_declares_nothing(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        response = client.get(URL, params={"time": TIME})

        assert response.status_code == 200
        assert "cache-control" not in response.headers

    def test_a_failed_tokened_preview_is_not_made_cacheable(self, client, fake_redis, seed_cog):
        seed(fake_redis, seed_cog)

        response = client.get(URL, params={"time": "2030-01-01T00:00:00Z", "v": "abcd1234"})

        assert response.status_code != 200
        assert response.headers.get("cache-control") != "public, max-age=31536000, immutable"
