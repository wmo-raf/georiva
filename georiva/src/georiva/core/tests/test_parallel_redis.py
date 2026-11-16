"""Each parallel test worker on its own Redis database (#690).

Django's ``--parallel`` gives every worker its own Postgres clone but leaves them
one Redis. The cache, the celery-singleton locks and the Celery broker all live
there, and tests read raw keys out of it, so workers sharing it see each other's
writes. The runner moves worker *k* to database ``base + k - 1``, where the base
is the database ``REDIS_URL`` names. These tests hold the arithmetic and that
everything Redis-backed follows the switch together.
"""

from urllib.parse import urlsplit, urlunsplit

from django.conf import settings
from django.core.cache import cache
from django.test import SimpleTestCase
from django_redis import get_redis_connection

from georiva.config.test_runner import (
    REDIS_BLOCK_SIZE,
    check_redis_block,
    use_redis_database,
    worker_redis_url,
)


def _database(url):
    return int(urlsplit(url).path.lstrip("/") or 0)


class WorkerRedisUrlTests(SimpleTestCase):
    def test_the_first_worker_uses_the_base_database(self):
        self.assertEqual(worker_redis_url("redis://georiva-redis:6379/16", 1), "redis://georiva-redis:6379/16")

    def test_later_workers_count_up_from_the_base(self):
        self.assertEqual(worker_redis_url("redis://georiva-redis:6379/16", 4), "redis://georiva-redis:6379/19")

    def test_credentials_and_query_survive(self):
        self.assertEqual(
            worker_redis_url("redis://u:p@host:6379/1?socket_timeout=5", 2),
            "redis://u:p@host:6379/2?socket_timeout=5",
        )

    def test_a_url_without_a_database_starts_at_zero(self):
        self.assertEqual(worker_redis_url("redis://host:6379", 3), "redis://host:6379/2")


class RedisBlockTests(SimpleTestCase):
    def test_workers_that_fit_in_the_block_pass(self):
        check_redis_block("redis://host:6379/16", REDIS_BLOCK_SIZE)
        check_redis_block("redis://host:6379/1", 4)  # CI: databases 1-4

    def test_workers_spilling_into_the_next_block_are_refused(self):
        # Base 16 with 17 workers would put the last one on 32: worktree slot 1's base.
        with self.assertRaisesMessage(ValueError, "block"):
            check_redis_block("redis://host:6379/16", REDIS_BLOCK_SIZE + 1)
        with self.assertRaisesMessage(ValueError, "block"):
            check_redis_block("redis://host:6379/1", REDIS_BLOCK_SIZE)


class UseRedisDatabaseTests(SimpleTestCase):
    def setUp(self):
        original = settings.REDIS_URL
        self.addCleanup(use_redis_database, original)
        # The last database of this run's block: no other worker of the run is on
        # it unless the run has a full block of workers, and the probe key is
        # unique either way. Not this process's own database, or the switch
        # would prove nothing.
        own = _database(original)
        last = own - own % REDIS_BLOCK_SIZE + REDIS_BLOCK_SIZE - 1
        target = last if last != own else last - 1
        self.other = urlunsplit(urlsplit(original)._replace(path=f"/{target}"))

    def test_everything_redis_backed_follows_the_switch(self):
        from celery_singleton.backends import get_backend
        from celery_singleton.config import Config

        from georiva.config.celery import app

        use_redis_database(self.other)

        self.assertEqual(settings.REDIS_URL, self.other)
        self.assertEqual(settings.CELERY_BROKER_URL, self.other)
        self.assertEqual(app.conf.broker_url, self.other)
        db = _database(self.other)
        self.assertEqual(get_redis_connection("default").connection_pool.connection_kwargs["db"], db)
        singleton = get_backend(Config(app))
        self.assertEqual(singleton.redis.connection_pool.connection_kwargs["db"], db)

    def test_the_cache_writes_to_the_new_database(self):
        use_redis_database(self.other)
        cache.set("parallel-redis-probe", "here", timeout=30)
        self.addCleanup(cache.delete, "parallel-redis-probe")

        raw = get_redis_connection("default")
        self.assertEqual(raw.connection_pool.connection_kwargs["db"], _database(self.other))
        self.assertTrue(any(b"parallel-redis-probe" in key for key in raw.keys("*parallel-redis-probe*")))


class ThisProcessTests(SimpleTestCase):
    """Whatever database this process was given, all of it agrees on one."""

    def test_cache_and_broker_name_the_same_database(self):
        from georiva.config.celery import app

        db = _database(settings.REDIS_URL)
        self.assertEqual(settings.CACHES["default"]["LOCATION"], settings.REDIS_URL)
        self.assertEqual(get_redis_connection("default").connection_pool.connection_kwargs["db"], db)
        self.assertEqual(app.conf.broker_url, settings.REDIS_URL)
