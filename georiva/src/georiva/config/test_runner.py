"""Test runner that puts the suite on a tenant host.

Tenancy resolves a request's Host to an organisation and 404s when it can't, so
a test client dialling ``testserver`` needs an organisation that answers there.
The first-setup bootstrap already provisions a central organisation on Wagtail's
default Site at ``GEORIVA_BASE_DOMAIN``; pointing that setting at ``testserver``
before the test databases are built makes every existing test run against an
ordinary single-organisation instance, which is what they were written for.

Tests that care about tenancy override ``GEORIVA_BASE_DOMAIN`` and provision
their own organisations on top.

It also swaps in a fast password hasher, builds the test-only tables (see
``setup_databases``), and under ``--parallel`` gives each worker its own Redis
database (see ``use_redis_database``).
"""

import multiprocessing
from urllib.parse import urlsplit, urlunsplit

from django.db import connection
from django.test import runner as django_runner
from django.test.runner import DiscoverRunner, ParallelTestSuite

TEST_BASE_DOMAIN = "testserver"

#: How many Redis databases one run may spread its workers over. A run's base
#: database (the one ``REDIS_URL`` names) and the workers counting up from it
#: stay inside one block of this size, so that runs given neighbouring blocks —
#: ``just test`` at 16, worktree slot N at 16 * (N + 1) — never share one.
REDIS_BLOCK_SIZE = 16


def redis_database(url):
    """The database number a Redis URL names (0 when it names none)."""
    return int(urlsplit(url).path.lstrip("/") or 0)


def worker_redis_url(base_url, worker_id):
    """The Redis URL for parallel worker ``worker_id`` (from 1): ``base + worker_id - 1``.

    The first worker keeps the base database, so a serial run and worker 1 of a
    parallel one agree on where they live.
    """
    parts = urlsplit(base_url)
    database = redis_database(base_url) + worker_id - 1
    return urlunsplit(parts._replace(path=f"/{database}"))


def check_redis_block(base_url, workers):
    """Refuse a worker count that would spill past the base's block of databases."""
    base = redis_database(base_url)
    if base % REDIS_BLOCK_SIZE + workers > REDIS_BLOCK_SIZE:
        raise ValueError(
            f"{workers} test workers from Redis database {base} would run past its block of "
            f"{REDIS_BLOCK_SIZE} (databases {base - base % REDIS_BLOCK_SIZE}-"
            f"{base - base % REDIS_BLOCK_SIZE + REDIS_BLOCK_SIZE - 1}) into another run's. Use fewer workers."
        )


def use_redis_database(url):
    """Point everything Redis-backed in this process at ``url``.

    Settings were read before the worker existed, so a changed ``REDIS_URL`` has
    to be carried by hand to each place that copied it: the cache (which the
    celery-singleton backend and raw ``get_redis_connection`` calls ride on), the
    Celery broker, and anything reading ``settings.REDIS_URL`` at call time. A
    cache ``KEY_PREFIX`` would not do instead: tests read raw keys, and neither
    the singleton backend nor the broker goes through the prefix.
    """
    import celery_singleton.backends
    from django.conf import settings
    from django.core.cache import caches
    from django.test.signals import setting_changed

    from georiva.config.celery import app

    settings.REDIS_URL = url
    settings.CELERY_BROKER_URL = url
    settings.CACHES = {**settings.CACHES, "default": {**settings.CACHES["default"], "LOCATION": url}}
    # What override_settings(CACHES=...) sends: Django drops its cache handlers
    # and builds new ones from the new settings on next use.
    setting_changed.send(sender=None, setting="CACHES", value=settings.CACHES, enter=True)
    caches["default"]  # noqa: B018 -- build it now, while nothing else is running

    app.conf.broker_url = url
    celery_singleton.backends._backend = None
    # Only an app already finalized can hold tasks with a cached backend; asking
    # an unfinalized one for its tasks would finalize it, and its
    # on_after_finalize handlers write periodic schedules into the database.
    if app.finalized:
        for task in app.tasks.values():
            if vars(task).get("_singleton_backend") is not None:
                task._singleton_backend = None


def configure_test_settings():
    """The settings every process running tests needs, the runner's and each worker's."""
    from django.conf import settings
    from django.contrib.auth.hashers import reset_hashers

    settings.GEORIVA_BASE_DOMAIN = TEST_BASE_DOMAIN
    # A dispatch site that loses the chain fails CI rather than production.
    settings.GEORIVA_TASK_CONTEXT_MODE = "strict"
    # PBKDF2 at production strength costs ~0.4s a hash, and most admin tests hash
    # twice (create_user, then client.login): over half the suite's runtime. No
    # test depends on which hasher is used. The cache only clears on the
    # setting_changed signal, which plain assignment does not send.
    settings.PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
    reset_hashers(setting="PASSWORD_HASHERS")


def _init_worker(counter, *args, **kwargs):
    """Django's worker setup, the runner's settings, then this worker's own Redis database.

    A worker starts from a fresh interpreter (see ``GeoRivaParallelTestSuite``),
    so Django sets it up again but the runner's own ``setup_test_environment``
    never ran in it: its settings are applied here.
    """
    from django.conf import settings

    django_runner._init_worker(counter, *args, **kwargs)
    configure_test_settings()
    use_redis_database(worker_redis_url(settings.REDIS_URL, django_runner._worker_id))


class GeoRivaParallelTestSuite(ParallelTestSuite):
    """Workers started from a fresh interpreter (forkserver), never forked from the runner.

    By the time workers start, the runner has imported every test module, and
    importing ``icechunk`` starts a tokio runtime in it: ten worker threads and
    an epoll instance. A forked worker inherits the epoll descriptor but not the
    threads, so its icechunk I/O is registered in the runner's runtime. Forked,
    the runner segfaulted partway through every full run, in a thread Python
    did not own. Forkserver children share nothing with the runner's runtime,
    and forkserver is Python 3.14's default on Linux anyway.
    """

    init_worker = _init_worker

    def run(self, result):
        multiprocessing.set_start_method("forkserver", force=True)
        return super().run(result)


class GeoRivaTestRunner(DiscoverRunner):
    parallel_test_suite = GeoRivaParallelTestSuite

    def setup_test_environment(self, **kwargs):
        super().setup_test_environment(**kwargs)
        configure_test_settings()

    def setup_databases(self, **kwargs):
        """Build the test database, add the test-only tables, then clone it per worker.

        ``StubPluginFeed`` (sources tests) and ``PluginThingHappened`` (events
        tests) are declared at module level, so they join the app registry for
        the whole run: *every* ``DataFeed`` or organisation deletion has its
        cascade collector walk their tables, including in suites that sort before
        the tests that declare them. Their tables are built here, at database
        setup, which decouples their lifetime from test ordering.

        Django would take the parallel clones inside its own setup, before those
        tables exist, so the database is built as for a serial run and cloned
        here afterwards. The clones are taken afresh every run, ``--keepdb`` or
        not: with ``keepdb`` Postgres's clone returns early when the clone already
        exists, and a kept clone would never see a migration applied since.

        ``old_config`` is empty when Django built no test database, which happens
        for any run whose tests are all ``SimpleTestCase``. Creating the tables
        then would issue DDL against whatever the connection actually points at
        — the real database, unmigrated — so the guard is not defensive tidiness.
        """
        from django.conf import settings
        from django.test.utils import setup_databases

        if self.parallel > 1:
            check_redis_block(settings.REDIS_URL, self.parallel)
        old_config = setup_databases(
            self.verbosity,
            self.interactive,
            time_keeper=self.time_keeper,
            keepdb=self.keepdb,
            debug_sql=self.debug_sql,
            parallel=0,
            **kwargs,
        )
        if old_config:
            self._create_stub_feed_table()
            self._create_plugin_event_table()
            if self.parallel > 1:
                self._clone_for_workers(old_config)
        return old_config

    def _clone_for_workers(self, old_config):
        for db_connection, _name, is_first_alias in old_config:
            if not is_first_alias:
                continue  # a mirror of an alias cloned already
            for index in range(self.parallel):
                with self.time_keeper.timed(f"  Cloning '{db_connection.alias}'"):
                    db_connection.creation.clone_test_db(suffix=str(index + 1), verbosity=self.verbosity, keepdb=False)

    def _create_plugin_event_table(self):
        """The plugin-shaped event type the events suite declares (events/tests/plugin_event.py)."""
        from georiva.events.tests.plugin_event import create_table

        create_table()

    def _create_stub_feed_table(self):
        from georiva.sources.tests.test_health_states import StubPluginFeed

        if StubPluginFeed._meta.db_table in connection.introspection.table_names():
            return  # idempotent, for --keepdb
        with connection.schema_editor() as editor:
            editor.create_model(StubPluginFeed)
