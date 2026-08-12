"""Test runner that puts the suite on a tenant host.

Tenancy resolves a request's Host to an organisation and 404s when it can't, so
a test client dialling ``testserver`` needs an organisation that answers there.
The first-setup bootstrap already provisions a central organisation on Wagtail's
default Site at ``GEORIVA_BASE_DOMAIN``; pointing that setting at ``testserver``
before the test databases are built makes every existing test run against an
ordinary single-organisation instance, which is what they were written for.

Tests that care about tenancy override ``GEORIVA_BASE_DOMAIN`` and provision
their own organisations on top.

The runner also builds the test-only ``DataFeed`` subclass table (see
``setup_databases``).
"""

from django.db import connection
from django.test.runner import DiscoverRunner

TEST_BASE_DOMAIN = "testserver"


class GeoRivaTestRunner(DiscoverRunner):
    def setup_test_environment(self, **kwargs):
        super().setup_test_environment(**kwargs)
        from django.conf import settings

        settings.GEORIVA_BASE_DOMAIN = TEST_BASE_DOMAIN

    def setup_databases(self, **kwargs):
        """Create the test-only ``DataFeed`` subclass table before any test runs.

        ``StubPluginFeed`` is declared at module level in the sources tests, so it
        joins the app registry for the whole run and *every* ``DataFeed`` deletion
        has its cascade collector walk the subclass — including deletions in suites
        that sort before the polymorphic tests. Creating the table only in that
        one ``setUpClass`` therefore leaves earlier suites querying a table that
        does not exist yet. Building it here, at database setup, decouples the
        table's lifetime from test ordering.

        This covers the serial run (the project default). Parallel clones are
        taken inside ``super().setup_databases()``, before this runs, so they do
        not inherit the table — the idempotent creation in ``PolymorphicHealthTests``
        stays as the fallback for those.
        """
        old_config = super().setup_databases(**kwargs)
        self._create_stub_feed_table()
        return old_config

    def _create_stub_feed_table(self):
        from georiva.sources.tests.test_health_states import StubPluginFeed

        if StubPluginFeed._meta.db_table in connection.introspection.table_names():
            return  # idempotent, for --keepdb
        with connection.schema_editor() as editor:
            editor.create_model(StubPluginFeed)
