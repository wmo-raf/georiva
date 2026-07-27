"""Test runner that puts the suite on a tenant host.

Tenancy resolves a request's Host to an organisation and 404s when it can't, so
a test client dialling ``testserver`` needs an organisation that answers there.
The first-setup bootstrap already provisions a central organisation on Wagtail's
default Site at ``GEORIVA_BASE_DOMAIN``; pointing that setting at ``testserver``
before the test databases are built makes every existing test run against an
ordinary single-organisation instance, which is what they were written for.

Tests that care about tenancy override ``GEORIVA_BASE_DOMAIN`` and provision
their own organisations on top.
"""
from django.test.runner import DiscoverRunner

TEST_BASE_DOMAIN = "testserver"


class GeoRivaTestRunner(DiscoverRunner):

    def setup_test_environment(self, **kwargs):
        super().setup_test_environment(**kwargs)
        from django.conf import settings

        settings.GEORIVA_BASE_DOMAIN = TEST_BASE_DOMAIN
