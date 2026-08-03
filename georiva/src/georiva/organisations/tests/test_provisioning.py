from django.contrib.auth.models import Group
from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings
from wagtail.models import GroupPagePermission, Site

from georiva.organisations.models import Organisation
from georiva.organisations.provisioning import (
    ORG_PAGE_PERMISSION_TYPES,
    bootstrap_central_org,
    org_page_group_name,
    provision_organisation,
    sync_site_ports,
)
from georiva.pages.home.models import HomePage


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test")
class ProvisionOrganisationTests(TestCase):

    def test_creates_site_root_page_and_group(self):
        organisation = provision_organisation(name="Kenya Met", slug="kenya")

        self.assertEqual(organisation.site.hostname, "kenya.georiva.test")
        self.assertIsInstance(organisation.site.root_page.specific, HomePage)
        self.assertEqual(organisation.site.root_page.title, "Kenya Met")

        group = Group.objects.get(name=org_page_group_name("kenya"))
        permissions = GroupPagePermission.objects.filter(group=group)
        self.assertEqual(permissions.count(), len(ORG_PAGE_PERMISSION_TYPES))
        for permission in permissions:
            self.assertEqual(permission.page_id, organisation.site.root_page_id)

    def test_each_organisation_gets_its_own_page_tree(self):
        kenya = provision_organisation(name="Kenya Met", slug="kenya")
        icpac = provision_organisation(name="ICPAC", slug="icpac")

        self.assertNotEqual(kenya.site.root_page_id, icpac.site.root_page_id)
        self.assertNotEqual(kenya.site.hostname, icpac.site.hostname)

    def test_invalid_slug_provisions_nothing(self):
        organisations_before = Organisation.objects.count()
        sites_before = Site.objects.count()
        pages_before = HomePage.objects.count()
        groups_before = Group.objects.count()

        with self.assertRaises(ValidationError):
            provision_organisation(name="Admin", slug="admin")

        self.assertEqual(Organisation.objects.count(), organisations_before)
        self.assertEqual(Site.objects.count(), sites_before)
        self.assertEqual(HomePage.objects.count(), pages_before)
        self.assertEqual(Group.objects.count(), groups_before)

    @override_settings(GEORIVA_SITE_PORT=443)
    def test_site_port_decides_the_scheme_of_advertised_urls(self):
        # The port is not bookkeeping: Wagtail reads the scheme off it, and
        # root_url is what every absolute STAC/EDR link is built from.
        organisation = provision_organisation(name="Kenya Met", slug="kenya")

        self.assertEqual(organisation.site.port, 443)
        self.assertTrue(organisation.site.root_url.startswith("https://"))

    def test_lean_settings_are_stored(self):
        organisation = provision_organisation(
            name="ICPAC",
            slug="icpac",
            description="Regional climate centre",
            contact_email="info@icpac.test",
            website="https://icpac.test",
            country="",
            default_provider="ICPAC",
            default_provider_url="https://icpac.test",
        )
        organisation.refresh_from_db()
        self.assertEqual(organisation.default_provider, "ICPAC")
        self.assertEqual(organisation.country, "")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test")
class BootstrapCentralOrgTests(TestCase):

    def setUp(self):
        # Back to a pre-tenancy instance: a default Site, no organisations. The
        # suite's own central org (bootstrapped on migrate) is what we're here
        # to re-create from scratch.
        Organisation.objects.all().delete()

    def test_binds_an_ordinary_org_to_the_default_site_at_the_apex(self):
        organisation = bootstrap_central_org(name="Central", slug="central")

        site = Site.objects.get(is_default_site=True)
        self.assertEqual(organisation.site, site)
        self.assertEqual(site.hostname, "georiva.test")
        self.assertTrue(Group.objects.filter(name=org_page_group_name("central")).exists())

    def test_central_org_is_indistinguishable_from_any_other(self):
        central = bootstrap_central_org(slug="central")
        kenya = provision_organisation(name="Kenya Met", slug="kenya")

        # Same shape: both resolve through their own Site, both own a root page,
        # neither carries a "this one is special" marker.
        self.assertEqual(type(central), type(kenya))
        self.assertIsNotNone(central.site.root_page_id)
        self.assertTrue(Group.objects.filter(name=org_page_group_name(central.slug)).exists())

    def test_is_idempotent(self):
        first = bootstrap_central_org(slug="central")
        second = bootstrap_central_org(slug="central")
        self.assertEqual(first.pk, second.pk)
        self.assertEqual(Organisation.objects.filter(slug="central").count(), 1)

    @override_settings(GEORIVA_SITE_PORT=443)
    def test_moves_the_default_site_off_wagtails_port_80(self):
        # Wagtail's default Site ships on port 80, so an instance behind TLS
        # would otherwise advertise http:// links from its central org.
        organisation = bootstrap_central_org(slug="central")

        self.assertEqual(organisation.site.port, 443)
        self.assertTrue(organisation.site.root_url.startswith("https://"))


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test")
class SyncSitePortsTests(TestCase):
    """The upgrade path: organisations that already exist when TLS arrives.

    The bootstrap returns early once any organisation exists, so nothing else
    ever revisits their Sites.
    """

    def test_moves_every_organisation_site_and_reports_the_count(self):
        kenya = provision_organisation(name="Kenya Met", slug="kenya")
        icpac = provision_organisation(name="ICPAC", slug="icpac")
        self.assertEqual(kenya.site.port, 80)

        with override_settings(GEORIVA_SITE_PORT=443):
            updated = sync_site_ports()

        for organisation in (kenya, icpac):
            organisation.site.refresh_from_db()
            self.assertEqual(organisation.site.port, 443)
            self.assertTrue(organisation.site.root_url.startswith("https://"))
        # The suite's own bootstrapped central org is moved too, so the count is
        # "at least these two" rather than exactly two.
        self.assertGreaterEqual(updated, 2)

    def test_is_idempotent(self):
        provision_organisation(name="Kenya Met", slug="kenya")

        with override_settings(GEORIVA_SITE_PORT=443):
            sync_site_ports()
            self.assertEqual(sync_site_ports(), 0)

    def test_leaves_sites_that_belong_to_no_organisation_alone(self):
        # A Site without an organisation is not ours to move: nothing serves it
        # under tenancy, and its port may mean something to whoever made it.
        orphan = Site.objects.create(
            hostname="orphan.georiva.test", port=8000,
            root_page=provision_organisation(name="Kenya Met", slug="kenya").site.root_page,
        )

        with override_settings(GEORIVA_SITE_PORT=443):
            sync_site_ports()

        orphan.refresh_from_db()
        self.assertEqual(orphan.port, 8000)
