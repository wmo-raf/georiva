from django.contrib.auth.models import Group
from django.core.exceptions import ValidationError
from django.template import Context
from django.test import RequestFactory, TestCase, override_settings
from wagtail.models import GroupPagePermission, Site

from georiva.core.templatetags.georiva_tags import datasets_index_url
from georiva.organisations.models import Organisation
from georiva.organisations.provisioning import (
    ORG_PAGE_PERMISSION_TYPES,
    bootstrap_central_org,
    org_page_group_name,
    provision_organisation,
    sync_site_domains,
    sync_site_ports,
)
from georiva.pages.datasets.models import DatasetsIndexPage
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

    def test_the_root_page_comes_with_its_datasets_index(self):
        """A portal root without a datasets index is not a portal.

        Every template linking to the listing goes through ``datasets_index_url``,
        whose fallback is the bare string ``/datasets/`` — on a host with no such
        page, a navbar link to a 404.
        """
        organisation = provision_organisation(name="Kenya Met", slug="kenya")

        index = DatasetsIndexPage.objects.descendant_of(organisation.site.root_page).get()
        self.assertEqual(index.get_parent().pk, organisation.site.root_page_id)
        self.assertEqual(index.slug, "datasets")
        self.assertTrue(index.live)

    @override_settings(ALLOWED_HOSTS=["*"])
    def test_each_organisation_gets_its_own_datasets_index(self):
        """The listing a portal links to is the one in its own tree, not the first
        on the instance — which on a second tenant's host would be somebody else's.
        """
        kenya = provision_organisation(name="Kenya Met", slug="kenya")
        icpac = provision_organisation(name="ICPAC", slug="icpac")

        kenya_index = DatasetsIndexPage.objects.descendant_of(kenya.site.root_page).get()
        icpac_index = DatasetsIndexPage.objects.descendant_of(icpac.site.root_page).get()
        self.assertNotEqual(kenya_index.pk, icpac_index.pk)

        # Each index sits at /datasets/ under its own Site root, so the two URLs
        # are the same string — and the same string the tag falls back to when it
        # finds no page at all. An assertion on them could not fail for the reason
        # it exists, so move one first: now only walking the right tree gets the
        # right answer.
        icpac_index.slug = "data"
        icpac_index.save()

        # With more than one Site in play Wagtail cannot relativise a page URL, so
        # what comes back names the host as well as the path — both of which are
        # the claim here.
        for organisation, path in ((kenya, "/datasets/"), (icpac, "/data/")):
            with self.subTest(organisation=organisation.slug):
                request = RequestFactory().get("/", SERVER_NAME=organisation.site.hostname)
                self.assertEqual(
                    datasets_index_url(Context({"request": request})),
                    organisation.site.root_url + path,
                )

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


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test")
class SyncSiteDomainsTests(TestCase):
    """The way back from a base domain that turned out to be the wrong one.

    Nothing else revisits a Site hostname once provisioning has written it, so
    without this the instance answers on neither domain.
    """

    def test_moves_every_organisation_onto_the_new_domain_keeping_its_label(self):
        with override_settings(GEORIVA_BASE_DOMAIN="wrong.test"):
            kenya = provision_organisation(name="Kenya Met", slug="kenya")
            icpac = provision_organisation(name="ICPAC", slug="icpac")
        self.assertEqual(kenya.site.hostname, "kenya.wrong.test")

        moves = sync_site_domains("wrong.test")

        kenya.site.refresh_from_db()
        icpac.site.refresh_from_db()
        self.assertEqual(kenya.site.hostname, "kenya.georiva.test")
        self.assertEqual(icpac.site.hostname, "icpac.georiva.test")
        self.assertEqual(len(moves), 2)
        self.assertIn(("kenya.wrong.test", "kenya.georiva.test"), moves)

    def test_moves_the_organisation_on_the_apex_to_the_new_apex(self):
        # The central org sits on the base domain itself, not a subdomain of it.
        Organisation.objects.all().delete()
        with override_settings(GEORIVA_BASE_DOMAIN="wrong.test"):
            central = bootstrap_central_org(slug="central")
        self.assertEqual(central.site.hostname, "wrong.test")

        sync_site_domains("wrong.test")

        central.site.refresh_from_db()
        self.assertEqual(central.site.hostname, "georiva.test")

    def test_is_idempotent(self):
        with override_settings(GEORIVA_BASE_DOMAIN="wrong.test"):
            provision_organisation(name="Kenya Met", slug="kenya")

        sync_site_domains("wrong.test")
        self.assertEqual(sync_site_domains("wrong.test"), [])

    def test_leaves_organisations_on_other_domains_alone(self):
        # Only the domain being left is rewritten; an organisation deliberately
        # parked elsewhere is not swept up.
        with override_settings(GEORIVA_BASE_DOMAIN="wrong.test"):
            kenya = provision_organisation(name="Kenya Met", slug="kenya")
        with override_settings(GEORIVA_BASE_DOMAIN="other.test"):
            icpac = provision_organisation(name="ICPAC", slug="icpac")

        sync_site_domains("wrong.test")

        kenya.site.refresh_from_db()
        icpac.site.refresh_from_db()
        self.assertEqual(kenya.site.hostname, "kenya.georiva.test")
        self.assertEqual(icpac.site.hostname, "icpac.other.test")

    def test_leaves_sites_that_belong_to_no_organisation_alone(self):
        with override_settings(GEORIVA_BASE_DOMAIN="wrong.test"):
            kenya = provision_organisation(name="Kenya Met", slug="kenya")
        orphan = Site.objects.create(
            hostname="orphan.wrong.test", port=80, root_page=kenya.site.root_page,
        )

        sync_site_domains("wrong.test")

        orphan.refresh_from_db()
        self.assertEqual(orphan.hostname, "orphan.wrong.test")

    def test_a_collision_moves_nothing(self):
        # A half-applied rename leaves some organisations reachable and others
        # not, which is worse than not having started.
        with override_settings(GEORIVA_BASE_DOMAIN="wrong.test"):
            kenya = provision_organisation(name="Kenya Met", slug="kenya")
            icpac = provision_organisation(name="ICPAC", slug="icpac")
        Site.objects.create(
            hostname="kenya.georiva.test", port=kenya.site.port, root_page=kenya.site.root_page,
        )

        with self.assertRaises(ValueError):
            sync_site_domains("wrong.test")

        kenya.site.refresh_from_db()
        icpac.site.refresh_from_db()
        self.assertEqual(kenya.site.hostname, "kenya.wrong.test")
        self.assertEqual(icpac.site.hostname, "icpac.wrong.test")

    def test_moving_to_the_domain_already_in_use_is_a_no_op(self):
        provision_organisation(name="Kenya Met", slug="kenya")

        self.assertEqual(sync_site_domains("georiva.test"), [])
