"""The shared cache of the anonymous capabilities document (#361).

A legacy GIS client re-fetches capabilities on every connection, and the
document enumerates every valid time and every run of every visible collection
— so the one document every anonymous caller receives is built once per
organisation and held for a few minutes.

Everything here is asserted at the HTTP boundary, because a cache is only ever
correct in terms of what the next request sees: a repeat fetch is the earlier
document, a fetch after expiry has caught up with the archive, and neither
happens across organisations or across credentials.
"""
import importlib
import pkgutil
import time
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.urls import reverse

from georiva.accounts.models import ApiKey
from georiva.core.models import Item, Variable
from georiva.organisations.testing import (
    dial_org, join_org, make_org_tree, make_organisation, org_host,
)
from georiva.wmts.cache import capabilities_cache_key
from georiva.wmts.testing import (
    NS, CapabilitiesReader, IsolatedCapabilitiesCache, make_tiered_catalog,
)


class WMTSCapabilitiesCacheTests(CapabilitiesReader, TestCase):
    """Hit, miss and expiry on one organisation's document.

    The three visibility tiers with an item under the public one, and a member
    holding a key: enough for the document to have something to go stale about,
    and something it must never reveal from an entry anybody can read.
    """

    def setUp(self):
        super().setUp()
        dial_org(self.client)
        self.organisation = make_organisation()
        tiers = make_tiered_catalog(self.organisation)
        self.public = tiers["public"]
        self.unit = tiers["unit"]
        Item.objects.create(
            collection=self.public,
            time=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
        )

        User = get_user_model()
        member = User.objects.create_user("member", password="x")
        join_org(member)
        _, self.member_key = ApiKey.objects.mint(user=member, name="qgis")

    def times(self, response):
        """The public layer's advertised Time values."""
        return [
            value.text
            for value in self.layer(response, self.PUBLIC)
            .findall("wmts:Dimension/wmts:Value", NS)
        ]

    def add_variable(self, slug="rh"):
        return Variable.objects.create(
            collection=self.public, slug=slug, name="Relative Humidity",
            unit=self.unit, value_min=0, value_max=100,
        )

    def test_a_repeat_fetch_inside_the_ttl_is_the_earlier_document(self):
        """Byte-identical, and provably not a rebuild: a variable added in
        between would have appeared in a document built now."""
        first = self.fetch()
        self.add_variable()
        self.assertEqual(self.fetch().content, first.content)

    def test_a_newly_ingested_item_appears_once_the_entry_expires(self):
        """The accepted cost of the cache (#354): the newest run lags by at
        most the TTL — and by no more than that. A whole second of it, because
        the setting is read as an integer and a test should hold the code to a
        value production can actually express."""
        with override_settings(GEORIVA_WMTS_CAPABILITIES_CACHE_SECONDS=1):
            self.assertEqual(self.times(self.fetch()), ["2026-03-01T00:00:00Z"])
            Item.objects.create(
                collection=self.public,
                time=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
            )
            time.sleep(1.2)
            self.assertEqual(
                self.times(self.fetch()),
                ["2026-03-01T00:00:00Z", "2026-03-01T12:00:00Z"],
            )

    def test_a_zero_ttl_switches_the_cache_off_rather_than_storing_nothing(self):
        """The operator's escape hatch: every request builds, so a document
        that looks wrong can be chased without waiting out an entry."""
        with override_settings(GEORIVA_WMTS_CAPABILITIES_CACHE_SECONDS=0):
            self.fetch()
            self.add_variable()
            self.assertIn("forecast:temperature:rh", self.identifiers(self.fetch()))

    def test_an_unreachable_cache_costs_a_rebuild_not_the_document(self):
        """Discovery worked without an entry before there was a cache, and has
        to keep working when there is one and it cannot be reached: a legacy
        client left unable to list any layer because Redis blinked would be a
        worse outage than the rebuild the cache spares."""
        with override_settings(CACHES={"default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": "redis://127.0.0.1:1/0",
            "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
        }}):
            self.assertEqual(self.identifiers(self.fetch()), [self.PUBLIC])

    def test_a_keyed_request_is_never_answered_from_the_shared_entry(self):
        """The anonymous document is public-only by construction; serving it to
        a key holder would hide exactly the layers the key is for."""
        self.assertEqual(self.identifiers(self.fetch()), [self.PUBLIC])
        keyed = self.fetch({"api_key": self.member_key})
        self.assertEqual(self.identifiers(keyed), [self.PRIVATE, self.PUBLIC])

    def test_a_keyed_document_never_becomes_the_shared_entry(self):
        """The document built first is the one a naive cache would store. It
        lists a private layer and carries the caller's own key in every URL
        (#360) — neither may reach the next anonymous caller."""
        self.fetch({"api_key": self.member_key})
        anonymous = self.fetch()
        self.assertEqual(self.identifiers(anonymous), [self.PUBLIC])
        self.assertNotIn(b"api_key", anonymous.content)

    def test_a_header_key_is_no_shared_entry_either_way(self):
        """The transport #360 gives precedence to gets the same treatment: the
        line is the credential, not how it travelled."""
        header = {"HTTP_AUTHORIZATION": f"Bearer {self.member_key}"}
        self.assertIn(self.PRIVATE, self.identifiers(self.fetch(**header)))
        self.assertEqual(self.identifiers(self.fetch()), [self.PUBLIC])
        self.assertIn(self.PRIVATE, self.identifiers(self.fetch(**header)))

    def test_a_session_request_is_not_answered_from_the_shared_entry_either(self):
        """Credential, not transport: a signed-in member's document is widened
        by the same ``visible_to``, so it is personal for the same reason."""
        self.fetch()
        self.client.login(username="member", password="x")
        self.assertIn(self.PRIVATE, self.identifiers(self.fetch()))

    def test_a_session_document_never_becomes_the_shared_entry(self):
        self.client.login(username="member", password="x")
        self.assertIn(self.PRIVATE, self.identifiers(self.fetch()))
        self.client.logout()
        self.assertEqual(self.identifiers(self.fetch()), [self.PUBLIC])


class WMTSCapabilitiesCacheKeyTests(IsolatedCapabilitiesCache, TestCase):
    """One entry per organisation, and never one entry for two.

    The fixture is the tenancy arrangement of ``make_org_tree``: two
    organisations whose catalog, collection and variable slugs are identical,
    so their documents differ only by the institution behind them. That is the
    arrangement in which a key missing its organisation segment shows up as
    the wrong institution's document rather than as an accidental pass — and
    the reason the palette cache is keyed org-first too (#267).
    """

    def setUp(self):
        super().setUp()
        self.mine = make_organisation()
        self.theirs = make_organisation("other-org")
        make_org_tree(self.mine, name="Ours")
        make_org_tree(self.theirs, name="Theirs")

    def fetch(self, organisation):
        response = self.client.get(
            reverse("wmts:capabilities", args=[organisation.slug]),
            HTTP_HOST=org_host(organisation.slug),
        )
        self.assertEqual(response.status_code, 200)
        return response

    def layers(self, response):
        document = ET.fromstring(response.content)
        return document.findall("wmts:Contents/wmts:Layer", NS)

    def titles(self, response):
        return [
            layer.findtext("ows:Title", namespaces=NS)
            for layer in self.layers(response)
        ]

    def test_the_second_organisation_gets_its_own_document(self):
        self.assertEqual(self.titles(self.fetch(self.mine)), ["Ours — Ours"])
        self.assertEqual(self.titles(self.fetch(self.theirs)), ["Theirs — Theirs"])

    def test_the_first_organisations_document_survives_the_seconds_fetch(self):
        """Neither direction: the second fetch must not overwrite the entry the
        first one filled."""
        self.fetch(self.mine)
        self.fetch(self.theirs)
        self.assertEqual(self.titles(self.fetch(self.mine)), ["Ours — Ours"])

    def test_a_cached_documents_urls_stay_on_the_organisations_own_host(self):
        """Every URL in the document is absolute against the organisation's own
        site, so a shared entry would hand a client another institution's
        hostname to fetch its tiles from."""
        self.fetch(self.mine)
        (layer,) = self.layers(self.fetch(self.theirs))
        template = layer.find("wmts:ResourceURL", NS).get("template")
        self.assertTrue(
            template.startswith(f"http://{org_host('other-org')}/"), template,
        )

    def test_colliding_slugs_across_organisations_key_differently(self):
        keys = {capabilities_cache_key(self.mine), capabilities_cache_key(self.theirs)}
        self.assertEqual(len(keys), 2, keys)
        self.assertIn(self.mine.slug, capabilities_cache_key(self.mine))


class WMTSCacheIsolationGuardTests(SimpleTestCase):
    """Nothing in this package may inherit a previous test's cached document.

    The entry outlives the database rollback, so a class that forgets
    :class:`IsolatedCapabilitiesCache` does not fail — it quietly reads the
    document another test's fixtures produced, and only some of the time. Held
    to here rather than trusted to memory, in the pattern of the tenancy guard
    that walks the admin.

    Database-touching classes only: a ``SimpleTestCase`` cannot build the rows
    a capabilities document is made of, so it has nothing to read stale.
    """

    def test_every_database_test_case_in_the_package_sweeps_the_cache(self):
        package = importlib.import_module("georiva.wmts.tests")
        unswept = []
        for module_info in pkgutil.iter_modules(package.__path__):
            module = importlib.import_module(
                f"{package.__name__}.{module_info.name}"
            )
            for name, member in vars(module).items():
                if not isinstance(member, type) or member.__module__ != module.__name__:
                    continue
                if issubclass(member, TestCase) and not issubclass(
                    member, IsolatedCapabilitiesCache
                ):
                    unswept.append(f"{module.__name__}.{name}")
        self.assertEqual(unswept, [], "Missing IsolatedCapabilitiesCache")
