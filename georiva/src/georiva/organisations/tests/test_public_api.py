"""Proof that each host's public API is one organisation's whole service.

The admin sweep in ``test_fail_closed`` covers signed-in operators chasing
another organisation's primary keys. This is the other plane and the harder one:
the callers are anonymous, they address rows by *slug* rather than by pk, and
those slugs are only unique per organisation (#267). So there are two distinct
failures to rule out, and a single-org fixture surfaces neither:

*The collision.* Kenya and Uganda both run a catalog called ``forecast``. An
unscoped lookup either raises ``MultipleObjectsReturned`` or — worse, because it
looks like it worked — serves whichever row the database happened to return
first. One national met service's data published under another's identity is the
cost of getting this wrong.

*The disclosure.* Where slugs do not collide, an unscoped listing quietly
enumerates the neighbours: their catalog names, their structure, the fact that
they are on this instance at all. Kenya's host must not admit that Uganda exists.

Every fixture is named after the organisation that owns it, so the other's name
appearing anywhere in a response *is* the leak — which is what lets the sweep at
the bottom check routes nobody thought to write a test for.
"""
import json
import re

from django.test import TestCase, override_settings
from django.urls import URLPattern, URLResolver, get_resolver, reverse

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.provisioning import provision_organisation

from .test_fail_closed import SHARED_SLUG, _flatten, build_org_tree

#: A slug only Uganda uses. The colliding fixture proves the right row wins;
#: this one proves a stranger's row is not found at all rather than served.
UGANDA_ONLY_SLUG = "uganda-only"


def build_uganda_only_tree(organisation):
    """A second Uganda catalog under a slug Kenya does not have."""
    catalog = Catalog.objects.create(
        organisation=organisation,
        name="Uganda Only",
        slug=UGANDA_ONLY_SLUG,
        file_format=Catalog.FileFormat.GEOTIFF,
    )
    collection = Collection.objects.create(
        catalog=catalog, name="Uganda Only", slug=UGANDA_ONLY_SLUG,
    )
    unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
    variable = Variable.objects.create(
        collection=collection, name="Uganda Only", slug=UGANDA_ONLY_SLUG,
        unit=unit, value_min=0, value_max=1,
    )
    item = Item.objects.create(collection=collection, time="2026-01-01T00:00:00Z")
    Asset.objects.create(item=item, variable=variable, href="uganda.tif")
    return {"catalog": catalog, "collection": collection, "variable": variable}


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class CrossOrgPublicApiTests(TestCase):
    """An anonymous caller on Kenya's host, asking for Uganda's data."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_tree = build_org_tree(cls.kenya, name="Kenya Forecast")
        cls.uganda_tree = build_org_tree(cls.uganda, name="Uganda Forecast")
        cls.uganda_only = build_uganda_only_tree(cls.uganda)

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    # -- STAC --------------------------------------------------------------

    def test_stac_root_is_this_organisations_catalog(self):
        response = self.client.get(reverse("stac:landing"))
        self.assertEqual(response.status_code, 200)
        body = response.json()

        self.assertEqual(body["id"], "kenya")
        child_titles = {link.get("title") for link in body["links"] if link["rel"] == "child"}
        self.assertIn("Kenya Forecast", child_titles)
        self.assertNotIn("Uganda Forecast", child_titles)
        self.assertNotIn("Uganda Only", child_titles)

    def test_stac_root_links_stay_on_the_requesting_host(self):
        response = self.client.get(reverse("stac:landing"))
        hrefs = [link["href"] for link in response.json()["links"]]
        self.assertTrue(hrefs)
        for href in hrefs:
            with self.subTest(href=href):
                self.assertTrue(href.startswith("http://kenya.georiva.test"), href)

    def test_stac_collection_listing_names_only_this_organisation(self):
        response = self.client.get(reverse("stac:catalog-list"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda")

    def test_a_colliding_catalog_slug_resolves_to_this_organisations_catalog(self):
        response = self.client.get(reverse("stac:catalog-detail", args=[SHARED_SLUG]))
        self.assertEqual(response.status_code, 200)
        body = response.json()

        # Bare id, deliberately: the organisation lives in the hostname (#271).
        self.assertEqual(body["id"], SHARED_SLUG)
        self.assertEqual(body["title"], "Kenya Forecast")

    def test_a_catalog_only_another_organisation_has_is_not_found(self):
        response = self.client.get(reverse("stac:catalog-detail", args=[UGANDA_ONLY_SLUG]))
        self.assertEqual(response.status_code, 404)

    def test_a_collection_only_another_organisation_has_is_not_found(self):
        url = reverse(
            "stac:collection-detail",
            args=[UGANDA_ONLY_SLUG, UGANDA_ONLY_SLUG, UGANDA_ONLY_SLUG],
        )
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_the_colliding_collection_address_serves_this_organisations_variable(self):
        url = reverse(
            "stac:collection-detail", args=[SHARED_SLUG, SHARED_SLUG, SHARED_SLUG],
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["title"], "Kenya Forecast")

    def test_stac_search_does_not_reach_across_organisations(self):
        response = self.client.get(reverse("stac:search"))
        self.assertEqual(response.status_code, 200)
        # Two organisations hold one item each under the colliding slug, plus
        # Uganda's extra catalog: an unscoped search returns three.
        self.assertEqual(response.json()["numberMatched"], 1)

    def test_stac_search_by_another_organisations_collection_id_matches_nothing(self):
        response = self.client.get(
            reverse("stac:search"),
            {"collections": f"{UGANDA_ONLY_SLUG}/{UGANDA_ONLY_SLUG}/{UGANDA_ONLY_SLUG}"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["numberMatched"], 0)

    def test_stac_search_by_bare_variable_slug_stays_inside_this_organisation(self):
        """The loosest address there is — a bare slug, matched across catalogs."""
        response = self.client.get(reverse("stac:search"), {"collections": SHARED_SLUG})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["numberMatched"], 1)

    def test_stac_queryables_for_another_organisations_catalog_are_not_found(self):
        url = reverse("stac:catalog-queryables", args=[UGANDA_ONLY_SLUG])
        self.assertEqual(self.client.get(url).status_code, 404)

    # -- EDR ---------------------------------------------------------------

    def test_edr_collection_listing_names_only_this_organisation(self):
        response = self.client.get(reverse("edr:collection-list"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda")
        self.assertEqual(len(response.json()["collections"]), 1)

    def test_edr_catalog_filter_cannot_name_another_organisations_catalog(self):
        response = self.client.get(
            reverse("edr:collection-list"), {"catalog": UGANDA_ONLY_SLUG},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["collections"], [])

    def test_edr_collection_only_another_organisation_has_is_not_found(self):
        url = reverse("edr:collection-detail", args=[UGANDA_ONLY_SLUG])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_the_colliding_edr_collection_serves_this_organisations_row(self):
        response = self.client.get(reverse("edr:collection-detail", args=[SHARED_SLUG]))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda")

    # -- analysis ----------------------------------------------------------

    def test_timeseries_refuses_another_organisations_variable(self):
        response = self.client.get(
            reverse("timeseries:point"),
            {
                "variable": f"{UGANDA_ONLY_SLUG}/{UGANDA_ONLY_SLUG}/{UGANDA_ONLY_SLUG}",
                "lat": 0.0,
                "lon": 36.0,
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("variable", response.json())

    # -- datasets ----------------------------------------------------------

    def test_dataset_dates_for_another_organisations_collection_are_not_found(self):
        url = reverse(
            "datasets:collection-available-dates",
            args=[UGANDA_ONLY_SLUG, UGANDA_ONLY_SLUG],
        )
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_dataset_dates_still_serve_this_organisations_collection(self):
        url = reverse(
            "datasets:collection-available-dates", args=[SHARED_SLUG, SHARED_SLUG],
        )
        self.assertEqual(self.client.get(url).status_code, 200)

    # -- jobs --------------------------------------------------------------

    def test_jobs_answer_identically_on_every_host(self):
        """task_ferry is deliberately org-agnostic: UUID capability, not tenancy.

        So the assertion is sameness, not success — an anonymous caller is
        refused on both hosts, and refused the same way. What must *not* happen
        is one host treating the endpoint as tenant data and 404ing it.
        """
        statuses = {}
        for host in ("kenya.georiva.test", "uganda.georiva.test"):
            self.client.defaults["HTTP_HOST"] = host
            statuses[host] = self.client.get(reverse("task_ferry:job-list")).status_code

        self.assertEqual(len(set(statuses.values())), 1, statuses)
        self.assertNotIn(404, statuses.values(), statuses)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class PublicApiUrlSweepTests(TestCase):
    """Every registered public API URL, dialled for another organisation's data.

    Generated rather than listed, for the same reason the admin sweep is: the
    endpoint that leaks is the one nobody remembered to test. Slug routes are
    filled with a slug only Uganda owns, so any 200 is Kenya's host answering
    with Uganda's rows.
    """

    #: Routes the sweep cannot drive as data lookups. ``jobs`` is org-agnostic
    #: by design (#265, story 26) and takes integer ids, not slugs; the
    #: tile-config endpoint is called by Titiler over an internal hostname that
    #: belongs to no organisation, and gains its org segment with the machine
    #: plane (#272).
    _SWEEP_EXEMPT_PREFIXES = ("api/jobs/", "api/tile-config/")

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        build_org_tree(cls.kenya, name="Kenya Forecast")
        build_org_tree(cls.uganda, name="Uganda Forecast")
        build_uganda_only_tree(cls.uganda)

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _foreign_api_urls(self):
        """Every ``/api/`` route whose parameters are all slugs, filled with Uganda's."""
        urls = []
        for pattern, prefix in _flatten(get_resolver().url_patterns):
            route = prefix + str(pattern.pattern)
            if not route.startswith("api/") or route.startswith(self._SWEEP_EXEMPT_PREFIXES):
                continue
            params = re.findall(r"<([a-z]+):(\w+)>", route)
            if not params or any(converter != "slug" for converter, _ in params):
                continue
            urls.append("/" + re.sub(r"<[^>]+>", UGANDA_ONLY_SLUG, route))
        return urls

    def test_no_public_api_url_serves_another_organisations_row(self):
        checked = 0
        for url in self._foreign_api_urls():
            response = self.client.get(url)
            if response.status_code in (301, 302, 405):
                continue
            checked += 1
            with self.subTest(url=url):
                self.assertEqual(
                    response.status_code, 404,
                    f"{url} answered {response.status_code} for another organisation's row",
                )
        self.assertGreater(checked, 4, "the sweep found too few API URLs to be meaningful")

    def test_no_public_api_listing_names_another_organisation(self):
        """The half no pk or slug reaches: roots, listings and search."""
        checked = 0
        for pattern, prefix in _flatten(get_resolver().url_patterns):
            route = prefix + str(pattern.pattern)
            if not route.startswith("api/") or re.search(r"<[^>]+>", route):
                continue
            if route.startswith(self._SWEEP_EXEMPT_PREFIXES):
                continue
            response = self.client.get("/" + route)
            if response.status_code != 200:
                continue
            checked += 1
            with self.subTest(url=route):
                self.assertNotIn(
                    b"Uganda", response.content,
                    f"/{route} names another organisation's data",
                )
        self.assertGreater(checked, 4, "the sweep found too few API listings to be meaningful")

    def test_the_sweep_would_notice_a_leak(self):
        """The sweep is only worth its runtime if a real leak fails it.

        Asking Uganda's own host for the same rows must succeed — otherwise
        every assertion above would pass on an API that 404s everything.
        """
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        checked = 0
        for url in self._foreign_api_urls():
            response = self.client.get(url)
            if response.status_code in (301, 302, 405):
                continue
            checked += 1
            with self.subTest(url=url):
                self.assertEqual(response.status_code, 200, url)
        self.assertGreater(checked, 4)

    def test_search_results_carry_only_this_organisations_items(self):
        """Search takes no slug, so the sweep above never reaches it."""
        response = self.client.get(reverse("stac:search"))
        body = json.loads(response.content)
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Uganda", json.dumps(body))
