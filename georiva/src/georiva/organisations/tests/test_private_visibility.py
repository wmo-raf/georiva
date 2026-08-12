"""Proof that ``private`` is served to members and to nobody else.

ADR 0012 made each host one organisation's whole public service, and everything
it serves is public. This is the tier above that: a collection an organisation
publishes to its own members and to no one else, reachable over a browser
session or an API key, and invisible — not forbidden, *invisible* — to everybody
else.

The distinction that carries the whole file is 404 versus 403. A private
collection a caller may not see does not exist as far as that caller is
concerned: it is absent from every listing and every search, and a direct fetch
by name is "no such collection", never "you are not allowed". A 403 would answer
the only question worth hiding, which is whether the collection is there at all.

Three tiers, three audiences, and the third is the one to keep honest:
``public`` to anyone, ``private`` to authenticated members of the owning
organisation, ``internal`` to nothing that serves — a derivation intermediate is
never a private dataset with a smaller audience, it is not a dataset at all.
"""

from django.test import RequestFactory, TestCase, override_settings
from django.urls import reverse

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.organisations.access import may_see_private
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, make_user

#: One slug per tier, so a response naming it says which tier leaked.
PUBLIC_SLUG = "public-forecast"
PRIVATE_SLUG = "private-forecast"
INTERNAL_SLUG = "internal-forecast"


def build_tiered_catalog(organisation):
    """One catalog carrying a collection at each of the three visibility tiers.

    They share a catalog deliberately: the catalog itself is public, so any
    filter that stops at the catalog and forgets the collection is caught here.
    """
    catalog = Catalog.objects.create(
        organisation=organisation,
        name=f"{organisation.name} Forecast",
        slug="forecast",
        file_format=Catalog.FileFormat.GEOTIFF,
    )
    unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
    built = {"catalog": catalog}
    for slug, visibility in (
        (PUBLIC_SLUG, Collection.Visibility.PUBLIC),
        (PRIVATE_SLUG, Collection.Visibility.PRIVATE),
        (INTERNAL_SLUG, Collection.Visibility.INTERNAL),
    ):
        collection = Collection.objects.create(
            catalog=catalog,
            name=slug,
            slug=slug,
            visibility=visibility,
        )
        variable = Variable.objects.create(
            collection=collection,
            name=slug,
            slug=slug,
            unit=unit,
            value_min=0,
            value_max=50,
        )
        item = Item.objects.create(collection=collection, time="2026-03-01T12:00:00Z")
        Asset.objects.create(item=item, variable=variable, href=f"{slug}.tif")
        built[visibility] = {
            "collection": collection,
            "variable": variable,
            "item": item,
        }
    return built


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class PrivateTierTestCase(TestCase):
    """Two organisations, each with all three tiers, dialled on Kenya's host."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_tree = build_tiered_catalog(cls.kenya)
        cls.uganda_tree = build_tiered_catalog(cls.uganda)

        cls.member = make_user("kenya-member")
        add_member(cls.member, cls.kenya)
        cls.outsider = make_user("uganda-member")
        add_member(cls.outsider, cls.uganda)
        cls.unaffiliated = make_user("nobody")
        cls.superuser = make_user("instance-admin", superuser=True)

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def login(self, user):
        self.client.login(username=user.username, password=PASSWORD)


class PrivateCollectionsOnTheStacPlaneTests(PrivateTierTestCase):
    """STAC listings, detail fetches and search across the three tiers.

    A STAC Collection here is a Variable — its id is ``{collection}/{variable}``
    — and the fixture gives each tier's collection and variable the same slug,
    so ``public-forecast/public-forecast`` is the public tier's STAC id.
    """

    @staticmethod
    def stac_id(slug):
        return f"{slug}/{slug}"

    def collection_slugs(self):
        response = self.client.get(reverse("stac:collection-list", args=["forecast"]))
        self.assertEqual(response.status_code, 200)
        return {c["id"] for c in response.json()["collections"]}

    def test_an_anonymous_caller_is_listed_only_the_public_collection(self):
        self.assertEqual(self.collection_slugs(), {self.stac_id(PUBLIC_SLUG)})

    def test_a_member_is_listed_the_private_collection_too(self):
        self.login(self.member)
        self.assertEqual(
            self.collection_slugs(),
            {self.stac_id(PUBLIC_SLUG), self.stac_id(PRIVATE_SLUG)},
        )

    def test_a_member_is_never_listed_the_internal_collection(self):
        self.login(self.member)
        self.assertNotIn(self.stac_id(INTERNAL_SLUG), self.collection_slugs())

    def test_another_organisations_member_is_listed_only_the_public_collection(self):
        self.login(self.outsider)
        self.assertEqual(self.collection_slugs(), {self.stac_id(PUBLIC_SLUG)})

    def test_a_signed_in_user_with_no_membership_sees_only_the_public_collection(self):
        self.login(self.unaffiliated)
        self.assertEqual(self.collection_slugs(), {self.stac_id(PUBLIC_SLUG)})

    def test_the_instance_admin_is_listed_the_private_collection(self):
        self.login(self.superuser)
        self.assertEqual(
            self.collection_slugs(),
            {self.stac_id(PUBLIC_SLUG), self.stac_id(PRIVATE_SLUG)},
        )

    def detail_status(self, slug):
        return self.client.get(reverse("stac:collection-detail", args=["forecast", slug, slug])).status_code

    def test_fetching_a_private_collection_anonymously_is_not_found(self):
        self.assertEqual(self.detail_status(PRIVATE_SLUG), 404)

    def test_fetching_a_private_collection_as_a_member_succeeds(self):
        self.login(self.member)
        self.assertEqual(self.detail_status(PRIVATE_SLUG), 200)

    def test_fetching_a_private_collection_from_the_wrong_organisation_is_not_found(self):
        self.login(self.outsider)
        self.assertEqual(self.detail_status(PRIVATE_SLUG), 404)

    def test_an_internal_collection_is_not_found_even_for_a_member(self):
        self.login(self.member)
        self.assertEqual(self.detail_status(INTERNAL_SLUG), 404)

    def test_the_public_collection_stays_reachable_anonymously(self):
        self.assertEqual(self.detail_status(PUBLIC_SLUG), 200)

    def catalog_child_hrefs(self):
        """The catalog document links onward to one collection per tier — or should not.

        A link is an address *and* a title, so an unfiltered child list announces
        a restricted collection's name and structure and then invites a fetch
        that 404s. The listing and the parent document have to agree.
        """
        response = self.client.get(reverse("stac:catalog-detail", args=["forecast"]))
        self.assertEqual(response.status_code, 200)
        return " ".join(link["href"] for link in response.json()["links"] if link["rel"] == "child")

    def test_the_catalog_never_links_to_an_internal_collection(self):
        self.login(self.member)
        self.assertNotIn(INTERNAL_SLUG, self.catalog_child_hrefs())

    def test_the_catalog_does_not_link_an_anonymous_caller_to_a_private_collection(self):
        hrefs = self.catalog_child_hrefs()
        self.assertIn(PUBLIC_SLUG, hrefs)
        self.assertNotIn(PRIVATE_SLUG, hrefs)

    def test_the_catalog_links_a_member_to_the_private_collection(self):
        self.login(self.member)
        self.assertIn(PRIVATE_SLUG, self.catalog_child_hrefs())

    def test_the_catalog_summaries_do_not_name_a_private_collections_variables(self):
        response = self.client.get(reverse("stac:catalog-detail", args=["forecast"]))
        variables = response.json()["summaries"].get("georiva:variables", [])
        self.assertEqual(variables, [PUBLIC_SLUG])

    def search_collection_ids(self):
        response = self.client.get(reverse("stac:search"))
        self.assertEqual(response.status_code, 200)
        return {f["collection"] for f in response.json()["features"]}

    def test_search_omits_private_items_from_an_anonymous_caller(self):
        self.assertEqual(self.search_collection_ids(), {self.stac_id(PUBLIC_SLUG)})

    def test_search_includes_private_items_for_a_member(self):
        self.login(self.member)
        self.assertEqual(
            self.search_collection_ids(),
            {self.stac_id(PUBLIC_SLUG), self.stac_id(PRIVATE_SLUG)},
        )

    def test_search_named_at_a_private_collection_returns_nothing_anonymously(self):
        response = self.client.get(reverse("stac:search"), {"collections": self.stac_id(PRIVATE_SLUG)})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["features"], [])

    def test_a_private_collection_of_another_organisation_is_never_reachable(self):
        """Even the owning org's own member cannot reach it from the wrong host."""
        self.login(self.member)
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        self.assertEqual(self.detail_status(PRIVATE_SLUG), 404)


class PrivateCollectionsOnTheEdrPlaneTests(PrivateTierTestCase):
    """The same three tiers, over EDR."""

    def collection_ids(self):
        response = self.client.get(reverse("edr:collection-list"))
        self.assertEqual(response.status_code, 200)
        return {c["id"] for c in response.json()["collections"]}

    def test_an_anonymous_caller_is_listed_only_the_public_collection(self):
        self.assertEqual(self.collection_ids(), {PUBLIC_SLUG})

    def test_a_member_is_listed_the_private_collection_too(self):
        self.login(self.member)
        self.assertEqual(self.collection_ids(), {PUBLIC_SLUG, PRIVATE_SLUG})

    def test_an_internal_collection_is_listed_to_nobody(self):
        self.login(self.superuser)
        self.assertNotIn(INTERNAL_SLUG, self.collection_ids())

    def detail_status(self, slug):
        return self.client.get(reverse("edr:collection-detail", args=[slug])).status_code

    def test_fetching_a_private_collection_anonymously_is_not_found(self):
        self.assertEqual(self.detail_status(PRIVATE_SLUG), 404)

    def test_fetching_a_private_collection_as_a_member_succeeds(self):
        self.login(self.member)
        self.assertEqual(self.detail_status(PRIVATE_SLUG), 200)


class PrivateCollectionsOnTheAnalysisPlaneTests(PrivateTierTestCase):
    """Submission-time enforcement, before a job id is ever handed out.

    ``/api/jobs/`` is guarded by an unguessable id rather than by tenancy
    (ADR 0012), so a job created over a collection the caller may not see would
    be a side door with no second gate behind it. The check therefore belongs at
    submission, which is where the caller is still identified.
    """

    def submit(self, slug):
        return self.client.get(
            reverse("timeseries:point"),
            {
                "variable": f"forecast/{slug}/{slug}",
                "lon": "36.8",
                "lat": "-1.3",
            },
        )

    def test_a_private_variable_is_rejected_for_an_anonymous_caller(self):
        self.assertEqual(self.submit(PRIVATE_SLUG).status_code, 400)

    def test_a_private_variable_is_accepted_for_a_member(self):
        self.login(self.member)
        self.assertNotEqual(self.submit(PRIVATE_SLUG).status_code, 400)

    def test_an_internal_variable_is_rejected_even_for_a_member(self):
        self.login(self.member)
        self.assertEqual(self.submit(INTERNAL_SLUG).status_code, 400)

    def test_a_public_variable_is_accepted_anonymously(self):
        self.assertNotEqual(self.submit(PUBLIC_SLUG).status_code, 400)


class PrivateCollectionsOnTheDatasetPortalTests(PrivateTierTestCase):
    """The portal's own JSON endpoints, which are easy to leave behind.

    A dataset page and the date picker embedded in it resolve the same
    collection by the same slug through two different code paths. If only one of
    them learns about the tier, the member gets a page whose controls 404 and
    everybody else gets a second opinion on whether the dataset exists.
    """

    def dates_status(self, slug):
        return self.client.get(
            reverse("datasets:collection-available-dates", args=["forecast", slug]),
            {"level": "years", "variable": slug},
        ).status_code

    def test_the_date_picker_answers_a_member_for_a_private_collection(self):
        self.login(self.member)
        self.assertEqual(self.dates_status(PRIVATE_SLUG), 200)

    def test_the_date_picker_is_not_found_for_an_anonymous_caller(self):
        self.assertEqual(self.dates_status(PRIVATE_SLUG), 404)

    def test_the_date_picker_never_answers_for_an_internal_collection(self):
        self.login(self.member)
        self.assertEqual(self.dates_status(INTERNAL_SLUG), 404)

    def test_the_date_picker_still_answers_anonymously_for_a_public_collection(self):
        self.assertEqual(self.dates_status(PUBLIC_SLUG), 200)


class MaySeePrivateTests(TestCase):
    """The one rule, read directly.

    Both planes reach the same function: the admin's role came from the
    middleware, but an API-key request is still anonymous when the middleware
    runs and only acquires its user inside the view. So the answer is resolved
    from ``request.user`` at the moment it is used, and this is what it says.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.member = make_user("kenya-member")
        add_member(cls.member, cls.kenya)
        cls.outsider = make_user("uganda-member")
        add_member(cls.outsider, cls.uganda)
        cls.superuser = make_user("instance-admin", superuser=True)

    def request_for(self, organisation, user=None):
        request = RequestFactory().get("/api/stac/")
        request.active_org = organisation
        request.user = user
        return request

    def test_an_anonymous_request_may_not(self):
        from django.contrib.auth.models import AnonymousUser

        self.assertFalse(self.request_may_see(self.kenya, AnonymousUser()))

    def request_may_see(self, organisation, user):
        return may_see_private(self.request_for(organisation, user))

    def test_a_request_with_no_user_at_all_may_not(self):
        self.assertFalse(may_see_private(self.request_for(self.kenya)))

    def test_a_member_may(self):
        self.assertTrue(self.request_may_see(self.kenya, self.member))

    def test_a_member_of_another_organisation_may_not(self):
        self.assertFalse(self.request_may_see(self.kenya, self.outsider))

    def test_a_superuser_may_on_any_host(self):
        self.assertTrue(self.request_may_see(self.kenya, self.superuser))
        self.assertTrue(self.request_may_see(self.uganda, self.superuser))

    def test_a_deactivated_member_may_not(self):
        self.member.is_active = False
        self.assertFalse(self.request_may_see(self.kenya, self.member))

    def test_a_revoked_membership_takes_effect_immediately(self):
        self.assertTrue(self.request_may_see(self.kenya, self.member))
        self.member.organisation_memberships.all().delete()
        self.assertFalse(self.request_may_see(self.kenya, self.member))
