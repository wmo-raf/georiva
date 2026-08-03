"""Proof that the tile side-door is shut.

ADR 0014 gave collections a ``private`` tier and hid them on all four Django
planes, then recorded the hole it could not close: Titiler and Martin answer
without resolving a tenant, so a private collection's *rasters* stayed readable
by anyone who could spell the URL. Operators were told in as many words not to
treat the tier as a security boundary for tiles.

This is the gate that lets them. Nginx asks Django before it proxies, and these
tests are that question and its answers — the same three audiences the other
planes already agree on, asked over a tile URL instead of a STAC one.

Two things are worth stating about the answers, because both look like
inconsistencies and neither is:

*Denial is 403 here and 404 everywhere else.* ``auth_request`` understands only
2xx, 401 and 403; anything else fails the outer request with a 500 rather than
denying it. So the view speaks the protocol and one ``error_page`` line in
``deploy/nginx/nginx.conf`` turns it into the 404 the caller sees. The tests
below assert the status the *gate* returns, which is the one the code controls.

*The host is checked here and not on tile-config.* Both carry the organisation
in the path, but this subrequest also carries the browser's real Host, so there
is something to check the segment against — and where there is, ADR 0013 says it
wins.
"""
from django.db import connection
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from georiva.accounts.models import ApiKey
from georiva.core.machine_plane import scope_of
from georiva.core.models import Collection
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from .factories import PASSWORD, add_member, make_user
from .test_private_visibility import (
    INTERNAL_SLUG,
    PRIVATE_SLUG,
    PUBLIC_SLUG,
    build_tiered_catalog,
)

#: Statuses the gate is allowed to answer. Nginx turns anything else into a 500,
#: so a fourth value appearing here would be a tile failing rather than a tile
#: being refused.
ALLOW = 204
DENY = 403


def titiler_uri(collection_slug, org="kenya", catalog="forecast", variable=None):
    """A Titiler tile URL of the shape ``core.machine_plane`` builds."""
    variable = variable or collection_slug
    return (
        f"/titiler/{org}/{catalog}/{collection_slug}/{variable}"
        f"/tiles/WebMercatorQuad/6/38/32.png?time=2026-03-01T12:00:00Z"
    )


def martin_uri(collection_slug, org="kenya", catalog="forecast"):
    """A Martin zonal-stats tile URL, triple in the query as ADR 0013 requires."""
    return (
        f"/martin/boundary_stats/6/38/32"
        f"?org={org}&catalog={catalog}&collection={collection_slug}"
        f"&variable={collection_slug}&time=2026-03-01T12:00:00Z&admin_level=1"
    )


class ScopeOfTests(TestCase):
    """The address reader, on its own.

    It is the inverse of the URL builders it sits beside, and the reason it sits
    beside them: a reader that drifts from the writer authorises one collection
    while the tile server reads another.
    """

    def test_a_titiler_tile_scopes_to_its_org_catalog_and_collection(self):
        self.assertEqual(
            tuple(scope_of(titiler_uri("rainfall"))),
            ("kenya", "forecast", "rainfall"),
        )

    def test_a_titiler_preview_scopes_the_same_way_as_a_tile(self):
        uri = "/titiler/kenya/forecast/rainfall/rain/preview.webp?time=x"
        self.assertEqual(tuple(scope_of(uri)), ("kenya", "forecast", "rainfall"))

    def test_a_titiler_url_stopping_short_of_a_variable_scopes_to_nothing(self):
        """It addresses no data, so there is nothing to authorise."""
        self.assertIsNone(scope_of("/titiler/kenya/forecast/rainfall/"))

    def test_the_tile_servers_own_docs_scope_to_nothing(self):
        self.assertIsNone(scope_of("/titiler/openapi.json"))

    def test_a_martin_tile_scopes_to_the_triple_in_its_query(self):
        self.assertEqual(
            tuple(scope_of(martin_uri("rainfall"))),
            ("kenya", "forecast", "rainfall"),
        )

    def test_a_martin_tile_missing_part_of_the_triple_scopes_to_nothing(self):
        self.assertIsNone(
            scope_of("/martin/boundary_stats/6/38/32?org=kenya&catalog=forecast")
        )

    def test_a_martin_tile_with_a_blank_triple_member_scopes_to_nothing(self):
        self.assertIsNone(
            scope_of("/martin/boundary_stats/6/38/32?org=kenya&catalog=forecast&collection=")
        )

    def test_a_repeated_parameter_scopes_to_nothing(self):
        """Which duplicate Martin would pick is not visible from here.

        Authorising one and having the tile server read the other is the whole
        failure this gate exists to prevent, so a request that leaves the choice
        open is refused rather than guessed at.
        """
        self.assertIsNone(
            scope_of(
                "/martin/boundary_stats/6/38/32"
                "?org=kenya&org=uganda&catalog=forecast&collection=rainfall"
            )
        )

    def test_martins_other_endpoints_scope_to_nothing(self):
        self.assertIsNone(scope_of("/martin/catalog"))

    def test_an_unrelated_path_scopes_to_nothing(self):
        self.assertIsNone(scope_of("/api/stac/forecast/"))

    def test_an_empty_uri_scopes_to_nothing(self):
        self.assertIsNone(scope_of(""))

    def test_a_percent_encoded_slug_is_left_encoded(self):
        """So it matches no row and is denied — the right answer for an address
        trying to be two things at once."""
        scope = scope_of("/titiler/kenya/forecast/priv%61te/v/preview.webp")
        self.assertEqual(scope.collection, "priv%61te")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class TileGatewayTestCase(TestCase):
    """Two organisations, each carrying all three tiers, asked on Kenya's host."""

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        build_tiered_catalog(cls.kenya)
        build_tiered_catalog(cls.uganda)

        cls.member = make_user("kenya-member")
        add_member(cls.member, cls.kenya)
        cls.outsider = make_user("uganda-member")
        add_member(cls.outsider, cls.uganda)
        cls.superuser = make_user("instance-admin", superuser=True)

        # Both credentials, minted once. A key is the only way to reach a
        # private tile from a client that holds a URL and nothing else, so it
        # belongs to the fixture rather than to the one class that names it.
        _, cls.member_key = ApiKey.objects.mint(user=cls.member, name="qgis")
        _, cls.outsider_key = ApiKey.objects.mint(user=cls.outsider, name="qgis")

    def setUp(self):
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def login(self, user):
        self.client.login(username=user.username, password=PASSWORD)

    def ask(self, uri, **extra):
        """Put nginx's question to the gate: may this URI be proxied?"""
        return self.client.get(
            reverse("tile_auth"), HTTP_X_ORIGINAL_URI=uri, **extra
        ).status_code


class TitilerTilesTests(TileGatewayTestCase):
    """The raster plane, across the three tiers and the three audiences."""

    def test_a_public_tile_is_served_anonymously(self):
        self.assertEqual(self.ask(titiler_uri(PUBLIC_SLUG)), ALLOW)

    def test_a_private_tile_is_refused_anonymously(self):
        self.assertEqual(self.ask(titiler_uri(PRIVATE_SLUG)), DENY)

    def test_a_private_tile_is_served_to_a_member(self):
        self.login(self.member)
        self.assertEqual(self.ask(titiler_uri(PRIVATE_SLUG)), ALLOW)

    def test_a_private_tile_is_refused_to_another_organisations_member(self):
        self.login(self.outsider)
        self.assertEqual(self.ask(titiler_uri(PRIVATE_SLUG)), DENY)

    def test_a_private_tile_is_served_to_the_instance_admin(self):
        self.login(self.superuser)
        self.assertEqual(self.ask(titiler_uri(PRIVATE_SLUG)), ALLOW)

    def test_an_internal_tile_is_refused_even_to_a_member(self):
        """``internal`` is in neither queryset, so it needs no rule of its own."""
        self.login(self.member)
        self.assertEqual(self.ask(titiler_uri(INTERNAL_SLUG)), DENY)

    def test_an_internal_tile_is_refused_to_the_instance_admin(self):
        self.login(self.superuser)
        self.assertEqual(self.ask(titiler_uri(INTERNAL_SLUG)), DENY)

    def test_a_tile_of_a_collection_that_does_not_exist_is_refused(self):
        self.assertEqual(self.ask(titiler_uri("no-such-collection")), DENY)

    def test_a_deactivated_collections_tile_is_refused(self):
        Collection.objects.filter(
            catalog__organisation=self.kenya, slug=PUBLIC_SLUG
        ).update(is_active=False)
        self.assertEqual(self.ask(titiler_uri(PUBLIC_SLUG)), DENY)

    def test_a_uri_the_grammar_does_not_recognise_is_refused(self):
        self.assertEqual(self.ask("/titiler/openapi.json"), DENY)

    def test_a_missing_original_uri_is_refused(self):
        self.assertEqual(self.client.get(reverse("tile_auth")).status_code, DENY)


class MartinZonalStatsTilesTests(TileGatewayTestCase):
    """The choropleth derived from a collection is gated as the collection is.

    ``georiva_boundary_stats`` reads statistics rows joined all the way back to
    the collection and filters on nothing about its visibility, so an ungated
    Martin is a second reading of the same private data — coarser, but the
    numbers are the numbers.
    """

    def test_a_public_choropleth_is_served_anonymously(self):
        self.assertEqual(self.ask(martin_uri(PUBLIC_SLUG)), ALLOW)

    def test_a_private_choropleth_is_refused_anonymously(self):
        self.assertEqual(self.ask(martin_uri(PRIVATE_SLUG)), DENY)

    def test_a_private_choropleth_is_served_to_a_member(self):
        self.login(self.member)
        self.assertEqual(self.ask(martin_uri(PRIVATE_SLUG)), ALLOW)

    def test_a_private_choropleth_is_refused_to_another_organisations_member(self):
        self.login(self.outsider)
        self.assertEqual(self.ask(martin_uri(PRIVATE_SLUG)), DENY)

    def test_an_internal_choropleth_is_refused_even_to_a_member(self):
        self.login(self.member)
        self.assertEqual(self.ask(martin_uri(INTERNAL_SLUG)), DENY)

    def test_martins_source_listing_is_refused(self):
        self.assertEqual(self.ask("/martin/catalog"), DENY)


class TheHostNamesTheOrganisationTests(TileGatewayTestCase):
    """The path segment is checked against the Host, because here there is one.

    ADR 0013 put the organisation in the path precisely where nothing could
    check it. This subrequest carries the browser's own Host, so the exception's
    stated reason does not hold and the ordinary rule applies: the Host is the
    authority and the segment may only agree with it.
    """

    def test_a_segment_naming_another_organisation_is_refused(self):
        self.login(self.member)
        self.assertEqual(
            self.ask(titiler_uri(PUBLIC_SLUG, org="uganda")), DENY,
        )

    def test_a_member_cannot_reach_their_own_private_tile_from_the_wrong_host(self):
        self.login(self.member)
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        self.assertEqual(self.ask(titiler_uri(PRIVATE_SLUG, org="kenya")), DENY)

    def test_the_same_slug_on_another_host_serves_that_hosts_collection(self):
        """Slugs collide across organisations (#267); the host decides whose."""
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        self.assertEqual(
            self.ask(titiler_uri(PUBLIC_SLUG, org="uganda")), ALLOW,
        )

    def test_a_host_belonging_to_no_organisation_is_refused(self):
        """And refused in a language nginx understands, rather than 404ing.

        ``auth_request`` fails the tile with a 500 on any status but 2xx/401/403,
        so the middleware lets this path through its unknown-host 404 and the
        view denies it here instead.
        """
        self.client.defaults["HTTP_HOST"] = "nobody.georiva.test"
        self.assertEqual(self.ask(titiler_uri(PUBLIC_SLUG)), DENY)


class ApiKeysReachPrivateTilesTests(TileGatewayTestCase):
    """The transport ADR 0014 built for clients that can only take a URL.

    QGIS and every web map library are handed a tile template and nothing else,
    so ``?api_key=`` is how a private raster reaches them. Nginx forwards the
    parameter onto the subrequest for exactly this.
    """

    def ask_with_key(self, uri, secret):
        return self.client.get(
            reverse("tile_auth"), {"api_key": secret}, HTTP_X_ORIGINAL_URI=uri,
        ).status_code

    def test_a_members_key_reaches_a_private_tile(self):
        self.assertEqual(
            self.ask_with_key(titiler_uri(PRIVATE_SLUG), self.member_key), ALLOW,
        )

    def test_a_members_key_reaches_a_private_choropleth(self):
        self.assertEqual(
            self.ask_with_key(martin_uri(PRIVATE_SLUG), self.member_key), ALLOW,
        )

    def test_another_organisations_key_does_not(self):
        """The key carries identity and no organisation; the host decides the rest."""
        self.assertEqual(
            self.ask_with_key(titiler_uri(PRIVATE_SLUG), self.outsider_key), DENY,
        )

    def test_a_members_key_still_does_not_reach_an_internal_tile(self):
        self.assertEqual(
            self.ask_with_key(titiler_uri(INTERNAL_SLUG), self.member_key), DENY,
        )

    def test_a_bearer_header_reaches_a_private_tile_too(self):
        self.assertEqual(
            self.ask(
                titiler_uri(PRIVATE_SLUG),
                HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
            ),
            ALLOW,
        )

    def test_a_broken_key_is_unauthorised_rather_than_absent(self):
        """The one denial nginx passes through unchanged.

        This answer is the same whether the collection exists or not, so it
        gives nothing away — and it is what tells a scripting user their
        credential expired instead of leaving them hunting a vanished dataset
        (ADR 0014). Nginx maps 403 to 404 and leaves 401 alone for that reason.
        """
        self.assertEqual(
            self.ask_with_key(titiler_uri(PUBLIC_SLUG), "grv_not-a-real-key"), 401,
        )


class ThePublicFastPathTests(TileGatewayTestCase):
    """One indexed query for the tile that most requests are.

    This plane answers per tile rather than per page, so the common case must
    not pay for the uncommon one: the public queryset is asked first and settles
    the answer on its own, and only a miss goes on to ask who is calling.

    Counted against the collection table rather than in total, because the
    request also carries the middleware's own host resolution and this is a
    claim about the gate's work, not the stack's.
    """

    def queries_against(self, table, uri, **extra):
        with CaptureQueriesContext(connection) as captured:
            self.ask(uri, **extra)
        return [q["sql"] for q in captured.captured_queries if table in q["sql"]]

    def collection_queries(self, uri, **extra):
        return self.queries_against(Collection._meta.db_table, uri, **extra)

    def membership_queries(self, uri, **extra):
        return self.queries_against(OrganisationMembership._meta.db_table, uri, **extra)

    def test_a_public_tile_is_settled_by_a_single_collection_query(self):
        self.assertEqual(len(self.collection_queries(titiler_uri(PUBLIC_SLUG))), 1)

    def test_that_query_narrows_on_the_indexed_columns(self):
        """org, catalog slug and collection slug — not a scan the tier filters."""
        sql = self.collection_queries(titiler_uri(PUBLIC_SLUG))[0]
        self.assertIn("organisation_id", sql)
        self.assertIn("slug", sql)

    def test_a_public_tile_reads_no_membership_row_at_all(self):
        self.assertEqual(self.membership_queries(titiler_uri(PUBLIC_SLUG)), [])

    def test_a_private_tile_pays_for_the_second_look(self):
        """The miss is the cost of the tier, and it falls only on the tier."""
        self.assertEqual(
            len(self.collection_queries(
                titiler_uri(PRIVATE_SLUG),
                HTTP_AUTHORIZATION=f"Bearer {self.member_key}",
            )),
            2,
        )
