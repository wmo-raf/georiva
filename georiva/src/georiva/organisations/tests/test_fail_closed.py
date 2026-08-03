"""Proof, rather than assertion, that the admin is closed to the wrong organisation.

Per-view tests can only cover the views somebody remembered to write a test for.
These walk the admin as it is actually registered — every URL Wagtail exposes for
an org-owned model, with another organisation's primary keys substituted in — so
a view added later without scoping fails here rather than in production.

The fixture is two complete organisations with colliding slugs. That collision is
deliberate: per-org slug uniqueness (#267) makes it legal, and it is the case
where an unscoped lookup does not merely leak but answers with the wrong
institution's data.
"""
import re

from django.contrib.auth.models import Group, Permission
from django.db import models
from django.test import TestCase, override_settings
from django.urls import get_resolver, reverse
from wagtail import hooks

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.ingestion.models import (
    ManualUploadConfig,
    ManualUploadConfigVariable,
    UploadSession,
)
from georiva.organisations import access
from georiva.organisations.access import LOOKUP_ATTR
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.sources.models import (
    DataFeed,
    DataFeedCollectionLink,
    DerivedProduct,
    FetchRun,
)
from georiva.sources.tests.support import ensure_base_datafeed_viewset

from .factories import PASSWORD, add_member, make_user
from .urlsweep import flatten_url_patterns

# The colliding slug both organisations use.
SHARED_SLUG = "forecast"

#: Admin URLs the listing sweep must not simply GET:
#: - failwhale is Wagtail's deliberate-500 page, where raising is the point;
#: - the SSE feeds never close, so a test client GET would hang forever. Their
#:   org filtering is covered directly in ingestion's SSE tests instead.
_SWEEP_EXEMPT_LISTINGS = frozenset({
    "admin/failwhale/",
    "admin/api/ingestion/events/",
    "admin/api/ingestion/acquisition/events/",
})


class Undeclared(models.Model):
    """A model nobody has ruled on, standing in for the one somebody forgets.

    Unmanaged and never queried — the point is what the scoping helpers do when
    handed a model that declares nothing, which they decide before touching the
    database.
    """

    class Meta:
        managed = False
        app_label = "organisations"


def build_org_page(organisation, *, name):
    """A page in the organisation's portal, named after it like everything else.

    Pages are org-owned through the Site → root-page link rather than an FK, so
    the sweeps reach them the same way they reach everything else: by putting
    this page's id into every admin URL that names a page.
    """
    from georiva.pages.home.models import HomePage

    page = HomePage(title=name, slug=SHARED_SLUG, hero_heading=name)
    organisation.site.root_page.add_child(instance=page)
    return page


def build_org_tree(organisation, *, name):
    """One organisation's whole data tree, down to an asset and an upload config."""
    catalog = Catalog.objects.create(
        organisation=organisation,
        name=name,
        slug=SHARED_SLUG,
        file_format=Catalog.FileFormat.GEOTIFF,
    )
    collection = Collection.objects.create(catalog=catalog, name=name, slug=SHARED_SLUG)
    unit, _ = Unit.objects.get_or_create(name="Celsius", symbol="C")
    variable = Variable.objects.create(
        collection=collection, name=name, slug=SHARED_SLUG,
        unit=unit, value_min=0, value_max=1,
    )
    item = Item.objects.create(collection=collection, time="2026-01-01T00:00:00Z")
    asset = Asset.objects.create(item=item, variable=variable, href="x.tif")
    feed = DataFeed.objects.create(name=name, catalog=catalog)
    link = DataFeedCollectionLink.objects.create(
        data_feed=feed, collection=collection, definition_key=SHARED_SLUG
    )
    product = DerivedProduct.objects.create(
        data_feed=feed, definition_key=SHARED_SLUG, recipe_type="promotion"
    )
    run = FetchRun.objects.create(data_feed=feed)
    config = ManualUploadConfig.objects.create(
        catalog=catalog, name=name, valid_time_format=ManualUploadConfig.ValidTimeFormat.YYYYMMDD
    )
    config_variable = ManualUploadConfigVariable.objects.create(
        config=config, collection=collection, variable_name=SHARED_SLUG
    )
    session = UploadSession.objects.create(catalog=catalog)
    return {
        "catalog": catalog,
        "collection": collection,
        "variable": variable,
        "item": item,
        "asset": asset,
        "feed": feed,
        "link": link,
        "product": product,
        "run": run,
        "config": config,
        "config_variable": config_variable,
        "session": session,
        "page": build_org_page(organisation, name=name),
    }


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class CrossOrgAdminTests(TestCase):
    """One organisation's member, dialling their own host, chasing the other's pks."""

    @classmethod
    def setUpTestData(cls):
        # The feed listing resolves edit/delete URLs through the viewset registry,
        # which production fills per plugin child model.
        ensure_base_datafeed_viewset()
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_tree = build_org_tree(cls.kenya, name="Kenya Forecast")
        cls.uganda_tree = build_org_tree(cls.uganda, name="Uganda Forecast")

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        # Everything an operator can normally do, so nothing below is refused for
        # want of a capability — leaving tenancy as the only thing under test.
        everything = Group.objects.create(name="everything")
        everything.permissions.add(*Permission.objects.all())
        self.user.groups.add(everything)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    # -- listings ----------------------------------------------------------

    def test_catalog_index_shows_only_this_organisations_catalogs(self):
        response = self.client.get(reverse("catalog:index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Kenya Forecast")
        self.assertNotContains(response, "Uganda Forecast")

    def test_data_feed_listing_shows_only_this_organisations_feeds(self):
        response = self.client.get(reverse("data_feed_list"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda Forecast")

    def test_admin_search_does_not_reach_across_organisations(self):
        response = self.client.get(reverse("catalog:index_results"), {"q": "Forecast"})
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda Forecast")

    # -- choosers ----------------------------------------------------------

    def test_chooser_list_offers_only_this_organisations_catalogs(self):
        response = self.client.get(reverse("catalog_chooser:choose"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda Forecast")

    def test_chooser_pk_endpoint_refuses_another_organisations_catalog(self):
        url = reverse("catalog_chooser:chosen", args=[self.uganda_tree["catalog"].pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_chooser_pk_endpoint_still_serves_this_organisations_catalog(self):
        url = reverse("catalog_chooser:chosen", args=[self.kenya_tree["catalog"].pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_chooser_multiple_endpoint_drops_another_organisations_catalog(self):
        response = self.client.get(
            reverse("catalog_chooser:chosen_multiple"),
            {"id": [self.kenya_tree["catalog"].pk, self.uganda_tree["catalog"].pk]},
        )
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda Forecast")

    # -- object views ------------------------------------------------------

    def test_editing_another_organisations_catalog_is_not_found(self):
        url = reverse("catalog:edit", args=[self.uganda_tree["catalog"].pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_deleting_another_organisations_catalog_is_not_found(self):
        url = reverse("catalog:delete", args=[self.uganda_tree["catalog"].pk])
        self.assertEqual(self.client.post(url).status_code, 404)
        self.assertTrue(Catalog.objects.filter(pk=self.uganda_tree["catalog"].pk).exists())

    def test_another_organisations_collection_items_are_not_found(self):
        url = reverse("collection_items_list", args=[self.uganda_tree["collection"].pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_another_organisations_data_feed_is_not_found(self):
        url = reverse("data_feed_detail", args=[self.uganda_tree["feed"].pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_another_organisations_upload_config_is_not_found(self):
        url = reverse("manual_upload_config_edit", args=[self.uganda_tree["config"].pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_another_organisations_item_preview_is_not_found(self):
        url = reverse("item_preview", args=[self.uganda_tree["item"].pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_the_same_views_serve_this_organisations_rows(self):
        # The mirror of every test above: scoping that 404s everything would pass
        # them all and break the admin.
        for name, args in [
            ("catalog:edit", [self.kenya_tree["catalog"].pk]),
            ("collection_items_list", [self.kenya_tree["collection"].pk]),
            ("data_feed_detail", [self.kenya_tree["feed"].pk]),
            ("manual_upload_config_edit", [self.kenya_tree["config"].pk]),
            ("item_preview", [self.kenya_tree["item"].pk]),
        ]:
            with self.subTest(view=name):
                self.assertEqual(self.client.get(reverse(name, args=args)).status_code, 200)

    # -- writes ------------------------------------------------------------

    def test_a_posted_foreign_catalog_id_fails_validation(self):
        """The residual a scoped chooser leaves: a hand-crafted POST."""
        response = self.client.post(
            reverse("collection:add"),
            {
                "catalog": self.uganda_tree["catalog"].pk,
                "name": "Smuggled",
                "slug": "smuggled",
                "visibility": Collection.Visibility.PUBLIC,
            },
        )
        self.assertNotEqual(response.status_code, 302)
        self.assertFalse(Collection.objects.filter(slug="smuggled").exists())

    # -- membership --------------------------------------------------------

    def test_a_revoked_membership_fails_closed_on_the_next_request(self):
        self.assertEqual(self.client.get(reverse("catalog:index")).status_code, 200)
        OrganisationMembership.objects.filter(
            user=self.user, organisation=self.kenya
        ).delete()
        self.assertEqual(self.client.get(reverse("catalog:index")).status_code, 403)

    def test_a_member_of_another_organisation_cannot_enter_this_admin(self):
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        self.assertEqual(self.client.get(reverse("catalog:index")).status_code, 403)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class AdminUrlSweepTests(TestCase):
    """Every registered admin URL taking a pk, dialled with a foreign one.

    Deliberately generated rather than listed: the point is to catch the view
    nobody thought to write a test for.
    """

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        build_org_tree(cls.kenya, name="Kenya Forecast")
        cls.uganda_tree = build_org_tree(cls.uganda, name="Uganda Forecast")

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        everything = Group.objects.create(name="everything")
        everything.permissions.add(*Permission.objects.all())
        self.user.groups.add(everything)
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    def _foreign_pks(self):
        """Uganda's primary keys, keyed by the URL id names that name them."""
        tree = self.uganda_tree
        return {
            "catalog": tree["catalog"].pk,
            "collection": tree["collection"].pk,
            "collection_pk": tree["collection"].pk,
            "item": tree["item"].pk,
            "item_pk": tree["item"].pk,
            "item_id": tree["item"].pk,
            "feed": tree["feed"].pk,
            "feed_pk": tree["feed"].pk,
            "product": tree["product"].pk,
            "product_pk": tree["product"].pk,
            "link": tree["link"].pk,
            "link_pk": tree["link"].pk,
            "run": tree["run"].pk,
            "run_pk": tree["run"].pk,
            "config": tree["config"].pk,
            "session": tree["session"].pk,
            "session_id": tree["session"].pk,
            "var": tree["config_variable"].pk,
            "var_pk": tree["config_variable"].pk,
            # Matches page_id, parent_page_id and page_to_move_id by substring;
            # destination_id names a page without saying so, so it is listed too.
            "page": tree["page"].pk,
            "destination_id": tree["page"].pk,
        }

    def _resolve_id(self, name, prefix, pks):
        """The foreign pk a URL id refers to, by its own name or its URL's subject.

        Both halves matter: `<int:pk>` says nothing on its own and has to be read
        from the path it sits in, while `<int:product_pk>` names its model even on
        a path about feeds.
        """
        if name in pks:
            return pks[name]
        for key, pk in pks.items():
            if key in name:
                return pk
        for key, pk in pks.items():
            if key in prefix:
                return pk
        return None

    def _foreign_admin_urls(self):
        """Every registered admin URL, with Uganda's pks in place of its ids.

        Multi-id routes included — feeds' products, links and runs all sit on
        two-id paths, and those are precisely the nested views a per-view test is
        most likely to miss.
        """
        pks = self._foreign_pks()
        urls = []
        for pattern, prefix in flatten_url_patterns(get_resolver().url_patterns):
            route = str(pattern.pattern)
            if not prefix.startswith("admin/"):
                continue
            names = re.findall(r"<(?:[a-z]+:)?(\w+)>", route)
            if not names:
                continue
            resolved = [self._resolve_id(name, prefix + route, pks) for name in names]
            if any(pk is None for pk in resolved):
                continue
            url = prefix + route
            for pk in resolved:
                url = re.sub(r"<[^>]+>", str(pk), url, count=1)
            urls.append("/" + url)
        return urls

    def test_the_sweep_reaches_the_page_admin(self):
        """Pages are the one org-owned thing with no FK, so prove they are swept."""
        page_urls = [url for url in self._foreign_admin_urls() if url.startswith("/admin/pages/")]
        self.assertGreater(len(page_urls), 5)

    def test_no_admin_url_serves_another_organisations_row(self):
        checked = 0
        for url in self._foreign_admin_urls():
            response = self.client.get(url)
            if response.status_code in (301, 302, 405):
                # A redirect or a POST-only view resolves nothing by itself.
                continue
            checked += 1
            with self.subTest(url=url):
                self.assertIn(
                    response.status_code, (403, 404),
                    f"{url} answered {response.status_code} for another organisation's row",
                )
        self.assertGreater(checked, 10, "the sweep found too few admin URLs to be meaningful")

    def test_no_admin_listing_names_another_organisation(self):
        """The other half: listings take no pk, so nothing above reaches them.

        Every organisation's fixtures are named after it, so the other's name
        appearing anywhere in a rendered admin page is the leak itself.
        """
        checked = 0
        for pattern, prefix in flatten_url_patterns(get_resolver().url_patterns):
            route = str(pattern.pattern)
            if not prefix.startswith("admin/") or re.search(r"<[^>]+>", prefix + route):
                continue
            if prefix + route in _SWEEP_EXEMPT_LISTINGS:
                continue
            response = self.client.get("/" + prefix + route)
            if response.status_code != 200 or not response.get("Content-Type", "").startswith(
                ("text/html", "application/json")
            ):
                continue
            checked += 1
            with self.subTest(url=prefix + route):
                self.assertNotIn(
                    b"Uganda Forecast", response.content,
                    f"/{prefix}{route} names another organisation's data",
                )
        self.assertGreater(checked, 5, "the sweep found too few admin listings to be meaningful")


class OrgOwnedLookupDeclarationTests(TestCase):
    """Every model in this codebase says where it stands on tenancy.

    This is what makes deny-by-default safe to enforce at the scoping seam: the
    helpers can refuse an undeclared model outright, because no model reaches
    production undeclared.
    """

    def _own_models(self):
        from django.apps import apps

        return [
            model for model in apps.get_models()
            if model.__module__.startswith(access.OWN_MODULE_PREFIX)
            # The undeclared stand-in below is undeclared on purpose.
            and model is not Undeclared
        ]

    def test_every_model_declares_its_tenancy(self):
        undeclared = [
            model._meta.label for model in self._own_models()
            if not getattr(model, LOOKUP_ATTR, None)
        ]
        self.assertEqual(
            undeclared, [],
            f"declare {LOOKUP_ATTR} on these: the ORM path to Organisation, or one of "
            f"SHARED_REFERENCE_DATA / ORGANISATION_SELF / NOT_ORM_SCOPABLE",
        )

    def test_declared_paths_are_real_orm_paths(self):
        checked = 0
        for model in self._own_models():
            lookup = getattr(model, LOOKUP_ATTR, None)
            if not lookup or lookup in access.SENTINELS:
                continue
            checked += 1
            with self.subTest(model=model._meta.label):
                # Raises FieldError if any segment of the declared path is wrong.
                model._default_manager.filter(**{f"{lookup}__isnull": True}).query
        self.assertGreater(checked, 5)

    def test_scoping_refuses_a_model_that_declares_nothing(self):
        """The property the test above buys: silence is refused, not passed through."""
        from django.core.exceptions import ImproperlyConfigured
        from django.test import RequestFactory

        request = RequestFactory().get("/admin/")
        request.active_org = self.kenya

        with self.assertRaises(ImproperlyConfigured):
            access.scope_or_pass(request, Undeclared.objects.all())

    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")


class NoUnscopedObjectLookupTests(TestCase):
    """The static half: org-owned apps do not resolve rows by bare pk.

    A grep, because the dynamic sweep above can only reach views that are
    registered and reachable — a helper resolving an object for a POST body is
    not, and this catches it at the source.
    """

    APPS = [
        "core", "sources", "ingestion", "visualization", "virtual_zarr", "analysis",
        # The public plane resolves the same rows from the same slugs, and its
        # callers are anonymous — it earns the rule rather than being exempt
        # from it (#271).
        "stac", "edr", "pages",
    ]

    def test_org_owned_apps_do_not_call_get_object_or_404(self):
        import pathlib

        root = pathlib.Path(__file__).resolve().parents[2]
        offenders = []
        for app in self.APPS:
            for path in (root / app).rglob("*.py"):
                if "test" in path.name or "/tests/" in str(path) or "migrations" in path.parts:
                    continue
                text = path.read_text()
                # The two sanctioned helpers are named so that neither contains
                # this substring: get_org_object_or_404 and
                # get_via_scoped_parent_or_404.
                if re.search(r"\bget_object_or_404\b", text):
                    offenders.append(str(path.relative_to(root)))
        self.assertEqual(
            offenders, [],
            "these resolve rows without an organisation; use "
            "organisations.access.get_org_object_or_404 instead",
        )
