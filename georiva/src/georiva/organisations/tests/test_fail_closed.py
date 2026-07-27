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
from django.test import TestCase, override_settings
from django.urls import URLPattern, URLResolver, get_resolver, reverse
from wagtail import hooks

from georiva.core.models import Asset, Catalog, Collection, Item, Unit, Variable
from georiva.ingestion.models import ManualUploadConfig
from georiva.organisations.access import LOOKUP_ATTR
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.sources.models import DataFeed
from georiva.sources.tests.support import ensure_base_datafeed_viewset

from .factories import PASSWORD, add_member, make_user

# The colliding slug both organisations use.
SHARED_SLUG = "forecast"


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
    config = ManualUploadConfig.objects.create(
        catalog=catalog, name=name, valid_time_format=ManualUploadConfig.ValidTimeFormat.YYYYMMDD
    )
    return {
        "catalog": catalog,
        "collection": collection,
        "variable": variable,
        "item": item,
        "asset": asset,
        "feed": feed,
        "config": config,
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

    def _admin_urls_with_a_single_pk(self):
        """Concrete admin URLs built by substituting Uganda's pks for the ids."""
        pk_by_pattern = {
            r"catalog": self.uganda_tree["catalog"].pk,
            r"collection": self.uganda_tree["collection"].pk,
            r"item": self.uganda_tree["item"].pk,
            r"feed": self.uganda_tree["feed"].pk,
            r"product": None,
        }
        urls = []
        for pattern, prefix in _flatten(get_resolver().url_patterns):
            route = str(pattern.pattern)
            if not prefix.startswith("admin/"):
                continue
            ids = re.findall(r"<(?:int:)?(\w+)>", route)
            if len(ids) != 1:
                continue
            name = ids[0]
            pk = next(
                (pk for key, pk in pk_by_pattern.items() if key in prefix or key in name),
                None,
            )
            if pk is None:
                continue
            urls.append("/" + re.sub(r"<[^>]+>", str(pk), prefix + route))
        return urls

    def test_no_admin_url_serves_another_organisations_row(self):
        checked = 0
        for url in self._admin_urls_with_a_single_pk():
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
        self.assertGreater(checked, 0, "the sweep found no admin URLs to check")


class OrgOwnedLookupDeclarationTests(TestCase):
    """Every model reachable from a catalog says how it reaches its organisation."""

    def test_declared_lookups_are_real_orm_paths(self):
        from django.apps import apps

        checked = 0
        for model in apps.get_models():
            lookup = getattr(model, LOOKUP_ATTR, None)
            if not lookup:
                continue
            checked += 1
            with self.subTest(model=model._meta.label):
                # Raises FieldError if any segment of the declared path is wrong.
                model._default_manager.filter(**{f"{lookup}__isnull": True}).query
        self.assertGreater(checked, 5)


class NoUnscopedObjectLookupTests(TestCase):
    """The static half: org-owned apps do not resolve rows by bare pk.

    A grep, because the dynamic sweep above can only reach views that are
    registered and reachable — a helper resolving an object for a POST body is
    not, and this catches it at the source.
    """

    APPS = ["core", "sources", "ingestion", "visualization", "virtual_zarr", "analysis"]

    def test_org_owned_apps_do_not_call_get_object_or_404(self):
        import pathlib

        root = pathlib.Path(__file__).resolve().parents[2]
        offenders = []
        for app in self.APPS:
            for path in (root / app).rglob("*.py"):
                if "test" in path.name or "/tests/" in str(path) or "migrations" in path.parts:
                    continue
                text = path.read_text()
                if re.search(r"(?<!org_)(?<!parent_or_404_)\bget_object_or_404\b", text):
                    offenders.append(str(path.relative_to(root)))
        self.assertEqual(
            offenders, [],
            "these resolve rows without an organisation; use "
            "organisations.access.get_org_object_or_404 instead",
        )


def _flatten(patterns, prefix=""):
    for entry in patterns:
        if isinstance(entry, URLResolver):
            yield from _flatten(entry.url_patterns, prefix + str(entry.pattern))
        elif isinstance(entry, URLPattern):
            yield entry, prefix
