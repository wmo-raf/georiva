"""
STAC serving must expose only `public` collections — `internal` derivation
intermediates are read by the engine but never served.
"""

from datetime import UTC, datetime

from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item, Unit, Variable
from georiva.core.testing import ANALYST_STOPS, make_style
from georiva.organisations.testing import dial_org, make_organisation


class STACVisibilityTests(TestCase):
    def setUp(self):
        # The API roots are per organisation and resolved from the Host, so the
        # client has to dial the org that owns these fixtures.
        dial_org(self.client)
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(), name="CMIP6", slug="cmip6", file_format="geotiff"
        )
        self.unit = Unit.objects.create(name="Celsius", symbol="C")

        self.public = Collection.objects.create(
            catalog=self.catalog,
            name="Tas",
            slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        Variable.objects.create(
            collection=self.public,
            slug="tas",
            name="tas",
            unit=self.unit,
            value_min=0,
            value_max=50,
        )

        self.internal = Collection.objects.create(
            catalog=self.catalog,
            name="Tas anomaly",
            slug="tas-anomaly",
            visibility=Collection.Visibility.INTERNAL,
        )
        Variable.objects.create(
            collection=self.internal,
            slug="tas",
            name="tas anomaly",
            unit=self.unit,
            value_min=-25,
            value_max=25,
        )

    def test_collection_list_excludes_internal(self):
        response = self.client.get(reverse("stac:collection-list", args=["cmip6"]))
        self.assertEqual(response.status_code, 200)

        ids = {c["id"] for c in response.json()["collections"]}
        self.assertIn("tas/tas", ids)
        self.assertNotIn("tas-anomaly/tas", ids)

    def test_collection_detail_404_for_internal(self):
        ok = self.client.get(reverse("stac:collection-detail", args=["cmip6", "tas", "tas"]))
        self.assertEqual(ok.status_code, 200)

        hidden = self.client.get(
            reverse(
                "stac:collection-detail",
                args=["cmip6", "tas-anomaly", "tas"],
            )
        )
        self.assertEqual(hidden.status_code, 404)


class STACRenderExtensionTests(TestCase):
    """Named styles are discovered through the standard Render extension
    (ADR 0023): no bespoke vocabulary, so STAC Browser and Titiler-based
    clients read them natively. The variable's STAC Collection enumerates
    each style with its colormap and rescale; its Items reference the same
    render names without repeating the colormap page after page.
    """

    RENDER_SCHEMA = "https://stac-extensions.github.io/render/v2.0.0/schema.json"

    def setUp(self):
        dial_org(self.client)
        self.catalog = Catalog.objects.create(
            organisation=make_organisation(),
            name="CMIP6",
            slug="cmip6",
            file_format="geotiff",
        )
        self.unit = Unit.objects.create(name="Celsius", symbol="C")
        collection = Collection.objects.create(
            catalog=self.catalog,
            name="Tas",
            slug="tas",
            visibility=Collection.Visibility.PUBLIC,
        )
        self.variable = Variable.objects.create(
            collection=collection,
            slug="tas",
            name="tas",
            unit=self.unit,
            value_min=0,
            value_max=50,
        )
        self.item = Item.objects.create(
            collection=collection,
            time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
        )
        make_style(self.variable, "official")
        make_style(self.variable, "analyst", is_default=False, stops=ANALYST_STOPS)

    def _collection(self, variable_slug="tas"):
        response = self.client.get(reverse("stac:collection-detail", args=["cmip6", "tas", variable_slug]))
        self.assertEqual(response.status_code, 200)
        return response.json()

    def _item(self):
        response = self.client.get(reverse("stac:item-detail", args=["cmip6", "tas", "tas", "20260301T120000Z"]))
        self.assertEqual(response.status_code, 200)
        return response.json()

    def test_the_collection_enumerates_each_style_by_slug(self):
        self.assertEqual(
            list(self._collection()["renders"]),
            ["official", "analyst"],
        )

    def test_a_render_entry_carries_asset_rescale_and_colormap(self):
        entry = self._collection()["renders"]["analyst"]
        self.assertEqual(entry["title"], "Analyst")
        self.assertEqual(entry["assets"], ["tas_cog"])
        self.assertEqual(entry["rescale"], [[0.0, 50.0]])
        self.assertEqual(entry["colormap"]["0"], [0, 0, 255, 255])

    def test_the_default_style_is_marked(self):
        renders = self._collection()["renders"]
        self.assertIs(renders["official"]["georiva:default"], True)
        self.assertNotIn("georiva:default", renders["analyst"])

    def test_the_collection_declares_the_extension(self):
        self.assertIn(self.RENDER_SCHEMA, self._collection()["stac_extensions"])

    def test_a_styleless_variable_declares_nothing(self):
        """`renders` is required wherever the extension is declared, so a
        still-grayscale variable carries neither the field nor the URI."""
        other = Collection.objects.create(
            catalog=self.catalog,
            name="Pr",
            slug="pr",
            visibility=Collection.Visibility.PUBLIC,
        )
        Variable.objects.create(
            collection=other,
            slug="pr",
            name="pr",
            unit=self.unit,
            value_min=0,
            value_max=10,
        )
        response = self.client.get(reverse("stac:collection-detail", args=["cmip6", "pr", "pr"])).json()
        self.assertNotIn("renders", response)
        self.assertNotIn(self.RENDER_SCHEMA, response["stac_extensions"])

    def test_an_item_references_the_same_render_names(self):
        item = self._item()
        renders = item["properties"]["renders"]
        self.assertEqual(list(renders), ["official", "analyst"])
        self.assertEqual(renders["analyst"]["assets"], ["tas_cog"])
        self.assertEqual(renders["analyst"]["rescale"], [[0.0, 50.0]])
        self.assertIn(self.RENDER_SCHEMA, item["stac_extensions"])

    def test_an_item_does_not_repeat_the_colormap(self):
        """Items list by the hundred; the colormap lives once, on the
        collection, and the render name is the join."""
        renders = self._item()["properties"]["renders"]
        self.assertNotIn("colormap", renders["official"])

    def test_a_private_collections_renders_are_as_invisible_as_it_is(self):
        """Styles add no access surface of their own (ADR 0023): a caller who
        cannot fetch the collection discovers none of its renderings."""
        Collection.objects.filter(slug="tas").update(
            visibility=Collection.Visibility.PRIVATE,
        )
        response = self.client.get(reverse("stac:collection-detail", args=["cmip6", "tas", "tas"]))
        self.assertEqual(response.status_code, 404)

    def test_a_styleless_variables_item_declares_nothing(self):
        other = Collection.objects.create(
            catalog=self.catalog,
            name="Pr",
            slug="pr",
            visibility=Collection.Visibility.PUBLIC,
        )
        Variable.objects.create(
            collection=other,
            slug="pr",
            name="pr",
            unit=self.unit,
            value_min=0,
            value_max=10,
        )
        Item.objects.create(
            collection=other,
            time=datetime(2026, 3, 1, 12, 0, tzinfo=UTC),
        )
        response = self.client.get(reverse("stac:item-detail", args=["cmip6", "pr", "pr", "20260301T120000Z"])).json()
        self.assertNotIn("renders", response["properties"])
        self.assertNotIn(self.RENDER_SCHEMA, response["stac_extensions"])
