"""
Chain diagram — the planned DAG (ADR-0008, issue #150).

build_chain_graph turns a feed's declarations + configured products into nodes
(collections) and edges (products), overlaid with each product's status and
readiness — the seam the server-rendered chain view draws. Declaration-driven,
no recipe execution, so configured-but-unrun (blocked) edges appear too.
"""
from unittest.mock import patch

from django.test import TestCase

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog
from georiva.sources.derivation_chain import build_chain_graph, item_lineage
from georiva.sources.models import DataFeed, DerivedProduct


def _definition(key="anomaly", inputs=None, outputs=None, **overrides):
    kwargs = dict(
        key=key,
        recipe_type="climatology",
        label="Rainfall anomaly",
        description="",
        config_schema=(),
        inputs=inputs or (InputRef(role="value", collection="rainfall", tier="staging"),),
        outputs=outputs or (OutputRef(role="anomaly", collection="rainfall-anomaly"),),
        trigger_mode="scheduled",
    )
    kwargs.update(overrides)
    return DerivedProductDefinition(**kwargs)


class BuildChainGraphTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def _product(self, definition, **overrides):
        return DerivedProduct.objects.create(
            data_feed=self.feed,
            definition_key=definition.key,
            recipe_type=definition.recipe_type,
            is_enabled=overrides.get("is_enabled", True),
        )

    def test_one_product_is_one_edge_between_its_collections(self):
        definition = _definition()
        self._product(definition)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            graph = build_chain_graph(self.feed)

        self.assertEqual({n.collection for n in graph.nodes}, {"rainfall", "rainfall-anomaly"})
        self.assertEqual(len(graph.edges), 1)
        edge = graph.edges[0]
        self.assertEqual(edge.inputs, ["rainfall"])
        self.assertEqual(edge.outputs, ["rainfall-anomaly"])
        self.assertEqual(edge.recipe_type, "climatology")
        self.assertEqual(edge.trigger_mode, "scheduled")
        self.assertEqual(edge.label, "Rainfall anomaly")

    def test_multi_input_product_edge_carries_all_inputs(self):
        # {raw, normals} -> anomaly
        definition = _definition(inputs=(
            InputRef(role="value", collection="rainfall", tier="staging"),
            InputRef(role="normals", collection="rainfall-normals", tier="published"),
        ))
        self._product(definition)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            graph = build_chain_graph(self.feed)

        edge = graph.edges[0]
        self.assertEqual(edge.inputs, ["rainfall", "rainfall-normals"])
        self.assertEqual(
            {n.collection for n in graph.nodes},
            {"rainfall", "rainfall-normals", "rainfall-anomaly"},
        )

    def test_edge_is_overlaid_with_status_and_blocked_readiness(self):
        # No runs and the required 'rainfall' input collection is empty:
        # a configured-but-unrun (blocked) edge with its reason.
        definition = _definition()
        self._product(definition)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            graph = build_chain_graph(self.feed)

        edge = graph.edges[0]
        self.assertEqual(edge.status, "idle")     # no DerivationRuns yet
        self.assertFalse(edge.ready)              # required input empty
        self.assertIn("value", edge.reason)       # blocking reason named

    def test_disabled_product_is_not_an_edge(self):
        definition = _definition()
        self._product(definition, is_enabled=False)

        with patch.object(DataFeed, "get_derived_products", return_value=[definition]):
            graph = build_chain_graph(self.feed)

        self.assertEqual(graph.edges, [])
        self.assertEqual(graph.nodes, [])


class ItemLineageTests(TestCase):
    def test_lineage_returns_the_source_items_of_a_produced_item(self):
        from datetime import datetime, timezone

        from georiva.core.models import Collection, Item
        from georiva.staging.models import (
            DerivationLink,
            StagingCollection,
            StagingItem,
        )

        t = datetime(2020, 1, 1, tzinfo=timezone.utc)
        catalog = Catalog.objects.create(name="C", slug="c", file_format="geotiff")

        # The produced (derived) item.
        out_col = Collection.objects.create(catalog=catalog, slug="anomaly", name="Anomaly")
        derived = Item.objects.create(collection=out_col, time=t)

        # A staging input and a published input.
        scol = StagingCollection.objects.create(catalog=catalog, slug="rainfall", name="Rainfall")
        staging_src = StagingItem.objects.create(collection=scol, datetime=t)
        pub_col = Collection.objects.create(catalog=catalog, slug="normals", name="Normals")
        published_src = Item.objects.create(collection=pub_col, time=t)

        DerivationLink.objects.create(
            derived_item=derived, source_staging_item=staging_src,
            recipe_id="climatology", recipe_version="1", input_hash="h",
        )
        DerivationLink.objects.create(
            derived_item=derived, source_published_item=published_src,
            recipe_id="climatology", recipe_version="1", input_hash="h",
        )

        sources = item_lineage(derived)

        self.assertEqual(len(sources), 2)
        self.assertIn(staging_src, sources)
        self.assertIn(published_src, sources)


class ChainViewTests(TestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model
        User = get_user_model()
        self.user = User.objects.create_superuser("admin_chain", "c@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Rain Feed", catalog=self.catalog)
        DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly", recipe_type="climatology",
        )

    def test_chain_page_renders_nodes_and_edge_labels(self):
        from django.urls import reverse

        with patch.object(DataFeed, "get_derived_products", return_value=[_definition()]):
            response = self.client.get(
                reverse("derived_product_chain", kwargs={"feed_pk": self.feed.pk})
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "rainfall")           # an input node
        self.assertContains(response, "rainfall-anomaly")   # an output node
        self.assertContains(response, "Rainfall anomaly")   # the product edge label


class ItemLineageViewTests(TestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model
        User = get_user_model()
        self.user = User.objects.create_superuser("admin_lin", "l@test.com", "pw")
        self.client.force_login(self.user)

    def test_lineage_page_lists_a_produced_items_inputs(self):
        from datetime import datetime, timezone

        from django.urls import reverse

        from georiva.core.models import Collection, Item
        from georiva.staging.models import (
            DerivationLink,
            StagingCollection,
            StagingItem,
        )

        t = datetime(2020, 1, 1, tzinfo=timezone.utc)
        catalog = Catalog.objects.create(name="C", slug="c", file_format="geotiff")
        out_col = Collection.objects.create(catalog=catalog, slug="anomaly", name="Anomaly")
        derived = Item.objects.create(collection=out_col, time=t)
        scol = StagingCollection.objects.create(catalog=catalog, slug="rainfall", name="Rainfall")
        staging_src = StagingItem.objects.create(collection=scol, datetime=t)
        DerivationLink.objects.create(
            derived_item=derived, source_staging_item=staging_src,
            recipe_id="climatology", recipe_version="1", input_hash="h",
        )

        response = self.client.get(
            reverse("item_lineage", kwargs={"item_pk": derived.pk})
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "rainfall")  # the input collection
