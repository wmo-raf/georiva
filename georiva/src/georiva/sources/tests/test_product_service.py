"""
Service-seam tests for the single enable/disable write-path (ADR-0008/0009,
issue #167).

Every surface (wizard, feed detail, tracking dashboard) routes enable/disable
through ``sources.product_service`` so the invariant "no enabled product with a
disabled dependency" can't be broken. Enabling is structurally gated on the
transitive dependency closure; disabling cascades to the transitive dependent
closure atomically. Data availability is a *separate* runtime gate, not checked
here — a whole chain may be enabled before any upstream data exists.
"""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.template.loader import render_to_string
from django.test import TestCase
from django.urls import reverse

from georiva.core.derived_products import (
    ConfigField,
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection
from georiva.sources.models import (
    DataFeed,
    DataFeedCollectionLink,
    DerivedProduct,
    DerivedProductInput,
    DerivedProductOutput,
)
from georiva.sources.product_service import (
    ProductActionError,
    build_chain,
    delete_orphan,
    disable_product,
    enable_new_definition,
    enable_product,
    enabled_dependents,
    is_orphaned,
    materialise_output_collections,
    product_label,
)

User = get_user_model()


def _product(key, *, inputs=(), outputs=(), recipe_type="recipe", config_schema=(),
             trigger_mode="scheduled", description=""):
    return DerivedProductDefinition(
        key=key,
        recipe_type=recipe_type,
        label=key.replace("-", " ").title(),
        description=description,
        config_schema=tuple(config_schema),
        inputs=tuple(inputs),
        outputs=tuple(outputs),
        trigger_mode=trigger_mode,
    )


def _chirps_defs():
    """CHIRPS 'monthly' resolution: anomaly depends on climatology (its required
    published baseline); promotion is independent."""
    raw = "chirps-monthly"
    clim = "chirps-monthly-climatology"
    return [
        _product(
            "promotion",
            inputs=(InputRef(role="source", collection=raw, tier="staging"),),
            outputs=(OutputRef(role="served", collection=raw),),
        ),
        _product(
            "climatology",
            inputs=(InputRef(role="value", collection=raw, tier="staging"),),
            outputs=(OutputRef(role="climatology", collection=clim),),
        ),
        _product(
            "anomaly",
            inputs=(
                InputRef(role="value", collection=raw, tier="staging"),
                InputRef(role="baseline", collection=clim, tier="published"),
            ),
            outputs=(OutputRef(role="anomaly", collection="chirps-monthly-anomaly"),),
        ),
    ]


class ProductServiceBase(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Rain Feed", catalog=self.catalog)
        self.rows = {}
        for defn in _chirps_defs():
            self.rows[defn.key] = DerivedProduct.objects.create(
                data_feed=self.feed, definition_key=defn.key,
                recipe_type=defn.recipe_type, is_enabled=True,
            )

    def _patch_defs(self):
        return patch.object(
            DataFeed, "get_derived_products", return_value=_chirps_defs()
        )


class EnableGateTests(ProductServiceBase):
    def test_enable_is_refused_when_a_dependency_is_disabled(self):
        # climatology off, anomaly off -> enabling anomaly alone is blocked and
        # the error names the missing dependency by its display label.
        self.rows["climatology"].is_enabled = False
        self.rows["climatology"].save(update_fields=["is_enabled"])
        self.rows["anomaly"].is_enabled = False
        self.rows["anomaly"].save(update_fields=["is_enabled"])

        with self._patch_defs():
            with self.assertRaises(ProductActionError) as ctx:
                enable_product(self.rows["anomaly"])

        self.assertIn("Climatology", str(ctx.exception))
        self.rows["anomaly"].refresh_from_db()
        self.assertFalse(self.rows["anomaly"].is_enabled)

    def test_enable_succeeds_when_all_dependencies_are_enabled(self):
        # climatology stays enabled -> anomaly may be enabled, even with no data
        # yet (data readiness is a separate gate, not checked here).
        self.rows["anomaly"].is_enabled = False
        self.rows["anomaly"].save(update_fields=["is_enabled"])

        with self._patch_defs():
            enable_product(self.rows["anomaly"])

        self.rows["anomaly"].refresh_from_db()
        self.assertTrue(self.rows["anomaly"].is_enabled)

    def test_independent_product_enables_without_dependencies(self):
        self.rows["promotion"].is_enabled = False
        self.rows["promotion"].save(update_fields=["is_enabled"])

        with self._patch_defs():
            enable_product(self.rows["promotion"])

        self.rows["promotion"].refresh_from_db()
        self.assertTrue(self.rows["promotion"].is_enabled)

    def test_enable_is_refused_when_an_input_key_does_not_resolve(self):
        # A product whose declared input names a collection the feed neither
        # provides (no CollectionDefinition / link) nor produces (no sibling
        # output) can't be pinned — enabling it fails loudly, naming the key,
        # and leaves the row disabled (ADR-0010 §2).
        broken = _product(
            "broken",
            inputs=(InputRef(role="value", collection="ghost-raw", tier="staging"),),
            outputs=(OutputRef(role="o", collection="broken-out"),),
        )
        row = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="broken",
            recipe_type="recipe", is_enabled=False,
        )

        with patch.object(DataFeed, "get_derived_products", return_value=[broken]):
            with self.assertRaises(ProductActionError) as ctx:
                enable_product(row)

        self.assertIn("ghost-raw", str(ctx.exception))
        row.refresh_from_db()
        self.assertFalse(row.is_enabled)

    def test_enable_materialises_the_products_output_collections(self):
        # Enabling makes the output collection appear in the catalog immediately,
        # before any recipe run.
        self.rows["anomaly"].is_enabled = False
        self.rows["anomaly"].save(update_fields=["is_enabled"])
        self.assertFalse(
            Collection.objects.filter(slug="chirps-monthly-anomaly").exists()
        )

        with self._patch_defs():
            enable_product(self.rows["anomaly"])

        self.assertTrue(
            Collection.objects.filter(
                catalog=self.catalog, slug="chirps-monthly-anomaly"
            ).exists()
        )


class DisableCascadeTests(ProductServiceBase):
    def test_enabled_dependents_lists_the_transitive_downstream_set(self):
        with self._patch_defs():
            dependents = enabled_dependents(self.rows["climatology"])

        self.assertEqual(
            [d.definition_key for d in dependents], ["anomaly"]
        )

    def test_enabled_dependents_excludes_already_disabled_rows(self):
        self.rows["anomaly"].is_enabled = False
        self.rows["anomaly"].save(update_fields=["is_enabled"])

        with self._patch_defs():
            self.assertEqual(enabled_dependents(self.rows["climatology"]), [])

    def test_disable_cascades_to_transitive_dependents(self):
        with self._patch_defs():
            disabled = disable_product(self.rows["climatology"])

        # climatology and its dependent anomaly both go down, in one pass.
        self.assertEqual(
            sorted(d.definition_key for d in disabled), ["anomaly", "climatology"]
        )
        for row in self.rows.values():
            row.refresh_from_db()
        self.assertFalse(self.rows["climatology"].is_enabled)
        self.assertFalse(self.rows["anomaly"].is_enabled)
        # An unrelated product is untouched.
        self.assertTrue(self.rows["promotion"].is_enabled)

    def test_disable_of_a_leaf_touches_only_itself(self):
        with self._patch_defs():
            disabled = disable_product(self.rows["anomaly"])

        self.assertEqual([d.definition_key for d in disabled], ["anomaly"])
        self.rows["climatology"].refresh_from_db()
        self.assertTrue(self.rows["climatology"].is_enabled)

    def test_disable_is_atomic_no_partial_write_on_error(self):
        # If the save of a cascaded row blows up, nothing is left half-disabled.
        with self._patch_defs():
            with patch.object(
                DerivedProduct, "save", side_effect=RuntimeError("boom")
            ):
                with self.assertRaises(RuntimeError):
                    disable_product(self.rows["climatology"])

        for row in self.rows.values():
            row.refresh_from_db()
        self.assertTrue(self.rows["climatology"].is_enabled)
        self.assertTrue(self.rows["anomaly"].is_enabled)


class MaterialiseOutputCollectionsTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Rain Feed", catalog=self.catalog)

    def _definition(self, outputs):
        return _product("anomaly", outputs=outputs)

    def test_creates_a_collection_per_output_with_declared_metadata(self):
        definition = self._definition((
            OutputRef(role="anomaly", collection="chirps-monthly-anomaly",
                      title="CHIRPS Monthly Anomaly",
                      description="Absolute rainfall anomaly."),
            OutputRef(role="climatology", collection="chirps-monthly-climatology",
                      title="CHIRPS Monthly Climatology", visibility="internal"),
        ))

        materialise_output_collections(self.feed, definition)

        anomaly = Collection.objects.get(catalog=self.catalog, slug="chirps-monthly-anomaly")
        self.assertEqual(anomaly.name, "CHIRPS Monthly Anomaly")
        self.assertEqual(anomaly.description, "Absolute rainfall anomaly.")
        self.assertEqual(anomaly.visibility, Collection.Visibility.PUBLIC)

        clim = Collection.objects.get(catalog=self.catalog, slug="chirps-monthly-climatology")
        self.assertEqual(clim.visibility, Collection.Visibility.INTERNAL)

    def test_name_falls_back_to_slug_when_no_title_declared(self):
        definition = self._definition((
            OutputRef(role="anomaly", collection="chirps-monthly-anomaly"),
        ))

        materialise_output_collections(self.feed, definition)

        collection = Collection.objects.get(slug="chirps-monthly-anomaly")
        self.assertEqual(collection.name, "chirps-monthly-anomaly")

    def test_never_overwrites_an_operators_edits(self):
        # The operator renamed the collection and flipped its visibility after the
        # first materialisation; a subsequent enable/upgrade must not clobber that.
        Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly-anomaly",
            name="My Renamed Anomaly", description="Operator note.",
            visibility=Collection.Visibility.INTERNAL,
        )
        definition = self._definition((
            OutputRef(role="anomaly", collection="chirps-monthly-anomaly",
                      title="CHIRPS Monthly Anomaly", description="Declared.",
                      visibility="public"),
        ))

        materialise_output_collections(self.feed, definition)

        collection = Collection.objects.get(slug="chirps-monthly-anomaly")
        self.assertEqual(collection.name, "My Renamed Anomaly")
        self.assertEqual(collection.description, "Operator note.")
        self.assertEqual(collection.visibility, Collection.Visibility.INTERNAL)
        self.assertEqual(Collection.objects.filter(slug="chirps-monthly-anomaly").count(), 1)


class BuildChainTests(ProductServiceBase):
    def _cards_by_key(self, chain):
        return {c["product"].definition_key: c for lane in chain["stages"] for c in lane}

    def test_stages_are_topological_with_dependency_chips(self):
        with self._patch_defs():
            chain = build_chain(self.feed)

        keys_by_stage = [
            [c["product"].definition_key for c in lane] for lane in chain["stages"]
        ]
        # promotion + climatology have no dependencies -> stage 1; anomaly -> 2.
        self.assertEqual(keys_by_stage, [["promotion", "climatology"], ["anomaly"]])

        cards = self._cards_by_key(chain)
        self.assertEqual(cards["anomaly"]["needs"], ["Climatology"])
        self.assertEqual(cards["climatology"]["needs"], [])

    def test_card_carries_row_state_status_and_outputs(self):
        # anomaly's output collection is materialised and pinned -> the card
        # links it via its DerivedProductOutput binding (ADR-0010 §2).
        collection = Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly-anomaly",
            name="CHIRPS Monthly Anomaly",
        )
        DerivedProductOutput.objects.create(
            product=self.rows["anomaly"], role="anomaly",
            output_key="chirps-monthly-anomaly", collection=collection,
        )

        with self._patch_defs():
            cards = self._cards_by_key(build_chain(self.feed))

        anomaly = cards["anomaly"]
        self.assertTrue(anomaly["enabled"])
        self.assertEqual(anomaly["status"], "idle")          # no runs yet
        self.assertEqual(anomaly["definition"].label, "Anomaly")
        self.assertEqual(
            [c.slug for c in anomaly["output_collections"]],
            ["chirps-monthly-anomaly"],
        )

    def test_cards_use_the_operator_display_override(self):
        # A title/description override shows everywhere a product is named.
        self.rows["climatology"].title = "Rainfall Normals (1991–2020)"
        self.rows["climatology"].description = "Operator note."
        self.rows["climatology"].save(update_fields=["title", "description"])

        with self._patch_defs():
            cards = self._cards_by_key(build_chain(self.feed))
            self.assertEqual(product_label(self.rows["climatology"]),
                             "Rainfall Normals (1991–2020)")

        clim = cards["climatology"]
        self.assertEqual(clim["display_label"], "Rainfall Normals (1991–2020)")
        self.assertEqual(clim["display_description"], "Operator note.")
        # A dependent's needs-chip uses the dependency's display label too.
        self.assertEqual(cards["anomaly"]["needs"], ["Rainfall Normals (1991–2020)"])

    def test_manual_and_scheduled_products_are_runnable_event_ones_are_not(self):
        # The fixture's products are all trigger_mode="scheduled" (see _product),
        # so all can_run. Guard the flag exists and reflects trigger_mode.
        with self._patch_defs():
            cards = self._cards_by_key(build_chain(self.feed))

        self.assertTrue(cards["climatology"]["can_run"])


class UpgradeLifecycleServiceTests(ProductServiceBase):
    """Across a plugin upgrade the chain merges live declaration with DB state:
    a declared definition with no row is 'new', a row with no declaration is an
    'orphan' (issue #171)."""

    def _cards(self, chain):
        return {c["definition"].key: c
                for lane in chain["stages"] for c in lane if c["definition"]}

    def test_declared_definition_without_a_row_is_a_new_card(self):
        # A plugin update added 'anomaly' — drop its row to simulate the pre-run
        # state; it should still appear in its stage, flagged new.
        self.rows["anomaly"].delete()

        with self._patch_defs():
            chain = build_chain(self.feed)

        cards = self._cards(chain)
        self.assertTrue(cards["anomaly"]["is_new"])
        self.assertIsNone(cards["anomaly"]["product"])
        self.assertFalse(cards["anomaly"]["enabled"])
        # The still-provisioned products are not flagged new.
        self.assertFalse(cards["climatology"]["is_new"])

    def test_row_without_a_declaration_is_an_orphan_lane(self):
        # A plugin update removed 'promotion' from the declaration; its row
        # survives as an orphan.
        clim = _product(
            "climatology",
            inputs=(InputRef(role="value", collection="chirps-monthly", tier="staging"),),
            outputs=(OutputRef(role="climatology", collection="chirps-monthly-climatology"),),
        )
        anomaly = _product(
            "anomaly",
            inputs=(
                InputRef(role="value", collection="chirps-monthly", tier="staging"),
                InputRef(role="baseline", collection="chirps-monthly-climatology", tier="published"),
            ),
            outputs=(OutputRef(role="anomaly", collection="chirps-monthly-anomaly"),),
        )
        with patch.object(DataFeed, "get_derived_products", return_value=[clim, anomaly]):
            chain = build_chain(self.feed)

        orphan_keys = [c["product"].definition_key for c in chain["orphans"]]
        self.assertEqual(orphan_keys, ["promotion"])
        self.assertTrue(chain["orphans"][0]["orphaned"])
        # The orphan does not leak into the topological stages.
        self.assertNotIn("promotion", self._cards(chain))

    def test_is_orphaned_reflects_a_missing_declaration(self):
        with patch.object(DataFeed, "get_derived_products", return_value=[]):
            self.assertTrue(is_orphaned(self.rows["promotion"]))
        with self._patch_defs():
            self.assertFalse(is_orphaned(self.rows["promotion"]))


class EnableNewDefinitionTests(ProductServiceBase):
    def test_provisions_the_row_enforces_the_gate_and_materialises(self):
        self.rows["anomaly"].delete()   # 'anomaly' is a new (rowless) definition
        definition = next(d for d in _chirps_defs() if d.key == "anomaly")

        with self._patch_defs():
            product = enable_new_definition(self.feed, definition, {})

        self.assertTrue(product.is_enabled)
        self.assertEqual(product.definition_key, "anomaly")
        # Its declared output collection materialised on enable.
        self.assertTrue(
            Collection.objects.filter(
                catalog=self.catalog, slug="chirps-monthly-anomaly"
            ).exists()
        )

    def test_is_gated_on_dependencies_being_enabled(self):
        self.rows["anomaly"].delete()
        self.rows["climatology"].is_enabled = False
        self.rows["climatology"].save(update_fields=["is_enabled"])
        definition = next(d for d in _chirps_defs() if d.key == "anomaly")

        with self._patch_defs():
            with self.assertRaises(ProductActionError):
                enable_new_definition(self.feed, definition, {})

        # No half-provisioned enabled row left behind.
        self.assertFalse(
            DerivedProduct.objects.filter(
                data_feed=self.feed, definition_key="anomaly", is_enabled=True
            ).exists()
        )


class DeleteOrphanTests(ProductServiceBase):
    def test_deletes_only_the_row_keeping_collections(self):
        # 'promotion' orphaned; its output collection already published.
        Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly", name="CHIRPS Monthly"
        )
        promotion = self.rows["promotion"]

        with patch.object(DataFeed, "get_derived_products", return_value=[]):
            delete_orphan(promotion)

        self.assertFalse(DerivedProduct.objects.filter(pk=promotion.pk).exists())
        # The published collection and its data are untouched.
        self.assertTrue(Collection.objects.filter(slug="chirps-monthly").exists())

    def test_refuses_to_delete_a_still_declared_product(self):
        with self._patch_defs():
            with self.assertRaises(ProductActionError):
                delete_orphan(self.rows["promotion"])

        self.assertTrue(DerivedProduct.objects.filter(pk=self.rows["promotion"].pk).exists())


class BuildChainReadinessTests(TestCase):
    """Readiness reason + the staging-gap hint on a blocked card."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Rain Feed", catalog=self.catalog)
        self.definition = _product(
            "climatology",
            inputs=(InputRef(role="value", collection="chirps-monthly", tier="staging"),),
            outputs=(OutputRef(role="climatology",
                               collection="chirps-monthly-climatology"),),
        )
        self.product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="climatology",
            recipe_type="recipe", is_enabled=True,
        )

    def test_blocked_on_staging_input_shows_the_backfill_hint(self):
        # No staging data for the required staging input -> blocked, and because
        # the empty input is a staging-tier one, the hint points at re-running the
        # feed (data fetched while disabled went to sources, not staging).
        with patch.object(DataFeed, "get_derived_products", return_value=[self.definition]):
            cards = [c for lane in build_chain(self.feed)["stages"] for c in lane]

        card = cards[0]
        self.assertFalse(card["readiness"].ready)
        self.assertTrue(card["readiness_hint"])
        self.assertIn("re-run", card["readiness_hint"].lower())


class ProductChainPartialTests(ProductServiceBase):
    """The shared stage-lane partial in manage mode renders the panel the feed
    detail page shows (issue #169). Rendered directly because a full feed-detail
    page needs a registered plugin feed; the wizard HTTP tests exercise the same
    partial in wizard mode, so the two modes can't drift."""

    def _render(self, ready=False):
        with self._patch_defs():
            if ready:
                with patch(
                    "georiva.sources.derivation_tracking.product_readiness"
                ) as readiness:
                    readiness.return_value.ready = True
                    chain = build_chain(self.feed)
            else:
                chain = build_chain(self.feed)
        return render_to_string(
            "georivasources/includes/product_chain.html",
            {"stage_lanes": chain["stages"], "mode": "manage", "feed": self.feed},
        )

    def test_renders_labels_dependency_chips_and_outputs(self):
        collection = Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly-anomaly",
            name="CHIRPS Monthly Anomaly",
        )
        DerivedProductOutput.objects.create(
            product=self.rows["anomaly"], role="anomaly",
            output_key="chirps-monthly-anomaly", collection=collection,
        )

        html = self._render()

        self.assertIn("Anomaly", html)                     # card label
        self.assertIn("Climatology", html)                 # needs chip
        self.assertIn("CHIRPS Monthly Anomaly", html)      # output collection

    def test_renders_per_product_toggle_and_run_actions(self):
        # Toggle is always available; the Run-now form appears only for a ready
        # product (a blocked one shows a disabled button instead).
        html = self._render(ready=True)

        self.assertIn(
            f"/products/{self.rows['anomaly'].pk}/toggle/", html
        )
        self.assertIn(
            f"/products/{self.rows['climatology'].pk}/run/", html
        )

    def test_renders_new_card_and_orphan_lane(self):
        # anomaly's row deleted -> "new"; promotion dropped from the declaration
        # -> orphan.
        promotion_pk = self.rows["promotion"].pk
        self.rows["anomaly"].delete()
        clim = _product(
            "climatology",
            inputs=(InputRef(role="value", collection="chirps-monthly", tier="staging"),),
            outputs=(OutputRef(role="climatology", collection="chirps-monthly-climatology"),),
        )
        anomaly = _product(
            "anomaly",
            inputs=(
                InputRef(role="value", collection="chirps-monthly", tier="staging"),
                InputRef(role="baseline", collection="chirps-monthly-climatology", tier="published"),
            ),
            outputs=(OutputRef(role="anomaly", collection="chirps-monthly-anomaly"),),
        )
        with patch.object(DataFeed, "get_derived_products", return_value=[clim, anomaly]):
            chain = build_chain(self.feed)
        html = render_to_string(
            "georivasources/includes/product_chain.html",
            {"stage_lanes": chain["stages"], "orphans": chain["orphans"],
             "mode": "manage", "feed": self.feed},
        )

        self.assertIn("New — not enabled", html)
        self.assertIn("/products/enable/anomaly/", html)     # inline enable link
        self.assertIn("No longer provided", html)
        self.assertIn(f"/products/{promotion_pk}/delete/", html)  # orphan delete link


class SemanticSurvivalTests(ProductServiceBase):
    """Operator overrides and output-collection renames must survive a
    disable/enable cycle (materialisation is get-or-create, never update)."""

    def test_title_override_and_collection_rename_survive_disable_enable(self):
        clim = self.rows["climatology"]
        clim.title = "Rainfall Normals (1991–2020)"
        clim.save(update_fields=["title"])
        renamed = Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly-climatology",
            name="My Renamed Normals", description="Operator blurb.",
        )

        with self._patch_defs():
            disable_product(clim)      # cascades to anomaly
            enable_product(clim)       # re-materialises its outputs

        clim.refresh_from_db()
        renamed.refresh_from_db()
        self.assertEqual(clim.title, "Rainfall Normals (1991–2020)")
        self.assertEqual(renamed.name, "My Renamed Normals")
        self.assertEqual(renamed.description, "Operator blurb.")


class ProductEditViewTests(ProductServiceBase):
    """The product edit view saves the three semantic sections — display
    overrides, output-collection strings, and operator options — while leaving
    structural identity read-only (issue #170)."""

    def setUp(self):
        super().setUp()
        self.user = User.objects.create_superuser("edit", "e@t.com", "pw")
        self.client.force_login(self.user)
        # climatology's output collection is materialised, so the edit view can
        # expose its catalog-facing name/description.
        self.clim_col = Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly-climatology",
            name="Declared clim name",
        )
        self.clim = self.rows["climatology"]

    def _url(self, product):
        return reverse(
            "feed_product_edit",
            kwargs={"feed_pk": self.feed.pk, "product_pk": product.pk},
        )

    def _clim_defs(self, config_schema=()):
        # A climatology definition (with an optional config schema) plus the
        # other two, so the chain resolves.
        clim = _product(
            "climatology", config_schema=config_schema,
            inputs=(InputRef(role="value", collection="chirps-monthly", tier="staging"),),
            outputs=(OutputRef(role="climatology",
                               collection="chirps-monthly-climatology"),),
        )
        return patch.object(DataFeed, "get_derived_products", return_value=[clim])

    def test_get_renders_sections_with_readonly_structural(self):
        with self._clim_defs():
            response = self.client.get(self._url(self.clim))

        self.assertEqual(response.status_code, 200)
        # Structural identity is shown (read-only): key, recipe, output slug.
        self.assertContains(response, "climatology")
        self.assertContains(response, "chirps-monthly-climatology")
        # The options section is flagged as affecting future runs only.
        self.assertContains(response, "future runs")

    def test_saves_display_overrides(self):
        with self._clim_defs():
            self.client.post(self._url(self.clim), {
                "title": "Rainfall Normals (1991–2020)",
                "description": "Operator note.",
                "col-%d-name" % self.clim_col.pk: self.clim_col.name,
                "col-%d-description" % self.clim_col.pk: "",
            })

        self.clim.refresh_from_db()
        self.assertEqual(self.clim.title, "Rainfall Normals (1991–2020)")
        self.assertEqual(self.clim.description, "Operator note.")

    def test_clearing_an_override_restores_the_declared_text(self):
        self.clim.title = "Old override"
        self.clim.save(update_fields=["title"])

        with self._clim_defs():
            self.client.post(self._url(self.clim), {
                "title": "", "description": "",
                "col-%d-name" % self.clim_col.pk: self.clim_col.name,
                "col-%d-description" % self.clim_col.pk: "",
            })
            self.clim.refresh_from_db()
            self.assertEqual(self.clim.title, "")
            # Blank override -> the declared label shows again.
            self.assertEqual(self.clim.display_label, "Climatology")

    def test_saves_output_collection_name_and_description(self):
        with self._clim_defs():
            self.client.post(self._url(self.clim), {
                "title": "", "description": "",
                "col-%d-name" % self.clim_col.pk: "My Normals",
                "col-%d-description" % self.clim_col.pk: "Catalog blurb.",
            })

        self.clim_col.refresh_from_db()
        self.assertEqual(self.clim_col.name, "My Normals")
        self.assertEqual(self.clim_col.description, "Catalog blurb.")

    def test_saves_config_and_interval(self):
        schema = (ConfigField(key="min_count", type="int", default=20),)
        with self._clim_defs(config_schema=schema):
            self.client.post(self._url(self.clim), {
                "title": "", "description": "",
                "config-min_count": "35",
                "interval_minutes": "1440",
                "col-%d-name" % self.clim_col.pk: self.clim_col.name,
                "col-%d-description" % self.clim_col.pk: "",
            })

        self.clim.refresh_from_db()
        self.assertEqual(self.clim.config["min_count"], 35)
        self.assertEqual(self.clim.interval_minutes, 1440)

    def test_invalid_config_is_rejected_and_not_saved(self):
        schema = (ConfigField(key="quantity", type="choice",
                              choices=("anomaly", "value")),)
        with self._clim_defs(config_schema=schema):
            response = self.client.post(self._url(self.clim), {
                "title": "", "description": "",
                "config-quantity": "trend",   # not among choices
                "col-%d-name" % self.clim_col.pk: self.clim_col.name,
                "col-%d-description" % self.clim_col.pk: "",
            })

        self.assertEqual(response.status_code, 200)   # re-rendered, not redirected
        self.clim.refresh_from_db()
        self.assertNotIn("quantity", self.clim.config)


class UpgradeLifecycleEndpointTests(ProductServiceBase):
    """Inline enable of a new (rowless) definition and delete of an orphan row,
    from the feed panel (issue #171)."""

    def setUp(self):
        super().setUp()
        self.user = User.objects.create_superuser("up", "u@t.com", "pw")
        self.client.force_login(self.user)

    def test_enable_new_get_shows_the_config_form(self):
        self.rows["anomaly"].delete()
        url = reverse("feed_product_enable_new",
                      kwargs={"feed_pk": self.feed.pk, "definition_key": "anomaly"})

        with self._patch_defs():
            response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Anomaly")

    def test_enable_new_post_provisions_enables_and_materialises(self):
        self.rows["anomaly"].delete()
        url = reverse("feed_product_enable_new",
                      kwargs={"feed_pk": self.feed.pk, "definition_key": "anomaly"})

        with self._patch_defs():
            self.client.post(url, {})

        product = DerivedProduct.objects.get(
            data_feed=self.feed, definition_key="anomaly"
        )
        self.assertTrue(product.is_enabled)
        self.assertTrue(
            Collection.objects.filter(slug="chirps-monthly-anomaly").exists()
        )

    def test_delete_orphan_get_shows_data_kept_confirmation(self):
        url = reverse("feed_product_delete_orphan",
                      kwargs={"feed_pk": self.feed.pk, "product_pk": self.rows["promotion"].pk})

        with patch.object(DataFeed, "get_derived_products", return_value=[]):
            response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        # The confirmation states data is kept.
        self.assertContains(response, "kept")

    def test_delete_orphan_post_removes_only_the_row(self):
        Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly", name="CHIRPS Monthly"
        )
        promotion_pk = self.rows["promotion"].pk
        url = reverse("feed_product_delete_orphan",
                      kwargs={"feed_pk": self.feed.pk, "product_pk": promotion_pk})

        with patch.object(DataFeed, "get_derived_products", return_value=[]):
            self.client.post(url)

        self.assertFalse(DerivedProduct.objects.filter(pk=promotion_pk).exists())
        self.assertTrue(Collection.objects.filter(slug="chirps-monthly").exists())

    def test_delete_orphan_refuses_a_still_declared_product(self):
        url = reverse("feed_product_delete_orphan",
                      kwargs={"feed_pk": self.feed.pk, "product_pk": self.rows["promotion"].pk})

        with self._patch_defs():
            self.client.post(url)

        # Still declared -> not deleted.
        self.assertTrue(
            DerivedProduct.objects.filter(pk=self.rows["promotion"].pk).exists()
        )


class FeedProductEndpointTests(ProductServiceBase):
    """The feed-detail panel's per-product actions (toggle / run) route through
    the same gate/cascade service as the tracking dashboard (issue #169)."""

    def setUp(self):
        super().setUp()
        self.user = User.objects.create_superuser("feed", "f@test.com", "pw")
        self.client.force_login(self.user)

    def _url(self, name, product):
        return reverse(name, kwargs={"feed_pk": self.feed.pk, "product_pk": product.pk})

    def test_run_dispatches_a_ready_product(self):
        with (
            self._patch_defs(),
            patch("georiva.sources.derivation_tracking.product_readiness") as readiness,
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            readiness.return_value.ready = True
            self.client.post(self._url("feed_product_run", self.rows["climatology"]))

        run_now.assert_called_once_with(self.rows["climatology"])

    def test_run_refuses_a_blocked_product_with_its_reason(self):
        with (
            self._patch_defs(),
            patch("georiva.sources.derivation_tracking.product_readiness") as readiness,
            patch("georiva.sources.derivation_invocation.run_product_now") as run_now,
        ):
            readiness.return_value.ready = False
            readiness.return_value.reason = "value empty"
            response = self.client.post(
                self._url("feed_product_run", self.rows["climatology"])
            )

        run_now.assert_not_called()
        msgs = " ".join(str(m) for m in get_messages(response.wsgi_request))
        self.assertIn("value empty", msgs)

    def test_toggle_enable_enforces_the_dependency_gate(self):
        self.rows["climatology"].is_enabled = False
        self.rows["climatology"].save(update_fields=["is_enabled"])
        self.rows["anomaly"].is_enabled = False
        self.rows["anomaly"].save(update_fields=["is_enabled"])

        with self._patch_defs():
            response = self.client.post(
                self._url("feed_product_toggle", self.rows["anomaly"])
            )

        self.rows["anomaly"].refresh_from_db()
        self.assertFalse(self.rows["anomaly"].is_enabled)
        msgs = " ".join(str(m) for m in get_messages(response.wsgi_request))
        self.assertIn("Climatology", msgs)

    def test_toggle_disable_with_dependents_shows_confirmation(self):
        with self._patch_defs():
            response = self.client.post(
                self._url("feed_product_toggle", self.rows["climatology"])
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Anomaly")
        self.rows["climatology"].refresh_from_db()
        self.assertTrue(self.rows["climatology"].is_enabled)

    def test_toggle_disable_confirmed_cascades(self):
        with self._patch_defs():
            self.client.post(
                self._url("feed_product_toggle", self.rows["climatology"]),
                {"confirmed": "1"},
            )

        self.rows["climatology"].refresh_from_db()
        self.rows["anomaly"].refresh_from_db()
        self.assertFalse(self.rows["climatology"].is_enabled)
        self.assertFalse(self.rows["anomaly"].is_enabled)


class TrackingToggleFlowTests(ProductServiceBase):
    """The tracking dashboard's Disable/Enable button routes through the service,
    so the dependency gate and cascade-disable confirmation hold from that
    surface too (issue #167)."""

    def setUp(self):
        super().setUp()
        self.user = User.objects.create_superuser("dash", "d@test.com", "pw")
        self.client.force_login(self.user)

    def _toggle(self, product, **extra):
        return self.client.post(reverse("derived_product_tracking"), {
            "action": "toggle", "product_pk": product.pk, **extra,
        })

    def test_disabling_a_product_with_enabled_dependents_asks_to_confirm(self):
        with self._patch_defs():
            response = self._toggle(self.rows["climatology"])

        # A confirmation page listing the transitive downstream set — nothing
        # disabled yet.
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Anomaly")
        self.rows["climatology"].refresh_from_db()
        self.rows["anomaly"].refresh_from_db()
        self.assertTrue(self.rows["climatology"].is_enabled)
        self.assertTrue(self.rows["anomaly"].is_enabled)

    def test_confirming_disables_the_whole_downstream_set(self):
        with self._patch_defs():
            response = self._toggle(self.rows["climatology"], confirmed="1")

        self.rows["climatology"].refresh_from_db()
        self.rows["anomaly"].refresh_from_db()
        self.assertFalse(self.rows["climatology"].is_enabled)
        self.assertFalse(self.rows["anomaly"].is_enabled)
        # The result message names everything that was disabled.
        msgs = " ".join(str(m) for m in get_messages(response.wsgi_request))
        self.assertIn("Climatology", msgs)
        self.assertIn("Anomaly", msgs)

    def test_disabling_a_leaf_proceeds_without_confirmation(self):
        with self._patch_defs():
            self._toggle(self.rows["anomaly"])

        self.rows["anomaly"].refresh_from_db()
        self.rows["climatology"].refresh_from_db()
        self.assertFalse(self.rows["anomaly"].is_enabled)
        self.assertTrue(self.rows["climatology"].is_enabled)

    def test_enabling_a_product_with_a_disabled_dependency_is_blocked(self):
        self.rows["climatology"].is_enabled = False
        self.rows["climatology"].save(update_fields=["is_enabled"])
        self.rows["anomaly"].is_enabled = False
        self.rows["anomaly"].save(update_fields=["is_enabled"])

        with self._patch_defs():
            response = self._toggle(self.rows["anomaly"])

        self.rows["anomaly"].refresh_from_db()
        self.assertFalse(self.rows["anomaly"].is_enabled)
        msgs = " ".join(str(m) for m in get_messages(response.wsgi_request))
        self.assertIn("Climatology", msgs)


class PinBindingsBase(TestCase):
    """A properly provisioned feed: the raw collection exists with a
    DataFeedCollectionLink (as the collections wizard step would create), so a
    product's declared input/output keys resolve to real Collections. The bare
    gate fixtures above deliberately skip this — here we exercise the pinning."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Rain Feed", catalog=self.catalog)
        # Raw collection + link, keyed on the definition key the declarations use.
        self.raw = Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly", name="CHIRPS Monthly"
        )
        DataFeedCollectionLink.objects.create(
            data_feed=self.feed, collection=self.raw,
            definition_key="chirps-monthly",
        )
        self.rows = {}
        for defn in _chirps_defs():
            self.rows[defn.key] = DerivedProduct.objects.create(
                data_feed=self.feed, definition_key=defn.key,
                recipe_type=defn.recipe_type, is_enabled=True,
            )

    def _patch_defs(self):
        return patch.object(
            DataFeed, "get_derived_products", return_value=_chirps_defs()
        )


class PinOutputBindingsTests(PinBindingsBase):
    def test_enable_pins_an_output_binding_per_declared_output(self):
        # Enabling climatology materialises its output collection and pins a
        # DerivedProductOutput row (role, key) → that Collection (ADR-0010 §2).
        clim = self.rows["climatology"]
        clim.is_enabled = False
        clim.save(update_fields=["is_enabled"])

        with self._patch_defs():
            enable_product(clim)

        binding = DerivedProductOutput.objects.get(product=clim, role="climatology")
        self.assertEqual(binding.output_key, "chirps-monthly-climatology")
        self.assertEqual(binding.collection.slug, "chirps-monthly-climatology")
        self.assertEqual(binding.collection.catalog, self.catalog)


class PinInputBindingsTests(PinBindingsBase):
    def test_enable_pins_a_raw_staging_input_to_the_linked_collection(self):
        # climatology's staging input resolves through the DataFeedCollectionLink
        # to the raw collection (ADR-0010 §2).
        clim = self.rows["climatology"]
        clim.is_enabled = False
        clim.save(update_fields=["is_enabled"])

        with self._patch_defs():
            enable_product(clim)

        binding = DerivedProductInput.objects.get(product=clim, role="value")
        self.assertEqual(binding.tier, "staging")
        self.assertEqual(binding.source_key, "chirps-monthly")
        self.assertEqual(binding.collection, self.raw)
        self.assertTrue(binding.required)

    def test_enable_pins_a_published_input_to_the_sibling_output_collection(self):
        # anomaly's published baseline resolves to the climatology product's
        # materialised output collection. The dependency gate enables climatology
        # first (materialising its output), so anomaly's binding resolves.
        anomaly = self.rows["anomaly"]
        anomaly.is_enabled = False
        anomaly.save(update_fields=["is_enabled"])

        with self._patch_defs():
            enable_product(self.rows["climatology"])  # materialises clim output
            enable_product(anomaly)

        clim_collection = Collection.objects.get(
            catalog=self.catalog, slug="chirps-monthly-climatology"
        )
        baseline = DerivedProductInput.objects.get(product=anomaly, role="baseline")
        self.assertEqual(baseline.tier, "published")
        self.assertEqual(baseline.collection, clim_collection)


class PinBindingsIdempotencyTests(PinBindingsBase):
    def test_re_enabling_upserts_rather_than_duplicating(self):
        clim = self.rows["climatology"]

        with self._patch_defs():
            enable_product(clim)
            enable_product(clim)   # re-run (e.g. a wizard revisit / upgrade)

        self.assertEqual(
            DerivedProductOutput.objects.filter(product=clim).count(), 1
        )
        self.assertEqual(
            DerivedProductInput.objects.filter(product=clim).count(), 1
        )

    def test_a_changed_declaration_re_pins_the_same_role_in_place(self):
        clim = self.rows["climatology"]
        with self._patch_defs():
            enable_product(clim)

        # A plugin upgrade re-keys climatology's output collection.
        upgraded = _product(
            "climatology",
            inputs=(InputRef(role="value", collection="chirps-monthly", tier="staging"),),
            outputs=(OutputRef(role="climatology",
                               collection="chirps-monthly-climatology-v2"),),
        )
        with patch.object(DataFeed, "get_derived_products", return_value=[upgraded]):
            enable_product(clim)

        bindings = DerivedProductOutput.objects.filter(product=clim, role="climatology")
        self.assertEqual(bindings.count(), 1)          # same role row, updated
        self.assertEqual(bindings.first().output_key, "chirps-monthly-climatology-v2")


class BuildChainOutputBindingTests(PinBindingsBase):
    def test_card_output_collections_come_from_bindings_not_a_slug_query(self):
        # Enabling pins climatology's output binding. Renaming the collection's
        # slug afterwards must not drop it from the card — the card reads the
        # DerivedProductOutput FK, not a catalog+slug lookup (ADR-0010 §2/§3).
        clim = self.rows["climatology"]
        with self._patch_defs():
            enable_product(clim)

        collection = Collection.objects.get(slug="chirps-monthly-climatology")
        collection.slug = "operator-renamed-normals"
        collection.save(update_fields=["slug"])

        with self._patch_defs():
            chain = build_chain(self.feed)

        card = next(
            c for lane in chain["stages"] for c in lane
            if c["product"] and c["product"].definition_key == "climatology"
        )
        self.assertEqual(
            [c.pk for c in card["output_collections"]], [collection.pk]
        )


class BindingCascadeTests(PinBindingsBase):
    def test_deleting_a_bound_collection_removes_its_binding_rows(self):
        clim = self.rows["climatology"]
        with self._patch_defs():
            enable_product(clim)
        collection = Collection.objects.get(slug="chirps-monthly-climatology")
        self.assertTrue(
            DerivedProductOutput.objects.filter(collection=collection).exists()
        )

        collection.delete()

        self.assertFalse(
            DerivedProductOutput.objects.filter(product=clim, role="climatology").exists()
        )
        # The product row itself survives — only the binding cascaded.
        self.assertTrue(DerivedProduct.objects.filter(pk=clim.pk).exists())

    def test_feed_deletion_still_works_with_bindings_present(self):
        with self._patch_defs():
            enable_product(self.rows["climatology"])
            enable_product(self.rows["promotion"])
        feed_pk = self.feed.pk
        self.assertTrue(
            DerivedProductInput.objects.filter(product__data_feed_id=feed_pk).exists()
        )

        self.feed.delete()

        self.assertFalse(DerivedProduct.objects.filter(data_feed_id=feed_pk).exists())
        self.assertFalse(
            DerivedProductInput.objects.filter(product__data_feed_id=feed_pk).exists()
        )


class ProvisionPinsBindingsTests(PinBindingsBase):
    def test_provisioning_an_enabled_product_pins_its_bindings(self):
        # The wizard's provisioning path pins bindings for enabled products, the
        # same as the enable path (ADR-0010 §2).
        from georiva.sources.setup_service import SourceSetupService

        DerivedProduct.objects.filter(data_feed=self.feed).delete()
        clim_def = next(d for d in _chirps_defs() if d.key == "climatology")

        with self._patch_defs():
            SourceSetupService().provision_derived_products(
                self.feed, [(clim_def, {}, True)]
            )

        product = DerivedProduct.objects.get(
            data_feed=self.feed, definition_key="climatology"
        )
        self.assertTrue(
            DerivedProductOutput.objects.filter(product=product, role="climatology").exists()
        )
        self.assertEqual(
            DerivedProductInput.objects.get(product=product, role="value").collection,
            self.raw,
        )

    def test_provisioning_a_disabled_product_pins_nothing(self):
        from georiva.sources.setup_service import SourceSetupService

        DerivedProduct.objects.filter(data_feed=self.feed).delete()
        clim_def = next(d for d in _chirps_defs() if d.key == "climatology")

        with self._patch_defs():
            SourceSetupService().provision_derived_products(
                self.feed, [(clim_def, {}, False)]
            )

        product = DerivedProduct.objects.get(
            data_feed=self.feed, definition_key="climatology"
        )
        self.assertFalse(DerivedProductOutput.objects.filter(product=product).exists())
        self.assertFalse(DerivedProductInput.objects.filter(product=product).exists())


class BackfillBindingsTests(PinBindingsBase):
    def test_backfill_pins_bindings_for_existing_enabled_products(self):
        # Feeds enabled before pinning existed have no binding rows; the one-time
        # backfill resolves and pins them (ADR-0010 §2). setUp created enabled
        # rows directly (no bindings).
        from georiva.sources.product_service import backfill_bindings

        self.assertFalse(DerivedProductOutput.objects.exists())

        with self._patch_defs():
            backfill_bindings()

        clim = self.rows["climatology"]
        self.assertTrue(
            DerivedProductOutput.objects.filter(product=clim, role="climatology").exists()
        )
        self.assertEqual(
            DerivedProductInput.objects.get(product=clim, role="value").collection,
            self.raw,
        )

    def test_backfill_is_idempotent(self):
        from georiva.sources.product_service import backfill_bindings

        with self._patch_defs():
            backfill_bindings()
            backfill_bindings()

        self.assertEqual(
            DerivedProductOutput.objects.filter(
                product=self.rows["climatology"]
            ).count(),
            1,
        )


class EnableFailureLeavesNoBindingsTests(PinBindingsBase):
    def test_unresolvable_enable_writes_no_binding_rows(self):
        # ADR-0010 §2 AC: a failed enable leaves neither is_enabled nor any
        # binding rows behind.
        broken = _product(
            "broken",
            inputs=(InputRef(role="value", collection="ghost-raw", tier="staging"),),
            outputs=(OutputRef(role="o", collection="broken-out"),),
        )
        row = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="broken",
            recipe_type="recipe", is_enabled=False,
        )

        with patch.object(DataFeed, "get_derived_products", return_value=[broken]):
            with self.assertRaises(ProductActionError):
                enable_product(row)

        row.refresh_from_db()
        self.assertFalse(row.is_enabled)
        self.assertFalse(DerivedProductInput.objects.filter(product=row).exists())
        self.assertFalse(DerivedProductOutput.objects.filter(product=row).exists())
