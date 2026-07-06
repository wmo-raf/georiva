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
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection
from georiva.sources.models import DataFeed, DerivedProduct
from georiva.sources.product_service import (
    ProductActionError,
    build_chain,
    disable_product,
    enable_product,
    enabled_dependents,
    materialise_output_collections,
)

User = get_user_model()


def _product(key, *, inputs=(), outputs=(), recipe_type="recipe"):
    return DerivedProductDefinition(
        key=key,
        recipe_type=recipe_type,
        label=key.replace("-", " ").title(),
        description="",
        config_schema=(),
        inputs=tuple(inputs),
        outputs=tuple(outputs),
        trigger_mode="scheduled",
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
        # anomaly's output collection is materialised -> the card links it.
        Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly-anomaly",
            name="CHIRPS Monthly Anomaly",
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

    def test_manual_and_scheduled_products_are_runnable_event_ones_are_not(self):
        # The fixture's products are all trigger_mode="scheduled" (see _product),
        # so all can_run. Guard the flag exists and reflects trigger_mode.
        with self._patch_defs():
            cards = self._cards_by_key(build_chain(self.feed))

        self.assertTrue(cards["climatology"]["can_run"])


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
        Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly-anomaly",
            name="CHIRPS Monthly Anomaly",
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
