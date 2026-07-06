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
from django.test import TestCase
from django.urls import reverse

from georiva.core.derived_products import (
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog
from georiva.sources.models import DataFeed, DerivedProduct
from georiva.sources.product_service import (
    ProductActionError,
    disable_product,
    enable_product,
    enabled_dependents,
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
