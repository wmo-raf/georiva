"""
Admin HTTP-seam tests for the wizard's Derived Products step (issue #165).

Operators opt individual derived products in/out via a checkbox per product.
The step pre-ticks from each definition's ``default_enabled`` (or the prior
session selection on a back-navigation), validates config only for ticked
products, and carries the selection through to provisioning — which writes a
row for *every* declared definition with the opt-out in ``is_enabled``.

The wizard resolves an operator-chosen source type to a concrete DataFeed
subclass; core ships none (they come from plugins), so these tests drive the
base DataFeed as the model, patching model resolution and the feed's declared
products.
"""
import re

from django.test import TestCase
from django.urls import reverse
from unittest.mock import patch

from georiva.core.derived_products import (
    ConfigField,
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from django.contrib.auth import get_user_model

from georiva.core.models import Catalog
from georiva.sources.models import DataFeed, DerivedProduct

User = get_user_model()

MODEL_NAME = "datafeed"
SESSION_KEY = f"georiva_setup_wizard_{MODEL_NAME}"


def _definition(**overrides):
    kwargs = dict(
        key="anomaly",
        recipe_type="climatology",
        label="Rainfall anomaly",
        description="Anomaly vs a baseline.",
        config_schema=(
            ConfigField(key="quantity", type="choice",
                        choices=("anomaly", "value"), default="anomaly"),
            ConfigField(key="min_years", type="int", default=30),
        ),
        inputs=(InputRef(role="value", collection="rainfall", tier="staging"),),
        outputs=(OutputRef(role="anomaly", collection="rainfall-anomaly"),),
        trigger_mode="scheduled",
    )
    kwargs.update(overrides)
    return DerivedProductDefinition(**kwargs)


def _chain_defs():
    """A CHIRPS-shaped pair with the one real edge: anomaly depends on
    climatology (its required published baseline names climatology's output)."""
    clim = _definition(
        key="climatology",
        label="Climatology",
        config_schema=(),
        inputs=(InputRef(role="value", collection="chirps-monthly", tier="staging"),),
        outputs=(OutputRef(role="climatology",
                           collection="chirps-monthly-climatology"),),
    )
    anomaly = _definition(
        key="anomaly",
        label="Rainfall anomaly",
        config_schema=(),
        inputs=(
            InputRef(role="value", collection="chirps-monthly", tier="staging"),
            InputRef(role="baseline",
                     collection="chirps-monthly-climatology", tier="published"),
        ),
        outputs=(OutputRef(role="anomaly", collection="chirps-monthly-anomaly"),),
    )
    return [clim, anomaly]


def _checkbox_is_checked(html, key):
    """Whether the enable checkbox for ``key`` is rendered ``checked``."""
    match = re.search(
        rf'<input[^>]*name="products"[^>]*value="{key}"[^>]*>', html
    )
    assert match, f"no enable checkbox rendered for product '{key}'"
    return "checked" in match.group(0)


class WizardStepBase(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("wiz", "w@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )

    def _set_session(self, **extra):
        session = self.client.session
        session[SESSION_KEY] = {"selected_collection_keys": ["chirps-monthly"], **extra}
        session.save()

    def _step4_url(self):
        return reverse("wizard_step4_products", kwargs={"model_name": MODEL_NAME})


class Step4EnableCheckboxTests(WizardStepBase):
    def test_get_pre_ticks_from_default_enabled(self):
        self._set_session()
        on = _definition(key="on", default_enabled=True)
        off = _definition(key="off", default_enabled=False)

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=[on, off]),
        ):
            response = self.client.get(self._step4_url())

        self.assertEqual(response.status_code, 200)
        html = response.content.decode()
        self.assertTrue(_checkbox_is_checked(html, "on"))
        self.assertFalse(_checkbox_is_checked(html, "off"))

    def test_get_pre_ticks_from_the_prior_session_selection(self):
        # A back-navigation restores the operator's earlier ticks, overriding the
        # declared defaults: 'on' was unticked and 'off' was ticked last time.
        self._set_session(selected_product_keys=["off"])
        on = _definition(key="on", default_enabled=True)
        off = _definition(key="off", default_enabled=False)

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=[on, off]),
        ):
            response = self.client.get(self._step4_url())

        html = response.content.decode()
        self.assertFalse(_checkbox_is_checked(html, "on"))
        self.assertTrue(_checkbox_is_checked(html, "off"))


class Step4PostTests(WizardStepBase):
    def test_post_records_the_selection_and_only_validates_ticked_products(self):
        self._set_session()
        ticked = _definition(key="ticked", config_schema=(
            ConfigField(key="min_years", type="int", default=30),
        ))
        optout = _definition(key="optout", config_schema=(
            ConfigField(key="quantity", type="choice",
                        choices=("anomaly", "value"), default="anomaly"),
        ))

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=[ticked, optout]),
        ):
            response = self.client.post(self._step4_url(), {
                "products": ["ticked"],          # optout is unticked
                "ticked-min_years": "40",
                # An out-of-choices value that WOULD fail validation if optout
                # were validated — it must be ignored because optout is unticked.
                "optout-quantity": "trend",
            })

        # No validation error -> straight through to provisioning.
        self.assertRedirects(
            response,
            reverse("wizard_provision", kwargs={"model_name": MODEL_NAME}),
            fetch_redirect_response=False,
        )
        session = self.client.session[SESSION_KEY]
        self.assertEqual(session["selected_product_keys"], ["ticked"])
        self.assertEqual(session["derived_products_config"]["ticked"], {"min_years": 40})
        # Unticked product carries no config -> provisions with schema defaults.
        self.assertEqual(session["derived_products_config"]["optout"], {})

    def test_post_reports_a_bad_config_on_a_ticked_product(self):
        self._set_session()
        ticked = _definition(key="ticked", config_schema=(
            ConfigField(key="quantity", type="choice",
                        choices=("anomaly", "value"), default="anomaly"),
        ))

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=[ticked]),
        ):
            response = self.client.post(self._step4_url(), {
                "products": ["ticked"],
                "ticked-quantity": "trend",   # not among choices
            })

        # Re-renders the step (no redirect) because a ticked product is invalid.
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("selected_product_keys", self.client.session.get(SESSION_KEY, {}))


class Step4DependencyTests(WizardStepBase):
    """The wizard enforces the chain server-side: a selection that enables a
    product without its dependencies is rejected, JS cascade or not."""

    def test_post_rejects_a_product_selected_without_its_dependency(self):
        self._set_session()

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=_chain_defs()),
        ):
            response = self.client.post(self._step4_url(), {
                "products": ["anomaly"],   # climatology missing
            })

        # Re-renders (no redirect) and names the missing dependency.
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Climatology")
        self.assertNotIn("selected_product_keys", self.client.session.get(SESSION_KEY, {}))

    def test_post_accepts_a_product_with_its_dependency_selected(self):
        self._set_session()

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=_chain_defs()),
        ):
            response = self.client.post(self._step4_url(), {
                "products": ["anomaly", "climatology"],
            })

        self.assertRedirects(
            response,
            reverse("wizard_provision", kwargs={"model_name": MODEL_NAME}),
            fetch_redirect_response=False,
        )
        self.assertEqual(
            set(self.client.session[SESSION_KEY]["selected_product_keys"]),
            {"anomaly", "climatology"},
        )

    def test_get_renders_stage_lanes_with_a_needs_chip_and_adjacency_data(self):
        self._set_session()

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=_chain_defs()),
        ):
            response = self.client.get(self._step4_url())

        html = response.content.decode()
        self.assertEqual(response.status_code, 200)
        # The dependent product advertises what it needs...
        self.assertIn("needs", html.lower())
        self.assertContains(response, "Climatology")
        # ...and the client-side cascade adjacency is emitted for the JS.
        self.assertContains(response, "product-chain-dependencies")


class WizardProvisionSeamTests(WizardStepBase):
    """The end of the seam: a completed wizard session provisions a row for every
    declared definition, with the opt-out landing as is_enabled=False and staying
    visible (disabled) in the tracking dashboard."""

    def _complete_session(self, **extra):
        session = self.client.session
        session[SESSION_KEY] = {
            "selected_collection_keys": ["chirps-monthly"],
            "catalog_mode": "select",
            "catalog_id": self.catalog.pk,
            "new_feed_name": "CHIRPS Feed",
            "new_feed_interval": 360,
            **extra,
        }
        session.save()

    def test_provision_creates_a_row_per_definition_with_selected_enablement(self):
        anomaly = _definition(key="anomaly", config_schema=())
        promotion = _definition(key="promotion", recipe_type="promotion", config_schema=())
        self._complete_session(
            selected_product_keys=["anomaly"],   # promotion unticked
            derived_products_config={"anomaly": {}, "promotion": {}},
        )

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=[anomaly, promotion]),
        ):
            response = self.client.get(
                reverse("wizard_provision", kwargs={"model_name": MODEL_NAME})
            )

        self.assertEqual(response.status_code, 302)
        rows = {p.definition_key: p for p in DerivedProduct.objects.all()}
        self.assertEqual(set(rows), {"anomaly", "promotion"})
        self.assertTrue(rows["anomaly"].is_enabled)
        self.assertFalse(rows["promotion"].is_enabled)

    def test_disabled_product_appears_in_the_tracking_dashboard(self):
        anomaly = _definition(key="anomaly", config_schema=())
        promotion = _definition(key="promotion", recipe_type="promotion", config_schema=())
        self._complete_session(
            selected_product_keys=["anomaly"],
            derived_products_config={"anomaly": {}, "promotion": {}},
        )

        with (
            patch("georiva.sources.views.get_child_model_by_name", return_value=DataFeed),
            patch.object(DataFeed, "get_derived_products", return_value=[anomaly, promotion]),
        ):
            self.client.get(
                reverse("wizard_provision", kwargs={"model_name": MODEL_NAME})
            )
            response = self.client.get(reverse("derived_product_tracking"))

        # The opted-out product is inert but still listed (disabled), ready to be
        # enabled later with one toggle.
        self.assertContains(response, "promotion")
        self.assertFalse(
            DerivedProduct.objects.get(definition_key="promotion").is_enabled
        )
