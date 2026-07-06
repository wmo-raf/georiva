"""
DataFeed's derived-product declaration hook (ADR-0008, issue #143).

A feed declares the derived products it offers via get_derived_products(); the
base default is none. Plugins override to return DerivedProductDefinitions bound
to their configured collections.
"""
from unittest.mock import patch

from django.test import TestCase

from georiva.core.derived_products import (
    ConfigField,
    DerivedProductDefinition,
    InputRef,
    OutputRef,
)
from georiva.core.models import Catalog, Collection
from georiva.sources.models import DataFeed, DataFeedCollectionLink, DerivedProduct
from georiva.sources.setup_service import SourceSetupService
from georiva.sources.views import (
    _transient_feed_for_products,
    build_product_config_form,
    selected_products_from_session,
)


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


class GetDerivedProductsTests(TestCase):
    def test_defaults_to_empty_list(self):
        catalog = Catalog.objects.create(name="CHIRPS", slug="chirps", file_format="geotiff")
        feed = DataFeed.objects.create(name="Feed", catalog=catalog)

        self.assertEqual(feed.get_derived_products(), [])


class DerivedProductModelTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def test_persists_as_a_child_of_a_data_feed(self):
        product = DerivedProduct.objects.create(
            data_feed=self.feed,
            definition_key="anomaly",
            recipe_type="climatology",
            config={"baseline": "1991-2020", "quantity": "anomaly"},
            interval_minutes=1440,
        )

        product.refresh_from_db()
        self.assertEqual(product.data_feed_id, self.feed.pk)
        self.assertEqual(product.config["baseline"], "1991-2020")
        self.assertEqual(product.interval_minutes, 1440)
        self.assertEqual(list(self.feed.derived_products.all()), [product])

    def test_is_enabled_defaults_to_true(self):
        product = DerivedProduct.objects.create(
            data_feed=self.feed, definition_key="anomaly", recipe_type="climatology",
        )
        self.assertTrue(product.is_enabled)


class ProvisionDerivedProductsTests(TestCase):
    def setUp(self):
        self.service = SourceSetupService()
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def test_creates_a_row_per_selected_definition(self):
        self.service.provision_derived_products(
            self.feed,
            [(_definition(), {"quantity": "anomaly", "min_years": "25"}, True)],
        )

        product = DerivedProduct.objects.get(data_feed=self.feed, definition_key="anomaly")
        self.assertEqual(product.recipe_type, "climatology")
        # Config is validated/coerced through the definition's config_schema.
        self.assertEqual(product.config, {"quantity": "anomaly", "min_years": 25})
        self.assertTrue(product.is_enabled)

    def test_unticked_definition_provisions_a_disabled_row(self):
        # Every declared product gets a row; an operator's opt-out is recorded as
        # is_enabled=False, not as a missing row — so it stays visible and
        # re-enablable later.
        self.service.provision_derived_products(
            self.feed, [(_definition(), {}, False)]
        )

        product = DerivedProduct.objects.get(data_feed=self.feed, definition_key="anomaly")
        self.assertFalse(product.is_enabled)

    def test_invalid_config_rejects_the_batch_without_writing_any_rows(self):
        good = _definition(key="promotion", recipe_type="promotion", config_schema=())
        bad = _definition()  # has a 'quantity' choice field

        with self.assertRaises(ValueError):
            self.service.provision_derived_products(
                self.feed,
                [
                    (good, {}, True),
                    (bad, {"quantity": "not-a-choice"}, True),
                ],
            )

        # The valid product must not have been left half-provisioned.
        self.assertEqual(DerivedProduct.objects.filter(data_feed=self.feed).count(), 0)

    def test_reprovisioning_updates_in_place_rather_than_duplicating(self):
        self.service.provision_derived_products(
            self.feed, [(_definition(), {"min_years": "30"}, True)]
        )
        self.service.provision_derived_products(
            self.feed, [(_definition(), {"min_years": "50"}, True)]
        )

        products = DerivedProduct.objects.filter(data_feed=self.feed, definition_key="anomaly")
        self.assertEqual(products.count(), 1)
        self.assertEqual(products.get().config["min_years"], 50)

    def test_creates_a_row_for_every_declared_definition_ticked_or_not(self):
        # Provisioning is "a row per declared definition", not "rows for what's
        # ticked" — the opt-out lives in is_enabled, not in row presence.
        anomaly = _definition(key="anomaly")
        promotion = _definition(key="promotion", recipe_type="promotion", config_schema=())

        self.service.provision_derived_products(
            self.feed,
            [(anomaly, {}, True), (promotion, {}, False)],
        )

        rows = DerivedProduct.objects.filter(data_feed=self.feed).order_by("definition_key")
        self.assertEqual(
            [(r.definition_key, r.is_enabled) for r in rows],
            [("anomaly", True), ("promotion", False)],
        )

    def test_reprovisioning_never_flips_an_existing_is_enabled(self):
        # is_enabled is a create-time default: once a row exists, re-running the
        # wizard edits config but must not clobber a toggle the operator changed
        # after setup.
        self.service.provision_derived_products(
            self.feed, [(_definition(), {"min_years": "30"}, True)]
        )
        # Operator later disables it out-of-band.
        product = DerivedProduct.objects.get(data_feed=self.feed, definition_key="anomaly")
        product.is_enabled = False
        product.save(update_fields=["is_enabled"])

        # Wizard re-run arrives with enabled=True again.
        self.service.provision_derived_products(
            self.feed, [(_definition(), {"min_years": "50"}, True)]
        )

        product.refresh_from_db()
        self.assertFalse(product.is_enabled)      # toggle preserved
        self.assertEqual(product.config["min_years"], 50)   # config still updated

    def test_unticked_product_provisions_with_schema_defaults(self):
        # An unticked product is validated with an empty config, so it lands with
        # its declared schema defaults filled in — ready to run if enabled later.
        self.service.provision_derived_products(
            self.feed, [(_definition(), {}, False)]
        )

        product = DerivedProduct.objects.get(data_feed=self.feed, definition_key="anomaly")
        self.assertEqual(product.config, {"quantity": "anomaly", "min_years": 30})

    def test_provisioning_enabled_product_materialises_its_output_collections(self):
        # A product enabled in the wizard has its outputs materialised at
        # provision time, with the declared metadata.
        defn = _definition(outputs=(
            OutputRef(role="anomaly", collection="rainfall-anomaly",
                      title="Rainfall Anomaly", visibility="internal"),
        ))

        self.service.provision_derived_products(self.feed, [(defn, {}, True)])

        collection = Collection.objects.get(
            catalog=self.catalog, slug="rainfall-anomaly"
        )
        self.assertEqual(collection.name, "Rainfall Anomaly")
        self.assertEqual(collection.visibility, Collection.Visibility.INTERNAL)

    def test_provisioning_disabled_product_does_not_materialise_outputs(self):
        # An unticked product's outputs stay latent — they materialise only when
        # it is enabled later.
        defn = _definition(outputs=(
            OutputRef(role="anomaly", collection="rainfall-anomaly"),
        ))

        self.service.provision_derived_products(self.feed, [(defn, {}, False)])

        self.assertFalse(
            Collection.objects.filter(slug="rainfall-anomaly").exists()
        )


class BuildProductConfigFormTests(TestCase):
    def test_form_has_a_field_per_config_option_with_defaults(self):
        form = build_product_config_form(_definition())()

        self.assertEqual(set(form.fields), {"quantity", "min_years"})
        self.assertEqual(form.fields["min_years"].initial, 30)

    def test_valid_submission_cleans_config_through_the_definition(self):
        form_cls = build_product_config_form(_definition())
        form = form_cls(data={"quantity": "value", "min_years": "40"})

        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_config, {"quantity": "value", "min_years": 40})

    def test_out_of_choices_value_is_a_form_error(self):
        form_cls = build_product_config_form(_definition())
        form = form_cls(data={"quantity": "trend", "min_years": "30"})

        self.assertFalse(form.is_valid())

    def test_definition_without_options_has_no_form(self):
        self.assertIsNone(build_product_config_form(_definition(config_schema=())))


class TransientFeedForProductsTests(TestCase):
    """The wizard's step 4 declares products on an unsaved feed (no
    collection_links yet), so the selected resolutions are stashed on the
    transient instance for an instance get_derived_products() to read."""

    def test_stashes_selected_collection_keys_from_the_session(self):
        session = {
            "catalog_mode": "create",
            "new_catalog_slug": "chirps",
            "new_catalog_name": "CHIRPS",
            "selected_collection_keys": ["chirps-monthly", "chirps-dekadal"],
        }

        feed = _transient_feed_for_products(DataFeed, session)

        self.assertIsNone(feed.pk)
        self.assertEqual(
            feed._wizard_selected_keys, ["chirps-monthly", "chirps-dekadal"]
        )

    def test_no_selection_stashes_an_empty_list(self):
        feed = _transient_feed_for_products(DataFeed, {"catalog_mode": "create"})

        self.assertEqual(feed._wizard_selected_keys, [])


class SelectedDefinitionKeysTests(TestCase):
    """A feed's active definition keys — what an instance get_derived_products()
    binds its products to — come from collection_links once saved, or the
    wizard's stash while still transient, and must agree across the two."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )

    def test_saved_feed_reads_keys_from_collection_links(self):
        feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)
        collection = Collection.objects.create(
            catalog=self.catalog, slug="chirps-monthly", name="CHIRPS Monthly"
        )
        DataFeedCollectionLink.objects.create(
            data_feed=feed, collection=collection, definition_key="chirps-monthly"
        )

        self.assertEqual(feed.selected_definition_keys(), ["chirps-monthly"])

    def test_transient_feed_reads_keys_from_the_wizard_stash(self):
        feed = DataFeed(name="Feed", catalog=self.catalog)
        feed._wizard_selected_keys = ["chirps-monthly"]

        self.assertEqual(feed.selected_definition_keys(), ["chirps-monthly"])

    def test_transient_feed_without_a_stash_has_no_keys(self):
        self.assertEqual(DataFeed(name="Feed").selected_definition_keys(), [])


class SelectedProductsFromSessionTests(TestCase):
    """The wizard hands provisioning a triple for *every* declared definition —
    enablement from the operator's tick selection, config from the step-4 form —
    so provisioning always writes a full row set with the opt-out in is_enabled."""

    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def test_yields_a_triple_for_every_declared_definition(self):
        anomaly = _definition(key="anomaly")
        promotion = _definition(key="promotion")
        session = {
            "derived_products_config": {"anomaly": {"min_years": 25}},
            "selected_product_keys": ["anomaly"],
        }

        with patch.object(DataFeed, "get_derived_products",
                          return_value=[anomaly, promotion]):
            triples = selected_products_from_session(self.feed, session)

        # A triple per declared definition; enabled reflects the tick selection,
        # config comes from the step-4 form (or {} for an unticked product).
        self.assertEqual(
            triples,
            [(anomaly, {"min_years": 25}, True), (promotion, {}, False)],
        )

    def test_empty_selection_disables_every_product(self):
        # Unticking all is a real choice — an empty list is honoured, not
        # treated as "no selection made".
        defn = _definition()
        session = {"selected_product_keys": []}

        with patch.object(DataFeed, "get_derived_products", return_value=[defn]):
            triples = selected_products_from_session(self.feed, session)

        self.assertEqual(triples, [(defn, {}, False)])

    def test_absent_selection_falls_back_to_default_enabled(self):
        # No selection stored (e.g. a stale/short-circuit path) → the plugin's
        # declared default_enabled decides, so nothing regresses to disabled.
        on = _definition(key="on", default_enabled=True)
        off = _definition(key="off", default_enabled=False)

        with patch.object(DataFeed, "get_derived_products", return_value=[on, off]):
            triples = selected_products_from_session(self.feed, {})

        self.assertEqual(triples, [(on, {}, True), (off, {}, False)])

    def test_ignores_config_for_undeclared_products(self):
        defn = _definition()
        session = {
            "derived_products_config": {"anomaly": {}, "ghost": {"x": 1}},
            "selected_product_keys": ["anomaly", "ghost"],
        }

        with patch.object(DataFeed, "get_derived_products", return_value=[defn]):
            triples = selected_products_from_session(self.feed, session)

        self.assertEqual(triples, [(defn, {}, True)])

    def test_no_declared_products_yields_nothing(self):
        with patch.object(DataFeed, "get_derived_products", return_value=[]):
            self.assertEqual(selected_products_from_session(self.feed, {}), [])
