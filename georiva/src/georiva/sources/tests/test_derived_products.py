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
from georiva.core.models import Catalog
from georiva.sources.models import DataFeed, DerivedProduct
from georiva.sources.setup_service import SourceSetupService
from georiva.sources.views import (
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
            [(_definition(), {"quantity": "anomaly", "min_years": "25"})],
        )

        product = DerivedProduct.objects.get(data_feed=self.feed, definition_key="anomaly")
        self.assertEqual(product.recipe_type, "climatology")
        # Config is validated/coerced through the definition's config_schema.
        self.assertEqual(product.config, {"quantity": "anomaly", "min_years": 25})
        self.assertTrue(product.is_enabled)

    def test_invalid_config_rejects_the_batch_without_writing_any_rows(self):
        good = _definition(key="promotion", recipe_type="promotion", config_schema=())
        bad = _definition()  # has a 'quantity' choice field

        with self.assertRaises(ValueError):
            self.service.provision_derived_products(
                self.feed,
                [
                    (good, {}),
                    (bad, {"quantity": "not-a-choice"}),
                ],
            )

        # The valid product must not have been left half-provisioned.
        self.assertEqual(DerivedProduct.objects.filter(data_feed=self.feed).count(), 0)

    def test_reprovisioning_updates_in_place_rather_than_duplicating(self):
        self.service.provision_derived_products(
            self.feed, [(_definition(), {"min_years": "30"})]
        )
        self.service.provision_derived_products(
            self.feed, [(_definition(), {"min_years": "50"})]
        )

        products = DerivedProduct.objects.filter(data_feed=self.feed, definition_key="anomaly")
        self.assertEqual(products.count(), 1)
        self.assertEqual(products.get().config["min_years"], 50)


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


class SelectedProductsFromSessionTests(TestCase):
    def setUp(self):
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="Feed", catalog=self.catalog)

    def test_pairs_each_stored_config_with_its_declared_definition(self):
        defn = _definition()
        session = {"derived_products_config": {"anomaly": {"min_years": 25}}}

        with patch.object(DataFeed, "get_derived_products", return_value=[defn]):
            pairs = selected_products_from_session(self.feed, session)

        self.assertEqual(pairs, [(defn, {"min_years": 25})])

    def test_ignores_config_for_undeclared_products(self):
        defn = _definition()
        session = {"derived_products_config": {"ghost": {}}}

        with patch.object(DataFeed, "get_derived_products", return_value=[defn]):
            pairs = selected_products_from_session(self.feed, session)

        self.assertEqual(pairs, [])

    def test_no_products_section_yields_nothing(self):
        with patch.object(DataFeed, "get_derived_products", return_value=[]):
            self.assertEqual(selected_products_from_session(self.feed, {}), [])
