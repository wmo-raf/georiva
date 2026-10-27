"""What a derivation attempt's failure rolls up under on the dashboard (ADR 0033)."""

from django.test import TestCase

from georiva.processing.models import DerivationAttemptFinished


class DerivationFailureGroupTests(TestCase):
    def test_it_groups_under_the_product_it_came_from(self):
        event = DerivationAttemptFinished(origin="derived_product:42", recipe_type="climatology")
        self.assertEqual(event.failure_group_key(), "derived_product:42")

    def test_without_a_product_it_groups_under_its_recipe(self):
        event = DerivationAttemptFinished(origin="", recipe_type="promotion")
        self.assertEqual(event.failure_group_key(), "recipe:promotion")

    def test_a_product_that_is_gone_is_labelled_by_its_recipe_with_no_link(self):
        group = DerivationAttemptFinished(origin="derived_product:999999", recipe_type="climatology").failure_group()
        self.assertEqual((group.label, group.url), ("climatology", None))
