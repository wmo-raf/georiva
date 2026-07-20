"""
Admin HTTP-seam tests for DataFeed deletion (issue #243).

Deleting a DataFeed cascades to its claimed Catalog and everything inside it.
The delete flow must show the operator what will be destroyed — the Catalog,
each Collection with its item count, the feed's own derived products, and any
other feeds' derived products bound to the doomed collections — and only
delete after an explicit POST. The cascade semantics themselves are unchanged.
"""
from datetime import datetime, timezone

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection, Item
from georiva.sources.models import (
    DataFeed,
    DerivedProduct,
    DerivedProductInput,
    DerivedProductOutput,
)

User = get_user_model()


def _make_item(collection, n):
    return Item.objects.create(
        collection=collection,
        time=datetime(2024, 1, 1 + n, tzinfo=timezone.utc),
        source_file=f"src/{collection.slug}/{n}.tif",
    )


class DataFeedDeleteBase(TestCase):
    def setUp(self):
        from georiva.sources.tests.support import ensure_base_datafeed_viewset
        ensure_base_datafeed_viewset()
        self.user = User.objects.create_superuser("op", "op@test.com", "pw")
        self.client.force_login(self.user)
        self.catalog = Catalog.objects.create(
            name="CHIRPS", slug="chirps", file_format="geotiff"
        )
        self.feed = DataFeed.objects.create(name="CHIRPS Feed", catalog=self.catalog)
        self.rainfall = Collection.objects.create(
            catalog=self.catalog, name="Rainfall Monthly", slug="rainfall-monthly"
        )
        self.anomaly_col = Collection.objects.create(
            catalog=self.catalog, name="Rainfall Anomaly", slug="rainfall-anomaly"
        )
        for n in range(2):
            _make_item(self.rainfall, n)
        _make_item(self.anomaly_col, 0)

    def _url(self, feed=None):
        return reverse("data_feed_delete", kwargs={"pk": (feed or self.feed).pk})


class ConfirmationPageTests(DataFeedDeleteBase):
    def test_confirmation_enumerates_catalog_and_collections_with_item_counts(self):
        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "CHIRPS")
        self.assertContains(response, "Rainfall Monthly")
        self.assertContains(response, "Rainfall Anomaly")
        # Live item counts, not the denormalised Collection.item_count field
        # (which stays 0 for directly-created rows).
        self.assertContains(response, "2 items")
        self.assertContains(response, "1 item")

    def test_confirmation_lists_own_derived_products(self):
        DerivedProduct.objects.create(
            data_feed=self.feed,
            definition_key="anomaly",
            recipe_type="climatology",
            title="Rainfall anomaly product",
        )

        response = self.client.get(self._url())

        self.assertContains(response, "Rainfall anomaly product")

    def test_confirmation_lists_other_feeds_products_bound_to_doomed_collections(self):
        other_catalog = Catalog.objects.create(
            name="Other", slug="other", file_format="geotiff"
        )
        other_feed = DataFeed.objects.create(name="Other Feed", catalog=other_catalog)
        external = DerivedProduct.objects.create(
            data_feed=other_feed,
            definition_key="drought-index",
            recipe_type="index",
            title="Drought index",
        )
        DerivedProductInput.objects.create(
            product=external,
            role="value",
            tier="published",
            source_key="rainfall-monthly",
            collection=self.rainfall,
        )

        response = self.client.get(self._url())

        self.assertContains(response, "Drought index")
        self.assertContains(response, "Other Feed")

    def test_unrelated_products_are_not_listed(self):
        other_catalog = Catalog.objects.create(
            name="Other", slug="other", file_format="geotiff"
        )
        other_feed = DataFeed.objects.create(name="Other Feed", catalog=other_catalog)
        DerivedProduct.objects.create(
            data_feed=other_feed,
            definition_key="unrelated",
            recipe_type="index",
            title="Unrelated product",
        )

        response = self.client.get(self._url())

        self.assertNotContains(response, "Unrelated product")

    def test_get_deletes_nothing(self):
        self.client.get(self._url())

        self.assertTrue(DataFeed.objects.filter(pk=self.feed.pk).exists())
        self.assertTrue(Catalog.objects.filter(pk=self.catalog.pk).exists())
        self.assertEqual(Collection.objects.count(), 2)
        self.assertEqual(Item.objects.count(), 3)

    def test_feed_without_catalog_renders(self):
        lone = DataFeed.objects.create(name="Lone Feed")

        response = self.client.get(self._url(lone))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Lone Feed")


class DeletionTests(DataFeedDeleteBase):
    def test_post_deletes_feed_and_cascades_to_catalog_tree(self):
        response = self.client.post(self._url())

        self.assertRedirects(response, reverse("data_feed_list"))
        self.assertFalse(DataFeed.objects.filter(pk=self.feed.pk).exists())
        self.assertFalse(Catalog.objects.filter(pk=self.catalog.pk).exists())
        self.assertEqual(Collection.objects.count(), 0)
        self.assertEqual(Item.objects.count(), 0)

    def test_post_deletes_feed_without_catalog(self):
        lone = DataFeed.objects.create(name="Lone Feed")

        response = self.client.post(self._url(lone))

        self.assertRedirects(response, reverse("data_feed_list"))
        self.assertFalse(DataFeed.objects.filter(pk=lone.pk).exists())
        # The other feed's catalog tree is untouched.
        self.assertTrue(Catalog.objects.filter(pk=self.catalog.pk).exists())
        self.assertEqual(Collection.objects.count(), 2)

    def test_external_product_survives_but_loses_its_bindings(self):
        other_catalog = Catalog.objects.create(
            name="Other", slug="other", file_format="geotiff"
        )
        other_feed = DataFeed.objects.create(name="Other Feed", catalog=other_catalog)
        external = DerivedProduct.objects.create(
            data_feed=other_feed,
            definition_key="drought-index",
            recipe_type="index",
            title="Drought index",
        )
        DerivedProductInput.objects.create(
            product=external,
            role="value",
            tier="published",
            source_key="rainfall-monthly",
            collection=self.rainfall,
        )
        out_col = Collection.objects.create(
            catalog=other_catalog, name="Drought", slug="drought"
        )
        DerivedProductOutput.objects.create(
            product=external,
            role="index",
            output_key="drought",
            collection=out_col,
        )

        self.client.post(self._url())

        external.refresh_from_db()
        self.assertEqual(external.input_bindings.count(), 0)
        self.assertEqual(external.output_bindings.count(), 1)


class PermissionTests(DataFeedDeleteBase):
    """The view keeps the delete-permission gate the viewset DeleteView had."""

    def _login_without_delete_perm(self):
        from django.contrib.auth.models import Permission

        limited = User.objects.create_user("dm", "dm@test.com", "pw")
        limited.user_permissions.add(
            Permission.objects.get(
                content_type__app_label="wagtailadmin", codename="access_admin"
            )
        )
        self.client.force_login(limited)
        return limited

    def test_get_without_delete_permission_is_denied(self):
        self._login_without_delete_perm()

        response = self.client.get(self._url())

        self.assertRedirects(response, reverse("wagtailadmin_home"))

    def test_post_without_delete_permission_deletes_nothing(self):
        self._login_without_delete_perm()

        response = self.client.post(self._url())

        self.assertRedirects(response, reverse("wagtailadmin_home"))
        self.assertTrue(DataFeed.objects.filter(pk=self.feed.pk).exists())
        self.assertTrue(Catalog.objects.filter(pk=self.catalog.pk).exists())

    def test_delete_permission_on_the_feed_model_grants_access(self):
        from django.contrib.auth.models import Permission

        limited = self._login_without_delete_perm()
        limited.user_permissions.add(
            Permission.objects.get(
                content_type__app_label="georivasources", codename="delete_datafeed"
            )
        )

        response = self.client.get(self._url())

        self.assertEqual(response.status_code, 200)


class DeleteUrlTests(DataFeedDeleteBase):
    def test_delete_url_points_at_the_confirmation_view(self):
        self.assertEqual(self.feed.delete_url, self._url())
