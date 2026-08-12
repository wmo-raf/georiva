from django.contrib.auth import get_user_model
from django.test import TestCase

User = get_user_model()


class DataMenuTests(TestCase):
    """Acquisition surfaces live under the "Data" menu group; Catalogs sits
    just above it as a top-level entry; derived products have no menu item at
    all — they are reached from each feed's dashboard."""

    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@a.com", "pw")

    def _request(self):
        from django.test import RequestFactory

        request = RequestFactory().get("/admin/")
        request.user = self.user
        return request

    def test_data_group_contains_the_acquisition_surfaces_in_order(self):
        from wagtail.admin.menu import admin_menu

        request = self._request()
        items = admin_menu.menu_items_for_request(request)
        data_item = next((i for i in items if str(i.label) == "Data"), None)
        self.assertIsNotNone(data_item, "No top-level 'Data' menu group found")
        sub_labels = [str(i.label) for i in data_item.menu.menu_items_for_request(request)]
        self.assertEqual(
            sub_labels,
            [
                "Add Data",
                "Automated Sources",
                "Manual Uploads",
            ],
        )

    def test_catalogs_is_a_top_level_item_sorted_above_the_data_group(self):
        from wagtail.admin.menu import admin_menu

        items = {str(i.label): i for i in admin_menu.menu_items_for_request(self._request())}
        self.assertIn("Catalogs", items)
        # menu_items_for_request does not sort; the rendered menu orders by
        # `order`, so compare that.
        self.assertLess(items["Catalogs"].order, items["Data"].order)

    def test_acquisition_surfaces_and_derived_products_are_not_top_level_items(self):
        from wagtail.admin.menu import admin_menu

        labels = [str(i.label) for i in admin_menu.menu_items_for_request(self._request())]
        for old in ("Automated Sources", "Manual Uploads", "Derived Products"):
            self.assertNotIn(old, labels)
