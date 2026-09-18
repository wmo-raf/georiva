from contextlib import contextmanager

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


class PublicationsMenuTests(TestCase):
    """Core owns the "Publications" group and publisher plugins fill it.

    Core knows nothing about any publisher — that is what makes them plugins —
    so the group is a container with a hook, ``register_publications_menu_item``,
    and no children of its own. It sits after Data, which is the order of the
    data's own life: catalogued, brought in, sent out. And it is absent rather
    than empty when nothing has registered, because a heading with nothing
    under it reads as a page that failed to load.
    """

    def setUp(self):
        self.user = User.objects.create_superuser("admin", "a@a.com", "pw")

    def _request(self):
        from django.test import RequestFactory

        request = RequestFactory().get("/admin/")
        request.user = self.user
        return request

    def _top_level(self):
        from wagtail.admin.menu import admin_menu

        return {str(i.label): i for i in admin_menu.menu_items_for_request(self._request())}

    @contextmanager
    def _publishers(self, *items):
        """The group as it reads with exactly these children registered.

        The menu caches what its hook returned in its instance dict for the life
        of the process, so the cache entry is what gets replaced and restored —
        the test runs beside whatever publisher plugins the instance actually has.
        """
        from unittest.mock import patch

        from georiva.core.menus import publications_menu

        with patch.dict(publications_menu.__dict__, {"registered_menu_items": list(items)}):
            yield

    def test_the_group_sits_between_data_and_color_ramps(self):
        from wagtail.admin.menu import MenuItem

        with self._publishers(MenuItem("Stub", "/stub/")):
            items = self._top_level()

        self.assertIn("Publications", items)
        self.assertLess(items["Data"].order, items["Publications"].order)
        self.assertLess(items["Publications"].order, items["Color Ramps"].order)

    def test_a_plugin_registers_into_it_through_the_hook(self):
        from wagtail import hooks
        from wagtail.admin.menu import Menu, MenuItem

        from georiva.core.menus import publications_menu

        with hooks.register_temporarily("register_publications_menu_item", lambda: MenuItem("Stub", "/stub/")):
            # A fresh menu over the same hook, so the process-wide cache on the
            # real one is neither read nor written.
            fresh = Menu(register_hook_name=publications_menu.register_hook_name)
            labels = [str(i.label) for i in fresh.menu_items_for_request(self._request())]

        self.assertIn("Stub", labels)

    def test_the_group_hides_itself_when_nothing_has_registered(self):
        with self._publishers():
            items = self._top_level()

        self.assertNotIn("Publications", items)
