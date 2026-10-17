from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from georiva.core.models import Catalog, Collection
from georiva.organisations.testing import dial_org, join_org, make_organisation

User = get_user_model()


class DataManagersGatingTests(TestCase):
    """Data managers use the guided flows; raw model editing is for advanced users."""

    def _data_manager(self):
        from django.contrib.auth.models import Group

        user = User.objects.create_user("dm", "dm@x.com", "pw")
        # Joining the organisation is what grants the group in production; the
        # explicit add here keeps the test honest about which layer it exercises.
        join_org(user)
        user.groups.add(Group.objects.get(name="Data Managers"))
        return user

    def test_group_provisioned_with_browse_but_not_edit_permissions(self):
        from django.contrib.auth.models import Group

        group = Group.objects.get(name="Data Managers")
        codenames = set(group.permissions.values_list("codename", flat=True))
        self.assertIn("access_admin", codenames)
        self.assertIn("view_catalog", codenames)
        self.assertIn("view_collection", codenames)
        for forbidden in (
            "add_catalog",
            "change_catalog",
            "delete_catalog",
            "add_collection",
            "change_collection",
            "delete_collection",
        ):
            self.assertNotIn(forbidden, codenames)

    def test_data_manager_can_browse_but_not_edit_catalogs(self):
        dial_org(self.client)
        self.client.force_login(self._data_manager())
        self.assertEqual(self.client.get(reverse("catalog:index")).status_code, 200)
        # Raw add/edit forms are denied server-side (redirect with permission error)
        self.assertEqual(self.client.get(reverse("catalog:add")).status_code, 302)

    def test_data_manager_cannot_edit_collections_raw(self):
        catalog = Catalog.objects.create(organisation=make_organisation(), name="C", slug="c", file_format="grib2")
        collection = Collection.objects.create(catalog=catalog, name="S", slug="s")
        dial_org(self.client)
        self.client.force_login(self._data_manager())
        self.assertEqual(self.client.get(reverse("collection:edit", args=[collection.pk])).status_code, 302)

    def test_advanced_user_keeps_the_raw_escape_hatch(self):
        admin = User.objects.create_superuser("root", "r@x.com", "pw")
        dial_org(self.client)
        self.client.force_login(admin)
        self.assertEqual(self.client.get(reverse("catalog:add")).status_code, 200)

    def test_data_manager_sees_the_data_menu(self):
        from django.test import RequestFactory
        from wagtail.admin.menu import admin_menu

        request = RequestFactory().get("/admin/")
        request.user = self._data_manager()
        labels = [str(i.label) for i in admin_menu.menu_items_for_request(request)]
        self.assertIn("Data", labels)
