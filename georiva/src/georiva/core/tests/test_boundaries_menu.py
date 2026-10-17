"""Where the boundary surfaces live in the admin, and who may see them.

Boundaries are configuration for the instance, not data an organisation browses,
so both pages hang off Settings rather than the top-level sidebar. Two of the
three assertions here guard things that are invisible when they break: the
superuser gate, and the suppression of the duplicate entry ``@register_setting``
adds. A Wagtail or ``adminboundarymanager`` upgrade could undo either one
without failing anything else.
"""

from adminboundarymanager.models import AdminBoundarySettings
from adminboundarymanager.views import data_sources
from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse
from wagtail.admin.menu import SubmenuMenuItem, admin_menu, settings_menu
from wagtail.models import GroupPagePermission, Page, Site

from georiva.core.wagtail_hooks import boundary_settings_url
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.tests.factories import PASSWORD, add_member, make_user

HOST = {"host": "kenya.georiva.test"}


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class BoundariesMenuPlacementTests(TestCase):
    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def _login(self, username, **kwargs):
        user = make_user(username, **kwargs)
        # Wagtail's own admin gate: everyone who reaches /admin/ needs this.
        admin_access = Group.objects.create(name=f"{username} admin access")
        admin_access.permissions.add(Permission.objects.get(codename="access_admin"))
        user.groups.add(admin_access)
        self.client.login(username=username, password=PASSWORD)
        return user

    def _request(self):
        return self.client.get(reverse("wagtailadmin_home"), headers=HOST).wsgi_request

    def test_boundaries_is_not_a_top_level_sidebar_item(self):
        self._login("root", superuser=True)
        labels = [item.label for item in admin_menu.menu_items_for_request(self._request())]
        self.assertNotIn("Boundaries", labels)

    def test_the_two_boundary_surfaces_sit_in_one_settings_submenu(self):
        self._login("root", superuser=True)
        request = self._request()

        group = next(item for item in settings_menu.menu_items_for_request(request) if item.label == "Boundaries")
        self.assertIsInstance(group, SubmenuMenuItem)
        self.assertEqual(
            [child.url for child in group.menu.menu_items_for_request(request)],
            [
                reverse("adminboundarymanager_preview_boundary"),
                boundary_settings_url(Site.objects.get(is_default_site=True)),
            ],
        )

    def test_the_superuser_gets_one_door_to_the_settings_page_not_two(self):
        """``@register_setting`` adds its own entry for the same page.

        It is permission-gated, so it only ever surfaces for someone who could
        already reach the page — the superuser. That makes this the only test
        position from which the de-duplication is observable at all: assert it
        anywhere else and the assertion passes whether or not the hook runs.
        """
        self._login("root", superuser=True)
        top_level = settings_menu.menu_items_for_request(self._request())

        self.assertNotIn(boundary_settings_url(), [item.url for item in top_level])

    def test_the_boundaries_submenu_is_hidden_from_an_org_admin(self):
        user = self._login("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.ADMIN)
        items = settings_menu.menu_items_for_request(self._request())

        self.assertNotIn("Boundaries", [item.label for item in items])
        self.assertNotIn(boundary_settings_url(), [item.url for item in items])


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class BoundaryLoaderUrlGateTests(TestCase):
    """The menu hides the loader from non-superusers; the URLs must too.

    ``adminboundarymanager`` gates both views on "holds any page permission",
    which every editor does, and a load rewrites the instance-wide
    ``AdminBoundary`` table (ADR 0011: shared reference data is written by the
    instance admin alone).
    """

    URL_NAMES = ["adminboundarymanager_load_boundary", "adminboundarymanager_preview_boundary"]

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")

    def _login(self, username, **kwargs):
        user = make_user(username, **kwargs)
        group = Group.objects.create(name=f"{username} access")
        group.permissions.add(Permission.objects.get(codename="access_admin"))
        # What makes an editor an editor: a page permission on some subtree.
        GroupPagePermission.objects.create(
            group=group,
            page=Page.get_first_root_node(),
            permission=Permission.objects.get(content_type__app_label="wagtailcore", codename="change_page"),
        )
        user.groups.add(group)
        self.client.login(username=username, password=PASSWORD)
        return user

    def test_an_editor_cannot_reach_either_view(self):
        user = self._login("amina")
        add_member(user, self.kenya, role=OrganisationMembership.Role.MEMBER)

        for name in self.URL_NAMES:
            for method in (self.client.get, self.client.post):
                with self.subTest(view=name, method=method.__name__):
                    response = method(reverse(name), headers=HOST)
                    self.assertIn(response.status_code, (302, 403))

    def test_the_superuser_still_reaches_both_views(self):
        self._login("root", superuser=True)

        for name in self.URL_NAMES:
            with self.subTest(view=name):
                response = self.client.get(reverse(name), headers=HOST)
                self.assertEqual(response.status_code, 200)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class BoundaryLoaderSettingsPinningTests(TestCase):
    """A load rewrites the instance-wide table, so its inputs must be instance-wide.

    ``AdminBoundarySettings`` is one row per site; without pinning, which row
    governed a load depended on the host the superuser happened to be on.
    """

    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.default_site = Site.objects.get(is_default_site=True)
        self.kenya_site = Site.objects.get(hostname="kenya.georiva.test")
        AdminBoundarySettings.objects.update_or_create(site=self.default_site, defaults={"data_source": "gadm41"})
        AdminBoundarySettings.objects.update_or_create(site=self.kenya_site, defaults={"data_source": "codabs"})
        make_user("root", superuser=True)
        self.client.login(username="root", password=PASSWORD)

    def test_the_load_reads_the_default_sites_settings_from_any_host(self):
        for host in ("kenya.georiva.test", self.default_site.hostname):
            with self.subTest(host=host):
                response = self.client.get(reverse("adminboundarymanager_load_boundary"), headers={"host": host})
                self.assertEqual(response.context["data_source"], data_sources["gadm41"])

    def test_the_settings_menu_entry_opens_the_default_sites_settings(self):
        request = self.client.get(reverse("wagtailadmin_home"), headers=HOST).wsgi_request
        group = next(i for i in settings_menu.menu_items_for_request(request) if i.label == "Boundaries")
        entry = group.menu.menu_items_for_request(request)[1]

        self.assertEqual(entry.url, boundary_settings_url(self.default_site))
