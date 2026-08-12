"""One organisation's portal is the whole of its admin's page world.

The fixture is deliberately the hard case: a user who belongs to *both*
organisations and holds every capability Wagtail knows. Wagtail's own page
permissions would already keep an ordinary single-org member out of another
institution's tree, so a test built on one would pass without any of this code.
What is under test is the tenancy layer over that — on Kenya's host, Kenya's
pages, whoever you are elsewhere.
"""

from django.contrib.auth.models import Group, Permission
from django.test import TestCase, override_settings
from django.urls import reverse

from georiva.organisations.provisioning import org_page_group_name, provision_organisation

from .factories import PASSWORD, add_member, make_user


def add_child_page(parent, title, slug):
    from georiva.pages.home.models import HomePage

    page = HomePage(title=title, slug=slug, hero_heading=title)
    parent.add_child(instance=page)
    return page


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class PageTreeScopingTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_root = cls.kenya.site.root_page
        cls.uganda_root = cls.uganda.site.root_page
        cls.kenya_page = add_child_page(cls.kenya_root, "Kenya Bulletin", "bulletin")
        cls.uganda_page = add_child_page(cls.uganda_root, "Uganda Bulletin", "bulletin")

    def setUp(self):
        self.user = make_user("amina")
        # A regional-centre user: a member of both institutions, with every
        # capability. Only the host may separate the two.
        add_member(self.user, self.kenya)
        add_member(self.user, self.uganda)
        everything = Group.objects.create(name="everything")
        everything.permissions.add(*Permission.objects.all())
        self.user.groups.add(everything)
        # Provisioning gives each organisation a group holding page permissions
        # over its own root; belonging to both is what makes this user able to
        # author in either tree, and therefore worth scoping by host.
        for slug in ("kenya", "uganda"):
            self.user.groups.add(Group.objects.get(name=org_page_group_name(slug)))
        self.client.login(username="amina", password=PASSWORD)
        self.client.defaults["HTTP_HOST"] = "kenya.georiva.test"

    # -- the explorer ------------------------------------------------------

    def test_the_explorer_at_the_tree_root_lists_only_this_organisations_root(self):
        response = self.client.get(reverse("wagtailadmin_explore_root"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Kenya Met")
        self.assertNotContains(response, "Uganda Met")

    def test_the_explorer_shows_this_organisations_children(self):
        response = self.client.get(reverse("wagtailadmin_explore", args=[self.kenya_root.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Kenya Bulletin")

    def test_exploring_another_organisations_root_is_not_found(self):
        url = reverse("wagtailadmin_explore", args=[self.uganda_root.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_the_admin_api_lists_only_this_organisations_pages(self):
        response = self.client.get("/admin/api/main/pages/", {"child_of": self.kenya_root.pk})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Uganda", response.content)

    # -- single-page views -------------------------------------------------

    def test_editing_another_organisations_page_is_not_found(self):
        url = reverse("wagtailadmin_pages:edit", args=[self.uganda_page.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_deleting_another_organisations_page_is_not_found(self):
        from wagtail.models import Page

        url = reverse("wagtailadmin_pages:delete", args=[self.uganda_page.pk])
        self.assertEqual(self.client.post(url).status_code, 404)
        self.assertTrue(Page.objects.filter(pk=self.uganda_page.pk).exists())

    def test_adding_a_child_under_another_organisations_page_is_not_found(self):
        url = reverse("wagtailadmin_pages:add_subpage", args=[self.uganda_root.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_moving_another_organisations_page_is_not_found(self):
        url = reverse("wagtailadmin_pages:move", args=[self.uganda_page.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_this_organisations_own_pages_are_still_editable(self):
        """The mirror: scoping that 404s everything would pass every test above."""
        url = reverse("wagtailadmin_pages:edit", args=[self.kenya_page.pk])
        self.assertEqual(self.client.get(url).status_code, 200)

    def test_the_page_history_of_another_organisations_page_is_not_found(self):
        url = reverse("wagtailadmin_pages:history", args=[self.uganda_page.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    # -- the tree root -----------------------------------------------------

    def test_adding_a_page_under_the_tree_root_is_not_found(self):
        """The root belongs to no organisation, so nothing may be authored there."""
        from wagtail.models import Page

        root = Page.get_first_root_node()
        url = reverse("wagtailadmin_pages:add_subpage", args=[root.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_the_explorer_may_still_list_the_tree_root(self):
        """The mirror: listing the root is where a superuser's explorer starts."""
        from wagtail.models import Page

        root = Page.get_first_root_node()
        response = self.client.get(reverse("wagtailadmin_explore", args=[root.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda Met")

    # -- bulk actions ------------------------------------------------------

    def test_a_bulk_action_on_another_organisations_page_is_not_found(self):
        """Bulk actions name their pages in the query string, not the path."""
        url = reverse("wagtail_bulk_action", args=["wagtailcore", "page", "delete"]) + f"?id={self.uganda_page.pk}"
        self.assertEqual(self.client.get(url).status_code, 404)

    def test_a_bulk_action_on_this_organisations_page_still_works(self):
        url = reverse("wagtail_bulk_action", args=["wagtailcore", "page", "delete"]) + f"?id={self.kenya_page.pk}"
        self.assertEqual(self.client.get(url).status_code, 200)

    # -- search and choosers -----------------------------------------------

    def test_page_search_does_not_reach_across_organisations(self):
        response = self.client.get(reverse("wagtailadmin_pages:search"), {"q": "Bulletin"})
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Kenya Bulletin")
        self.assertNotContains(response, "Uganda Bulletin")

    def test_the_page_chooser_offers_only_this_organisations_pages(self):
        response = self.client.get(reverse("wagtailadmin_choose_page"))
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Uganda Bulletin")

    def test_the_page_chooser_cannot_browse_into_another_organisations_tree(self):
        url = reverse("wagtailadmin_choose_page_child", args=[self.uganda_root.pk])
        self.assertEqual(self.client.get(url).status_code, 404)

    # -- the other host ----------------------------------------------------

    def test_the_same_user_sees_the_other_tree_on_the_other_host(self):
        """Host-scoped, not user-scoped: this is what the org-hopper is for."""
        self.client.defaults["HTTP_HOST"] = "uganda.georiva.test"
        response = self.client.get(reverse("wagtailadmin_explore", args=[self.uganda_root.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Uganda Bulletin")
        self.assertNotContains(response, "Kenya Bulletin")
