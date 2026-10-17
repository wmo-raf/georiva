"""The choke point itself: does it filter, and does it fail closed?

These tests drive `access.py` through real models rather than stubs — the whole
value of the module is that the declared route from a row to its organisation is
the one the ORM actually walks.
"""

from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.http import Http404
from django.test import RequestFactory, TestCase, override_settings

from georiva.core.models import Catalog, Collection
from georiva.core.models.catalog import Topic
from georiva.organisations import access
from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation

from .factories import add_member, make_user


def make_catalog(organisation, slug="rainfall", **fields):
    return Catalog.objects.create(
        organisation=organisation,
        name=slug.title(),
        slug=slug,
        file_format=Catalog.FileFormat.GEOTIFF,
        **fields,
    )


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class AccessHelperTests(TestCase):
    def setUp(self):
        self.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        self.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        self.kenya_catalog = make_catalog(self.kenya)
        # Same slug in both organisations: the case per-org uniqueness allows and
        # the one an unscoped lookup answers wrongly.
        self.uganda_catalog = make_catalog(self.uganda)
        self.factory = RequestFactory()

    def _request(self, organisation, *, user=None, role=None):
        request = self.factory.get("/admin/")
        request.active_org = organisation
        request.user = user
        request.active_org_role = role
        return request

    # -- the declared route ------------------------------------------------

    def test_organisation_lookup_is_read_from_the_model(self):
        self.assertEqual(access.organisation_lookup(Catalog), "organisation")
        self.assertEqual(access.organisation_lookup(Collection), "catalog__organisation")

    def test_a_model_without_a_declared_route_is_a_configuration_error(self):
        with self.assertRaises(ImproperlyConfigured):
            access.organisation_lookup(Topic)

    def test_organisation_of_walks_the_declared_route(self):
        collection = Collection.objects.create(catalog=self.kenya_catalog, name="Daily", slug="daily")
        self.assertEqual(access.organisation_of(collection), self.kenya)

    def test_organisation_of_is_none_when_the_route_is_broken(self):
        from georiva.sources.models import DataFeed

        # DataFeed.catalog is nullable, so a feed can exist owned by nobody.
        feed = DataFeed.objects.create(name="Orphan")
        self.assertIsNone(access.organisation_of(feed))

    # -- scoped_queryset ---------------------------------------------------

    def test_scoped_queryset_keeps_only_the_requests_organisation(self):
        request = self._request(self.kenya)
        rows = access.scoped_queryset(request, Catalog.objects.all())
        self.assertEqual(list(rows), [self.kenya_catalog])

    def test_scoped_queryset_refuses_a_request_with_no_organisation(self):
        request = self._request(None)
        with self.assertRaises(Http404):
            access.scoped_queryset(request, Catalog.objects.all())

    def test_scoped_queryset_does_not_widen_for_superusers(self):
        # Superusers skip the membership gate, not the host: a superuser reading
        # Kenya's admin still reads Kenya's rows, so nothing they create there
        # can be filed under another institution.
        request = self._request(self.kenya, user=make_user("root", superuser=True))
        rows = access.scoped_queryset(request, Catalog.objects.all())
        self.assertEqual(list(rows), [self.kenya_catalog])

    # -- object resolution -------------------------------------------------

    def test_get_org_object_or_404_resolves_within_the_organisation(self):
        request = self._request(self.kenya)
        found = access.get_org_object_or_404(request, Catalog, slug="rainfall")
        self.assertEqual(found, self.kenya_catalog)

    def test_get_org_object_or_404_hides_another_organisations_row(self):
        request = self._request(self.kenya)
        with self.assertRaises(Http404):
            access.get_org_object_or_404(request, Catalog, pk=self.uganda_catalog.pk)

    def test_get_org_object_or_404_accepts_a_queryset(self):
        request = self._request(self.kenya)
        found = access.get_org_object_or_404(request, Catalog.objects.filter(is_active=True), pk=self.kenya_catalog.pk)
        self.assertEqual(found, self.kenya_catalog)

    def test_require_org_object_rejects_a_foreign_row(self):
        request = self._request(self.kenya)
        with self.assertRaises(Http404):
            access.require_org_object(request, self.uganda_catalog)

    def test_require_org_object_returns_an_owned_row(self):
        request = self._request(self.kenya)
        self.assertEqual(access.require_org_object(request, self.kenya_catalog), self.kenya_catalog)

    # -- role gates --------------------------------------------------------

    def test_require_org_member_rejects_a_non_member(self):
        request = self._request(self.kenya, user=make_user("stranger"), role=None)
        with self.assertRaises(PermissionDenied):
            access.require_org_member(request)

    def test_require_org_member_accepts_a_member(self):
        user = make_user("amina")
        add_member(user, self.kenya)
        request = self._request(self.kenya, user=user, role=OrganisationMembership.Role.MEMBER)
        self.assertIsNone(access.require_org_member(request))

    def test_require_org_admin_rejects_a_plain_member(self):
        request = self._request(self.kenya, user=make_user("amina"), role=OrganisationMembership.Role.MEMBER)
        with self.assertRaises(PermissionDenied):
            access.require_org_admin(request)

    def test_require_org_admin_accepts_an_org_admin(self):
        request = self._request(self.kenya, user=make_user("amina"), role=OrganisationMembership.Role.ADMIN)
        self.assertIsNone(access.require_org_admin(request))
