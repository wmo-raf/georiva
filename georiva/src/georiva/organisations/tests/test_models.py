from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings

from georiva.organisations.models import OrganisationMembership
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.validators import validate_org_slug

from .factories import make_user


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test")
class OrganisationSlugValidationTests(TestCase):

    def test_accepts_lowercase_and_internal_hyphens(self):
        for slug in ["kenya", "kenya-met", "icpac2", "a", "a1-b2-c3"]:
            with self.subTest(slug=slug):
                validate_org_slug(slug)

    def test_rejects_uppercase(self):
        with self.assertRaises(ValidationError):
            validate_org_slug("KenyaMet")

    def test_rejects_bad_characters(self):
        for slug in ["kenya met", "kenya_met", "kenya.met", "kenya/met", "kenya!"]:
            with self.subTest(slug=slug):
                with self.assertRaises(ValidationError):
                    validate_org_slug(slug)

    def test_rejects_leading_trailing_and_doubled_hyphens(self):
        for slug in ["-kenya", "kenya-", "kenya--met"]:
            with self.subTest(slug=slug):
                with self.assertRaises(ValidationError):
                    validate_org_slug(slug)

    def test_rejects_over_length(self):
        with self.assertRaises(ValidationError):
            validate_org_slug("a" * 51)

    def test_rejects_reserved_words(self):
        for slug in ["admin", "api", "static", "media", "www", "martin"]:
            with self.subTest(slug=slug):
                with self.assertRaises(ValidationError):
                    validate_org_slug(slug)

    def test_model_validation_rejects_reserved_slug(self):
        with self.assertRaises(ValidationError):
            provision_organisation(name="Admin", slug="admin")

    def test_model_validation_rejects_duplicate_slug(self):
        provision_organisation(name="Kenya Met", slug="kenya")
        with self.assertRaises(ValidationError):
            provision_organisation(name="Kenya Met Again", slug="kenya")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test")
class OrganisationSlugImmutabilityTests(TestCase):

    def setUp(self):
        self.organisation = provision_organisation(name="Kenya Met", slug="kenya")

    def test_slug_cannot_change_after_creation(self):
        self.organisation.slug = "kenya-met"
        with self.assertRaises(ValidationError) as ctx:
            self.organisation.full_clean()
        self.assertIn("slug", ctx.exception.message_dict)

    def test_name_stays_editable(self):
        self.organisation.name = "Kenya Meteorological Department"
        self.organisation.full_clean()
        self.organisation.save()
        self.organisation.refresh_from_db()
        self.assertEqual(self.organisation.name, "Kenya Meteorological Department")
        self.assertEqual(self.organisation.slug, "kenya")

    def test_unchanged_slug_validates(self):
        self.organisation.full_clean()

    def test_slug_change_is_refused_even_without_validation(self):
        self.organisation.slug = "kenya-met"
        with self.assertRaises(ValidationError):
            self.organisation.save()
        self.organisation.refresh_from_db()
        self.assertEqual(self.organisation.slug, "kenya")


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test")
class OrganisationMembershipTests(TestCase):

    def setUp(self):
        self.organisation = provision_organisation(name="Kenya Met", slug="kenya")
        self.other = provision_organisation(name="ICPAC", slug="icpac")
        self.user = make_user("amina")

    def test_user_can_belong_to_several_organisations(self):
        OrganisationMembership.objects.create(user=self.user, organisation=self.organisation)
        OrganisationMembership.objects.create(
            user=self.user,
            organisation=self.other,
            role=OrganisationMembership.Role.ADMIN,
        )
        self.assertEqual(self.user.organisation_memberships.count(), 2)

    def test_membership_is_unique_per_user_and_organisation(self):
        OrganisationMembership.objects.create(user=self.user, organisation=self.organisation)
        duplicate = OrganisationMembership(user=self.user, organisation=self.organisation)
        with self.assertRaises(ValidationError):
            duplicate.full_clean()

    def test_default_role_is_member(self):
        membership = OrganisationMembership.objects.create(user=self.user, organisation=self.organisation)
        self.assertEqual(membership.role, OrganisationMembership.Role.MEMBER)

    def test_membership_for_returns_live_row(self):
        self.assertIsNone(self.organisation.membership_for(self.user))
        membership = OrganisationMembership.objects.create(user=self.user, organisation=self.organisation)
        self.assertEqual(self.organisation.membership_for(self.user), membership)
        membership.delete()
        self.assertIsNone(self.organisation.membership_for(self.user))

