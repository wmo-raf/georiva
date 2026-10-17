"""Tenancy models: who the instance's institutions are and who belongs to them.

An ``Organisation`` is one institution occupying one tenant of a shared GeoRiva
instance. It is always bound to exactly one ``wagtailcore.Site`` — the Site is
how a request finds its organisation (Host → Site → Organisation), so an
organisation without one would be unreachable.

``OrganisationMembership`` is the only thing that makes a user part of an
organisation. Django superusers are the *instance* admin and deliberately hold
no membership rows; every permission check short-circuits on ``is_superuser``.
"""

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django_countries.fields import CountryField

from .lookups import ORGANISATION_SELF
from .validators import ORG_SLUG_MAX_LENGTH, validate_org_slug


class Organisation(models.Model):
    """One institution on the instance, reachable at its own Site hostname."""

    ORGANISATION_LOOKUP = ORGANISATION_SELF

    name = models.CharField(max_length=255, help_text="Display name of the institution.")
    slug = models.SlugField(
        max_length=ORG_SLUG_MAX_LENGTH,
        unique=True,
        validators=[validate_org_slug],
        help_text=(
            "Immutable. The organisation's subdomain, and the segment its storage paths "
            "will be filed under. Cannot be changed after creation."
        ),
    )
    site = models.OneToOneField(
        "wagtailcore.Site",
        on_delete=models.PROTECT,
        related_name="organisation",
        help_text="The Wagtail Site this organisation is served from.",
    )

    description = models.TextField(blank=True)
    contact_email = models.EmailField(blank=True)
    website = models.URLField(blank=True)
    country = CountryField(blank=True, help_text="ISO 3166-1 alpha-2. Blank for regional or continental centres.")

    default_provider = models.CharField(
        max_length=255,
        blank=True,
        help_text="Prefills the provider name on new catalogs. Catalog-level values win.",
    )
    default_provider_url = models.URLField(
        blank=True,
        help_text="Prefills the provider URL on new catalogs. Catalog-level values win.",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]
        verbose_name = "Organisation"
        verbose_name_plural = "Organisations"

    def __str__(self):
        return self.name

    def _stored_slug(self):
        if not self.pk:
            return None
        return type(self).objects.filter(pk=self.pk).values_list("slug", flat=True).first()

    def clean(self):
        super().clean()
        stored = self._stored_slug()
        if stored is not None and stored != self.slug:
            raise ValidationError(
                {"slug": "The organisation slug is immutable and cannot be changed after creation."},
                code="immutable_org_slug",
            )

    def save(self, *args, **kwargs):
        # Immutability is enforced here and not only in clean(), because a slug
        # change is not a validation nicety: `{slug}/` storage keys and published
        # links already point at the old one. Code paths that skip full_clean()
        # must not be able to sneak one through.
        stored = self._stored_slug()
        if stored is not None and stored != self.slug:
            raise ValidationError(
                {"slug": "The organisation slug is immutable and cannot be changed after creation."},
                code="immutable_org_slug",
            )
        return super().save(*args, **kwargs)

    @property
    def hostname(self):
        return self.site.hostname

    @property
    def root_page(self):
        """The root of this organisation's page tree — its whole portal.

        Named here so that callers ask the organisation for its pages rather than
        walking Site → root_page themselves, and so the Site → root-page link
        stays a provisioning detail (see ``organisations.pages``).
        """
        return self.site.root_page

    def membership_for(self, user):
        """The user's live membership row, or ``None``. Never cached."""
        if not user or not user.is_authenticated:
            return None
        return self.memberships.filter(user=user).first()


class OrganisationMembership(models.Model):
    """Links a user to an organisation with one of the two launch roles."""

    ORGANISATION_LOOKUP = "organisation"

    class Role(models.TextChoices):
        ADMIN = "admin", "Org Admin"
        MEMBER = "member", "Member"

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="organisation_memberships",
    )
    organisation = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="memberships",
    )
    role = models.CharField(max_length=16, choices=Role.choices, default=Role.MEMBER)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("user", "organisation")
        ordering = ["organisation__name"]
        verbose_name = "Organisation membership"
        verbose_name_plural = "Organisation memberships"

    def __str__(self):
        return f"{self.user} in {self.organisation} ({self.get_role_display()})"
