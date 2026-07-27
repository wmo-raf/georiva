"""Admin forms for organisations.

Two forms, because creating and editing an organisation are different acts.
Creating provisions infrastructure (Site, root page, group) and so runs through
``provision_organisation`` rather than a plain model save. Editing may touch
anything *except* the slug — renaming it would orphan every `{slug}/` storage
key and break every published link — so the slug simply is not on the form.
"""
from django import forms

from .models import Organisation
from .provisioning import provision_organisation

SETTINGS_FIELDS = [
    "description",
    "contact_email",
    "website",
    "country",
    "default_provider",
    "default_provider_url",
]


class OrganisationCreateForm(forms.ModelForm):
    """Provisions a whole organisation on save."""

    class Meta:
        model = Organisation
        fields = ["name", "slug"] + SETTINGS_FIELDS

    def save(self, commit=True):
        data = self.cleaned_data
        return provision_organisation(
            name=data["name"],
            slug=data["slug"],
            **{field: data[field] for field in SETTINGS_FIELDS},
        )


class OrganisationEditForm(forms.ModelForm):
    """Everything but the slug, which is immutable after creation."""

    class Meta:
        model = Organisation
        fields = ["name"] + SETTINGS_FIELDS
