"""Admin forms for organisations.

Two forms for the organisation itself, because creating and editing one are
different acts. Creating provisions infrastructure (Site, root page, group) and
so runs through ``provision_organisation`` rather than a plain model save.
Editing may touch anything *except* the slug — renaming it would orphan every
`{slug}/` storage key and break every published link — so the slug simply is not
on the form.

The member forms are the org admin's, not the instance admin's: they only ever
act on the organisation serving the request, which is why neither carries an
organisation field.
"""
from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import UserCreationForm
from django.db import transaction
from django.utils.translation import gettext_lazy as _

from .models import Organisation, OrganisationMembership
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


class MemberCreateForm(UserCreationForm):
    """A new account, and its membership of this organisation, in one step.

    Members are onboarded by direct account creation — there are no email
    invitations to accept (decision #257) — so the person who adds somebody also
    sets their first password and tells them out of band.

    The account itself is instance-wide: usernames and emails are unique across
    GeoRiva, and somebody who already works with two institutions holds one
    account with two memberships. Adding an *existing* user is therefore a
    separate act from creating one, and this form does not do it.
    """

    role = forms.ChoiceField(
        choices=OrganisationMembership.Role.choices,
        initial=OrganisationMembership.Role.MEMBER,
        label=_("Role"),
        help_text=_(
            "Members do all the data work. Org admins additionally manage members "
            "and organisation settings."
        ),
    )

    class Meta(UserCreationForm.Meta):
        model = get_user_model()
        fields = ["username", "first_name", "last_name", "email"]

    def __init__(self, *args, organisation=None, **kwargs):
        self.organisation = organisation
        super().__init__(*args, **kwargs)
        self.fields["email"].required = True

    @transaction.atomic
    def save(self, commit=True):
        user = super().save(commit=True)
        # The membership grants the standard data capabilities through the
        # post-save receiver in signals.py, so a new member can work at once.
        OrganisationMembership.objects.create(
            user=user,
            organisation=self.organisation,
            role=self.cleaned_data["role"],
        )
        return user


class MemberRoleForm(forms.ModelForm):
    """The one thing an org admin changes about an existing membership."""

    class Meta:
        model = OrganisationMembership
        fields = ["role"]
