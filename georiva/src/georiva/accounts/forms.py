from django import forms
from django.utils.translation import gettext_lazy as _


class ApiKeyCreateForm(forms.Form):
    """Everything a user decides about a new key, which is deliberately little.

    No organisation field, because a key has no organisation (see
    ``models``); no scopes or permissions, because a key grants exactly what its
    holder already has. What is left is a label so they can tell their keys
    apart, and an optional end date.
    """

    name = forms.CharField(
        label=_("Name"),
        max_length=80,
        help_text=_("What will hold this key — 'QGIS on my laptop', 'ingest cron'."),
    )
    expires_at = forms.DateTimeField(
        label=_("Expires"),
        required=False,
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}),
        help_text=_("Optional. Leave blank for a key that never expires on its own."),
    )
