"""What an organisation administers about *itself*, on its own host.

Distinct from ``viewsets.py``, which is the instance admin's surface: creating an
organisation hands out a subdomain and a storage prefix and stays a superuser
act. Adjusting the institution's own description, contact details and provider
defaults does not, and an org admin should not have to ask.

This is also where the role split becomes visible. Members do the data work —
catalogs, feeds, uploads — with the same capabilities as their admins; what an
admin has in addition is this page.
"""
from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.translation import gettext as _

from .access import require_active_org, require_org_admin
from .forms import OrganisationEditForm


def organisation_settings(request):
    organisation = require_active_org(request)
    require_org_admin(request)

    if request.method == "POST":
        form = OrganisationEditForm(request.POST, instance=organisation)
        if form.is_valid():
            form.save()
            messages.success(request, _("Organisation settings updated."))
            return redirect("organisation_settings")
    else:
        form = OrganisationEditForm(instance=organisation)

    return render(request, "organisations/organisation_settings.html", {
        "breadcrumbs_items": [
            {"url": reverse("wagtailadmin_home"), "label": _("Home")},
            {"url": None, "label": _("Organisation settings")},
        ],
        "header_title": organisation.name,
        "header_icon": "group",
        "organisation": organisation,
        "form": form,
    })
