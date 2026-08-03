"""What an organisation administers about *itself*, on its own host.

Distinct from ``viewsets.py``, which is the instance admin's surface: creating an
organisation hands out a subdomain and a storage prefix and stays a superuser
act. Running the institution's own account — its settings, and who belongs to it
— does not, and an org admin should not have to ask.

This is also where the role split becomes visible. Members do the data work —
catalogs, feeds, uploads — with the same capabilities as their admins; what an
admin has in addition is these pages. Every one of them calls
``require_org_admin`` itself rather than trusting the menu that led here.
"""
from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.translation import gettext as _

from .access import get_org_object_or_404, require_active_org, require_org_admin, scoped_queryset
from .forms import MemberCreateForm, MemberRoleForm, OrganisationEditForm
from .models import OrganisationMembership


def _breadcrumbs(*trail):
    return [{"url": reverse("wagtailadmin_home"), "label": _("Home")}, *trail]


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
        "breadcrumbs_items": _breadcrumbs({"url": None, "label": _("Organisation settings")}),
        "header_title": organisation.name,
        "header_icon": "group",
        "organisation": organisation,
        "form": form,
        "members_url": reverse("organisation_members"),
    })


def organisation_members(request):
    """Who belongs to this organisation — never to any other.

    The listing is scoped like any other tenant data, so an org admin sees their
    own institution's people and no one else's, superuser or not.
    """
    organisation = require_active_org(request)
    require_org_admin(request)

    memberships = (
        scoped_queryset(request, OrganisationMembership.objects.all())
        .select_related("user")
        .order_by("user__username")
    )

    return render(request, "organisations/organisation_members.html", {
        "breadcrumbs_items": _breadcrumbs({"url": None, "label": _("Members")}),
        "header_title": _("Members of %s") % organisation.name,
        "header_icon": "user",
        "organisation": organisation,
        "memberships": memberships,
        "add_url": reverse("organisation_member_add"),
    })


def organisation_member_add(request):
    organisation = require_active_org(request)
    require_org_admin(request)

    if request.method == "POST":
        form = MemberCreateForm(request.POST, organisation=organisation)
        if form.is_valid():
            user = form.save()
            messages.success(
                request,
                _("%(user)s can now sign in at %(host)s.")
                % {"user": user.get_username(), "host": organisation.hostname},
            )
            return redirect("organisation_members")
    else:
        form = MemberCreateForm(organisation=organisation)

    return render(request, "organisations/organisation_member_form.html", {
        "breadcrumbs_items": _breadcrumbs(
            {"url": reverse("organisation_members"), "label": _("Members")},
            {"url": None, "label": _("Add member")},
        ),
        "header_title": _("Add a member"),
        "header_icon": "user",
        "organisation": organisation,
        "form": form,
        "cancel_url": reverse("organisation_members"),
    })


def organisation_member_edit(request, pk):
    organisation = require_active_org(request)
    require_org_admin(request)
    # Scoped resolution, so another organisation's membership row is not found
    # here even with its id in hand.
    membership = get_org_object_or_404(
        request, OrganisationMembership.objects.select_related("user"), pk=pk
    )

    if request.method == "POST":
        form = MemberRoleForm(request.POST, instance=membership)
        if form.is_valid():
            form.save()
            messages.success(request, _("Role updated."))
            return redirect("organisation_members")
    else:
        form = MemberRoleForm(instance=membership)

    return render(request, "organisations/organisation_member_form.html", {
        "breadcrumbs_items": _breadcrumbs(
            {"url": reverse("organisation_members"), "label": _("Members")},
            {"url": None, "label": str(membership.user)},
        ),
        "header_title": _("Role for %s") % membership.user,
        "header_icon": "user",
        "organisation": organisation,
        "membership": membership,
        "form": form,
        "cancel_url": reverse("organisation_members"),
    })


def organisation_member_remove(request, pk):
    """Removes the membership, never the account.

    A user may belong to several institutions, and deleting the account here
    would take their work elsewhere with it. Losing the membership is enough:
    the middleware re-reads it, so access to this organisation ends on their very
    next request.
    """
    organisation = require_active_org(request)
    require_org_admin(request)
    membership = get_org_object_or_404(
        request, OrganisationMembership.objects.select_related("user"), pk=pk
    )

    if request.method == "POST":
        username = str(membership.user)
        membership.delete()
        messages.success(
            request,
            _("%(user)s is no longer a member of %(org)s.")
            % {"user": username, "org": organisation.name},
        )
        return redirect("organisation_members")

    return render(request, "organisations/organisation_member_confirm_remove.html", {
        "breadcrumbs_items": _breadcrumbs(
            {"url": reverse("organisation_members"), "label": _("Members")},
            {"url": None, "label": _("Remove")},
        ),
        "header_title": _("Remove %s") % membership.user,
        "header_icon": "user",
        "organisation": organisation,
        "membership": membership,
        "cancel_url": reverse("organisation_members"),
    })
