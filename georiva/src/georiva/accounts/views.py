"""The panel where a person manages their own API keys.

Every lookup here is scoped by ``request.user`` and by nothing else. That is not
the organisation scoping the rest of the admin uses, and the difference is the
point: a key belongs to a person, not to an institution, so an org admin has no
claim over their members' credentials and neither does the instance admin.
Somebody else's key is reported as absent, exactly as another organisation's row
would be.
"""

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import Http404
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.translation import gettext as _

from .forms import ApiKeyCreateForm
from .models import ApiKey


def _own_key(request, pk):
    """One of the signed-in user's own keys, or 404.

    Written out rather than reaching for ``get_object_or_404`` on the whole
    table: the filter *is* the access check here, and putting it in the query
    means there is no version of this lookup that forgets it.
    """
    key = ApiKey.objects.filter(user=request.user, pk=pk).first()
    if key is None:
        raise Http404("No such API key.")
    return key


def _breadcrumbs(*trail):
    return [{"url": reverse("wagtailadmin_home"), "label": _("Home")}, *trail]


@login_required
def api_keys(request):
    """List the user's keys, and mint a new one.

    A newly minted secret is rendered straight into the response rather than
    surviving a redirect: it is the one moment it exists, and stashing it in the
    session to survive a round trip would put it somewhere it can be read again.
    Reloading the page loses it, which is the intended behaviour and what the
    page says.
    """
    new_secret = None
    form = ApiKeyCreateForm()

    if request.method == "POST":
        form = ApiKeyCreateForm(request.POST)
        if form.is_valid():
            new_key, new_secret = ApiKey.objects.mint(
                user=request.user,
                name=form.cleaned_data["name"],
                expires_at=form.cleaned_data["expires_at"],
            )
            form = ApiKeyCreateForm()
            messages.success(
                request,
                _("'%s' created. Copy it now — it is shown once.") % new_key.name,
            )

    return render(
        request,
        "accounts/api_keys.html",
        {
            "breadcrumbs_items": _breadcrumbs({"url": None, "label": _("API keys")}),
            "header_title": _("API keys"),
            "header_icon": "key",
            "keys": ApiKey.objects.filter(user=request.user).order_by("-created"),
            "form": form,
            "new_secret": new_secret,
        },
    )


@login_required
def api_key_revoke(request, pk):
    """Retire one of the user's own keys. POST only — a GET must not change anything."""
    key = _own_key(request, pk)

    if request.method != "POST":
        return render(
            request,
            "accounts/api_key_revoke.html",
            {
                "breadcrumbs_items": _breadcrumbs(
                    {"url": reverse("api_keys"), "label": _("API keys")},
                    {"url": None, "label": _("Revoke %s") % key.name},
                ),
                "header_title": _("Revoke %s") % key.name,
                "header_icon": "key",
                "key": key,
            },
        )

    key.revoke()
    messages.success(request, _("'%s' will no longer be accepted.") % key.name)
    return redirect("api_keys")
