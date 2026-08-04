"""Project-level views: the healthcheck, and the pages Django shows when a request fails.

The error pages are split by plane. A failure under ``/admin/`` is dressed in the
admin's own chrome, a failure anywhere else in the public site's, because the two
look nothing alike and an error that arrives in the wrong clothes reads as a
different site having answered. The split is done by passing the shell down as
``error_base`` and letting each status template ``{% extends error_base %}``,
rather than by keeping two near-identical copies of every page in step by hand.

The admin shell is deliberately ``wagtailadmin/admin_base.html`` — the sidebar-less
one the login page uses — and not the full admin chrome. Rendering an
organisation's menus to somebody the membership guard has just turned away would
undo the guard in the act of explaining it.

The 500 page takes no part in any of this. It lives at ``500.html``, extends
nothing, loads no static file and reads no context, because it is the one page
that has to render when the thing that broke might be the template engine, the
database or a Wagtail hook. Django's own ``server_error`` already renders exactly
that template with an empty context, so there is no handler for it here.
"""
from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render

#: The shells the status templates extend, by plane.
ADMIN_ERROR_BASE = "georiva/errors/base_admin.html"
PUBLIC_ERROR_BASE = "georiva/errors/base_public.html"


def health(request):
    return HttpResponse("ok")


def error_base_for(request):
    """The shell ``request``'s error page should wear.

    Gated on the same prefix ``OrganisationMiddleware._guard_admin`` gates on, so
    the page a refused admin request gets is dressed like the page it asked for.
    """
    return ADMIN_ERROR_BASE if request.path.startswith(settings.GEORIVA_ADMIN_PATH_PREFIX) else PUBLIC_ERROR_BASE


def is_non_member_refusal(request):
    """Whether this refusal is ``_guard_admin`` turning a signed-in stranger away.

    Worth singling out because it is the only 403 whose way out is to *end* the
    session, and offering that to anyone else would be wrong. The test is the
    guard's own condition read back: under the admin prefix, signed in, and
    holding no role in the organisation this hostname serves.

    Note that a member who is merely not an organisation admin never reaches this
    view at all. ``require_org_admin`` raises inside a view, and Wagtail's
    ``require_admin_access`` catches ``PermissionDenied`` from there and redirects
    to the admin home with a message instead (``wagtail/admin/auth.py``). Only a
    refusal raised *before* the view — which is to say, this middleware's —
    propagates to ``handler403``.
    """
    user = getattr(request, "user", None)
    return (
        request.path.startswith(settings.GEORIVA_ADMIN_PATH_PREFIX)
        and user is not None
        and user.is_authenticated
        and getattr(request, "active_org", None) is not None
        and getattr(request, "active_org_role", None) is None
    )


def permission_denied(request, exception=None):
    """``handler403``: the page behind every ``PermissionDenied`` on the HTML planes.

    In practice one refusal dominates — the membership guard turning a signed-in
    non-member away from an organisation's admin — and that one needs a way out
    the others must not be offered. See :func:`is_non_member_refusal`. Everything
    else gets a plain refusal with a link home.

    ``TileAuthView`` never reaches here: it answers nginx with a bare DRF 403,
    which is the only thing ``auth_request`` understands (ADR 0015). DRF's own
    API denials are rendered by DRF and never reach here either.
    """
    return render(
        request,
        "georiva/errors/403.html",
        {
            "error_base": error_base_for(request),
            "non_member": is_non_member_refusal(request),
            "organisation": getattr(request, "active_org", None),
        },
        status=403,
    )


def page_not_found(request, exception=None):
    """``handler404``, replacing the stock page.

    Reached both for a genuinely missing URL and for the unknown-host 404
    ``OrganisationMiddleware`` raises, which is why the page names no
    organisation: on the second of those there is no organisation to name.
    """
    return render(
        request,
        "georiva/errors/404.html",
        {"error_base": error_base_for(request)},
        status=404,
    )


def csrf_failure(request, reason=""):
    """``CSRF_FAILURE_VIEW``: a rejected POST, in the same clothes as the rest.

    Worth a real page rather than Django's stock one because the commonest cause
    here is benign and self-correcting — a login or sign-out form left open long
    enough for its token to lapse — and the user needs to be told to reload
    rather than shown a bare accusation.
    """
    return render(
        request,
        "georiva/errors/csrf.html",
        {"error_base": error_base_for(request), "reason": reason},
        status=403,
    )
