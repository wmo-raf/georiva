"""The org-hopper: how one person crosses between the organisations they belong to.

Host-scoped admin means the subdomain *is* the switcher — there is no session-org
state to flip, and nothing here creates any. The hopper is pure wayfinding: it
shows which organisation the current admin belongs to, and hands out plain
cross-host links to the others' ``/admin/``. Clicking one is an ordinary
navigation to another hostname, where the middleware resolves that organisation
from the Host exactly as it did for this one.

Two things follow from that, and both are why this module is small:

*It writes nothing.* No cookie, no session key, no "last organisation" memory. A
link is the whole mechanism, so a stale tab cannot leave a user pointed at an
organisation they have left — the host they are on decides, on every request.

*It is the one deliberately cross-organisation surface in the admin.* Everything
else on an organisation's host answers for that organisation alone. The hopper
names the others by design, and is therefore the one place where *who may be
listed* has to be stated rather than inherited: a member sees the organisations
they hold a membership row in, and nobody else's; the instance admin sees all of
them, which is what ``is_superuser`` already buys everywhere else (ADR 0011).

The block itself is rendered server-side and delivered by :func:`org_hopper_script`,
because Wagtail's global admin-chrome hooks are handed no request and so cannot
render per-user markup themselves. See ADR 0017.
"""
import json
from dataclasses import dataclass

from django.conf import settings
from django.http import HttpResponse
from django.template.loader import render_to_string

from .models import Organisation

#: How many organisations the list may hold before it gets a search box. Only a
#: superuser on a large instance ever crosses it; a member of three does not want
#: a search field over three rows.
SEARCH_THRESHOLD = 8

#: Number of avatar colours defined in ``org_hopper.css`` as ``gr-orghop__avatar--N``.
#: The colour is decoration, but it must be *stable* decoration: the same
#: organisation wears the same colour on every host, so the eye can use it.
AVATAR_PALETTE_SIZE = 8


@dataclass(frozen=True)
class OrgHopperEntry:
    """One organisation as the hopper shows it."""

    name: str
    slug: str
    host: str
    url: str
    letters: str
    avatar_index: int
    is_current: bool


def avatar_letters(slug):
    """Up to three letters standing in for the organisation in the avatar.

    The opening of the slug's first word, not true initials: "meteo-rwanda"
    gives "MET", not "MR". Three letters of one word read as a mark; initials
    drawn from a five-word French institution name do not.
    """
    return slug.replace("-", " ").split(" ")[0][:3].upper()


def avatar_index(slug):
    """A stable palette slot for ``slug``.

    Deterministic on the slug rather than on the primary key, so an organisation
    keeps its colour across instances and across a database rebuild.
    """
    digest = 0
    for character in slug:
        digest = (digest * 31 + ord(character)) & 0xFFFF
    return digest % AVATAR_PALETTE_SIZE


def admin_url(request, organisation):
    """The URL of ``organisation``'s admin, as this browser would have to reach it.

    Built from the organisation's own Site hostname — the same record the
    middleware resolves a request *back* to — rather than re-derived from the
    slug and the base domain. Those normally agree, and where they do not the
    Site is the one that answers.

    Scheme-relative on purpose: behind this deployment's TLS terminator Django
    cannot tell an HTTPS request from an HTTP one, and the browser can. ADR 0017
    has the argument. The port comes from this request's ``Host``, which nginx
    forwards verbatim, so a dev instance on ``:8000`` links to ``:8000``.
    """
    host = organisation.site.hostname
    _, _, port = request.get_host().partition(":")
    if port:
        host = f"{host}:{port}"
    return f"//{host}{settings.GEORIVA_ADMIN_PATH_PREFIX}"


def hoppable_organisations(user):
    """The organisations ``user`` may hop to, in ``Organisation``'s own order.

    The membership row is the whole rule — the same one
    ``access.resolve_org_role`` applies per request — so an organisation a user
    has been removed from leaves the list on their next page load.
    """
    organisations = Organisation.objects.select_related("site")
    if user.is_superuser:
        return organisations
    return organisations.filter(memberships__user=user)


def org_hopper_context(request):
    """Template context for the workspace block, or ``None`` if there is none to show.

    ``None`` for an anonymous request and for a request no organisation serves —
    both reachable under ``/admin/`` (the login page is one), and neither has an
    organisation to name.
    """
    organisation = getattr(request, "active_org", None)
    user = getattr(request, "user", None)
    if organisation is None or user is None or not user.is_authenticated:
        return None

    entries = [_entry(request, org, organisation) for org in hoppable_organisations(user)]
    if not any(entry.is_current for entry in entries):
        # Belt to the middleware's braces. `_guard_admin` already turns a
        # signed-in non-member away before any admin URL is served, so on the
        # admin plane this cannot happen — but the block's whole job is to name
        # the organisation whose host this is, and naming somebody else's while
        # the list disagrees would be worse than naming none. Kept so that a
        # future caller reaching this outside the admin gate cannot get a block
        # with no current entry.
        entries.insert(0, _entry(request, organisation, organisation))

    current = next(entry for entry in entries if entry.is_current)
    return {
        "current": current,
        "entries": entries,
        "is_interactive": len(entries) > 1,
        "is_searchable": len(entries) > SEARCH_THRESHOLD,
        "lists_all_organisations": user.is_superuser,
    }


def _entry(request, organisation, active):
    return OrgHopperEntry(
        name=organisation.name,
        slug=organisation.slug,
        host=organisation.site.hostname,
        url=admin_url(request, organisation),
        letters=avatar_letters(organisation.slug),
        avatar_index=avatar_index(organisation.slug),
        is_current=organisation.pk == active.pk,
    )


def org_hopper_script(request):
    """The per-request half of the block: its markup, handed to the mounted script.

    A view rather than a hook because ``insert_global_admin_js`` is called with no
    arguments — it cannot see the request, so it cannot know which organisations
    to name. It can emit a ``<script src>`` pointing here, and this can answer for
    the user who asked. See ADR 0017.

    Answers with an empty script rather than a 404 when there is nothing to show:
    a missing block is a page without a hopper, not a broken page.
    """
    context = org_hopper_context(request)
    body = ""
    if context is not None:
        markup = render_to_string("organisations/org_hopper_block.html", context, request=request)
        body = f"window.georivaOrgHopper.mount({_js_string(markup)});\n"

    response = HttpResponse(body, content_type="text/javascript")
    # Per-user markup on a URL every admin page loads: it must never be held in a
    # shared cache, and a browser must not carry one user's block into the next
    # session on the same machine.
    response["Cache-Control"] = "private, no-store"
    response["Vary"] = "Cookie"
    return response


#: The same three characters ``django.utils.html.json_script`` escapes, for the
#: same reason: ``</script>`` anywhere in the markup would close the element
#: early. Django's helper is not reusable here because it emits a whole
#: ``<script type="application/json">`` element, and what this needs is a bare
#: string literal to pass to a function. The two Unicode line separators need no
#: entry — ``json.dumps`` escapes every non-ASCII character on its own.
_JS_ESCAPES = {
    "<": "\\u003C",
    ">": "\\u003E",
    "&": "\\u0026",
}


def _js_string(value):
    """``value`` as a JavaScript string literal that is safe to inline in a script."""
    encoded = json.dumps(value)
    for character, escape in _JS_ESCAPES.items():
        encoded = encoded.replace(character, escape)
    return encoded
