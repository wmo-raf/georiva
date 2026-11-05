from django.conf import settings
from django.contrib import admin
from django.urls import include, path
from wagtail import urls as wagtail_urls
from wagtail.admin import urls as wagtailadmin_urls
from wagtail.admin.auth import require_admin_access
from wagtail.documents import urls as wagtaildocs_urls

from georiva.api import urls as georiva_urls
from georiva.core.machine_plane.auth_view import TileAuthView
from georiva.core.web_apps import first_segments, web_app_urlpatterns
from georiva.organisations.hopper import org_hopper_script
from georiva.organisations.pages import OrgScopedPageSearchView
from georiva.organisations.reports import report_urlpatterns

from .views import health, page_not_found, permission_denied

# Django's stock error pages are unbranded and, in the 403's case, a dead end for
# the user who hits it most: a signed-in non-member turned away by
# `OrganisationMiddleware._guard_admin`. `config.views` explains the split.
# There is deliberately no `handler500` — the stock one renders `500.html` with
# an empty context, which is exactly what that page is written for.
handler403 = permission_denied
handler404 = page_not_found

# The one place the admin's mount point is written down. `OrganisationMiddleware`
# and the page-tree guard both gate on `GEORIVA_ADMIN_PATH_PREFIX`, so the routes
# are derived from it rather than repeating the literal and drifting from it.
ADMIN_PREFIX = settings.GEORIVA_ADMIN_PATH_PREFIX.lstrip("/")

#: Core's own routes. Named so the ordering below is readable and so a test can
#: ask about core's surface with a plugin's pages appended to it rather than
#: mixed into it.
core_urlpatterns = [
    path("django-admin/", admin.site.urls),
    # Page search, scoped to the host organisation's page tree, standing in front
    # of Wagtail's own route at the same path — Wagtail's `register_admin_urls`
    # hook can only append, so replacing a view means claiming its URL here
    # (#269). `require_admin_access` is the decorator Wagtail applies to its own
    # admin patterns and that this one would otherwise miss.
    path(
        f"{ADMIN_PREFIX}pages/search/",
        require_admin_access(OrgScopedPageSearchView.as_view()),
        name="georiva_org_scoped_page_search",
    ),
    path(
        f"{ADMIN_PREFIX}pages/search/results/",
        require_admin_access(OrgScopedPageSearchView.as_view(results_only=True)),
        name="georiva_org_scoped_page_search_results",
    ),
    # Wagtail's six admin reports, scoped to the host organisation (#297). Same
    # technique as page search above; the menu already hides them, but the URL is
    # what has to refuse.
    *report_urlpatterns(ADMIN_PREFIX),
    # The org-hopper's per-request half (#270). Routed here rather than through
    # `register_admin_urls` because that hook wraps its URLs in
    # `require_admin_access`, whose login redirect the login page's own copy of
    # this script tag would follow into HTML. See ADR 0017.
    path(
        f"{ADMIN_PREFIX}org-hopper.js",
        org_hopper_script,
        name="organisation_hopper_script",
    ),
    path(ADMIN_PREFIX, include(wagtailadmin_urls)),
    path("documents/", include(wagtaildocs_urls)),
    path("api/", include(georiva_urls), name="georiva_api"),
    # Nginx's tile gateway (#274). Deliberately not under "api/": that prefix is
    # one organisation's whole public service (ADR 0012), and this answers no
    # part of it — it is the proxy asking the proxy's own question, and nginx
    # marks the location `internal` so nothing outside can ask it.
    path("internal/tile-auth/", TileAuthView.as_view(), name="tile_auth"),
    path("health/", health),
    path("", include("adminboundarymanager.urls")),
]

if settings.DEBUG:
    from django.conf.urls.static import static
    from django.contrib.staticfiles.urls import staticfiles_urlpatterns

    # Serve static and media files from development server
    core_urlpatterns += staticfiles_urlpatterns()
    core_urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

#: For anything not caught by a more specific rule, hand over to Wagtail's page
#: serving mechanism. This is last, and everything above it is reached first.
page_tree_urlpatterns = [
    path("", include(wagtail_urls)),
    # Alternatively, if you want Wagtail pages to be served from a subpath
    # of your site, rather than the site root:
    #    path("pages/", include(wagtail_urls)),
]


def build_urlpatterns():
    """The instance's URL conf, in the order that is load-bearing.

    Plugin-contributed page routes go between the two lists above (ADR 0039).
    After core's, for the reason ADR 0028 appends a plugin's API routes: a plugin
    must not be able to take a path core already answers. Before the page tree's
    catch-all, because Wagtail's resolver answers for every path under it and
    would otherwise swallow a mount the operator put at the site root.

    A function so that a test can assemble the real composition with a plugin
    discovered, rather than restate it and assert against the copy.
    """
    return core_urlpatterns + web_app_urlpatterns(reserved_segments()) + page_tree_urlpatterns


def reserved_segments():
    """The first path segments core answers on, which no plugin page may take.

    Derived here rather than in `core.web_apps` because core's patterns are this
    module's to know; the page mechanism is handed the answer.
    """
    return first_segments(core_urlpatterns)


urlpatterns = build_urlpatterns()
