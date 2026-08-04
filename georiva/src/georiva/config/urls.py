from django.conf import settings
from django.contrib import admin
from django.urls import include, path
from wagtail import urls as wagtail_urls
from wagtail.admin import urls as wagtailadmin_urls
from wagtail.documents import urls as wagtaildocs_urls

from wagtail.admin.auth import require_admin_access

from georiva.api import urls as georiva_urls
from georiva.core.tile_auth_view import TileAuthView
from georiva.organisations.hopper import org_hopper_script
from georiva.organisations.pages import OrgScopedPageSearchView
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

urlpatterns = [
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
    urlpatterns += staticfiles_urlpatterns()
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

urlpatterns = urlpatterns + [
    # For anything not caught by a more specific rule above, hand over to
    # Wagtail's page serving mechanism. This should be the last pattern in
    # the list:
    path("", include(wagtail_urls)),
    # Alternatively, if you want Wagtail pages to be served from a subpath
    # of your site, rather than the site root:
    #    path("pages/", include(wagtail_urls)),
]
