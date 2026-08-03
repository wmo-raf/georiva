from django.conf import settings
from django.contrib import admin
from django.urls import include, path
from wagtail import urls as wagtail_urls
from wagtail.admin import urls as wagtailadmin_urls
from wagtail.documents import urls as wagtaildocs_urls

from georiva.api import urls as georiva_urls
from georiva.core.tile_auth_view import TileAuthView
from .views import health

urlpatterns = [
    path("django-admin/", admin.site.urls),
    path("admin/", include(wagtailadmin_urls)),
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
