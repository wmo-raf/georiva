from django.urls import include, path

from georiva.core.machine_plane.config_view import TileConfigView
from georiva.core.plugins import api_urlpatterns as plugin_api_urlpatterns
from georiva.edr import urls as edr_urls
from georiva.ingestion.dashboard_views import upload_session_status_api
from georiva.stac import urls as georiva_stac_urls
from georiva.wmts import urls as georiva_wmts_urls

#: Core's own routes, named so the ordering rule below is readable and so that
#: tests can ask about core's surface without a plugin's routes in the answer.
core_urlpatterns = [
    path("stac/", include(georiva_stac_urls), name="stac"),
    path("edr/", include(edr_urls), name="edr"),
    path("wmts/", include(georiva_wmts_urls), name="wmts"),
    path("jobs/", include("task_ferry.api.urls", namespace="task_ferry")),
    path(
        "tile-config/<slug:org_slug>/<slug:catalog_slug>/<slug:collection_slug>/<slug:variable_slug>/",
        TileConfigView.as_view(),
        name="tile_config",
    ),
    path("analysis/", include("georiva.analysis.urls")),
    path("datasets/", include("georiva.pages.datasets.urls", namespace="datasets")),
    path("upload-sessions/<int:session_id>/status/", upload_session_status_api, name="upload_session_status_api"),
]

# Plugin-contributed routes, **after** core's. `/api/` is one organisation's whole
# public service (ADR 0012), and a plugin that publishes something over it should
# not have to ask core to be routed — but it must not be able to take a route
# core already answers. Appending is the whole of that rule: the resolver stops
# at the first match, so every pattern above is unreachable from below (ADR 0028).
urlpatterns = core_urlpatterns + plugin_api_urlpatterns()
