from django.urls import path, include

from georiva.core.tile_config_view import TileConfigView
from georiva.edr import urls as edr_urls
from georiva.ingestion.dashboard_views import upload_session_status_api
from georiva.stac import urls as georiva_stac_urls

urlpatterns = [
    path('stac/', include(georiva_stac_urls), name='stac'),
    path('edr/', include(edr_urls), name='edr'),
    path('jobs/', include('task_ferry.api.urls', namespace='task_ferry')),
    path(
        'tile-config/<slug:catalog_slug>/<slug:collection_slug>/<slug:variable_slug>/',
        TileConfigView.as_view(),
        name='tile_config',
    ),
    path("analysis/", include("georiva.analysis.urls")),
    path('datasets/', include('georiva.pages.datasets.urls', namespace='datasets')),
    path('upload-sessions/<int:session_id>/status/', upload_session_status_api, name='upload_session_status_api'),
]
