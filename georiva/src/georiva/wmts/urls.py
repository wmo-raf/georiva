from django.urls import path

from . import views

app_name = 'wmts'

urlpatterns = [
    # The one WMTS route Django serves; must spell what
    # core.machine_plane.wmts_capabilities_url writes (ADR 0013).
    path(
        '<slug:org_slug>/WMTSCapabilities.xml',
        views.WMTSCapabilitiesView.as_view(),
        name='capabilities',
    ),
]
