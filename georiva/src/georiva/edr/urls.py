from django.urls import path, register_converter

from . import views
from .ids import EDRCollectionIdConverter

register_converter(EDRCollectionIdConverter, "edrid")

app_name = "edr"

urlpatterns = [
    # Landing page + conformance
    path("", views.EDRLandingPageView.as_view(), name="landing"),
    path("conformance/", views.EDRConformanceView.as_view(), name="conformance"),
    path("collections/", views.EDRCollectionListView.as_view(), name="collection-list"),
    # ``{catalog}.{collection}``, or a bare slug that redirects to it (ADR 0045).
    path("collections/<edrid:collection_id>/", views.EDRCollectionDetailView.as_view(), name="collection-detail"),
    # ── Future (data queries — not implemented yet) ──────────────────────
    # path('collections/<edrid:collection_id>/position/', views.EDRPositionView.as_view(), name='position'),
    # path('collections/<edrid:collection_id>/area/',     views.EDRAreaView.as_view(),     name='area'),
    # path('collections/<edrid:collection_id>/locations/',views.EDRLocationsView.as_view(),name='locations'),
    # path('collections/<edrid:collection_id>/instances/',views.EDRInstancesView.as_view(),name='instances'),
]
