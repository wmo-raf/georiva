from django.urls import path, register_converter

from . import views
from .ids import EDRCollectionIdConverter

register_converter(EDRCollectionIdConverter, "edrid")

app_name = "edr"

urlpatterns = [
    path("", views.EDRLandingPageView.as_view(), name="landing"),
    path("conformance/", views.EDRConformanceView.as_view(), name="conformance"),
    path("collections/", views.EDRCollectionListView.as_view(), name="collection-list"),
    path("collections/<edrid:collection_id>/", views.EDRCollectionDetailView.as_view(), name="collection-detail"),
]
