from django.urls import path

from . import views

app_name = 'edr'

urlpatterns = [
    # Landing page + conformance
    path('', views.EDRLandingPageView.as_view(), name='landing'),
    path('conformance/', views.EDRConformanceView.as_view(), name='conformance'),
    
    # Collections
    path('collections/', views.EDRCollectionListView.as_view(), name='collection-list'),
    path('collections/<slug:collection_slug>/', views.EDRCollectionDetailView.as_view(), name='collection-detail'),
    
    # ── Future (data queries — not implemented yet) ──────────────────────
    # path('collections/<slug:collection_slug>/position/', views.EDRPositionView.as_view(), name='position'),
    # path('collections/<slug:collection_slug>/area/',     views.EDRAreaView.as_view(),     name='area'),
    # path('collections/<slug:collection_slug>/locations/',views.EDRLocationsView.as_view(),name='locations'),
    # path('collections/<slug:collection_slug>/instances/',views.EDRInstancesView.as_view(),name='instances'),
]
