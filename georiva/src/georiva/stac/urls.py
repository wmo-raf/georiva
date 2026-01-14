"""
GeoRiva STAC API URL Configuration

Mount these URLs in your project's main urls.py:
    path('stac/', include('georiva_stac.urls')),
"""

from django.urls import path

from .views import (
    STACLandingPageView,
    STACConformanceView,
    STACCollectionsView,
    STACCollectionDetailView,
    STACItemsView,
    STACItemDetailView,
    STACSearchView,
    STACQueryablesView,
)
from .openapi import openapi_view

app_name = 'stac'

urlpatterns = [
    # Landing page
    path('', STACLandingPageView.as_view(), name='landing'),
    
    # Conformance
    path('conformance/', STACConformanceView.as_view(), name='conformance'),
    
    # Collections
    path('collections/', STACCollectionsView.as_view(), name='collections'),
    path(
        'collections/<slug:catalog_slug>/<slug:collection_slug>',
        STACCollectionDetailView.as_view(),
        name='collection-detail'
    ),
    
    # Items
    path(
        'collections/<slug:catalog_slug>/<slug:collection_slug>/items',
        STACItemsView.as_view(),
        name='items'
    ),
    path(
        'collections/<slug:catalog_slug>/<slug:collection_slug>/items/<str:item_id>',
        STACItemDetailView.as_view(),
        name='item-detail'
    ),
    
    # Queryables
    path('queryables/', STACQueryablesView.as_view(), name='queryables'),
    path(
        'collections/<slug:catalog_slug>/<slug:collection_slug>/queryables',
        STACQueryablesView.as_view(),
        name='collection-queryables'
    ),
    
    # Search
    path('search/', STACSearchView.as_view(), name='search'),
    
    # OpenAPI documentation
    path('openapi/', openapi_view, name='openapi'),
]
