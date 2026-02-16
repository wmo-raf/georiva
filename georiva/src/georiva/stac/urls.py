from django.urls import path

from . import views

app_name = 'stac'

urlpatterns = [
    # Root
    path('', views.STACLandingPageView.as_view(), name='landing'),
    path('conformance/', views.STACConformanceView.as_view(), name='conformance'),
    
    # Search
    path('search/', views.STACSearchView.as_view(), name='search'),
    
    # Global queryables
    path('queryables/', views.STACQueryablesView.as_view(), name='queryables'),
    
    # Catalogs (top-level collections)
    path('collections/', views.STACCatalogListView.as_view(), name='catalog-list'),
    path('collections/<slug:catalog_slug>', views.STACCatalogDetailView.as_view(), name='catalog-detail'),
    path('collections/<slug:catalog_slug>/queryables/', views.STACQueryablesView.as_view(), name='catalog-queryables'),
    
    # Variable collections within a Catalog
    path('collections/<slug:catalog_slug>/collections/', views.STACCollectionListView.as_view(),
         name='collection-list'),
    
    # Variable as Collection
    path('collections/<slug:catalog_slug>/<slug:variable_slug>', views.STACCollectionDetailView.as_view(),
         name='collection-detail'),
    path('collections/<slug:catalog_slug>/<slug:variable_slug>/queryables/', views.STACQueryablesView.as_view(),
         name='collection-queryables'),
    
    # Items
    path('collections/<slug:catalog_slug>/<slug:variable_slug>/items', views.STACItemsView.as_view(), name='items'),
    path('collections/<slug:catalog_slug>/<slug:variable_slug>/items/<str:item_id>', views.STACItemDetailView.as_view(),
         name='item-detail'),
]
