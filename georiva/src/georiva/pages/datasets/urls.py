from django.urls import path

from . import views

app_name = 'datasets'

urlpatterns = [
    path(
        'collections/<slug:catalog_slug>/<slug:collection_slug>/dates/',
        views.collection_available_dates,
        name='collection-available-dates',
    ),
]
