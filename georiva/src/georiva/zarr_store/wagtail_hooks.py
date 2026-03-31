from django.urls import path
from wagtail import hooks

from .views import (
    zarr_rebuild_collection_view,
    zarr_collection_detail_view
)


@hooks.register('register_admin_urls')
def urlconf_zarr_store_urls():
    return [
        path(
            'zarr/rebuild-collection/<int:pk>/',
            zarr_rebuild_collection_view,
            name="zarr_rebuild_collection",
        ),
        path(
            'zarr/collection/<int:pk>/',
            zarr_collection_detail_view,
            name='zarr_collection_detail',
        ),
    ]
