from django.urls import path
from wagtail import hooks

from .views import item_preview


@hooks.register('register_admin_urls')
def urlconf_viz():
    return [
        path('item-preview/<int:item_id>/', item_preview, name='item_preview'),
    ]
