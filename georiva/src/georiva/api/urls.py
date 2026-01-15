from django.urls import path, include

from georiva.ingestion.views import minio_event_webhook
from georiva.stac import urls as georiva_stac_urls

urlpatterns = [
    path('webhook/', minio_event_webhook, name='minio_event_webhook'),
    path('stac/', include(georiva_stac_urls), name='stac')
]
