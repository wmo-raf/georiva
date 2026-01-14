from django.urls import path

from georiva.core.views import minio_event_webhook

urlpatterns = [
    path('webhook/', minio_event_webhook, name='minio_event_webhook')
]
