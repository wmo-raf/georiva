from django.urls import path
from wagtail import hooks

from .panels import IngestionActivityPanel


@hooks.register("register_admin_urls")
def register_ingestion_dashboard_urls():
    from .dashboard_views import (
        ingestion_dashboard_api,
        collection_loader_runs_api,
        collection_ingestion_logs_api
    )
    
    return [
        path("api/ingestion/dashboard/", ingestion_dashboard_api, name="ingestion_dashboard_api"),
        path("api/ingestion/collections/<int:collection_id>/loader-runs/", collection_loader_runs_api,
             name="collection_loader_runs_api"),
        path("api/ingestion/collections/<int:collection_id>/ingestion-logs/", collection_ingestion_logs_api,
             name="collection_ingestion_logs_api"),
    ]


@hooks.register('construct_homepage_panels')
def add_ingestion_activity_panel(request, panels):
    panels.append(IngestionActivityPanel())
