from django.urls import path
from wagtail import hooks

from .panels import IngestionActivityPanel


@hooks.register("register_admin_urls")
def register_ingestion_dashboard_urls():
    from .dashboard_views import (
        ingestion_dashboard_api,
        collection_data_arrivals_api,
        collection_ingestion_logs_api,
        collection_ingestion_jobs_api,
    )

    return [
        path("api/ingestion/dashboard/", ingestion_dashboard_api, name="ingestion_dashboard_api"),
        path("api/ingestion/collections/<int:collection_id>/arrivals/", collection_data_arrivals_api,
             name="collection_data_arrivals_api"),
        path("api/ingestion/collections/<int:collection_id>/ingestion-logs/", collection_ingestion_logs_api,
             name="collection_ingestion_logs_api"),
        path("api/ingestion/collections/<int:collection_id>/ingestion-jobs/", collection_ingestion_jobs_api,
             name="collection_ingestion_jobs_api"),
    ]


@hooks.register("register_admin_urls")
def register_upload_wizard_urls():
    from .upload_wizard_views import (
        upload_wizard_step1,
        upload_wizard_step2,
        upload_wizard_step3,
        upload_wizard_step4,
        upload_wizard_step5,
        upload_wizard_step6,
        upload_wizard_provision,
    )

    return [
        path("manual-uploads/wizard/step1/", upload_wizard_step1, name="upload_wizard_step1"),
        path("manual-uploads/wizard/step2/", upload_wizard_step2, name="upload_wizard_step2"),
        path("manual-uploads/wizard/step3/", upload_wizard_step3, name="upload_wizard_step3"),
        path("manual-uploads/wizard/step4/", upload_wizard_step4, name="upload_wizard_step4"),
        path("manual-uploads/wizard/step5/", upload_wizard_step5, name="upload_wizard_step5"),
        path("manual-uploads/wizard/step6/", upload_wizard_step6, name="upload_wizard_step6"),
        path("manual-uploads/wizard/provision/", upload_wizard_provision, name="upload_wizard_provision"),
    ]


@hooks.register('construct_homepage_panels')
def add_ingestion_activity_panel(request, panels):
    panels.append(IngestionActivityPanel())
