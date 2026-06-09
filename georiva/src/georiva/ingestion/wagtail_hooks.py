from django.urls import path, reverse_lazy
from wagtail import hooks
from wagtail.admin.menu import MenuItem

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


@hooks.register("register_admin_menu_item")
def register_manual_uploads_menu():
    from django.utils.translation import gettext as _
    return MenuItem(
        _("Manual Uploads"),
        reverse_lazy("manual_upload_config_list"),
        icon_name="upload",
        order=850,
    )


@hooks.register("register_admin_urls")
def register_manual_upload_config_urls():
    from .manual_upload_views import (
        manual_upload_config_list,
        manual_upload_config_edit,
        manual_upload_config_delete,
    )
    return [
        path("manual-uploads/", manual_upload_config_list, name="manual_upload_config_list"),
        path("manual-uploads/<int:pk>/edit/", manual_upload_config_edit, name="manual_upload_config_edit"),
        path("manual-uploads/<int:pk>/delete/", manual_upload_config_delete, name="manual_upload_config_delete"),
    ]


@hooks.register("register_admin_urls")
def register_upload_wizard_urls():
    from .upload_wizard_views import (
        upload_wizard_step1,
        upload_wizard_step2,
        upload_wizard_step3,
        upload_wizard_step4,
        upload_wizard_step5,
        upload_wizard_provision,
        upload_wizard_upload_sample,
    )

    return [
        path("manual-uploads/wizard/step1/", upload_wizard_step1, name="upload_wizard_step1"),
        path("manual-uploads/wizard/step2/", upload_wizard_step2, name="upload_wizard_step2"),
        path("manual-uploads/wizard/step3/", upload_wizard_step3, name="upload_wizard_step3"),
        path("manual-uploads/wizard/step4/", upload_wizard_step4, name="upload_wizard_step4"),
        path("manual-uploads/wizard/step5/", upload_wizard_step5, name="upload_wizard_step5"),
        path("manual-uploads/wizard/provision/", upload_wizard_provision, name="upload_wizard_provision"),
        path("manual-uploads/wizard/upload-sample/", upload_wizard_upload_sample, name="upload_wizard_upload_sample"),
    ]


@hooks.register('construct_homepage_panels')
def add_ingestion_activity_panel(request, panels):
    panels.append(IngestionActivityPanel())
