from django.urls import path
from wagtail import hooks
from wagtail.snippets.models import register_snippet
from wagtail.snippets.views.snippets import SnippetViewSet

from georiva.organisations.scoping import OrgScopedViewSetMixin

from .models import RunIngestion
from .panels import IngestionActivityPanel


class RunIngestionViewSet(OrgScopedViewSetMixin, SnippetViewSet):
    model = RunIngestion


register_snippet(RunIngestionViewSet)

# No menu item: the Ingestion Activity page is reached through the homepage
# activity panel, and acquisition monitoring lives on each feed's dashboard
# (ADR 0019).


@hooks.register("register_admin_urls")
def register_ingestion_dashboard_urls():
    from .activity_views import ingestion_activity_feed
    from .dashboard_views import (
        collection_fetch_runs_api,
        collection_ingestion_jobs_api,
        collection_ingestion_logs_api,
        collection_upload_sessions_api,
        ingestion_dashboard_api,
    )
    from .run_views import file_ingestion_detail, run_ingestion_detail
    from .sse_views import ingestion_events_sse

    return [
        path("ingestion/runs/<int:run_ingestion_id>/", run_ingestion_detail, name="run_ingestion_detail"),
        path(
            "ingestion/files/<int:file_ingestion_id>/",
            file_ingestion_detail,
            name="file_ingestion_detail",
        ),
        path("ingestion/activity/", ingestion_activity_feed, name="ingestion_activity_feed"),
        path("api/ingestion/events/", ingestion_events_sse, name="ingestion_events_sse"),
        path("api/ingestion/dashboard/", ingestion_dashboard_api, name="ingestion_dashboard_api"),
        path(
            "api/ingestion/collections/<int:collection_id>/ingestion-logs/",
            collection_ingestion_logs_api,
            name="collection_ingestion_logs_api",
        ),
        path(
            "api/ingestion/collections/<int:collection_id>/ingestion-jobs/",
            collection_ingestion_jobs_api,
            name="collection_ingestion_jobs_api",
        ),
        path(
            "api/ingestion/collections/<int:collection_id>/fetch-runs/",
            collection_fetch_runs_api,
            name="collection_fetch_runs_api",
        ),
        path(
            "api/ingestion/collections/<int:collection_id>/upload-sessions/",
            collection_upload_sessions_api,
            name="collection_upload_sessions_api",
        ),
    ]


# "Push feeds" is reached via the "Data" menu group registered in
# core/wagtail_hooks.py, not a top-level menu item.


@hooks.register("register_admin_urls")
def register_push_feed_urls():
    from .push_feed_views import push_feed_delete, push_feed_list
    from .upload_views import (
        push_feed_extract_times,
        push_feed_page,
        push_feed_submit,
    )

    return [
        path("push-feeds/", push_feed_list, name="push_feed_list"),
        path("push-feeds/<int:pk>/delete/", push_feed_delete, name="push_feed_delete"),
        path("push-feeds/<int:pk>/", push_feed_page, name="push_feed_page"),
        path("push-feeds/<int:pk>/extract-times/", push_feed_extract_times, name="push_feed_extract_times"),
        path("push-feeds/<int:pk>/submit/", push_feed_submit, name="push_feed_submit"),
    ]


@hooks.register("register_admin_urls")
def register_upload_wizard_urls():
    from .upload_wizard_views import (
        upload_wizard_provision,
        upload_wizard_step1,
        upload_wizard_step2,
        upload_wizard_step3,
        upload_wizard_step4,
        upload_wizard_step5,
        upload_wizard_upload_sample,
    )

    return [
        path("push-feeds/wizard/step1/", upload_wizard_step1, name="upload_wizard_step1"),
        path("push-feeds/wizard/step2/", upload_wizard_step2, name="upload_wizard_step2"),
        path("push-feeds/wizard/step3/", upload_wizard_step3, name="upload_wizard_step3"),
        path("push-feeds/wizard/step4/", upload_wizard_step4, name="upload_wizard_step4"),
        path("push-feeds/wizard/step5/", upload_wizard_step5, name="upload_wizard_step5"),
        path("push-feeds/wizard/provision/", upload_wizard_provision, name="upload_wizard_provision"),
        path("push-feeds/wizard/upload-sample/", upload_wizard_upload_sample, name="upload_wizard_upload_sample"),
    ]


@hooks.register("construct_homepage_panels")
def add_ingestion_activity_panel(request, panels):
    panels.append(IngestionActivityPanel())
