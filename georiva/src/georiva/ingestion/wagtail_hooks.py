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
    from .push_feed_views import (
        delivery_retry,
        georiva_push_download,
        push_feed_delete,
        push_feed_edit,
        push_feed_list,
        push_feed_list_results,
        push_sender_add,
        push_sender_revoke,
        push_sender_rotate,
    )
    from .unit_range import convert_range_view
    from .upload_views import (
        push_feed_complete,
        push_feed_extract_times,
        push_feed_page,
        push_feed_prepare,
        push_feed_submit,
    )

    return [
        path("push-feeds/", push_feed_list, name="push_feed_list"),
        path("push-feeds/convert-range/", convert_range_view, name="push_feed_convert_range"),
        path("push-feeds/georiva_push.py", georiva_push_download, name="georiva_push_download"),
        path("push-feeds/<int:pk>/edit/", push_feed_edit, name="push_feed_edit"),
        # Results-only fragment the search box swaps in via AJAX.
        path("push-feeds/results/", push_feed_list_results, name="push_feed_list_results"),
        path("push-feeds/<int:pk>/delete/", push_feed_delete, name="push_feed_delete"),
        path("push-feeds/<int:pk>/", push_feed_page, name="push_feed_page"),
        path("push-feeds/<int:pk>/extract-times/", push_feed_extract_times, name="push_feed_extract_times"),
        path("push-feeds/<int:pk>/submit/", push_feed_submit, name="push_feed_submit"),
        path("push-feeds/<int:pk>/prepare/", push_feed_prepare, name="push_feed_prepare"),
        path("push-feeds/<int:pk>/complete/", push_feed_complete, name="push_feed_complete"),
        path("push-feeds/<int:pk>/deliveries/<int:delivery_pk>/retry/", delivery_retry, name="delivery_retry"),
        path("push-feeds/<int:pk>/senders/add/", push_sender_add, name="push_sender_add"),
        path("push-feeds/<int:pk>/senders/<int:sender_pk>/rotate/", push_sender_rotate, name="push_sender_rotate"),
        path("push-feeds/<int:pk>/senders/<int:sender_pk>/revoke/", push_sender_revoke, name="push_sender_revoke"),
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


@hooks.register("register_permissions")
def register_push_sender_permission():
    from django.contrib.auth.models import Permission

    return Permission.objects.filter(
        content_type__app_label="georivaingestion",
        codename="manage_push_senders",
    )
