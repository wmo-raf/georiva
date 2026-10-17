"""Wagtail's admin reports, narrowed to the host organisation (#297).

The six reports under the admin's ``reports/`` prefix list pages, moderation
queues and audit logs across the whole instance. Nothing in them consults the
Active organisation, so a user who belongs to several institutions — and the
instance admin, who may enter all of them — would see another institution's
pages on any host. The main menu hides the reports, but nothing else keeps a
person from typing the path, so the control has to be the URL.

Each report is scoped through the dispatcher in ``ownership`` at the one seam
its view leaves open:

* locked pages, aging pages and the two workflow reports hand their queryset out
  of ``get_queryset``;
* site history builds a union of per-log-model querysets and filters each one
  through ``filter_queryset``, so that is where it is narrowed — after the union
  there would be nothing left to filter;
* page types usage lists content types, which belong to nobody, and counts the
  pages of each. Its counts are scoped by pinning the report's own *site* filter
  to the organisation's root page, which both counts and "last edited" read.

The filter dropdowns are narrowed too (#452): they name users, workflows, tasks
and content types, and Wagtail builds each list from the whole instance. Every
filterset below narrows its own choices to what this organisation's rows use.

Wagtail's views are claimed at their own paths ahead of Wagtail's routes, the way
page search is (``pages.OrgScopedPageSearchView``): the ``register_admin_urls``
hook can only append, so replacing a view means claiming its URL.
"""

from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.urls import path
from django.views.decorators.cache import never_cache
from wagtail.admin.auth import require_admin_access
from wagtail.admin.urls import display_custom_404
from wagtail.admin.views.reports.aging_pages import AgingPagesView
from wagtail.admin.views.reports.audit_logging import LogEntriesView, SiteHistoryReportFilterSet
from wagtail.admin.views.reports.locked_pages import LockedPagesReportFilterSet, LockedPagesView
from wagtail.admin.views.reports.page_types_usage import (
    PageTypesUsageReportFilterSet,
    PageTypesUsageReportView,
    SiteFilter,
)
from wagtail.admin.views.reports.workflows import (
    WorkflowReportFilterSet,
    WorkflowTasksReportFilterSet,
    WorkflowTasksView,
    WorkflowView,
)
from wagtail.log_actions import registry as log_action_registry
from wagtail.models import TaskState, Workflow, WorkflowState

from .access import require_active_org
from .models import OrganisationMembership
from .ownership import scope_rows
from .pages import org_root_page


class OrgScopedQuerysetReportMixin:
    """For reports that hand their rows out of ``get_queryset``."""

    def get_queryset(self):
        return scope_rows(self.request, super().get_queryset())


def _members_of_host(request, users):
    """``users`` limited to members of the organisation serving ``request``."""
    members = OrganisationMembership.objects.filter(organisation=require_active_org(request))
    return users.filter(pk__in=members.values("user"))


class OrgScopedLockedPagesFilterSet(LockedPagesReportFilterSet):
    """ "Locked by" offers members of the host organisation, not everyone holding a lock."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        locked_by = self.filters["locked_by"]
        locked_by.queryset = _members_of_host(self.request, locked_by.get_queryset(self.request))


class OrgScopedWorkflowFilterSet(WorkflowReportFilterSet):
    """ "Requested by" offers members of the host organisation, "workflow" those its states ran."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        states = scope_rows(self.request, WorkflowState.objects.all())
        self.filters["workflow"].queryset = Workflow.objects.filter(pk__in=states.values("workflow"))
        requested_by = self.filters["requested_by"]
        requested_by.queryset = _members_of_host(self.request, requested_by.get_queryset(self.request))


class OrgScopedWorkflowTasksFilterSet(WorkflowTasksReportFilterSet):
    """Workflow and task filters offer only those this organisation's rows have run.

    Workflows and tasks are configuration shared by the instance and declare no
    owner, so what scopes them is use: the ones behind a workflow or task state
    the dispatcher lets this organisation see.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        states = scope_rows(self.request, WorkflowState.objects.all())
        task_states = scope_rows(self.request, TaskState.objects.all())
        self.filters["workflow"].queryset = Workflow.objects.filter(pk__in=states.values("workflow"))
        self.filters["task"].queryset = self.filters["task"].queryset.filter(pk__in=task_states.values("task"))


class OrgScopedSiteHistoryFilterSet(SiteHistoryReportFilterSet):
    """Users, types and actions offered are those of the log entries this organisation can see."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters["object_type"].queryset = self._content_types()

    def _log_entries(self):
        for log_model in log_action_registry.get_log_entry_models():
            yield scope_rows(self.request, log_model.objects.viewable_by_user(self.request.user))

    def _content_types(self):
        ids = set()
        for entries in self._log_entries():
            ids.update(entries.get_content_type_ids())
        return ContentType.objects.filter(pk__in=ids).order_by("model")

    def get_users_queryset(self):
        ids = set()
        for entries in self._log_entries():
            ids.update(entries.get_user_ids())
        User = get_user_model()
        return User.objects.filter(pk__in=ids).order_by(User.USERNAME_FIELD)

    def get_action_choices(self):
        actions = set()
        for entries in self._log_entries():
            actions.update(entries.get_actions())
        return [choice for choice in log_action_registry.get_choices() if choice[0] in actions]


class OrgScopedLockedPagesView(OrgScopedQuerysetReportMixin, LockedPagesView):
    filterset_class = OrgScopedLockedPagesFilterSet


class OrgScopedAgingPagesView(OrgScopedQuerysetReportMixin, AgingPagesView):
    pass


class OrgScopedWorkflowView(OrgScopedQuerysetReportMixin, WorkflowView):
    filterset_class = OrgScopedWorkflowFilterSet


class OrgScopedWorkflowTasksView(OrgScopedQuerysetReportMixin, WorkflowTasksView):
    filterset_class = OrgScopedWorkflowTasksFilterSet


class OrgScopedLogEntriesView(LogEntriesView):
    """Site history. Each log model's rows are scoped before they are unioned."""

    filterset_class = OrgScopedSiteHistoryFilterSet

    def filter_queryset(self, queryset):
        return super().filter_queryset(scope_rows(self.request, queryset))


class OrgScopedPageTypesUsageFilterSet(PageTypesUsageReportFilterSet):
    """The usage report's filters, with the site fixed to the host organisation's.

    Wagtail offers a site picker listing every site on the instance — which would
    name other institutions — and counts pages under whichever it picks. The
    picker is hidden, its one remaining choice is the organisation's own site, and
    the value is pinned by the form, so the counts and the "last edited" column
    are limited to the organisation's tree whatever the query string says.

    The filter is kept rather than removed: Wagtail drops it itself on a
    single-site instance, and the queryset is filtered by whatever the form
    cleaned, so a pinned value needs a filter to land on.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        root = org_root_page(self.request)
        self.sites_filter_enabled = False
        self.filters["site"] = SiteFilter(label="Site", choices=[(root.path, str(root))], empty_label=None)

    def get_form_class(self):
        root_path = org_root_page(self.request).path
        base = super().get_form_class()

        class OrgPinnedForm(base):
            def clean(self):
                cleaned = super().clean()
                cleaned["site"] = root_path
                return cleaned

        return OrgPinnedForm


class OrgScopedPageTypesUsageView(PageTypesUsageReportView):
    filterset_class = OrgScopedPageTypesUsageFilterSet


_REPORT_ROUTES = [
    ("locked/", "locked_pages", OrgScopedLockedPagesView),
    ("workflow/", "workflow", OrgScopedWorkflowView),
    ("workflow_tasks/", "workflow_tasks", OrgScopedWorkflowTasksView),
    ("site-history/", "site_history", OrgScopedLogEntriesView),
    ("aging-pages/", "aging_pages", OrgScopedAgingPagesView),
    ("page-types-usage/", "page_types_usage", OrgScopedPageTypesUsageView),
]

#: Each report and its results-only twin: URL-name suffix, path suffix, view kwargs.
_VARIANTS = [("", "", {}), ("_results", "results/", {"results_only": True})]

#: Wagtail's report URL name → the scoped view that stands in for it. Read by the
#: test that fails when a Wagtail release adds a report nobody has claimed.
ORG_SCOPED_REPORTS = {f"{name}{suffix}": view for _, name, view in _REPORT_ROUTES for suffix, _, _ in _VARIANTS}


def _as_wagtail_serves_it(view):
    """``view`` wrapped as Wagtail wraps every admin URL: admin access, a Wagtail 404, never cached.

    Claiming a route means standing in for the decorators Wagtail applies to its
    own admin patterns (``wagtail.admin.urls``), or the report would lose them.
    """
    return never_cache(display_custom_404(require_admin_access(view)))


def report_urlpatterns(admin_prefix):
    """The claims — each report and its results-only twin — under ``admin_prefix``."""
    return [
        path(
            f"{admin_prefix}reports/{route}{path_suffix}",
            _as_wagtail_serves_it(view.as_view(**kwargs)),
            name=f"georiva_org_scoped_report_{name}{name_suffix}",
        )
        for route, name, view in _REPORT_ROUTES
        for name_suffix, path_suffix, kwargs in _VARIANTS
    ]
