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

Wagtail's views are claimed at their own paths ahead of Wagtail's routes, the way
page search is (``pages.OrgScopedPageSearchView``): the ``register_admin_urls``
hook can only append, so replacing a view means claiming its URL.
"""

from django.urls import path
from wagtail.admin.auth import require_admin_access
from wagtail.admin.views.reports.aging_pages import AgingPagesView
from wagtail.admin.views.reports.audit_logging import LogEntriesView
from wagtail.admin.views.reports.locked_pages import LockedPagesView
from wagtail.admin.views.reports.page_types_usage import (
    PageTypesUsageReportFilterSet,
    PageTypesUsageReportView,
    SiteFilter,
)
from wagtail.admin.views.reports.workflows import WorkflowTasksView, WorkflowView

from .ownership import scope_rows
from .pages import org_root_page


class OrgScopedQuerysetReportMixin:
    """For reports that hand their rows out of ``get_queryset``."""

    def get_queryset(self):
        return scope_rows(self.request, super().get_queryset())


class OrgScopedLockedPagesView(OrgScopedQuerysetReportMixin, LockedPagesView):
    pass


class OrgScopedAgingPagesView(OrgScopedQuerysetReportMixin, AgingPagesView):
    pass


class OrgScopedWorkflowView(OrgScopedQuerysetReportMixin, WorkflowView):
    pass


class OrgScopedWorkflowTasksView(OrgScopedQuerysetReportMixin, WorkflowTasksView):
    pass


class OrgScopedLogEntriesView(LogEntriesView):
    """Site history. Each log model's rows are scoped before they are unioned."""

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


#: Wagtail's report URL name → the scoped view that stands in for it. Read by the
#: URL claims below and by the test that fails when a Wagtail release adds a report.
ORG_SCOPED_REPORTS = {}

_REPORT_ROUTES = [
    ("locked/", "locked_pages", OrgScopedLockedPagesView),
    ("workflow/", "workflow", OrgScopedWorkflowView),
    ("workflow_tasks/", "workflow_tasks", OrgScopedWorkflowTasksView),
    ("site-history/", "site_history", OrgScopedLogEntriesView),
    ("aging-pages/", "aging_pages", OrgScopedAgingPagesView),
    ("page-types-usage/", "page_types_usage", OrgScopedPageTypesUsageView),
]


def report_urlpatterns(admin_prefix):
    """The twelve claims — each report and its results-only twin — under ``admin_prefix``."""
    patterns = []
    for route, name, view in _REPORT_ROUTES:
        for suffix, kwargs in (("", {}), ("_results", {"results_only": True})):
            full_name = f"{name}{suffix}"
            ORG_SCOPED_REPORTS[full_name] = view
            patterns.append(
                path(
                    f"{admin_prefix}reports/{route}{'results/' if suffix else ''}",
                    require_admin_access(view.as_view(**kwargs)),
                    name=f"georiva_org_scoped_report_{full_name}",
                )
            )
    return patterns
