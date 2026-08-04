"""The admin dashboard, narrowed to the organisation whose host was dialled.

Four of Wagtail's dashboard panels resolve pages: the two workflow moderation
queues, the recent-edits list and the locked-pages list. All four ask *who* the
signed-in user is and none of them asks *where* they are, so an operator who
belongs to two institutions — and the instance admin, who belongs to all of them
— was shown one organisation's pages on another organisation's dashboard.

The dashboard is the one page surface none of the four mechanisms in ADR 0016
could reach: a panel resolves its pages inside a context method, takes no page
id, runs no queryset hook and exposes no queryset to narrow. What it does offer
is ``construct_homepage_panels``, which hands over the assembled list — so each
stock panel is swapped for a scoped subclass of itself, by class, in place.

*In place*, never rebuilt. GeoRiva's own modules append panels of their own
through the same hook, and this app is registered after them, so its hook runs
last: a wholesale rebuild here would silently delete their work.

Two ways of narrowing, and which one a panel gets is decided by one thing —
whether its result is sliced:

* the recent-edits list is limited to a handful of rows *in the database*, so it
  has to be narrowed while the query is still being built. Narrowing afterwards
  would let another institution's edits consume the limit and quietly return
  fewer rows, or none. Its log entries carry a real foreign key to the page, so
  the page-tree narrowing applies directly — at the cost of restating Wagtail's
  query, which is the only place here that is worth paying.
* the other three are unsliced — an unevaluated queryset and two lists — so they
  are narrowed after calling the stock implementation. Lossless, and it
  duplicates nothing.
"""
from django.conf import settings
from django.db.models import Max
from wagtail.admin.views.home import (
    LockedPagesPanel,
    RecentEditsPanel,
    UserObjectsInWorkflowModerationPanel,
    WorkflowObjectsToModeratePanel,
)
from wagtail.models import Page, PageLogEntry

from .ownership import belongs_to_active_org
from .pages import PAGE_EDIT_ACTION, scope_page_log_entries, scope_pages


class OrgScopedRecentEditsPanel(RecentEditsPanel):
    """The recent-edits list, limited to this organisation's rows.

    Wagtail's implementation is restated rather than called, because the row
    limit is applied by the database inside it and there is no seam between the
    filter and the slice. The added clause is the page-tree narrowing; everything
    else is Wagtail's, and drifts from it exactly as any vendored query does — a
    Wagtail release that changes how this panel queries changes what an operator
    sees here, and is the release to re-read this against.
    """

    def get_context_data(self, parent_context):
        request = parent_context["request"]
        # Deliberately skipping RecentEditsPanel's own implementation: calling it
        # would run the sliced query this method exists to replace.
        context = super(RecentEditsPanel, self).get_context_data(parent_context)

        edit_count = getattr(settings, "WAGTAILADMIN_RECENT_EDITS_LIMIT", 5)
        last_edits_dates = (
            scope_page_log_entries(
                request,
                PageLogEntry.objects.filter(user=request.user, action=PAGE_EDIT_ACTION),
            )
            .values("page_id")
            .annotate(latest_date=Max("timestamp"))
            .order_by("-latest_date")[:edit_count]
        )
        pages_mapping = (
            Page.objects.specific()
            .prefetch_workflow_states()
            .annotate_approved_schedule()
            .in_bulk([log["page_id"] for log in last_edits_dates])
        )
        context["last_edits"] = [
            (log["latest_date"], pages_mapping[log["page_id"]])
            for log in last_edits_dates
            if log["page_id"] in pages_mapping
        ]
        context["request"] = request
        return context


class OrgScopedLockedPagesPanel(LockedPagesPanel):
    """The locks this user holds, in this organisation's tree only.

    Wagtail leaves the queryset unevaluated, so the narrowing is the same
    page-tree filter the explorer uses, added to it — still lazy, still resolved
    by the database.
    """

    def get_context_data(self, parent_context):
        context = super().get_context_data(parent_context)
        context["locked_pages"] = scope_pages(
            parent_context["request"], context["locked_pages"]
        )
        return context


class OrgScopedUserObjectsInWorkflowModerationPanel(UserObjectsInWorkflowModerationPanel):
    """The "waiting on a moderator" queue, for this organisation only.

    The panel has already materialised its states into a list by the time it
    returns — it drops the ones whose generic foreign key points at nothing — so
    this filters that list rather than the queryset behind it. Each state is
    judged by the object it moderates, which may be a page or a snippet, by
    whatever that model has declared — with the one hole ``ownership`` names: a
    snippet from outside this codebase reads as shared and is admitted.
    """

    def get_context_data(self, parent_context):
        context = super().get_context_data(parent_context)
        request = parent_context["request"]
        context["workflow_states"] = [
            state for state in context["workflow_states"]
            if belongs_to_active_org(request, state.content_object)
        ]
        return context


class OrgScopedWorkflowObjectsToModeratePanel(WorkflowObjectsToModeratePanel):
    """The "awaiting your review" queue, for this organisation only."""

    def get_context_data(self, parent_context):
        context = super().get_context_data(parent_context)
        request = parent_context["request"]
        context["states"] = [
            state for state in context["states"]
            if belongs_to_active_org(request, state["obj"])
        ]
        return context


#: Every stock Wagtail dashboard panel that resolves a page, and what replaces
#: it. A Wagtail release that adds a panel is not in here, and the dashboard
#: tests fail on it rather than letting it appear unscoped.
SCOPED_DASHBOARD_PANELS = {
    RecentEditsPanel: OrgScopedRecentEditsPanel,
    LockedPagesPanel: OrgScopedLockedPagesPanel,
    UserObjectsInWorkflowModerationPanel: OrgScopedUserObjectsInWorkflowModerationPanel,
    WorkflowObjectsToModeratePanel: OrgScopedWorkflowObjectsToModeratePanel,
}


def scope_dashboard_panels(panels):
    """Swap each stock panel for its scoped equivalent, leaving the rest alone.

    Matched on the exact class, so a panel another module contributed — or a
    subclass somebody registered deliberately — passes through untouched.

    Takes no request: which panels are replaced is a question about classes, and
    each replacement reads the request it is rendered for from its own parent
    context.
    """
    for index, panel in enumerate(panels):
        replacement = SCOPED_DASHBOARD_PANELS.get(type(panel))
        if replacement is not None:
            panels[index] = replacement()
