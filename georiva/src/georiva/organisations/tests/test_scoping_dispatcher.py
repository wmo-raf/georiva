"""The dispatcher answers for every kind of declaration, both ways round.

Two organisations, each with a real page tree, and fixtures that differ *only*
by which organisation owns them — the arrangement in which a dropped filter
stops being invisible.

Every case is asserted twice: the queryset entry point and the object entry
point. They are separate code paths over one vocabulary, and a surface that
listed rows by one rule and admitted them by another would be the bug this
module exists to prevent.
"""

from contextlib import contextmanager

from django.core.exceptions import ImproperlyConfigured
from django.test import RequestFactory, TestCase, override_settings
from wagtail.log_actions import log
from wagtail.models import (
    GroupApprovalTask,
    ModelLogEntry,
    Page,
    PageLogEntry,
    TaskState,
    Workflow,
    WorkflowState,
    WorkflowTask,
)

from georiva.organisations.lookups import (
    NOT_ORM_SCOPABLE,
    PAGE_TREE,
    SHARED_REFERENCE_DATA,
    content_object_fields,
    is_orm_path,
    kind_of,
    related_path,
    via_content_object,
    via_related,
)
from georiva.organisations.ownership import (
    belongs_to_active_org,
    declaration_of,
    is_scopable,
    require_active_org_object,
    scope_rows,
)
from georiva.organisations.provisioning import provision_organisation
from georiva.organisations.testing import make_org_tree

from .factories import add_member, make_user
from .test_page_trees import add_child_page


@contextmanager
def declaring(model, declared):
    """``model`` declaring ``declared`` for the duration, then back as it was.

    For the two failure modes no real model may have — a declaration naming a
    relation that does not exist, and a pair of models delegating to each other.
    Both are refusals the dispatcher owes a developer who mistypes one.
    """
    original = model.ORGANISATION_LOOKUP
    model.ORGANISATION_LOOKUP = declared
    try:
        yield
    finally:
        model.ORGANISATION_LOOKUP = original


class VocabularyTests(TestCase):
    """The declaration strings, read back by the helpers that parse them."""

    def test_a_related_declaration_round_trips(self):
        self.assertEqual(related_path(via_related("workflow_state")), "workflow_state")

    def test_a_content_object_declaration_round_trips(self):
        self.assertEqual(
            content_object_fields(via_content_object("base_content_type", "object_id")),
            ("base_content_type", "object_id"),
        )

    def test_the_parameterised_kinds_are_not_orm_paths(self):
        """The trap the ``SENTINELS`` membership test would have fallen into.

        A parameterised declaration carries its argument in the string, so it is
        in no frozenset. Read as an ORM path it would become a ``filter()`` on a
        column called ``via-related:page``.
        """
        for declared in (via_related("page"), via_content_object("content_type", "object_id")):
            with self.subTest(declared=declared):
                self.assertFalse(is_orm_path(declared))
        self.assertFalse(is_orm_path(PAGE_TREE))
        self.assertTrue(is_orm_path("collection__catalog__organisation"))

    def test_every_declaration_resolves_to_exactly_one_kind(self):
        """The one question the dispatcher asks, answered for each spelling.

        Both entry points and ``is_scopable`` switch on this and nothing else, so
        a kind that came out wrong here would not be a wrong branch in one of
        them — it would be the same wrong branch in all three.
        """
        from georiva.organisations import lookups

        expected = {
            SHARED_REFERENCE_DATA: SHARED_REFERENCE_DATA,
            lookups.ORGANISATION_SELF: lookups.ORGANISATION_SELF,
            PAGE_TREE: PAGE_TREE,
            NOT_ORM_SCOPABLE: lookups.NO_ROUTE,
            None: lookups.NO_ROUTE,
            "": lookups.NO_ROUTE,
            via_related("page"): lookups.VIA_RELATED,
            via_content_object("content_type", "object_id"): lookups.VIA_CONTENT_OBJECT,
            "collection__catalog__organisation": lookups.ORM_PATH,
        }
        for declared, kind in expected.items():
            with self.subTest(declared=declared):
                self.assertEqual(kind_of(declared), kind)

    def test_a_page_class_we_did_not_write_is_still_judged_by_the_tree(self):
        """Wagtail's own ``Page`` could not have declared anything.

        Left to ``declared_lookup`` it would read as shared reference data —
        every organisation's pages, admitted everywhere — because that is what
        this codebase says about a model from outside it.
        """
        self.assertEqual(declaration_of(Page), PAGE_TREE)


@override_settings(GEORIVA_BASE_DOMAIN="georiva.test", ALLOWED_HOSTS=["*"])
class DispatcherTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.kenya = provision_organisation(name="Kenya Met", slug="kenya")
        cls.uganda = provision_organisation(name="Uganda Met", slug="uganda")
        cls.kenya_page = add_child_page(cls.kenya.site.root_page, "Nairobi", "nairobi")
        cls.uganda_page = add_child_page(cls.uganda.site.root_page, "Kampala", "kampala")
        cls.kenya_catalog = make_org_tree(cls.kenya)["catalog"]
        cls.uganda_catalog = make_org_tree(cls.uganda)["catalog"]

    def setUp(self):
        self.user = make_user("amina")
        add_member(self.user, self.kenya)
        add_member(self.user, self.uganda)

    def request(self, organisation=None):
        request = RequestFactory().get("/admin/")
        request.active_org = organisation or self.kenya
        request.user = self.user
        return request

    def assert_scopes_to(self, queryset, expected, *, request=None):
        """``queryset`` narrows to exactly ``expected``, and each row agrees.

        The second half is the point: a listing and a detail view must not
        disagree about the same row.

        Compared by primary key, because a page queryset yields ``Page`` where
        the fixture holds the specific class, and Django calls those two rows
        unequal.
        """
        request = request or self.request()
        rows = list(scope_rows(request, queryset))
        expected_pks = {row.pk for row in expected}
        self.assertEqual({row.pk for row in rows}, expected_pks)
        for row in queryset:
            with self.subTest(row=repr(row)):
                self.assertEqual(belongs_to_active_org(request, row), row.pk in expected_pks)

    def start_workflow(self, page):
        """Put ``page`` into moderation and hand back its workflow state."""
        task = GroupApprovalTask.objects.create(name=f"Moderate {page.slug}")
        workflow = Workflow.objects.create(name=f"Moderation {page.slug}")
        WorkflowTask.objects.create(workflow=workflow, task=task, sort_order=0)
        page.save_revision(user=self.user)
        return workflow.start(page, self.user)

    # -- pages, by tree position -------------------------------------------

    def test_pages_are_narrowed_to_the_active_organisations_tree(self):
        self.assert_scopes_to(
            Page.objects.filter(pk__in=[self.kenya_page.pk, self.uganda_page.pk]),
            [self.kenya_page],
        )

    def test_the_other_host_sees_the_other_tree(self):
        """The mirror. A dispatcher narrowing to nothing would pass the above."""
        self.assert_scopes_to(
            Page.objects.filter(pk__in=[self.kenya_page.pk, self.uganda_page.pk]),
            [self.uganda_page],
            request=self.request(self.uganda),
        )

    # -- via_related -------------------------------------------------------

    def test_page_log_entries_follow_their_page(self):
        mine = log(instance=self.kenya_page, action="wagtail.edit", user=self.user)
        theirs = log(instance=self.uganda_page, action="wagtail.edit", user=self.user)
        self.assert_scopes_to(PageLogEntry.objects.filter(pk__in=[mine.pk, theirs.pk]), [mine])

    def test_a_log_entry_whose_page_has_gone_is_nobodys(self):
        """Not hypothetical: that foreign key has no database constraint.

        Wagtail keeps the page audit log under ``db_constraint=False`` and
        ``DO_NOTHING``, so an entry outlives the page it describes. The object
        half has to read that the same way the queryset half does — as nobody's
        — rather than raising on the attribute.
        """
        entry = log(instance=self.kenya_page, action="wagtail.edit", user=self.user)
        PageLogEntry.objects.filter(pk=entry.pk).update(page_id=999999)
        orphan = PageLogEntry.objects.get(pk=entry.pk)
        self.assertFalse(belongs_to_active_org(self.request(), orphan))
        self.assertEqual(list(scope_rows(self.request(), PageLogEntry.objects.filter(pk=entry.pk))), [])

    def test_a_page_child_follows_its_page(self):
        """A declaration on one of our own models, not on one of Wagtail's."""
        from georiva.pages.home.models import FeaturedCatalog

        mine = FeaturedCatalog.objects.create(page=self.kenya_page, catalog=self.kenya_catalog, sort_order=0)
        theirs = FeaturedCatalog.objects.create(page=self.uganda_page, catalog=self.uganda_catalog, sort_order=0)
        self.assert_scopes_to(FeaturedCatalog.objects.filter(pk__in=[mine.pk, theirs.pk]), [mine])

    # -- via_content_object ------------------------------------------------

    def test_workflow_states_follow_the_object_under_moderation(self):
        mine = self.start_workflow(self.kenya_page)
        theirs = self.start_workflow(self.uganda_page)
        self.assert_scopes_to(WorkflowState.objects.filter(pk__in=[mine.pk, theirs.pk]), [mine])

    def test_task_states_follow_their_workflow_state(self):
        """Two hops: ``via_related`` onto a ``via_content_object`` onto a page."""
        mine = self.start_workflow(self.kenya_page)
        theirs = self.start_workflow(self.uganda_page)
        self.assert_scopes_to(
            TaskState.objects.filter(workflow_state__in=[mine.pk, theirs.pk]),
            TaskState.objects.filter(workflow_state=mine),
        )

    def test_a_generic_subject_scoped_by_an_orm_path(self):
        """The recursion's other leg: the subject is a snippet, not a page."""
        mine = log(instance=self.kenya_catalog, action="wagtail.create", user=self.user)
        theirs = log(instance=self.uganda_catalog, action="wagtail.create", user=self.user)
        self.assert_scopes_to(ModelLogEntry.objects.filter(pk__in=[mine.pk, theirs.pk]), [mine])

    def test_a_generic_subject_that_belongs_everywhere_is_admitted(self):
        """Shared reference data under moderation is nobody's and everybody's.

        Admitted whole, without a per-row id list — the open question #296 asked,
        answered the way ``belongs_to_active_org`` already answered it for an
        object in hand.
        """
        from georiva.core.models import Topic

        topic = Topic.objects.create(name="Rainfall", slug="rainfall")
        entry = log(instance=topic, action="wagtail.create", user=self.user)
        self.assert_scopes_to(ModelLogEntry.objects.filter(pk=entry.pk), [entry])

    def test_rows_whose_generic_subject_has_gone_are_dropped(self):
        """A dangling id names a subject no organisation can be shown to own."""
        entry = log(instance=self.kenya_catalog, action="wagtail.create", user=self.user)
        ModelLogEntry.objects.filter(pk=entry.pk).update(object_id="999999")
        self.assertEqual(list(scope_rows(self.request(), ModelLogEntry.objects.filter(pk=entry.pk))), [])
        self.assertFalse(belongs_to_active_org(self.request(), ModelLogEntry.objects.get(pk=entry.pk)))

    def test_a_generic_subject_that_belongs_everywhere_agrees_when_its_row_has_gone(self):
        """The two halves must not part company over a dangling shared id.

        The queryset half admits a shared content type whole, without listing
        its primary keys — so the object half must admit it without resolving
        the row either. Resolve it and a deleted topic turns one row into a
        listing that shows it and a detail view that 404s.
        """
        from georiva.core.models import Topic

        topic = Topic.objects.create(name="Rainfall", slug="rainfall")
        entry = log(instance=topic, action="wagtail.create", user=self.user)
        topic.delete()
        self.assert_scopes_to(ModelLogEntry.objects.filter(pk=entry.pk), [entry])

    def test_a_null_relation_is_nobodys_in_both_halves(self):
        """Even when the model at the end of the relation belongs everywhere.

        The queryset half could cheaply admit these — a shared target needs no
        subquery, so the null rows come along free — while the object half reads
        a null as nobody's. Only one of those can be right, and a row naming no
        owner is nobody's.

        ``DataFeed`` is the fixture because its catalog is genuinely nullable; it
        stands in here for any model that delegates through a link it may not
        have.
        """
        from georiva.core.models import Catalog
        from georiva.sources.models import DataFeed

        feed = DataFeed.objects.create(name="Kenya feed", catalog=self.kenya_catalog)
        queryset = DataFeed.objects.filter(pk=feed.pk)
        with declaring(DataFeed, via_related("catalog")), declaring(Catalog, SHARED_REFERENCE_DATA):
            self.assertEqual(list(scope_rows(self.request(), queryset)), [feed])
            self.assertTrue(belongs_to_active_org(self.request(), feed))

            DataFeed.objects.filter(pk=feed.pk).update(catalog=None)
            orphan = DataFeed.objects.get(pk=feed.pk)
            self.assertEqual(list(scope_rows(self.request(), queryset)), [])
            self.assertFalse(belongs_to_active_org(self.request(), orphan))

    # -- the kinds that were already there ---------------------------------

    def test_an_orm_path_still_reaches_access(self):
        from georiva.core.models import Catalog

        self.assert_scopes_to(
            Catalog.objects.filter(pk__in=[self.kenya_catalog.pk, self.uganda_catalog.pk]),
            [self.kenya_catalog],
        )

    def test_shared_reference_data_is_passed_through_untouched(self):
        from georiva.core.models import Topic

        Topic.objects.create(name="Rainfall", slug="rainfall")
        self.assertEqual(list(scope_rows(self.request(), Topic.objects.all())), list(Topic.objects.all()))

    # -- refusals ----------------------------------------------------------

    def test_a_model_with_no_route_is_refused_by_both_entry_points(self):
        from georiva.accounts.models import ApiKey

        with self.assertRaises(ImproperlyConfigured):
            scope_rows(self.request(), ApiKey.objects.all())
        with self.assertRaises(ImproperlyConfigured):
            belongs_to_active_org(self.request(), ApiKey())

    def test_a_generic_subject_that_reaches_no_organisation_is_refused(self):
        """#296's open question, at the seam it was actually about.

        Refusing a top-level model is the easy half. The judgement call was what
        a *subject* with no route should do to an otherwise fine listing, and the
        answer is the same one everywhere else: fail loudly, because the sweep
        guarantees no model in this codebase gets here undeclared and a model
        from outside it reads as shared.
        """
        from georiva.core.models import Catalog

        entry = log(instance=self.kenya_catalog, action="wagtail.create", user=self.user)
        with declaring(Catalog, NOT_ORM_SCOPABLE):
            with self.assertRaises(ImproperlyConfigured):
                list(scope_rows(self.request(), ModelLogEntry.objects.filter(pk=entry.pk)))
            with self.assertRaises(ImproperlyConfigured):
                belongs_to_active_org(self.request(), ModelLogEntry.objects.get(pk=entry.pk))

    def test_a_declaration_naming_a_relation_that_is_not_one_is_refused(self):
        """A typo in a declaration fails loudly rather than filtering nothing."""
        from georiva.pages.home.models import FeaturedCatalog

        with declaring(FeaturedCatalog, via_related("nonesuch")):
            with self.assertRaises(ImproperlyConfigured):
                list(scope_rows(self.request(), FeaturedCatalog.objects.all()))

    def test_a_cycle_of_declarations_is_refused(self):
        """Two models delegating to each other would otherwise recurse forever."""
        from georiva.core.models import Catalog, Collection

        with declaring(Catalog, via_related("collections")), declaring(Collection, via_related("catalog")):
            with self.assertRaises(ImproperlyConfigured):
                list(scope_rows(self.request(), Catalog.objects.all()))

    # -- the enforcing form ------------------------------------------------

    def test_require_refuses_another_organisations_row(self):
        from django.http import Http404

        request = self.request()
        self.assertIs(require_active_org_object(request, self.kenya_page), self.kenya_page)
        with self.assertRaises(Http404):
            require_active_org_object(request, self.uganda_page)

    def test_everything_here_is_scopable(self):
        for model in (Page, PageLogEntry, ModelLogEntry, WorkflowState, TaskState):
            with self.subTest(model=model._meta.label):
                self.assertTrue(is_scopable(model))
