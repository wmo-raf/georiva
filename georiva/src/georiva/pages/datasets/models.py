from django.core.paginator import Paginator
from django.db import models
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, render
from django.utils.dateparse import parse_date, parse_datetime
from wagtail.admin.paginator import WagtailPaginator
from wagtail.admin.panels import FieldPanel, MultiFieldPanel
from wagtail.contrib.routable_page.models import RoutablePageMixin, path
from wagtail.models import Page

from georiva.core.models import Catalog, Collection, Topic, Item, Asset

ITEMS_PER_PAGE = 24


class DatasetsIndexPage(RoutablePageMixin, Page):
    """
    Landing page for browsing datasets.

    Routes:
        /datasets/                              → all collections, filterable + paginated
        /datasets/<catalog_slug>/               → catalog detail + its collections
        /datasets/<catalog_slug>/<collection_slug>/  → collection detail
        /datasets/<catalog_slug>/<collection_slug>/items/<item_id>/  → item detail
    """

    # Operator-configurable fields
    intro_text = models.TextField(
        blank=True,
        verbose_name="Introduction",
        help_text="Optional intro shown above the dataset listing."
    )
    collections_per_page = models.PositiveIntegerField(
        default=20,
        help_text="Number of collections to show per page."
    )

    max_count = 1
    parent_page_types = ['home.HomePage']
    subpage_types = []

    content_panels = Page.content_panels + [
        MultiFieldPanel([
            FieldPanel('intro_text'),
            FieldPanel('collections_per_page'),
        ], heading="Configuration"),
    ]

    class Meta:
        verbose_name = "Datasets Index Page"

    # -------------------------------------------------------------------------
    # Routes
    # -------------------------------------------------------------------------

    @path('')
    def index(self, request):
        """All collections — filterable by topic, resolution, catalog. Paginated."""

        collections = self._base_collections_qs()
        collections, filters = self._apply_filters(request, collections)

        paginator = Paginator(collections, self.collections_per_page)
        page = paginator.get_page(request.GET.get('page'))

        return render(request, 'datasets/index.html', {
            'page': self,
            'collections': page,
            'paginator': paginator,
            'filters': filters,
            **self._filter_context(),
        })

    @path('<slug:catalog_slug>/')
    def catalog_detail(self, request, catalog_slug):
        """Catalog landing page — shows catalog metadata + all its collections."""

        catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
        collections = (
            self._base_collections_qs()
            .filter(catalog=catalog)
        )

        return render(request, 'datasets/catalog_detail.html', {
            'page': self,
            'catalog': catalog,
            'collections': collections,
        })

    @path('<slug:catalog_slug>/<slug:collection_slug>/')
    def collection_detail(self, request, catalog_slug, collection_slug):

        catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
        collection = get_object_or_404(
            Collection,
            catalog=catalog,
            slug=collection_slug,
            is_active=True,
        )

        variables = list(
            collection.variables.filter(is_active=True).order_by('sort_order')
        )

        # --- Filters ---
        active_var_slug = request.GET.get('variable', '').strip()
        date_str = request.GET.get('date', '').strip()
        run_str = request.GET.get('run', '').strip()

        # Validate active variable — must belong to this collection
        if active_var_slug not in {v.slug for v in variables}:
            active_var_slug = variables[0].slug if variables else ''

        # --- Variable coverage ---
        # Check which variables have at least one COG asset in the current
        # filtered item set. Used to disable variables with no data and to
        # fall back the active variable if it has no coverage.
        base_items_qs = Item.objects.filter(collection=collection)
        if date_str:
            parsed_date = parse_date(date_str)
            if parsed_date:
                base_items_qs = base_items_qs.filter(time__date=parsed_date)
        if run_str:
            parsed_run = parse_datetime(run_str)
            if parsed_run:
                base_items_qs = base_items_qs.filter(reference_time=parsed_run)

        coverage_qs = (
            Asset.objects
            .filter(
                item__in=base_items_qs,
                format=Asset.Format.COG,
                variable__is_active=True,
            )
            .values_list('variable__slug', flat=True)
            .distinct()
        )
        coverage = set(coverage_qs)

        # Annotate variables with COG coverage flag
        for variable in variables:
            variable.has_coverage = variable.slug in coverage

        # Fall back active variable to first one with COG coverage
        if not active_var_slug or active_var_slug not in coverage:
            fallback = next((v for v in variables if v.has_coverage), None)
            active_var_slug = fallback.slug if fallback else active_var_slug

        # --- Item queryset ---
        # Only items that have a COG asset for the active variable.
        items_qs = (
            base_items_qs
            .filter(
                assets__variable__slug=active_var_slug,
                assets__format=Asset.Format.COG,
                assets__variable__is_active=True,
            )
            .distinct()
            .order_by('-time')
        )

        page_number = request.GET.get('p', 1)
        paginator = WagtailPaginator(items_qs, ITEMS_PER_PAGE)
        items_page = paginator.get_page(page_number)
        page_range = paginator.get_elided_page_range(items_page.number)

        # --- Forecast runs ---
        forecast_runs = []
        if collection.is_forecast:
            forecast_runs = (
                Item.objects
                .filter(collection=collection, reference_time__isnull=False)
                .values_list('reference_time', flat=True)
                .distinct()
                .order_by('-reference_time')[:50]
            )

        filters = {
            'variable': active_var_slug,
            'date': date_str,
            'run': run_str,
        }

        return render(request, 'datasets/collection_detail.html', {
            'page': self,
            'catalog': catalog,
            'collection': collection,
            'variables': variables,
            'active_var_slug': active_var_slug,
            'items': items_page,
            'page_range': page_range,
            'filters': filters,
            'forecast_runs': forecast_runs,
            'catalog_slug': catalog.slug,
            'collection_slug': collection.slug,
        })

    @path('<slug:catalog_slug>/<slug:collection_slug>/items/<int:item_id>/')
    def item_detail(self, request, catalog_slug, collection_slug, item_id):

        catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
        collection = get_object_or_404(
            Collection,
            catalog=catalog,
            slug=collection_slug,
            is_active=True,
        )
        item = get_object_or_404(Item, pk=item_id, collection=collection)

        return render(request, 'datasets/item_detail.html', {
            'page': self,
            'catalog': catalog,
            'collection': collection,
            'item': item,
        })

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _base_collections_qs(self):
        return (
            Collection.objects
            .filter(is_active=True)
            .select_related('catalog')
            .prefetch_related('catalog__topics', 'variables')
            .order_by('catalog__name', 'sort_order', 'name')
        )

    def _apply_filters(self, request, qs):
        """
        Apply GET param filters to a collections queryset.
        Returns (filtered_qs, active_filters_dict).
        """
        filters = {
            'topic': request.GET.get('topic', ''),
            'resolution': request.GET.get('resolution', ''),
            'catalog': request.GET.get('catalog', ''),
            'q': request.GET.get('q', ''),
        }

        if filters['q']:
            from django.db.models import Q
            qs = qs.filter(
                Q(name__icontains=filters['q']) |
                Q(description__icontains=filters['q']) |
                Q(catalog__name__icontains=filters['q'])
            )

        if filters['topic']:
            qs = qs.filter(catalog__topics__slug=filters['topic'])

        if filters['resolution']:
            qs = qs.filter(time_resolution=filters['resolution'])

        if filters['catalog']:
            qs = qs.filter(catalog__slug=filters['catalog'])

        return qs, filters

    def _filter_context(self):
        """Context data needed to render the sidebar filters."""
        active_resolutions = (
            Collection.objects
            .filter(is_active=True)
            .exclude(time_resolution='')
            .values_list('time_resolution', flat=True)
            .distinct()
        )
        choices = dict(Collection.TimeResolution.choices)

        return {
            'topics': Topic.objects.filter(catalogs__is_active=True).distinct().order_by('sort_order', 'name'),
            'catalogs': Catalog.objects.filter(is_active=True).order_by('name'),
            'time_resolutions': [
                (value, choices[value])
                for value in Collection.TimeResolution.values
                if value in active_resolutions
            ],
        }
