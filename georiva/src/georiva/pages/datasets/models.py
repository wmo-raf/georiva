from django.core.paginator import Paginator
from django.db import models
from django.shortcuts import get_object_or_404, render
from django.utils.dateparse import parse_date, parse_datetime
from wagtail.admin.paginator import WagtailPaginator
from wagtail.admin.panels import FieldPanel, MultiFieldPanel
from wagtail.contrib.routable_page.models import RoutablePageMixin, path
from wagtail.models import Page

from georiva.core.models import Catalog, Collection, Topic, Item, Asset
from georiva.core.utils import get_full_url_by_request

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
        """All catalogs — filterable by topic and time resolution. Paginated."""

        catalogs = self._base_catalogs_qs()
        catalogs, filters = self._apply_catalog_filters(request, catalogs)

        paginator = Paginator(catalogs, self.collections_per_page)
        page_obj = paginator.get_page(request.GET.get('page'))

        return render(request, 'datasets/index.html', {
            'page': self,
            'catalogs': page_obj,
            'filters': filters,
            **self._catalog_filter_context(),
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
            base_items_qs = self._apply_date_filter(base_items_qs, date_str)
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
            .order_by('time' if collection.is_forecast else '-time')
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
    def collection_item_detail(self, request, catalog_slug, collection_slug, item_id):
        
        catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
        collection = get_object_or_404(
            Collection,
            catalog=catalog,
            slug=collection_slug,
            is_active=True,
        )
        item = get_object_or_404(Item, pk=item_id, collection=collection)
        
        # COG assets for the map — one per variable
        cog_assets = list(
            item.assets
            .filter(format=Asset.Format.COG, variable__is_active=True)
            .select_related('variable', 'variable__unit', 'variable__palette')
            .order_by('variable__sort_order')
        )
        
        # All downloadable assets for the downloads section
        downloadable_assets = (
            item.assets
            .filter(
                format__in=[
                    Asset.Format.COG,
                    Asset.Format.GEOTIFF,
                ],
                variable__is_active=True,
            )
            .select_related('variable')
            .order_by('variable__sort_order', 'format')
        )
        
        # Resolve active variable from ?variable= param
        active_var_slug = request.GET.get('variable', '').strip()
        cog_slugs = [a.variable.slug for a in cog_assets]
        if active_var_slug not in cog_slugs:
            active_var_slug = cog_slugs[0] if cog_slugs else ''
        
        # Previous / next item by time
        prev_item = (
            Item.objects
            .filter(collection=collection, time__lt=item.time)
            .order_by('-time')
            .values('pk')
            .first()
        )
        next_item = (
            Item.objects
            .filter(collection=collection, time__gt=item.time)
            .order_by('time')
            .values('pk')
            .first()
        )
        
        # Map layer config for WeatherLayers — serialised as JSON in the template
        map_layers = [
            {
                'slug': a.variable.slug,
                'name': a.variable.name,
                'units': a.variable.units,
                'url': a.url,
                'palette': a.variable.weather_layers_palette,
                'value_min': a.variable.value_min,
                'value_max': a.variable.value_max,
            }
            for a in cog_assets
        ]
        
        # Group downloadable assets by variable for the template
        downloads_by_variable = {}
        for asset in downloadable_assets:
            slug = asset.variable.slug
            if slug not in downloads_by_variable:
                downloads_by_variable[slug] = {
                    'variable': asset.variable,
                    'assets': [],
                }
            downloads_by_variable[slug]['assets'].append(asset)
        
        return render(request, 'datasets/item_detail.html', {
            'page': self,
            'catalog': catalog,
            'collection': collection,
            'item': item,
            'active_var_slug': active_var_slug,
            'cog_assets': cog_assets,
            'map_layers': map_layers,
            'downloads_by_variable': downloads_by_variable,
            'prev_item': prev_item,
            'next_item': next_item,
            'collection_url': f"{self.url}{catalog.slug}/{collection.slug}/",
            'catalog_slug': catalog.slug,
            'collection_slug': collection.slug,
            'boundary_stats_levels': collection.boundary_stats_levels or [],
            "martin_base_url": get_full_url_by_request(request, '/martin'),
        })
    
    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    
    def _apply_date_filter(self, qs, date_str: str):
        """
        Apply a date filter to an Item queryset, interpreting date_str
        according to the collection's picker type.

        picker_type 'date'   → date_str is YYYY-MM-DD → filter by exact date
        picker_type 'month'  → date_str is YYYY-MM    → filter by year + month
        picker_type 'number' → date_str is YYYY       → filter by year
        """
        try:
            parts = date_str.split('T')[0].split('-')  # strip time component first
            if len(parts) == 1:
                # Year only — e.g. "2025"
                return qs.filter(time__year=int(parts[0]))
            if len(parts) == 2:
                # Year + month — e.g. "2025-03"
                return qs.filter(time__year=int(parts[0]), time__month=int(parts[1]))
            # Full date or datetime — e.g. "2025-03-15" or "2025-03-15T06:00:00Z"
            parsed = parse_date(parts[0] + '-' + parts[1] + '-' + parts[2])
            if parsed:
                if 'T' in date_str:
                    # Has hour component
                    hour = int(date_str.split('T')[1].split(':')[0])
                    return qs.filter(time__date=parsed, time__hour=hour)
                return qs.filter(time__date=parsed)
        except (ValueError, AttributeError, IndexError):
            pass
        return qs
    
    def _base_catalogs_qs(self):
        from django.db.models import Count, Max, Q
        return (
            Catalog.objects
            .filter(is_active=True)
            .prefetch_related('topics')
            .annotate(
                collection_count=Count('collections', filter=Q(collections__is_active=True)),
                latest_updated=Max('collections__time_end'),
            )
            .order_by('name')
        )

    def _apply_catalog_filters(self, request, qs):
        from django.db.models import Q
        filters = {
            'topic': request.GET.get('topic', ''),
            'resolution': request.GET.get('resolution', ''),
            'q': request.GET.get('q', ''),
        }

        if filters['q']:
            qs = qs.filter(
                Q(name__icontains=filters['q']) |
                Q(description__icontains=filters['q'])
            )

        if filters['topic']:
            qs = qs.filter(topics__slug=filters['topic'])

        if filters['resolution']:
            qs = qs.filter(
                collections__time_resolution=filters['resolution'],
                collections__is_active=True,
            ).distinct()

        return qs, filters

    def _catalog_filter_context(self):
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
            'time_resolutions': [
                (value, choices[value])
                for value in Collection.TimeResolution.values
                if value in active_resolutions
            ],
        }

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
