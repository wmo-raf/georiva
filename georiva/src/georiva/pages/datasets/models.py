from django.core.paginator import Paginator
from django.db import models
from wagtail.admin.panels import FieldPanel, MultiFieldPanel
from wagtail.contrib.routable_page.models import RoutablePageMixin, path
from wagtail.models import Page

from georiva.core.models import Catalog, Collection, Topic


class DatasetsIndexPage(RoutablePageMixin, Page):
    """
    Landing page for browsing datasets.

    Routes:
        /datasets/                              → all collections, filterable + paginated
        /datasets/<catalog_slug>/               → catalog detail + its collections
        /datasets/<catalog_slug>/<collection_slug>/  → collection detail
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
    
    max_count = 1  # only one instance allowed in the page tree
    parent_page_types = ['home.HomePage']
    subpage_types = []  # no children — all sub-URLs handled by routes
    
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
        from django.shortcuts import render
        
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
        from django.shortcuts import get_object_or_404, render
        
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
        from django.shortcuts import get_object_or_404, render

        catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
        collection = get_object_or_404(
            Collection,
            catalog=catalog,
            slug=collection_slug,
            is_active=True,
        )

        return render(request, 'datasets/collection_detail.html', {
            'page': self,
            'catalog': catalog,
            'collection': collection,
            'variables': collection.variables.filter(is_active=True).order_by('sort_order'),
        })

    @path('<slug:catalog_slug>/<slug:collection_slug>/items/<str:item_id>/')
    def item_detail(self, request, catalog_slug, collection_slug, item_id):
        from django.shortcuts import get_object_or_404, render

        catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
        collection = get_object_or_404(
            Collection,
            catalog=catalog,
            slug=collection_slug,
            is_active=True,
        )

        return render(request, 'datasets/item_detail.html', {
            'page': self,
            'catalog': catalog,
            'collection': collection,
            'item_id': item_id,
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
