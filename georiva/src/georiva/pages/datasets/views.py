from django.http import JsonResponse
from django.shortcuts import get_object_or_404

from georiva.core.models import Catalog, Collection, Item, Asset


def collection_available_dates(request, catalog_slug, collection_slug):
    """
    Available dates for the cascading date picker.

    GET /datasets/collections/<catalog>/<collection>/dates/

    Query params:
        level    — 'years' | 'months' | 'days' | 'hours'
        variable — variable slug (required)
        year     — int (required for months / days / hours)
        month    — int (required for days / hours)
        day      — int (required for hours)

    Response:
        {"values": [2023, 2024, 2025]}    years
        {"values": [1, 3, 6, 9, 12]}      months (1-indexed)
        {"values": [1, 5, 10, 15, 20]}    days
        {"values": [0, 6, 12, 18]}         hours (UTC)
    """
    catalog = get_object_or_404(Catalog, slug=catalog_slug, is_active=True)
    collection = get_object_or_404(
        Collection,
        catalog=catalog,
        slug=collection_slug,
        is_active=True,
    )
    
    level = request.GET.get('level', 'years')
    variable_slug = request.GET.get('variable', '').strip()
    
    # Items that have a COG asset for the requested variable
    qs = (
        Item.objects
        .filter(
            collection=collection,
            assets__variable__slug=variable_slug,
            assets__format=Asset.Format.COG,
            assets__variable__is_active=True,
        )
        .distinct()
    )
    
    try:
        if level == 'years':
            values = (
                qs
                .dates('time', 'year')
                .values_list('time__year', flat=True)
                .distinct()
                .order_by('time__year')
            )
            return JsonResponse({'values': list(values)})
        
        if level == 'months':
            year = int(request.GET.get('year', 0))
            values = (
                qs
                .filter(time__year=year)
                .dates('time', 'month')
                .values_list('time__month', flat=True)
                .distinct()
                .order_by('time__month')
            )
            return JsonResponse({'values': list(values)})
        
        if level == 'days':
            year = int(request.GET.get('year', 0))
            month = int(request.GET.get('month', 0))
            values = (
                qs
                .filter(time__year=year, time__month=month)
                .dates('time', 'day')
                .values_list('time__day', flat=True)
                .distinct()
                .order_by('time__day')
            )
            return JsonResponse({'values': list(values)})
        
        if level == 'hours':
            year = int(request.GET.get('year', 0))
            month = int(request.GET.get('month', 0))
            day = int(request.GET.get('day', 0))
            values = (
                qs
                .filter(time__year=year, time__month=month, time__day=day)
                .values_list('time__hour', flat=True)
                .distinct()
                .order_by('time__hour')
            )
            return JsonResponse({'values': list(values)})
    
    except (ValueError, TypeError):
        pass
    
    return JsonResponse({'values': []})
