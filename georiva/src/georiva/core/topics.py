"""Which topics a given set of catalogs puts on offer.

Topics are shared reference data: instance-global, curated once, read by every
organisation (decision #259). *Which* of them a surface should offer is not
shared at all — it follows from the catalogs that surface is showing, and on a
portal those are one organisation's. Both the datasets index and the portal
template tags need that narrowing, and an unnarrowed topic list is a sidebar
advertising subject areas the portal has nothing to show for — and, on a shared
instance, hinting at what the neighbours publish.
"""


def topics_of(catalogs):
    """The topics carried by ``catalogs``, in display order.

    Takes a queryset rather than a request so the caller keeps ownership of the
    scoping: pass the catalogs you are already showing, and the topics match.
    """
    from georiva.core.models import Topic

    return (
        Topic.objects
        .filter(catalogs__in=catalogs)
        .distinct()
        .order_by('sort_order', 'name')
    )
