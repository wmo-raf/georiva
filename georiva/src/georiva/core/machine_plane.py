"""The addresses of the services that answer without a Host.

Titiler and Martin read object storage and the database directly. Neither sees a
usable Host header — the browser dials them through nginx on whatever hostname
the portal happens to run on, and Titiler dials Django back on an internal
container name that belongs to no organisation at all. So neither can resolve a
tenant the way ``OrganisationMiddleware`` does, and neither should try:
tenancy logic copied into a second and third service is tenancy logic that
drifts out of step with the first.

The organisation therefore travels *in the address*. Titiler's routes open with
an ``{org}`` segment, Martin's function takes an ``org`` parameter, and both stay
purely conventional — they read the org they were handed and join or concatenate
with it, deciding nothing. Deciding is Django's job, and this module is where it
happens: every machine-plane URL on the instance is built here, from an object
whose organisation was already established by the request that fetched it. Host
and path then agree because the same request produced both.

The grammar mirrors the storage grammar deliberately (``{org}/{catalog}/…``, see
``core.path_resolution``): a Titiler tile URL and the COG key behind it differ
only by prefix, so an operator reading one can find the other.
"""
from urllib.parse import urlencode

TITILER_PREFIX = "/titiler"
MARTIN_PREFIX = "/martin"

#: Martin's function source, mounted under its own tile path.
MARTIN_BOUNDARY_STATS_SOURCE = "boundary_stats"


def org_slug_of(collection) -> str:
    """The slug of the organisation owning ``collection``'s catalog.

    Takes a ``Collection`` and nothing else. An earlier version accepted
    anything with a ``.collection`` attribute, which quietly did the wrong thing
    for an ``Asset`` (whose ``.collection`` route runs through ``.item``) — and
    the wrong thing here is a URL under another institution's prefix. Callers
    holding an item or a variable pass ``obj.collection``; it is one attribute,
    and it says which chain is being walked.

    Deliberately not defensive beyond that: a missing link is a broken row, and
    a blank org segment would address nothing or somebody else.
    """
    return collection.catalog.organisation.slug


def titiler_variable_root(org_slug, catalog_slug, collection_slug, variable_slug) -> str:
    """The Titiler route prefix identifying one variable of one organisation."""
    return f"{TITILER_PREFIX}/{org_slug}/{catalog_slug}/{collection_slug}/{variable_slug}"


def titiler_preview_url(item, variable_slug) -> str:
    """A rendered preview of ``item``'s ``variable_slug`` band.

    The organisation comes from the item rather than from the caller, so a page
    cannot hand Titiler a catalog of one tenant under the org segment of
    another: the two segments are read from the same row.
    """
    collection = item.collection
    params = {"time": item.time_iso}
    if item.reference_time:
        params["reftime"] = item.reference_time_iso
    root = titiler_variable_root(
        org_slug_of(collection), collection.catalog.slug, collection.slug, variable_slug,
    )

    return f"{root}/preview.webp?{urlencode(params)}"


def martin_boundary_stats_url(collection, base) -> str:
    """Martin's zonal-stats tile URL for ``collection``, less the varying params.

    Carries the ``org``/``catalog``/``collection`` triple that pins the tile to
    one organisation's rows; the caller drawing the map appends the parameters
    that change as it is driven (``variable``, ``time``, ``admin_level``,
    ``reference_time``). Those name nothing outside the triple, so a client
    editing them can only ever move within the collection it was given.

    ``base`` is required rather than defaulted to :data:`MARTIN_PREFIX`: the map
    fetches these cross-origin in some deployments, so every real caller builds
    an absolute URL from the request, and a default would only ever be a wrong
    answer that looked like a working one.
    """
    params = urlencode({
        "org": org_slug_of(collection),
        "catalog": collection.catalog.slug,
        "collection": collection.slug,
    })
    return f"{base.rstrip('/')}/{MARTIN_BOUNDARY_STATS_SOURCE}/{{z}}/{{x}}/{{y}}?{params}"
