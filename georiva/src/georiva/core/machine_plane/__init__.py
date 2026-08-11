"""Where the tenant travels in the address, because there is no Host to read.

Ordinary requests resolve their organisation from the hostname, and
``OrganisationMiddleware`` does it once for everybody. Titiler and Martin
cannot: they read object storage and the database directly, the browser dials
them through nginx on whatever hostname the portal happens to run on, and
Titiler dials Django back on an internal container name that belongs to no
organisation at all. Copying tenancy logic into a second and third service
would only give it two more places to drift.

So on this plane the organisation travels *in the address* —
``/titiler/{org}/{catalog}/…``, Martin's required ``org`` parameter, the
``georiva:palette:{org}:…`` cache key. This is the only place in GeoRiva where
a path names the tenant, and it is justified by the one thing that makes it
necessary: there is no Host for it to disagree with. Django decides which
organisation and writes every such address; Titiler and Martin only concatenate
and join. See ADR 0013, ADR 0015, and the *Machine plane* entry in CONTEXT.md.

The package:

``addresses``      builds every machine-plane address, and reads one back.
                   ``scope_of`` is the exact inverse of the builders beside it,
                   and lives with them for that reason: an inverse that drifts
                   from its function is a gate that authorises one collection
                   while the tile server reads another
``auth_view``      the tile gateway — the nginx ``auth_request`` endpoint that
                   decides whether a machine-plane request may be proxied at all
``config_view``    Titiler's callback for a variable's render config; the
                   documented ``public``-only exception, since Titiler forwards
                   no credential and there is nobody to ask
``palette_cache``  the one machine-plane address that is not a URL: the Redis
                   keys Titiler reads while colouring tiles

The grammar deliberately mirrors ``core.storage``'s key grammar, so a tile URL
and the COG behind it differ only by prefix.

The re-exports below are the surface the old top-level ``core/machine_plane.py``
offered, kept so its callers did not have to move with it. The other three
modules are imported by their own paths.
"""

from .addresses import (  # noqa: F401
    MARTIN_BOUNDARY_STATS_SOURCE,
    MARTIN_PREFIX,
    TITILER_PREFIX,
    WMTS_REFTIME_DIMENSION,
    WMTS_TIME_DIMENSION,
    MachineScope,
    martin_boundary_stats_url,
    org_slug_of,
    render_config_token,
    scope_of,
    style_version_token,
    titiler_encoded_preview_url,
    titiler_preview_url,
    titiler_variable_root,
    wmts_capabilities_url,
    wmts_kvp_endpoint,
    wmts_layer_identifier,
    wmts_rest_tile_template,
)
