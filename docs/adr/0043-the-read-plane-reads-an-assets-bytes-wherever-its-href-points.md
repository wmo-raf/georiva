# The read plane reads an asset's bytes wherever its href points

## Status

accepted — see wmo-raf/georiva#605. Read-side counterpart of
[ADR-0042](0042-remote-collections-are-harvested-and-the-mirror-governs-only-what-it-created.md); leaves
[ADR-0013](0013-org-in-the-path-on-the-machine-plane.md) and
[ADR-0015](0015-nginx-auth-request-gateway-for-tiles.md) untouched.

## Context

ADR 0042 puts upstream URLs in `Asset.href`. The field admits them, but every reader treats `href` as a key in the
assets bucket: `Asset.url` prefixes the storage base URL, zonal statistics call `storage.assets.read_bytes(href)` and
hold the whole COG in memory, orphan cleanup compares hrefs against bucket listings, and the delete path removes "the
object the href names". Patching each reader as a feature first trips over it would copy an "is it remote?" test into
half a dozen places and leave the rest wrong.

## Decision

**A remote-backed collection is a read-plane peer: tiles, WMTS, STAC, EDR, timeseries and zonal statistics, all by
range-reading upstream bytes on demand.**

1. **One seam answers "where are this asset's bytes".** A single place says whether an asset is local or remote and
   gives a readable location for it (a bucket key, or the upstream URL). Every reader goes through it: `Asset.url`,
   the STAC serializer, the tile-config handoff to Titiler, timeseries, zonal statistics, virtual Zarr, and the
   delete/cleanup paths. "Still points upstream" in ADR 0042's jurisdiction rule is this seam's definition, not a
   second one.

2. **Nothing downstream distinguishes local from remote beyond that.** A collection may hold both (a mixed
   collection). Addressing, tenancy and visibility are the collection's, unchanged: a remote asset's tiles live at
   the owning organisation's machine-plane address like any other.

3. **Titiler is handed the href and range-reads it.** Tile auth is unchanged: nginx `auth_request` decides, Titiler
   holds no secret and makes no decision. That is why upstreams are anonymous-only: a credentialed upstream would put
   a secret in the tile server.

4. **The STAC API exposes the upstream href as the asset's href**, so a consumer's own cloud-native tools read the
   same bytes directly.

5. **The read path never writes.** An upstream read failure is an error for that request and nothing else: no row is
   marked, deleted or repaired, and a tile request never triggers a statistics computation. Only a harvest changes
   harvested rows.

6. **Null asset statistics are legal for remote assets.** Rendering falls back to the Variable's style bounds
   (ADR 0042 §10); no rendering path may require them.

7. **Readers stream; they do not download.** Anything that reads a whole asset (zonal statistics) opens it by range,
   local or remote, rather than pulling its bytes into memory.

8. **Our cleanup never touches upstream.** Deleting a remote asset's row deletes nothing anywhere, and orphan-object
   cleanup ignores remote hrefs: there is no object of ours to follow the row out (ADR 0032).

9. **No byte cache.** GDAL / VSICURL settings are tuned and first-view latency is measured in deployment; a cache is
   designed from those measurements, not before them.

## Consequences

- First-view tiles over a remote collection are visibly slower than local ones, and an upstream outage is an outage
  of those tiles. Materializing the hot window (ADR 0042 §8) is the operator's answer.
- Zonal statistics over a remote asset cost one streamed read of it per boundary set, once.
- The harvest-time allowlist is the guarantee about where remote reads go. As a second layer, the deployment should
  keep GDAL from following redirects into private address ranges and restrict the tile server's egress; neither is
  relied on.
- Virtual Zarr manifests skip remote assets, so a mixed collection's virtual store covers only its local window.

## Alternatives considered

- **Patch each reader where it first breaks.** Smaller slices, the same test in five places, and the cleanup paths
  — which no feature exercises — left treating a URL as a bucket key.
- **Proxy remote bytes through Django or nginx.** One more hop on every range request for no gain: the upstreams are
  anonymous and the consumer can already reach them.
- **A byte/range cache from day one.** Sized by guesswork, on the small disks this feature exists to spare.
