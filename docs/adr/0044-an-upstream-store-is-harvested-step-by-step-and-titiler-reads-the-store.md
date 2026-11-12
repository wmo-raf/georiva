# An upstream store is harvested step by step, and Titiler reads the store

## Status

accepted — see wmo-raf/georiva#605 (its second wave). Extends
[ADR-0042](0042-remote-collections-are-harvested-and-the-mirror-governs-only-what-it-created.md) and
[ADR-0043](0043-the-read-plane-reads-an-assets-bytes-wherever-its-href-points.md), which left "native Zarr upstreams,
with the Zarr serving path" out of scope. Uses [ADR-0026](0026-a-model-run-is-a-first-class-ingestion-record.md)'s closed-run rule.
Revises ADR 0043 §9 for stores only.

Amended after a second real store was read (ECMWF AIFS Single, virtual): §11 now says which kinds of place a store's
data may be pinned in, and what happens when an operator confirms some and not others. Nothing is reversed.

## Context

Some Upstreams publish no items at all. dynamical.org's catalog is static JSON with 43 collections, each one a single
Icechunk repository holding a cube (`init_time × lead_time × latitude × longitude`), described by the datacube
extension. The Upstream survey refuses these today with the verdict "store".

They come in two kinds, told apart by chunk shape:

- **Map-shaped**: a chunk is one whole field at one step. In this catalog every such store is *virtual*: its chunks
  are references to GRIB messages in the producer's own bucket, decoded by a codec the publisher names in the store
  (`gribberish`).
- **Series-shaped**: a chunk is a small square of ground over many steps, rewritten into standard Zarr. A point's
  whole forecast is two chunks; one map of Africa is tens of megabytes to gigabytes.

What was measured against the GFS stores, and shaped the decisions below:

- A map-shaped field reads in 0.4–2.7 s and decodes to 8 MB. There are no overviews and no partial read.
- Opening the store cold costs several seconds, plus more for the first array.
- The store commits several times a minute while a run arrives. Coordinates are allocated ahead of the data, steps
  land out of order, and a step with no data yet reads back as a field of NaN without an error.
- Whether a step holds data can be asked of the store directly, without decoding anything.
- An Icechunk repository carries its own configuration for where virtual chunks live, and that configuration can
  name any endpoint.

Pulling from a store into local COGs is easy and is not this decision. #605 is about pointing.

## Decision

**A Harvest feed may point at an upstream store. It writes rows for the steps the store holds, every asset's href is
the store, and Titiler draws maps by reading the store itself.**

1. **Same feed, same wizard, a second accepted shape.** A store is not a new feed species. The Upstream survey gains
   a second verdict it proceeds on, and the feed records which shape it was created for; like the Upstream, that is
   fixed from then on. The harvest keeps its window, Mirror, Jurisdiction and run tracking and swaps only how it
   enumerates what exists upstream. The Upstream catalog client stays the one seam: it learns to read a catalog with
   no API, to describe a store's arrays and grid, and to say which steps hold data in a window.

2. **A store becomes rows per step.** One `Item` per (reference time, valid time) inside the window and one `Asset`
   per mapped Variable, so STAC, EDR, `/api/runs/`, WMTS capabilities and tile-config need no second path that asks a
   store what times exist.

3. **The href is the store; the Item's own coordinates are the slice.** Every asset of the collection carries the
   same href — the store's `https` address as the Upstream publishes it — with `format=ZARR`. Which slice it means is
   the Item's reference time and valid time plus the array's path in the store, kept on the asset beside its
   Provenance so it outlives the feed. The STAC API exposes the store href unchanged and names the array, so a
   consumer's own tools open the same slice.

4. **One store, one collection.** A time-optimised and a map-optimised store of the same model are two collections.
   They are not the same data twice: they disagree on which variables exist and, for reprocessed fields, on values.

5. **A store's shape decides which read planes it offers.** The survey reads the shape from the published chunking.
   Only map-shaped stores are accepted now, and they offer maps. Series-shaped stores stay refused, with the reason;
   accepting them means a Published collection with no map, which is deferred with them.

6. **A row is written only for a step the store holds.** Each harvest asks, per mapped Variable, which steps of the
   runs in its window hold data, and writes rows for those alone. A run is closed (ADR 0026) when every step the
   store expects of it is held for every mapped Variable; a harvest of an open run adds the steps that have arrived
   since. Closed runs are not asked again.

7. **A forecast store's window rolls, on run time.** The feed keeps runs initialised in the last *N* days (default
   3). The Mirror is authoritative within that window as before, so rows of a run that has aged out are removed by
   the same rule that removes rows gone upstream. Analysis stores, which have no runs, keep the fixed window.

8. **Only plain fields on a latitude/longitude grid can be mapped.** An array is offered when its dimensions are
   exactly run, step, latitude and longitude — or time, latitude and longitude. Arrays with levels, ensemble members
   or statistics, and projected grids, are listed by the wizard as not yet supported, each with its reason. The
   Variable mapping's key is the array's full path in the store, so pinning a level later adds to it rather than
   changing it.

9. **Titiler opens the store.** The tile server gains a store reader beside its COG reader, and the decoder the store
   names. Django publishes what it needs — store address, array path, run and step, and the feed's Pinned
   locations — the way it publishes a Remote asset's href. Values and units are whatever the store's own decode
   chain produces; nothing is de-accumulated or converted on the way.

10. **Titiler follows the store's branch and refreshes on a miss.** It keeps the store open. When a requested step is
    empty in the snapshot it holds, it reopens the branch once and looks again; if the step is still empty the tile
    is an error. A blank tile is never served for a step that has a row. Rows, not the store, decide what is offered.

11. **A store's reader follows only what an operator pinned.** The store's own bucket and each bucket its chunks live
    in are proposed by the survey, from the collection's declaration, and confirmed as Pinned locations. On opening,
    the reader discards the container configuration inside the repository and builds its own from the pins:
    anonymous, the public endpoint, those prefixes only. A chunk pointing anywhere else fails to read.

    **The kinds of place are a closed list: an `s3://` bucket or a `gs://` bucket.** Each is read anonymously at its
    provider's public endpoint and no further than the bucket. A store declaring any other kind is refused by the
    survey, the kind named; a kind is added when its public endpoint is known and anonymous reading has been seen
    to work. A chunk location is handed to the store's reader as a prefix and is never translated to an `https`
    host, unlike a COG's Pinned location (ADR 0042 §9): the reader refuses a chunk outside its prefixes by itself.

    **Each declared place is confirmed separately, and the sample read of §12 is the proof.** It runs with the
    confirmed places only. A chosen field that needs a declined place stops the wizard, naming the place; the
    operator confirms it or drops the field. A declined place no sample read needed is allowed, and the review says
    that data kept there will not draw.

12. **The survey proves the store decodes.** A store naming a codec we cannot decode is refused, the codec named.
    Otherwise the wizard reads one field of the newest closed run per chosen array, to estimate the Variable's value
    range and to prove the store is readable without credentials. Core therefore carries the decoder too.

13. **Titiler keeps a small cache of decoded fields per worker**, bounded by a setting, and concurrent tiles for the
    same field wait on one read. This revises ADR 0043 §9 for stores only: a COG tile range-reads the bytes it
    needs, while every tile of a store's map needs the same whole field.

## Consequences

- The Mirror now deletes rows because time passed, not only because the Upstream changed.
- An open run has holes in its timeline, because steps land out of order. `latest_complete` is what a consumer uses
  to avoid them.
- A harvest writes no `FileIngestion`, so the run record cannot open from one completing. The harvest opens and
  closes it itself, and "the store holds every expected step" is a new closing signal beside the existing ones.
- How to open a store — which pins, the container override — lives in two codebases with separate locks, core and
  the tile server. Tests on each side keep them in agreement.
- The first tile after new data arrives pays a reopen, in each worker separately. An animation across a run reads
  every field of it once. Both are what pointing costs.
- The cache is memory: its size times 8 MB or so, times the tile server's workers, on machines that are often small.
- A store declares where its data *may* live, not where each run's does, and finding out means walking every
  reference. So a run older or newer than the sampled one can keep its data in a place the operator declined. Its
  tiles are errors, never blanks, and say to re-check the Upstream.
- When an Upstream publishes a store under a new address (a version bump), tiles keep reading the old repository, or
  fail, until an operator re-checks the Upstream. When it moves its source files to another bucket, tiles fail until
  the same re-check confirms the new one.
- A third-party decoder runs inside the tile server. The publisher of a store chooses it, not us; another Upstream
  may name another.
- Precipitation and other accumulated fields from a virtual store are the model's raw accumulations, where the store
  has them at all.

## Out of scope

- Series-shaped stores, and so time series read from a store and a Published collection with no map.
- Arrays with a level, then ensemble members and projected grids.
- Time series and zonal statistics over store assets: a time series request says so, and zonal backfill skips them.
- A cache shared across tile server workers, until it has been measured.
- Linking a time-optimised and a map-optimised store into one collection.
- Credentialed stores, as for any Upstream.

## Alternatives considered

- **Pull from the store into local COGs.** Everything downstream works unchanged and no new reader is needed. It is
  the Materialize path, and it copies pixels, which is what #605 exists to avoid.
- **One row for the store, no Items.** Every read plane would need a second code path that asks the store what
  exists, at request time.
- **An href that encodes the slice.** A grammar of our own that no outside tool understands, and a second statement
  of what the Item's coordinates already say.
- **Resolve each chunk to its GRIB byte range and read that with GDAL.** No new dependency in the tile server, but it
  works only for virtual stores, ties rows to references the Upstream may rewrite, and has to reproduce the store's
  decode chain (orientation, longitude range, unit) by hand.
- **A row for every coordinate.** The newest run would show blank maps for every step not yet arrived.
- **Rows only for whole runs.** A run would be invisible for the hours it takes to arrive.
- **Pin the tile server to the snapshot the harvest read.** Rows and tiles would agree by construction, but a store
  that commits several times a minute may expire snapshots, and a pinned one could vanish between harvests.
- **Trust the repository's own container configuration.** The allowlist would guard the address of the store while
  the store's configuration chose where its reader goes next.
- **Pin `s3://` buckets only.** ECMWF AIFS Single's virtual store declares an S3 and a Google bucket and, in every
  field sampled, keeps its data in the Google one; it reads anonymously with what is already installed.
- **One question for all of a store's declared places, or pinning silently what the sample read used.** The first
  pins a place on the store's say-so; the second pins one nobody agreed to.
- **Offer every read plane on every shape.** One map of a series-shaped archive can be gigabytes through the tile
  server.
