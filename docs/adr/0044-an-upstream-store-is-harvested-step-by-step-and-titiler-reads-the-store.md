# An upstream store is harvested step by step, and Titiler reads the store

## Status

accepted — see wmo-raf/georiva#605 (its second wave). Extends
[ADR-0042](0042-remote-collections-are-harvested-and-the-mirror-governs-only-what-it-created.md) and
[ADR-0043](0043-the-read-plane-reads-an-assets-bytes-wherever-its-href-points.md), which left "native Zarr upstreams,
with the Zarr serving path" out of scope. Uses [ADR-0026](0026-a-model-run-is-a-first-class-ingestion-record.md)'s closed-run rule.
Revises ADR 0043 §9 for stores only.

Amended after a second real store was read (ECMWF AIFS Single, virtual): §11 now says which kinds of place a store's
data may be pinned in, and what happens when an operator confirms some and not others. Nothing is reversed.

Amended when stores of ensembles and statistics were surveyed (dynamical.org's ECMWF IFS ENS and Google WeatherNext 3,
wmo-raf/georiva#830): §8 now offers an array with extra axes, read at one value of each — its Selection — where the
collection lists their values and the store keeps one map per value. Arrays with levels whose values are not listed
stay out, refused by name. Nothing is reversed.

Amended when the first store keeping its data on a web host was surveyed (dynamical.org's Google WeatherNext,
wmo-raf/georiva#828): §11's closed list of kinds of place gains a folder of an `https` host, judged as a COG's file
host is and checked for redirects, which its reader would follow. Nothing is reversed.

Amended when the first time-invariant field was mapped (wmo-raf/georiva#782): §6 now expects a Fixed field, one
the operator declares the store holds at a run's first step only, at that step alone. Nothing is reversed.

Amended after the field trial against dynamical.org's GFS store (wmo-raf/georiva#665): §6 now counts a run's expected
steps per array, because a field defined over the interval before each step is never held at a run's first step;
the measured costs
of opening a store are recorded under Context and Consequences; a cache shared across tile server workers is
decided against, with the numbers, under Out of scope. That last one settles what was left open; nothing else is
reversed.

Amended when the open was warmed in advance (wmo-raf/georiva#684): each tile server worker now opens the stores of
published Store assets as it starts, in the background, so the cost recorded under Consequences falls on the worker
rather than a user's first map. Nothing is reversed.

Amended by [ADR-0052](0052-stac-says-what-exists-a-store-says-how-it-is-laid-out.md): §5's shape is now read from
the store's own metadata, with the collection's published chunking as a fallback, so §12's opening of the store moves
ahead of the shape. Series-shaped stores stay refused.

Superseded in part by [ADR-0046](0046-values-are-stored-as-produced-and-converted-when-read.md): §9's "nothing is
converted on the way" no longer holds for units. A store's values are still read as its decode chain produces them,
in the Variable's source unit, and converted when they leave; still nothing is de-accumulated.

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

Measured in the field trial (#665), from a developer machine rather than an African deployment, against GFS:

- Opening the repository takes 3.5 s. The first array opened on a snapshot then takes 16–26 s, whichever array it
  is; every later array on that snapshot opens at once. The cost is per snapshot, not per array, and a fresh
  repository object pays it again. GFS's history is 84,000 snapshots; while a run arrives it commits every 14 s at the median.
- Snapshots 30 days old still read. The history runs from the repository's first commit straight to one eight weeks
  before the trial, which suggests older snapshots are expired; §10 does not depend on it.
- 50 of GFS's 255 plain arrays — averages, accumulations, fluxes, extremes — are never held at a run's first step.
- Every one of the 255 decodes with the codec the store names; read and decoded directly, a field takes 0.8 s at the
  median, 2.7 s at worst.
  In a spot-check of six fields at three points, values match the producer's own GRIB, decoded independently, to the
  last digit.

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
   runs in its window hold data, and writes rows for those alone. A run is closed (ADR 0026) when every mapped
   Variable holds every step its array is published at; a harvest of an open run adds the steps that have arrived
   since. Closed runs are not asked again.

   **Expected steps are counted per array, not per store.** A store's step coordinate is the whole forecast, but a
   field defined over the interval before each step — an average, an accumulation, a flux, an extreme — has no
   value at a run's first step, so its array never holds that step. Counting the store's steps for every array, a
   run with such a field mapped would never close and `latest_complete` would never move. The store declares no
   per-array list of steps, so an array is expected at the run's expected steps **from its own first Held step
   on** (wmo-raf/georiva#681). That needs nothing more of the store than the listing already reads, works on a
   store's first run, and holds for any field that starts late, whatever kind it is.

   *Amended by [ADR-0051](0051-a-variable-declares-its-time-statistic-a-non-instant-field-has-no-step-0.md):*
   which steps a Variable is expected at is now its own declaration, not the steps the store holds. An array is
   published at the run's expected steps, and a Variable whose time statistic is not `instant` has no lead-0 step.
   The AIFS store holds its running totals at lead 0, as a field of zeros, which the inference above read as a step.

   **A Fixed field is expected at its first Held step only** (wmo-raf/georiva#782). A time-invariant field —
   orography, a land–sea mask — sits on the run's whole step axis like every other array, but the store writes its
   first step alone. Counted from its first Held step on, it is expected at all of them, and the run never closes.
   The store says so nowhere a harvest can read: `expected_forecast_length` is the run's, not the array's, and the
   only sign is a free-text comment. So the operator declares it, on the Variable mapping row (`fixed_field`), and
   the Harvest Feed Wizard proposes it when the run it samples holds the array at its first step only while another
   array of that run holds more. Not inferred at harvest time: an array that is late would read as one that ends at
   its first step, and its run would close before it arrived. A Fixed field held at later steps too gets a row at
   each, and is still expected at its first only.

   **Which steps are held, not how many** (wmo-raf/georiva#672). A run is complete when each mapped Variable has a
   row at every one of its array's expected lead times. A step the store holds past the run's
   `expected_forecast_length` still gets a row, but is not expected, so it cannot make up for a missing one — nor can
   rows left from a longer expected length the store has since cut. Each mapped Variable's expected lead times are
   kept on the run record, so a closed run is judged again, and settled, without asking the store; a record that
   kept only a count says not which steps, and its run is asked again. Its limits, accepted:

   - a run whose first step arrives after its later ones looks complete early. Stores write a run's steps in lead
     order;
   - an array's first step gone upstream is not noticed by a re-check, which counts from the first step still held;
   - an array a store holds at no step of a run keeps that run open, as before: there is no first step to count from.
     So does an array held only past the run's expected length: it holds none of the steps the run is expected at.

7. **A forecast store's window rolls, on run time.** The feed keeps runs initialised in the last *N* days (default
   3). The Mirror is authoritative within that window as before, so rows of a run that has aged out are removed by
   the same rule that removes rows gone upstream. Analysis stores, which have no runs, keep the fixed window.

   Removal is not atomic per run, and that is accepted. The Mirror deletes an aged-out run's assets in batches,
   each its own transaction, then the Items left empty, then forgets the run; a reader during that harvest sees an
   old run still recorded as closed, with all its Items but no assets on its earliest steps. Nothing that publishes
   reads an aged-out run — a run is published when it closes, and the latest complete run is never the one ageing
   out — so the cost is a few minutes of a half-emptied old run in run listings, STAC and tiles. Removing a run
   whole (forget it first, then delete its Items run by run, the assets going with them) is the fix if something
   ever reads old runs while a harvest is under way.

8. **Only plain fields on a latitude/longitude grid can be mapped.** An array is offered when its dimensions are
   exactly run, step, latitude and longitude — or time, latitude and longitude. Arrays with levels, ensemble members
   or statistics, and projected grids, are listed by the wizard as not yet supported, each with its reason. The
   Variable mapping's key is the array's full path in the store, so pinning a level later adds to it rather than
   changing it.

   *Amended by wmo-raf/georiva#830:* **an array with extra axes is mapped at a Selection.** A Map-shaped store can
   hold one map per step for each ensemble member, statistic or level. Where it keeps one chunk per value of each
   such axis, the map at one value of each is still one chunk, and Streamable for maps (ADR 0052 §7). So the survey
   offers such an array with each extra axis and its values, and the operator picks, per Variable, one value of each:
   its **Selection**, kept on the mapping row beside the array's path. One array may be mapped at several Selections,
   each its own Variable. Every reader — the harvest, the sample read of §12, core's value reader and Titiler —
   reads the array at the selected value of each axis; a step is Held for a Variable only where its own map is.

   - **The value is kept, not its index**, and each reader finds it among the store's own coordinate when it opens
     the store. An Upstream inserting a member or level does not shift a Variable onto another's map; the
     coordinate costs one small read per opening. A value that is gone is a misdescribed store: a harvest stops
     naming it and a tile is an error, never a blank map.
   - **An axis is offered only where its values can be listed**: the collection's `cube:dimensions` gives them
     (`values`), or an extent of whole numbers it divides evenly (`step`, or `size`). Any other is refused as before,
     by what it is. dynamical.org's `pressure_level` (10 to 1000 over 14 uneven levels) is such a one, until the
     survey reads a store's coordinates, which it does not (ADR 0052 §1: metadata only). An axis the store keeps more
     than one value of in a chunk is refused too: reading one value would read every one.
   - **A Vector Variable's u and v share one Selection** (ADR 0048): its sources name each part by its array's path
     with the Selection after it, and one over parts at two Selections is refused, and not indexed.
   - **The default name says the Selection**: "2 metre temperature (statistic mean)", "… (ensemble member 0)". The
     store does not say which member is a control, or what a statistic is called in words; the operator renames.
   - **A Fixed field is not special-cased.** A time-invariant field the store keeps for one member only is Held for
     that member's Selection alone.

   Out of this amendment: reducing an axis (an ensemble mean, spread or probabilities computed from members), which
   reads every member's chunk and is a pull (ADR 0052 §8); and letting a map's user switch the Selection at view
   time — one Variable per Selection instead.

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

    **The kinds of place are a closed list: an `s3://` bucket, a `gs://` bucket, or a folder of an `https` host.**
    A bucket is read anonymously at its provider's public endpoint and no further than the bucket; a web place at
    its own host, anonymously, no further than its folder. A store declaring any other kind is refused by the
    survey, the kind named; a kind is added when its public endpoint is known and anonymous reading has been seen
    to work. A chunk location is handed to the store's reader as a prefix and is never translated to an `https`
    host, unlike a COG's Pinned location (ADR 0042 §9): the reader refuses a chunk outside its prefixes by itself.

    **A web place is judged as a COG's file host is, and more** (#828). The reader normalises a chunk's address
    before matching it against the prefixes, so a web place is taken only spelled as it will be matched: `https`,
    a lower-case host name (never an address, never a port or credentials), folders with no dot segment, escape,
    query or fragment, and a closing slash. `https://host/chunks/` then matches nothing under `https://host/other/`,
    however a chunk's address is spelled. Its host must resolve to public addresses only — when the sample read of
    §12 proves it, and again at the start of every harvest, as the store's own address is (#749). Every request
    is bounded as the store's own are; no limit on size is set, since a chunk's length is the store's reference.

    **The store's reader follows redirects, so we ask first.** Icechunk's HTTP reader follows a redirect to any
    host, and nothing turns that off. So a file of a web place is asked for (one byte) by our own reader, which
    connects only at an address it judged and follows no redirect: by the sample read, the file each chosen field
    is read from; and by every harvest, one file of each pinned web place, found among the newest step of each run
    in the window — settled runs too, which a harvest does not list again but the tile server still reads. A web
    place no run in the window reads from has nothing to serve, and is not asked about. The file is found by a
    read that follows the store's data nowhere, whose refusal names it, so nothing is fetched to find it. A host that answers
    with a redirect, does not have the file or does not answer is refused, naming why: the sample fails the field;
    a harvest stops (a re-check's harvest too). Between those asks, the tile server's reader can still follow a
    redirect a host starts answering with, and resolves the host itself; as for a Remote asset read by GDAL
    (ADR 0043), restricting the tile server's egress is the second layer, and neither is relied on alone.

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
  every field of it once. Both are what pointing costs. Measured on GFS: a worker's first tile ever takes 27 s (9 s
  on AIFS), a reopen on a miss 3.5 s, the first tile of another Variable 3.5 s, the next step of one already shown
  0.5 s, and a tile of a field already read 0.1 s. The first of these is paid as a worker starts, not by a user:
  each worker opens every published store, and every array published from it, in the background before a tile asks
  (#684). A store first published while a worker runs, or one whose pins a re-check changed, is still opened by its
  first tile.
- The cache is memory: its size times 8 MB or so, times the tile server's workers, on machines that are often small.
  Each open store costs memory of its own besides, outside the cache's bound: measured with the default 128 MB, a
  worker went from 155 MB idle to 427 MB after its first field and settled at 570 MB with two stores open.
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
- Arrays with a level, then ensemble members and projected grids. *Since #830:* an array with extra axes whose values
  are listed is read at one value of each (§8); reducing over an axis, and levels whose values are not listed, are
  still out.
- Time series and zonal statistics over store assets: a time series request says so, and zonal backfill skips them.
- A cache shared across tile server workers. Measured, it would save the 0.5 s a worker takes to read a new field
  (0.8 s at the median when read directly), at most once
  per worker, and some of the 128 MB each worker spends. It would not touch what a worker pays to open a store
  (27 s on GFS), which is spent once per process, can be warmed in advance, and cannot be shared.
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
- **Pin a web place's addresses in its reader.** Icechunk's HTTP reader resolves the host itself and takes no
  address to connect to. Judging at the sample read and at every harvest is what a COG's host gets in Titiler.
- **Refuse web places until the reader can be told not to follow a redirect.** dynamical.org's WeatherNext stores
  keep all their data on one; asking for a file first catches a host that redirects, short of one that starts
  between asks.
- **Pin `s3://` buckets only.** ECMWF AIFS Single's virtual store declares an S3 and a Google bucket and, in every
  field sampled, keeps its data in the Google one; it reads anonymously with what is already installed.
- **One question for all of a store's declared places, or pinning silently what the sample read used.** The first
  pins a place on the store's say-so; the second pins one nobody agreed to.
- **Offer every read plane on every shape.** One map of a series-shaped archive can be gigabytes through the tile
  server.
