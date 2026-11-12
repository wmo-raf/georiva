# Remote collections are harvested, and the mirror governs only what it created

## Status

accepted — see wmo-raf/georiva#605. Builds on [ADR-0031](0031-fetch-runs-stop-on-source-failures-and-retry-their-own-plan.md)
(run lifecycle), [ADR-0034](0034-push-feeds-replace-manual-upload.md) (one feed per catalog) and
[ADR-0025](0025-the-ingestion-queue-admits-only-fetch-and-extraction.md) / [ADR-0037](0037-bookkeeping-has-its-own-queue-and-worker.md)
(queues). Its read-side counterpart is [ADR-0043](0043-the-read-plane-reads-an-assets-bytes-wherever-its-href-points.md).

## Context

GeoRiva serves cloud-natively but acquires like a download portal: nothing is visible until its bytes are in MinIO.
For an NMHS on a thin pipe and a small disk, browsing CHIRPS or Digital Earth Africa means mirroring decades of it
first. The upstream already publishes COGs behind a STAC index; the only thing GeoRiva lacks is the index.

## Decision

**A harvest feed copies an upstream STAC index into ordinary Published rows whose assets point upstream. Pixels are
never copied.**

1. **Harvested structure is ordinary core rows.** `Catalog` / `Collection` / `Variable` / `Item` / `Asset`, owned by
   the harvesting organisation like any other (ADR 0011–0018 unchanged). A **remote asset** is an `Asset` whose `href`
   is an upstream URL. No staging tier: there is nothing to extract or promote. Sharing with other organisations is
   the existing `public` visibility; there is no instance-wide catalog tier.

2. **The harvester is a pull feed, not a third kind of feed.** It is a `DataFeed` species, so "a catalog has at most
   one feed" (ADR 0034) stands. Its configuration: upstream catalog location, upstream collection id, time window,
   cadence, an explicit **variable mapping** (upstream asset key → curated `Variable`), and an **asset host
   allowlist**. Only anonymously readable upstreams; saving the feed checks unauthenticated reachability.

3. **Operator-mapped variables only.** An upstream asset key with no mapping is not indexed, silently. Extending the
   mapping and harvesting again indexes it; nothing is lost by skipping it first.

4. **It rides the run-tracking spine, not the Loader's loop.** Scheduling, `FetchRun` lifecycle (status, stop reason,
   retry lineage, health, dashboard) are shared; the loop body is the harvester's own. The `FetchRun` lifecycle is
   extracted from `Loader.run()` into a seam both loops use. Harvested items never pass through
   `FileRequest` / `FetchResult` / the storage write path, and a harvest run has **no `FetchedFile` rows**: there are
   no files. Its counts (indexed, updated, removed, rejected) live on the run.

5. **A harvest's plan is its window.** A harvest is idempotent over `(feed, collection, window)`, so a retry
   re-harvests the same window — ADR 0031 §7's "a source with no plan to copy asks the source again", which for a
   mirror is the right answer rather than a fallback. Upstream unreachable is a `source` failure: the run stops
   `FAILED`, rows untouched, and retries on the usual backoff.

6. **Mirror semantics, inside the window.** Each completed harvest is authoritative for the feed's window: an item
   gone upstream loses its rows here; an item whose upstream `updated` changed is re-synced (href, stats, checksum).
   **Removal happens only after the whole window was listed successfully.** A listing that fails part-way removes
   nothing, so an upstream outage can never read as "everything was deleted".

7. **Jurisdiction is explicit: mine, and still remote.** Every harvested row carries durable provenance naming the
   feed that created it, set at creation. The mirror may delete or rewrite exactly the assets that (a) bear its own
   feed's provenance **and** (b) still have a remote href; both are checked. It is never inferred from the href alone:
   a remote-href row without provenance is nobody's to delete. An `Item` goes when it bears the feed's provenance and
   no asset of any kind is left on it.

8. **Materializing leaves the mirror's jurisdiction.** The same feed may fetch a window's bytes into local storage
   through the normal fetch → ingestion path. That rewrites the asset's href to a local key, which fails condition
   (b); the provenance stays as history. A **mixed collection** — local hot window, remote deep archive — is a
   first-class state, and hydration is a per-window dial.

9. **Upstream decides what we index, never where our servers connect.** Asset hrefs come from upstream content and
   are later opened from inside our network. At harvest time each href must be `https` (plain `http` only if the
   operator allows it), on a host in the feed's allowlist (default: the catalog's own host; operator-extensible,
   since APIs often serve assets from another bucket host), and must not resolve to a private, loopback or link-local
   address. A failing asset is skipped and counted as rejected on the run; it does not fail the harvest.

10. **Display stats are lifted, never computed on read.** From upstream metadata (`raster:bands` statistics or
    equivalent) when present, else null. The mapped `Variable`'s style bounds guarantee rendering without them.
    Zonal backfill may fill them in as a side effect; nothing may depend on that.

11. **Queues.** The harvest runs on `georiva-default`; the ingestion queue's admitted set stays closed (ADR 0025).
    Zonal backfill for remote assets fans out on `georiva-bookkeeping` (ADR 0037), bounded by the feed's window and
    never the upstream's full depth. Virtual-Zarr manifests are not built for remote assets.

12. **The write plane stays local-input-only.** Derivation recipes do not take remote assets as inputs; a recipe
    that needs them materializes the window first. Derived outputs are always local.

13. **All remoteness is behind one seam: the upstream catalog client** (list collections; list items with assets in
    a window). It is the only thing faked in tests; no test reaches a real upstream.

## Consequences

- An upstream that honestly answers "this window is empty" empties the window here, and the zonal statistics computed
  for those assets go with them. Re-harvesting restores the index in seconds; the statistics cost one remote read per
  asset again. No deletion threshold is added until an upstream gives us a reason.
- `FetchRun`'s file counters and "outcome is derived from `FetchedFile` children" do not describe a harvest run;
  whatever reads them (health, dashboard, retry) must take the harvest's own counts.
- The allowlist is the guarantee against a hostile or careless upstream reaching MinIO or other internal services
  through Titiler. Redirects and DNS that changes after harvest are a deployment concern (ADR 0043).
- An upstream that reprocesses an item without changing `updated` is not re-synced.
- Deleting a harvest feed leaves its rows with provenance pointing nowhere: they are then nobody's, and only an
  operator removes them.

## Out of scope

- Virtualized NWP archives (GRIB/NetCDF → virtual Zarr) and native Zarr upstreams: wave 2, with the Zarr serving path.
- Credentialed upstreams of any kind (static keys, requester-pays, SAS/OAuth token vending).
- An instance-wide catalog tier.

## Alternatives considered

- **Live federation** (proxy the upstream catalog per request). Every listing, search and tile would depend on
  upstream latency and availability, and curated variables, styles and zonal statistics need local rows anyway.
- **Thread harvested items through the Loader's file loop.** It would mean fake files, fake storage writes and
  `FetchedFile` rows that name nothing.
- **Infer jurisdiction from the href scheme.** One fewer column, and a sync that deletes rows it never created.
- **Auto-create variables for every upstream asset key.** Fills the admin with guessed units and styles, and breaks
  the guarantee in (10) that a harvested variable always has a curated style.
