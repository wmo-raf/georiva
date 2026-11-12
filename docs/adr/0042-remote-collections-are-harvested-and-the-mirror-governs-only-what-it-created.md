# Remote collections are harvested, and the mirror governs only what it created

## Status

accepted — see wmo-raf/georiva#605. Builds on [ADR-0031](0031-fetch-runs-stop-on-source-failures-and-retry-their-own-plan.md)
(run lifecycle), [ADR-0034](0034-push-feeds-replace-manual-upload.md) (one feed per catalog) and
[ADR-0025](0025-the-ingestion-queue-admits-only-fetch-and-extraction.md) / [ADR-0037](0037-bookkeeping-has-its-own-queue-and-worker.md)
(queues). Its read-side counterpart is [ADR-0043](0043-the-read-plane-reads-an-assets-bytes-wherever-its-href-points.md).

Amended after the first real Upstream was tried (Digital Earth Africa's CHIRPS): §2 and §9 now say how a feed is set
up and how a redirect and an `s3://` href are treated. Nothing is reversed: the Upstream still proposes and a person
still decides.

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

   **The operator is guided, not asked.** A harvest feed is created by a wizard that takes the Upstream's root
   address and nothing else an operator must already know. It **surveys** the Upstream: which collections it has,
   how the chosen one's items are organised, which raster assets they carry and where those files live. It proceeds
   only for the shape a harvest can index — one COG per time step per asset key — and names the reason for any
   other (many items at one time, a Zarr or NetCDF store, files that need an account, a catalog with no API): no
   feed is created for those. The Variables are created by the wizard from a **sample item**: unit, nodata and
   title lifted from its metadata, and a value range lifted from upstream statistics or else estimated from the
   sample asset's coarsest overview. That read is also the proof that the file is a COG readable without
   credentials. Upstream collection and catalog are fixed once the feed exists; the rest is edited on the feed.

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

   The collection's own description (title, extent) is read best-effort: a catalog whose collection pages do not
   answer while its item listing does is still harvested. But an empty listing is believed only of a collection the
   upstream described in that run. Neither described nor listed, the run is a `source` failure and removes nothing.

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

   Two refinements keep that rule while admitting real Upstreams:

   - **A redirect is followed only to where the operator already pointed.** The catalog client follows a redirect
     whose target has the scheme, host and port of the catalog address the operator entered, up to a small number
     of hops — the rule a `next` link already obeys. Any other redirect is a `source` failure naming the target.
   - **An `s3://` href is translated, by a translation a person confirmed.** Catalogs commonly name a public
     bucket by `s3://bucket/key`, which is not an address we may open: inside our deployment `s3://` means our own
     object store, with our credentials. The survey works out the bucket's public `https` host, checks that it
     answers an anonymous range read, and proposes it in plain words; on the operator's yes the feed **pins** the
     bucket → host translation and the host joins the allowlist. A harvest translates with pinned translations
     only, then judges and stores the `https` form, so every reader downstream sees an ordinary Remote asset
     (ADR 0043 unchanged). An `s3://` href whose bucket is not pinned is rejected and counted, like any href on a
     host not allowed. The same survey proposes any other `https` host the assets live on; the operator types no
     host.

   The Upstream's content therefore proposes hosts only while a person is present to agree, never during a
   harvest.

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
- An Upstream that moves its files to another bucket or host has every asset rejected from then on, until an
  operator re-checks the Upstream on the feed and confirms the new location. The run's rejected count is the signal.
- The wizard makes an outbound raster read from the web process, once per sampled asset. It is bounded by a short
  timeout, and a failure is a message on the wizard step, not a feed.

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
