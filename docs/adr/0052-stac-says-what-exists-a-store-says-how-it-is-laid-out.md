# STAC says what exists; a store says how it is laid out

## Status

proposed — see wmo-raf/georiva#825. Amends [ADR-0044](0044-an-upstream-store-is-harvested-step-by-step-and-titiler-reads-the-store.md) §5 (where
the survey reads a store's shape from) and §12 (when the survey opens a store). Reaffirms ADR 0044's refusal of
Series-shaped stores. Nothing else is reversed.

## Context

Two established STAC APIs were tried for ERA5. Both publish Zarr, and each puts it somewhere different from the other
and from dynamical.org, the Upstream ADR 0044 was written against:

- **Planetary Computer, `era5-pds`.** One item per month per kind (`an`, `fc`), each holding **one store per
  variable** (`abfs://era5/ERA5/2020/12/<variable>.zarr`). The collection describes the arrays in `cube:variables`
  but not their chunking. The only file on the collection itself is `era5_static.nc`, a NetCDF of geopotential,
  land–sea mask and soil type. Read from a store's own `.zmetadata`, a field is chunked `[372, 150, 150]`: half a
  month on a 150 × 150 tile. The stores need a Planetary Computer token, and the newest item is December 2020, though
  the collection's time range is left open.
- **Earth Data Hub (DestinE), collection `era5`.** Thirteen items, and **each item is a whole store**
  (`era5-land`, `era5-land-daily`, …), its `cube:variables` on the item and its asset untyped. The chunking appears
  only in the description's prose: every ERA5 store is Series-shaped, from 1440 × 50 × 100 (hourly ERA5-Land) to
  120 × 200 × 200 (monthly). Every store needs an API key, and each is plain Zarr, not Icechunk.

The survey told the operator "the collection does not say how `surface_air_pressure` is divided up" for the first.
That was wrong twice over. The survey takes any NetCDF, HDF or GRIB asset as a store, so it paired the
static-fields file with a cube describing other stores. It then read the shape from the datacube extension, which
has no word for chunking and which this provider had not filled in. The real reasons — one store per variable per
item, Series-shaped, a token required — never surfaced.

What both cases show:

- **STAC standardises discovery, not layout.** It says what exists, where and when, and links to it. How the bytes
  behind the link are arranged is left to the provider, on purpose. The datacube extension describes dimensions; a
  chunk shape, where one is given at all, is a provider's convention.
- **The variety is in a few axes, not in each provider.** Every Upstream seen so far differs along four, each with
  a handful of values:

  | Axis | Values seen |
  |---|---|
  | **Store placement** — where one readable store sits | per collection (dynamical.org) · per item (Earth Data Hub) · per item per variable (`era5-pds`) |
  | **Store format** | Icechunk · plain Zarr |
  | **Store shape** (ADR 0044) | Map-shaped · Series-shaped |
  | **Access** | anonymous · a signed token (Planetary Computer) · an API key (Earth Data Hub) |

  Items of COGs, the first shape ADR 0042 accepted, sit beside these as their own case.

- **Why a value is accepted was never written down.** Harvesting exists to avoid duplicating an Upstream's data:
  ADR 0042 copies the index and never the pixels, and ADR 0043 §7 has readers stream rather than download. But no
  decision says which shapes that rules in. Each was judged on its own: COGs because they range-read, Map-shaped
  stores because one chunk is one field, Series-shaped stores refused because a map costs gigabytes.

## Decision

**An Upstream shape is accepted for a read plane only when it is Streamable there: answering one request on that
plane reads upstream bytes in proportion to the request, not to the archive.** This is the admission test every
axis value below is judged by; the lists in §4 and §5 are its results, not choices of their own. A collection is
harvested only when it is Streamable for maps (§6); the other planes it is Streamable for are what it offers
besides.

**What exists, where and when comes from STAC. How a store is laid out — its arrays, their chunking and their
codecs — comes from the store's own metadata. The datacube extension is a fallback, used only when the store cannot
be opened.**

1. **The survey opens a store before it judges its shape.** For a store at a place the survey would let a harvest
   read (ADR 0042 §9, ADR 0044 §11), it reads the store's own metadata: Icechunk's repository and snapshot, or a
   plain Zarr's `.zmetadata` / `zarr.json`. It reads metadata only, never chunks, so it costs one small request
   and decodes nothing. The shape is then worked out from the chunking found there. Opening still never happens
   anywhere the allowlist refuses; the place is judged first, as now.

2. **The datacube extension is the fallback, and says so.** When a store cannot be opened (a place the survey does
   not read from, a store that needs an account), the survey falls back to the collection's `cube:variables`. If
   those name a chunking, the shape is read from them and the verdict names both the shape and why the store itself
   was not read. If they do not, the verdict names only why the store was not read. It never blames the collection
   for leaving out what STAC does not ask for.

3. **A store is a store by its format, not by being a file.** Only a Zarr or Icechunk asset counts as an Upstream
   store. A lone NetCDF, HDF or GRIB file on the collection is a file, not the store behind `cube:variables`.

4. **Each axis is one closed list in core, not a plugin per provider.** Core holds one small piece of code per value
   of each axis — an enumerator per Store placement, a reader per Store format, and, if credentials are ever
   accepted, a signer per kind of access. The survey reports which value of each axis it found, and the Harvest Feed
   Wizard follows from those values. A provider gets no code of its own. A value is added to a list when an Upstream
   worth harvesting needs it, with the decision recorded, as ADR 0044 §11 already does for kinds of place. Sources
   that are not STAC at all (CDS, an FTP drop) stay source plugins, as now.

5. **What is accepted today is unchanged**, and each refusal is the admission test failing or not yet tried: Store placement per collection, Store format Icechunk, Map-shaped,
   anonymous access. Every other value is recognised and refused by name, so an operator is told which axis stops
   the collection, in an operator's words:

   - per item: "Each item of this collection is a dataset with a store of its own. GeoRiva reads one store per
     collection."
   - per item per variable: "Each item of this collection keeps a store of its own for each variable. GeoRiva reads
     one store per collection."
   - plain Zarr: "This collection's data is a Zarr store of a kind GeoRiva cannot read yet."
   - Series-shaped and needing an account: the existing reasons.

   Where several axes stop a collection, the survey names the one an operator can do least about first: placement,
   then shape, then format, then access.

   Series-shaped fails the test for maps. A store per item, plain Zarr and credentialed access are not failures of
   it: each may stream, but reading it is not built, and is added per §4 when an Upstream worth harvesting needs it.

6. **GeoRiva stays map-first.** A collection must be Streamable for maps to be harvested at all. A Series-shaped
   store is Streamable for a point time series and not for maps, so it is never drawn as a map, and a Published collection with no
   map is still deferred (ADR 0044 §5). The one form in which a Series-shaped store may come in later is as the
   **analysis companion** of a Map-shaped store of the same data, read by the time-series plane only. Earth Data
   Hub's Climate DT already publishes such twins (`*-hourly-high-maps` and `*-hourly-high-timeseries`). That is a
   decision of its own when an Upstream worth harvesting offers both openly.

7. **Streamable is judged per read plane**, by the most one request may read upstream:

   | Read plane | One request reads at most | COG | Map-shaped store | Series-shaped store |
   |---|---|---|---|---|
   | Map tile | the field at one step, by range; overviews welcome, not required | yes (tiles in view, from an overview) | yes (one chunk: the whole field at the step) | no (every chunk of the area, each spanning many steps) |
   | Point time series | the chunks holding the point, over the requested steps | yes (one range per step) | yes (one chunk per step) | yes (one or two chunks) |
   | Zonal statistics | the fields at the requested steps, by range (ADR 0043 §7) | yes | yes | no |

   A lone NetCDF or GRIB file over `https` is usually read whole and is not Streamable. The same file indexed by a
   virtual store, as dynamical.org's GRIB-backed stores are, is: the store's references are the ranges. So such a
   file is accepted only through a store that indexes it (§3).

8. **Pixels are held in two places only, each bounded, and neither is duplication:**

   - the **materialization window** (ADR 0042 §8): Remote COGs copied inside the Jurisdiction on purpose, for a
     window the operator sets, and removed as they leave it;
   - Titiler's **decoded-field cache** (ADR 0044 §13): memory per worker, bounded by a setting, gone when the worker
     is.

   Anything else that would keep an Upstream's pixels — a byte cache, a mirror, a copy made to answer a plane a shape
   is not Streamable for — is a decision of its own, against this one.

## Consequences

- Every future "can GeoRiva harvest this?" is one question per read plane: what does one request read? A new axis
  value is argued on that, not on the provider.
- A store's shape is right even where the provider describes nothing, as on Earth Data Hub, and a provider's
  datacube metadata can no longer contradict its store.
- The survey opens every candidate store at an allowed place, not only Map-shaped ones. Opening an Icechunk
  repository costs seconds (ADR 0044, Context), so a refusal of a Series-shaped Icechunk store now pays that
  where it used to cost nothing.
- Core reads a plain Zarr's metadata without being able to read its data. The reader stays a list of one format
  until a plain Zarr store is worth harvesting.
- `_STORE_TYPES` loses NetCDF, HDF and GRIB. A collection whose only data file is one of those is surveyed as a
  collection with no store.
- The four axes and their values become glossary terms (`CONTEXT.md`), and each refusal names one of them.

## Out of scope

- **Credentials for an Upstream** — Planetary Computer tokens and Earth Data Hub keys alike. One topic, parked:
  Upstreams stay anonymously readable (ADR 0042). If it is taken up, a signer per kind of access is the shape §4
  leaves room for.
- **Reading plain Zarr data.** Waits for a Map-shaped, openly readable plain Zarr store published through STAC.
  ARCO-ERA5's `full_37-1h-0p25deg-chunk-1.zarr-v3` is Map-shaped and open but has no STAC catalog.
- **Store placement per item.** Letting the operator pick one item as the store. It would only lead to the next
  refusal while each such item seen so far is Series-shaped and credentialed.
- **Copying a Series-shaped store into local COGs**, in batches along its time chunks. That is a pull, not a
  harvest, and a decision of its own.
- **A Series-shaped store as an analysis companion** (§6).

## Alternatives considered

- **Plugins per STAC provider.** A plugin's contract would be "for a time and a variable, give a field that can be
  read", which is the Upstream catalog client seam that already exists. Each plugin would carry its own copy of the
  four axes, and two providers with the same layout would share nothing.
- **Require the datacube extension to carry chunking.** Neither provider tried does, and the extension does not
  define it. Every refusal would be "the collection does not say".
- **Admit by format name** ("COG and Zarr are cloud-native"). A Series-shaped Zarr store is as cloud-native as a
  Map-shaped one and still costs a map hundreds of megabytes; the name says nothing about which requests are cheap.
- **Accept Series-shaped stores for maps, slowly.** One daily map of Africa from Earth Data Hub's ERA5-Land reads
  15 to 24 chunks of 75 days each, 200–300 MB, to draw one day. That is not map-first.
