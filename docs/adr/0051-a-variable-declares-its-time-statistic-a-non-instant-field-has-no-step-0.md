# A Variable declares its time statistic; a non-instant field has no step 0

## Status

accepted — see wmo-raf/georiva#818. Amends [ADR-0044](0044-an-upstream-store-is-harvested-step-by-step-and-titiler-reads-the-store.md)
§6 (which steps of a run an array is expected at). Nothing else is reversed.

## Context

The first map of a forecast run (lead 0) was blank for precipitation and some radiation variables. Those are running
totals since the run started: at lead 0 the window is zero hours long and every value is 0. That is not missing data,
and ECMWF's own charts skip the step.

Sources treat lead 0 in two ways:

- **Running totals since run start** are present at lead 0 as a field of zeros: IFS open data `tp` / `ssrd`, the AIFS
  store's `*_run_total_*` arrays, GFS `*_run_total_*`.
- **Statistics over the preceding interval** are absent at lead 0: GFS 1–6 h `total_precipitation_surface`, the `avg`
  arrays, `maximum_temperature_2m` / `minimum_temperature_2m`.

GeoRiva had no single place that recorded which kind a Variable is. Harvest inferred it from the first step a store
holds (an array was expected "from its own first Held step on"), which the AIFS store disproves. Forti hardcoded it
(`ACCUMULATED_FROM_RUN_START = ("tp",)`). GRIB ingestion ignored `GRIB_stepType`. Yet every source already says which
kind a field is: a store array's `step_type` (with `_run_total` in its name or a comment naming the window), GRIB's
`stepType`, NetCDF's `cell_methods`.

## Decision

1. **A Variable declares its time statistic**, as two fields:
   - `time_statistic`: `instant` / `sum` / `mean` / `max` / `min`. Default `instant`, never null.
   - `statistic_window`: `run_start` / `preceding_interval`, required for a statistic, empty for `instant`.

   A Variable with no information behaves exactly as before.

2. **The source sets it when the Variable is created** (`core/time_statistic.py` reads each source's word):
   - the Harvest Feed Wizard, from the array's `step_type` plus its `_run_total` name or its comment;
   - the upload wizard, from GRIB `stepType` (an accumulation is taken to run from the run's start — GRIB's
     stepType does not say where its window starts) or NetCDF `cell_methods` (a CF statistic is over the cell's own
     preceding interval);
   - a plugin's collection definition (`CollectionVariable.time_statistic` / `statistic_window`: ECMWF `tp`,
     `ssrd`, `10fg`; CDS; CHIRPS).

   A promotion's output Variables are the feed's own, so they carry its declaration. Other recipes' outputs stay
   `instant` for now. After creation the admin's value wins: a source that disagrees with the stored value is logged
   as a warning naming the Variable (`Variable.agrees_with`), by a harvest, an ingestion or a re-provision, and
   nothing is changed. Existing Variables stay `instant` until an admin changes them; the warnings name the ones
   that need it.

3. **One rule on `Variable` decides whether a step exists**: `Variable.has_step(reference_time, time)` — a Variable
   that is not `instant` has no lead-0 step. A step of no run always exists. Everything asks it, and nothing keeps
   its own copy:
   - the harvester, when it writes rows;
   - the expected steps of a store run (`Variable.expected_leads`), so runs still close. Every array of a run is now
     published at the run's expected lead times; the "expected from its own first Held step" inference is gone;
   - GRIB / NetCDF ingestion, when it writes assets. A file whose every timestamp is such a step writes nothing and
     succeeds;
   - Forti, which supplies its lead-0 zero for a Variable that is a running total since the run started
     (`Variable.zero_at_run_start`), replacing `ACCUMULATED_FROM_RUN_START`.

4. **Lead 0 of a non-instant Variable is never written.** It is not stored and then hidden, so the viewer, WMTS, EDR
   data and analysis need no change.

5. **No cleanup command or data migration.** Existing lead-0 assets of running totals stay until they leave their
   window or retention. A run a harvest asks again (open, or re-checked by "Run now") loses its lead-0 row, as the
   Mirror removes a place the Upstream no longer gives.

6. **Collections without runs declare it too** (CHIRPS daily rainfall is `sum` / `preceding_interval`). The lead-0
   rule applies only to items that have a `reference_time`.

7. **EDR** publishes it as the parameter's `measurementType.method` (`sum` / `mean` / `maximum` / `minimum`).
   `period` is left out: a running total's window grows with each step, and an interval's length is not recorded.
   STAC is left for later.

## Consequences

- A forecast's first map of precipitation or radiation is no longer a blank field of zeros: the step is absent.
- A store harvest's existing `instant` Variable over an array the store does not hold at lead 0 (a GFS average) is
  now expected at lead 0, so its runs stay open until an admin declares it. The harvest logs which Variable on every
  run, from the array's own `step_type`. This is the cost of the declaration deciding instead of the store's
  holdings.
- GFS GRIB accumulation buckets read through GRIB ingestion are proposed as `run_start`; the lead-0 rule is the same
  either way, and an admin may correct the window.
- Turning running totals into interval totals for display ("rain in the last 6 h") is a follow-up that builds on
  this field; the interval arithmetic already exists in Forti's `accumulation_window`.
