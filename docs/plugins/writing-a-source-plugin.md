# Writing a Source Plugin

An orientation guide for implementing a new GeoRiva source plugin, using
`georiva-source-chirps` as the worked reference. For installing and running
plugins (production and local dev), see [installation.md](installation.md).

---

## The one-sentence version

A plugin is a **pip-installable Python package that is also a Django app**. It
contributes two things: a `DataFeed` model subclass (the operator-facing config,
rendered by Wagtail) and a `BaseDataSource` subclass (pure logic that turns a
time window into a list of files to fetch). Core owns everything else — fetching,
storage, ingestion, STAC.

---

## The five moving parts

### 1. Packaging and discovery

Files: `pyproject.toml`, `georiva_plugin_info.json`,
`src/<module>/config/settings/settings.py`, `src/<module>/apps.py`

Flat PEP 621 package: repo root is the package, code lives under `src/<module>/`.
No `setup.py`, no `requirements/`.

Two non-obvious rules:

- **Never declare `georiva` as a dependency.** Core is provided by the runtime
  environment (the GeoRiva Docker image, or the uv workspace where core is a
  sibling member in the same virtualenv). Libraries core already ships
  (`rasterio`, `requests`, `xarray`, …) don't need declaring either. CHIRPS
  declares `dependencies = []`.
- **`config/settings/settings.py` must define `setup(settings)`** which appends
  your module to `INSTALLED_APPS`. Core calls this after building Django
  settings but before Django starts. That is the entire discovery hook:

  ```python
  def setup(settings):
      if "georiva_source_chirps" not in settings.INSTALLED_APPS:
          settings.INSTALLED_APPS += ["georiva_source_chirps"]
  ```

The Django app label is derived from the package under `src/`, **not** from the
checkout folder name — so a plugin can be cloned into any directory.

`georiva_plugin_info.json` is installer metadata (name, version, author, and
`requires_env` listing environment variables the plugin needs).

### 2. The `DataFeed` subclass — `models.py`

`DataFeed` (`georiva/src/georiva/sources/models.py:24`) is a polymorphic Wagtail
snippet. You subclass it, add your operator-configurable fields, and override a
handful of hooks.

CHIRPS ships **no `wagtail_hooks.py` and no `views.py`** — core auto-builds the
admin viewset for every `DataFeed` subclass. The cookiecutter boilerplate still
includes those files; you most likely do not need them.

The hooks, in the order the setup wizard calls them:

| Hook                                   | Purpose                                                                    |
|----------------------------------------|----------------------------------------------------------------------------|
| `get_catalog_defaults()`               | Pre-fills wizard step 1 — catalog name, `file_format`, description         |
| `get_collection_definitions()`         | The finite checklist of collections this plugin can create                 |
| `get_link_config_for_definition(defn)` | Bakes non-editable per-link fields from the definition key                 |
| `get_collection_link_model()`          | Your `DataFeedCollectionLink` subclass, if per-collection config is needed |
| `get_derived_products()`               | Optional; **instance** method (see ADR-0008)                               |
| `data_source_cls`                      | Property returning your `BaseDataSource` subclass                          |
| `get_loader_config()`                  | Feed-level config dict passed to the data source                           |
| `get_wizard_defaults()`                | Field values applied when the wizard creates the instance                  |

`get_link_config_for_definition` is worth understanding: CHIRPS derives `period`
(`monthly` / `pentadal` / `dekadal`) from the definition key so the operator
never sees it as an editable field. Use it for anything that is a property of
the collection rather than a choice.

### 3. `CollectionDefinition` — the data-model declaration

`parse_collection_defs(COLLECTIONS)`
(`georiva/src/georiva/sources/collection_definitions.py:133`) lets you write a
plain dict instead of constructing dataclasses. This dict is the canonical spec
of your plugin — edit it to add or remove collections:

```python
COLLECTIONS = {
    "chirps-monthly": {
        "name": "CHIRPS Monthly",
        "time_resolution": "monthly",
        "default_interval_minutes": 43200,
        "variables": [
            {
                "key": "precip",
                "name": "Precipitation",
                "source_units": "mm",
                "source_variable": "band_1",
                "value_range": (0.0, 300.0),
            }
        ],
    },
}
```

Each selected entry provisions one `Collection`, one `Variable` per declared
variable, and one `DataFeedCollectionLink`.

Key per-variable fields:

- `source_variable` — how the format plugin finds it in the file (`band_1` for
  GeoTIFF, a short name for GRIB). Accepts a string shorthand or a dict with
  `name` and `level`.
- `source_units` vs `output_units` — the raw unit as it leaves the file vs the
  unit you expose. If they differ, the ingestion pipeline converts via pint.
- `value_range` — drives default rendering.
- `transform` / `components` — for vector-derived variables
  (`vector_magnitude`, `vector_direction`).
- `groups` — pure UX grouping in the wizard; no effect on the data model.

### 4. The `BaseDataSource` subclass — `source.py`

This is the only genuinely source-specific code. The contract
(`georiva/src/georiva/sources/source.py:68`) is narrow:

- **`generate_requests(start_time, end_time, variables) -> Iterator[FileRequest]`**
  — the core method. It does *not* fetch; it yields descriptors. A `FileRequest`
  carries `identifier`, `filename`, `valid_time`, `reference_time` (`None` for
  non-forecasts), `params` (including `url`), `expected_format`, `variables`.
- **`name`, `source_type`** — abstract properties. `source_type` is a
  `DataSourceType`: `FORECAST`, `REANALYSIS`, `SATELLITE`, `OBSERVATION`,
  `DERIVED`.
- **`config`** arrives as `get_loader_config()` merged with the per-collection
  link's `config` property (`sources/models.py:203-209`). That is how one CHIRPS
  feed gives each collection its own `period`.
- **`advance_start_from_latest(latest, collection=)`** — override so incremental
  runs don't refetch the last period. CHIRPS jumps to the next
  month/pentad/dekad boundary.
- **`get_default_start_date()`** — the backfill floor when nothing is stored yet.
- **`get_latest_available()`** — optional remote probing for the newest file.
- **`post_process_fetched_file(request, local_path) -> (path, filename)`** — the
  escape hatch, called after fetch and before storage. CHIRPS uses it to gunzip
  `.tif.gz` → `.tif`, validate with rasterio, and stamp nodata
  (`source.py:407`).

Two subtleties that will bite you:

**The filename is a contract with the format plugin.** CHIRPS embeds
`YYYY-MM-DDTHH:MM:SS` in every filename because `GeoTIFFFormatPlugin.get_timestamps()`
parses it back out. Check what your target format plugin expects before you
choose a naming scheme.

**Fetch strategy is a separate concern.** `HTTPFetchStrategy` / `FTPFetchStrategy`
know *how* to retrieve; your source knows *what*. Pass the strategy **class**
(not an instance) — `Loader` instantiates it (`sources/loader.py:138`). For
queued APIs there is `FetchMode.ASYNC` with `check_status(job_id)`; look at
`georiva-source-cds` rather than CHIRPS for that pattern.

### 5. Derived products — optional

Only relevant if your plugin produces outputs beyond raw ingestion. CHIRPS
declares three products per resolution (promotion, climatology, anomaly) via
`DerivedProductDefinition`, with `InputRef` / `OutputRef` bound to collection
slugs and tiers (`staging` / `published`), plus recipes under `recipes/`
registered in `AppConfig.ready()`.

> `ready()` must run in **every** process — web *and* the processing worker — or
> units drop with "Unknown recipe". See ADR-0007.

Read `docs/adr/0007`–`0010` and [derived-products.md](derived-products.md)
before touching this. A plain ingest-and-serve feed can skip it entirely.

A useful CHIRPS convention: keep every collection slug behind a helper function
in `constants.py` (`source_slug`, `climatology_slug`, `anomaly_slug`, …) so the
product declarations and the recipes can never disagree about a name.

---

## What the runtime does for you

`DataFeed.run_now()` → `Loader.run()`, and for each `FileRequest`:

1. Skip if the file already exists in storage (it also checks sibling
   collections, so a shared file isn't downloaded twice).
2. Fetch to a temp path via the fetch strategy.
3. Call your `post_process_fetched_file()`.
4. Store at the tier bucket path.
5. The MinIO-event → ingestion pipeline takes over from there.

You never write storage paths, STAC items, or Celery tasks.

For the full picture — scheduling, tier routing, the MinIO/Redis handoff, the
staging vs. published split, and recovery sweeps — see
[architecture/runtime-flow.md](../architecture/runtime-flow.md).

---

## Suggested order of work

1. `cookiecutter source-plugin-boilerplate`; fix `pyproject.toml` and
   `config/settings/settings.py`.
2. Write the `COLLECTIONS` dict — it forces the data-model decisions up front.
3. Write `source.py` `generate_requests()` and test it standalone (just print
   the URLs it yields).
4. Wire the `models.py` hooks, make migrations, run the setup wizard in the admin.
5. Add `post_process_fetched_file()` only if the raw file isn't directly readable
   by a format plugin.

Two things to settle before you start: **which format plugin will read your
files** (and what it expects from the filename), and **whether your fetch is
sync or async**. Those two decisions shape most of `source.py`.

---

## Reference files

- `georiva/src/georiva/sources/source.py` — `DataSource` protocol, `BaseDataSource`
- `georiva/src/georiva/sources/models.py` — `DataFeed`, `DataFeedCollectionLink`
- `georiva/src/georiva/sources/collection_definitions.py` — `CollectionDefinition`, `parse_collection_defs`
- `georiva/src/georiva/sources/fetch/base.py` — `FileRequest`, `FetchResult`, `BaseFetchStrategy`
- `georiva/src/georiva/sources/loader.py` — the run loop
- `source-plugin-boilerplate/` — cookiecutter template
- `dev-plugins/georiva-source-chirps/` — the worked reference (HTTP, sync)
- `dev-plugins/georiva-source-cds/` — async / queued-API reference
