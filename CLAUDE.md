# GeoRiva

Geospatial Raster Ingestion, Visualization & Analysis platform. Ingests gridded raster data (GRIB, NetCDF, GeoTIFF) from
plugins or MinIO drop zones, processes into cloud-optimized COGs, indexes as STAC-compliant catalogs, and serves via
STAC API, EDR API, WMTS, and the Titiler/Martin tile servers.

Target domain: spatio-temporal environmental data, such as weather, climate, and ocean models, for African National
Meteorological Services.

## Tech Stack

- **Framework**: Django 5.x + Wagtail 7.x (CMS/admin)
- **Database**: PostgreSQL 18 + TimescaleDB + PostGIS (TimescaleDB HA image), fronted by PgBouncer
- **Object Storage**: MinIO (S3-compatible), multi-bucket (incoming, sources, archive, assets, zarr)
- **Task Queue**: Celery + Redis (three queues: `georiva-default`, `georiva-ingestion`, `georiva-processing`)
- **Event Bus**: MinIO bucket notifications → Redis list → `minio-consumer` / `staging-consumer` (no MQTT/Mosquitto)
- **Tile Servers**: Titiler (raster, FastAPI, `titiler-app/`); Martin (vector/MVT from PostGIS)
- **Data Libraries**: xarray, rasterio, cfgrib, dask, geopandas; virtualizarr + virtual-tiff (kerchunk)
- **API**: Django REST Framework + drf-spectacular (OpenAPI)
- **Frontend**: STAC Browser (Radiant Earth), Vue.js for dashboards
- **Container**: Docker Compose (15 services), Nginx reverse proxy + tile auth gateway (`georiva-web-proxy`)
- **Package Manager**: uv — core is a standalone project (`georiva/pyproject.toml` + `georiva/uv.lock`); repo-root
  `pyproject.toml` is a dev workspace overlay that ties in plugins
- **Python**: 3.12+

## Project Structure

```
georiva/src/georiva/          # Main Django application
├── config/                   # Django settings, URLs, Celery, WSGI/ASGI
│   └── settings/             # base.py, dev.py, production.py
├── core/                     # STAC-aligned data models (Catalog, Collection, Variable, Item, Asset)
│   ├── models/               # Split per-entity: catalog.py, collection.py, item.py, variable.py
│   ├── storage/              # Objects in buckets and the keys naming them
│   │                         #   manager.py (Bucket + StorageManager singleton),
│   │                         #   path_resolution.py, filename.py, asset_cleanup.py
│   ├── machine_plane/        # Where the tenant travels in the address (ADR 0013/0015)
│   │                         #   addresses.py, auth_view.py, config_view.py, palette_cache.py
│   ├── derived_products/     # The derived-product contract (ADR 0008/0009)
│   │                         #   definitions.py, chain.py
│   ├── views/                # Wagtail admin surface: admin.py, viewsets.py,
│   │                         #   tables.py, summary_items.py
│   └── tests/                # One module per subject
├── ingestion/                # Data ingestion pipeline
│   ├── service.py            # Main IngestionService (orchestrates full pipeline)
│   ├── tasks.py              # Celery tasks (process_incoming_file, sweep_unprocessed)
│   ├── models.py             # IngestionLog (distributed locking, state machine)
│   ├── extractor.py          # Data extraction
│   ├── asset_writer.py       # COG asset writer (encoded textures on-demand, ADR 0021)
│   └── clipper.py            # Boundary clipping
├── formats/                  # Format handler plugins (GRIB, NetCDF, GeoTIFF)
│   ├── base.py               # BaseFormatPlugin ABC
│   └── registry.py           # FormatRegistry (decorator-based registration)
├── sources/                  # Data source plugin framework
│   ├── source.py             # DataSource protocol + BaseDataSource ABC
│   ├── loader.py             # Loader orchestrates fetch → store
│   ├── registry.py           # LoaderProfileViewSetRegistry
│   ├── product_service.py    # Provision DerivedProducts, pin bindings (ADR 0010)
│   └── derivation_invocation.py  # Product-driven dispatch: arriving input → recipe run
├── staging/                  # Staging tier — source-grained STAC models + DerivationLink (ADR 0004)
├── processing/               # Derivation engine — the write-side counterpart of the Loader (ADR 0005)
│   ├── engine.py             # Generic run loop: enumerate → resolve → readiness → compute → register
│   ├── recipe.py             # BaseRecipe ABC (input selectors, units, transform, outputs)
│   ├── registry.py           # RecipeRegistry (decorator-based registration)
│   └── recipes/              # climatology.py, promotion.py
├── geoprocessing/            # Pure compute library — no Django, no storage, no request layer
│                             #   algebra.py, calendar.py, regrid.py, temporal.py, zonal.py
├── organisations/            # Multi-tenancy (ADR 0011–0018)
│   ├── ownership.py          # scope_rows / belongs_to_active_org — the tenancy choke point
│   ├── access.py             # may_see_private, require_active_org
│   └── pages.py              # Closes Wagtail's own pk-taking page views (ADR 0016)
├── stac/                     # STAC API (views, serializers, URLs)
├── edr/                      # EDR API — metadata plane only (data queries not yet implemented)
├── analysis/                 # Analysis modules: timeseries/ + zonal_stats/ (no operator registry)
├── wmts/                     # WMTS capabilities (per-org); GetTile lives in titiler-app/app/wmts.py
├── virtual_zarr/             # Per-Variable virtual Zarr (kerchunk / Icechunk) manifests over COG assets
├── visualization/            # Wagtail admin hooks (views are a stub; viz via tile-config/Titiler/Martin)
├── accounts/                 # Per-user identity: API keys (grv_…) + DRF auth + account panel
├── pages/                    # Wagtail CMS pages (home, datasets)
└── utils/                    # Shared utilities
```

**Other top-level directories:**

- `titiler-app/` — Custom Titiler tile server (FastAPI)
- `source-plugin-boilerplate/` — Cookiecutter template for new source plugins
- `docs/` — Architecture docs, plugin guides, data model guide
- `deploy/` — Nginx config, plugin installation scripts

## Build & Run

All commands via Makefile. The app runs entirely in Docker.

```bash
# Development
make dev-build                     # Build dev images
make dev-up                        # Start with hot-reload
make dev-down                      # Stop
make dev-logs                      # All service logs
make dev-app-logs                  # Django app logs
make dev-worker-ingestion-logs     # Ingestion worker logs
make dev-shell                     # Shell into app container
make dev-migrate                   # Run migrations
make dev-makemigrations            # Create migrations

# Production
make build && make up              # Build and start
make logs                          # All logs
make shell                         # Shell into container
```

**Inside container**: `georiva` is the management command (alias for `python manage.py`).

**Docker entry points** (see `docker-entrypoint.sh`):

- `django-dev` — Dev server with auto-setup
- `celery-ingestion-worker-dev` / `celery-default-worker-dev` — Workers with auto-reload
- `gunicorn-wsgi` / `gunicorn-asgi` — Production servers

## Configuration

Environment variables in `.env` (see `.env.sample` for all options):

- `GEORIVA_STORAGE_BACKEND` — `s3` or `local`
- `GEORIVA_LOG_LEVEL` — Logging level
- `AWS_*` / `MINIO_*` — S3/MinIO connection
- `GEORIVA_CELERY_*_WORKER_CONCURRENCY` — Worker scaling

Settings split: `config/settings/base.py` (shared) → `dev.py` / `production.py` (overrides).

## API Endpoints

Defined in `api/urls.py`:

- `/api/stac/` — STAC API (collections, items, search, queryables)
- `/api/edr/` — Environmental Data Retrieval API (metadata plane only so far)
- `/api/wmts/<org>/WMTSCapabilities.xml` — WMTS capabilities; `GetTile` is the KVP shim in `titiler-app`
  at `/titiler/<org>/wmts?REQUEST=GetTile` (gated by nginx like every other tile route)
- `/api/jobs/` — Async job status (task_ferry)
- `/api/analysis/` — Analysis API (e.g. `timeseries/point`, `timeseries/area`)
- `/api/tile-config/<org>/<catalog>/<collection>/<variable>/` — Tile/render config (machine plane; see ADR 0013)
- `/api/datasets/` — Dataset pages API
- `/admin/` — Wagtail CMS admin (mounted in `config/urls.py`)

> MinIO events arrive via a Redis list consumed by `minio-consumer`, **not** an HTTP webhook
> endpoint. Vector tiles for zonal stats are served by Martin at `/martin/boundary_stats/{z}/{x}/{y}`,
> which requires `org`, `catalog`, `collection` and `variable` query params. Titiler raster tiles live
> under `/titiler/<org>/<catalog>/<collection>/<variable>/…`. Build every such URL through
> `core/machine_plane/addresses.py` — never by hand (ADR 0013).

## Key Conventions

- **Models**: Split into separate files under `core/models/`, re-exported from `__init__.py`
- **Plugins**: Register via decorator (`@FormatRegistry.register`) or programmatic `registry.register()`
- **Singletons**: `storage`, `format_registry`, `loader_profile_viewset_registry` — import from their modules
- **Celery tasks**: Late imports inside task body to avoid circular imports; `bind=True` + `acks_late=True`
- **Celery queues**: Heavy processing on `georiva-ingestion`, lightweight on `georiva-default`
- **Celery retries**: ingestion tasks use `max_retries=0` (recovery via the `sweep_unprocessed` periodic task); some
  newer tasks (e.g. `zonal_stats`) use bounded `max_retries`
- **Wagtail hooks**: Each app owns its admin integration via `wagtail_hooks.py`
- **Serving visibility**: never filter `visibility` by hand — go through `Collection.objects.visible_to(request)`
  (or `visible_visibilities(request)` where you need the tiers). `public` → anyone, `private` → members of the
  host's org, `internal` → nobody. Membership is `organisations.access.may_see_private` (ADR 0014). Titiler and
  Martin are gated at the proxy instead, by the nginx `auth_request` into `core/machine_plane/auth_view.py` (ADR 0015) —
  never add tenancy logic to either tile server. The one exception is `core/machine_plane/config_view.py`: Titiler forwards
  no credential on that callback, so there is nobody to ask and it stays `public`-only
- **Tenancy declarations**: every model says where it stands via `ORGANISATION_LOOKUP` — an ORM path, or one of
  `SHARED_REFERENCE_DATA` / `ORGANISATION_SELF` / `PAGE_TREE` / `NOT_ORM_SCOPABLE` / `via_related(path)` /
  `via_content_object(ct_field, id_field)`; a model that declares nothing cannot be scoped and raises.
  `ORGANISATION_GLOBAL_TIER` beside a nullable path means null = the instance-wide tier — read by every org,
  written only by the instance admin (ADR 0011). Scope through the dispatcher — `organisations/ownership.py`'s
  `scope_rows` for a queryset, `belongs_to_active_org` for an object in hand — never by reading declarations
  yourself. Wagtail pages are org-owned through the Site → root-page link; the dispatcher scopes them, and
  Wagtail's own pk-taking page views are closed by `organisations/pages.py` (ADR 0016)
- **Storage paths**: Org-first, time-partitioned: `{org}/{catalog}/{collection}/{variable}/{year}/{month}/{day}/`
  — the first segment of every key on every bucket is the owning organisation's slug
- **Dependencies**: managed with uv; core deps in `georiva/pyproject.toml` + `georiva/uv.lock` (no
  `requirements.txt`). Add via `make uv-add pkg="..."`; `uv sync --all-packages` builds the local dev env
- **Source plugins**: flat PEP 621 packages (repo root = package, code under `src/<module>/`; no
  `setup.py`/`requirements/`). For dev, cloned into `dev-plugins/` (gitignored); discovery derives the Django app
  name from the package under `src/`, not the folder name

## Existing Documentation

- `docs/architecture/README.md` — Full system architecture design document
- `docs/georiva-data-model-guide.md` — Data model explanation
- `docs/format-plugins.md` — Format plugin development guide
- `docs/contributing.md` — Contribution guidelines
- `docs/plugins/` — Plugin-specific documentation

## Additional Documentation (Claude-specific)

Check these when working on related areas:

- `.claude/docs/architectural_patterns.md` — Registry pattern, plugin base classes, service layer, distributed locking,
  Celery conventions, storage architecture, partial failure handling, and other cross-cutting patterns with file:line
  references
- `.claude/docs/template_conventions.md` — **Read before writing or editing any HTML template.** Admin pages extend
  `wagtailadmin/generic/base.html` (slim header + breadcrumbs rendered from `breadcrumbs_items`; content in
  `{% block main_content %}` — never include `wagtailadmin/shared/header.html`), CSS/JS placement rules, and the
  `--w-color-*` / `--gr-*` CSS variable namespaces

## Adding new features or fixing bugs

**IMPORTANT**: When you work on a new feature or bug fix, create a git branch first. Then work on changes in that
branch for the reminder of the session

**IMPORTANT**: Run `make format` before committing Python changes — it sorts imports, applies ruff's safe fixes and
formats. CI runs `make lint` (the same checks, reporting only) and fails the PR on any difference. Rules live in
`ruff.toml` at the repo root and cover core and `titiler-app/` together; migrations and
`source-plugin-boilerplate/` are excluded.

## Agent skills

### Issue tracker

Issues live in GitHub Issues (`wmo-raf/georiva`). See `docs/agents/issue-tracker.md`.

### Triage labels

Using the five canonical default label strings. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context repo: one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.