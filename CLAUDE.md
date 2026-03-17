# GeoRiva

Geospatial Raster Ingestion, Visualization & Analysis platform. Ingests gridded raster data (GRIB, NetCDF, GeoTIFF) from
plugins or MinIO drop zones, processes into cloud-optimized formats (COG, encoded PNG), indexes as STAC-compliant
catalogs, and serves via STAC API, EDR API, and Titiler tile server.

Target domain: spatio-temporal environmental data, such as weather, climate, and ocean models, for African National
Meteorological Services.

## Tech Stack

- **Framework**: Django 5.x + Wagtail 7.x (CMS/admin)
- **Database**: PostgreSQL 16 + TimescaleDB + PostGIS
- **Object Storage**: MinIO (S3-compatible), multi-bucket (incoming, sources, archive, assets)
- **Task Queue**: Celery + Redis (two queues: `georiva-default`, `georiva-ingestion`)
- **Tile Server**: Titiler (FastAPI, separate container in `titiler-app/`)
- **Data Libraries**: xarray, rasterio, cfgrib, dask, geopandas
- **API**: Django REST Framework + drf-spectacular (OpenAPI)
- **Frontend**: STAC Browser (Radiant Earth), Vue.js for dashboards
- **Container**: Docker Compose (11 services), Nginx reverse proxy
- **Python**: 3.10+

## Project Structure

```
georiva/src/georiva/          # Main Django application
├── config/                   # Django settings, URLs, Celery, WSGI/ASGI
│   └── settings/             # base.py, dev.py, production.py
├── core/                     # STAC-aligned data models (Catalog, Collection, Variable, Item, Asset)
│   └── models/               # Split per-entity: catalog.py, collection.py, item.py, variable.py
│   └── storage.py            # Multi-bucket StorageManager singleton
├── ingestion/                # Data ingestion pipeline
│   ├── service.py            # Main IngestionService (orchestrates full pipeline)
│   ├── tasks.py              # Celery tasks (process_incoming_file, sweep_unprocessed)
│   ├── models.py             # IngestionLog (distributed locking, state machine)
│   ├── extractor.py          # Data extraction
│   ├── encoder.py            # PNG encoding
│   ├── asset_writer.py       # COG/PNG/JSON asset writers
│   └── clipper.py            # Boundary clipping
├── formats/                  # Format handler plugins (GRIB, NetCDF, GeoTIFF)
│   ├── base.py               # BaseFormatPlugin ABC
│   └── registry.py           # FormatRegistry (decorator-based registration)
├── sources/                  # Data source plugin framework
│   ├── source.py             # DataSource protocol + BaseDataSource ABC
│   ├── loader.py             # Loader orchestrates fetch → store
│   └── registry.py           # LoaderProfileViewSetRegistry
├── stac/                     # STAC API (views, serializers, URLs)
├── edr/                      # EDR API (views, serializers, URLs)
├── analysis/                 # Analysis engine with operator registry
├── visualization/            # Map visualization layer
├── pages/                    # Wagtail CMS pages (home, datasets)
├── sample_plugins/           # Example plugins (CHIRPS, ECMWF)
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
- `GEORIVA_PLUGIN_GIT_REPOS` — External plugin Git URLs

Settings split: `config/settings/base.py` (shared) → `dev.py` / `production.py` (overrides).

## API Endpoints

Defined in `api/urls.py:7-11`:

- `/api/stac/` — STAC API (collections, items, search, queryables)
- `/api/edr/` — Environmental Data Retrieval API
- `/api/webhook/` — MinIO event webhook
- `/admin/` — Wagtail CMS admin

## Key Conventions

- **Models**: Split into separate files under `core/models/`, re-exported from `__init__.py`
- **Plugins**: Register via decorator (`@FormatRegistry.register`) or programmatic `registry.register()`
- **Singletons**: `storage`, `format_registry`, `loader_profile_viewset_registry` — import from their modules
- **Celery tasks**: Late imports inside task body to avoid circular imports; `bind=True` + `acks_late=True`
- **Celery queues**: Heavy processing on `georiva-ingestion`, lightweight on `georiva-default`
- **No Celery retries**: `max_retries=0`; retries handled by `sweep_unprocessed` periodic task
- **Wagtail hooks**: Each app owns its admin integration via `wagtail_hooks.py`
- **Storage paths**: Time-partitioned: `{catalog}/{collection}/{variable}/{year}/{month}/{day}/`

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

## Adding new features or fixing bugs

**IMPORTANT**: When you work on a new feature or bug fix, create a git branch first. Then work on changes in that
branch for the reminder of the session