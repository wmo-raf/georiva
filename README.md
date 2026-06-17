# GeoRiva

> WARNING: This project is in active development and breaking changes may occur without backwards compatibility. Until
> we reach a stable release, we recommend treating the codebase as not production-ready. We welcome contributions and
> feedback on the architecture, but please be aware that APIs, data models, and plugin interfaces may change as we
> iterate.

**Geospatial Raster Ingestion, Visualization & Analysis**

GeoRiva is a geospatial backend platform for automated ingestion, processing, visualization, and analysis of gridded
raster data. Built on Django/Wagtail, it provides a plugin-driven architecture for pulling data from diverse sources,
serving it through modern standards-compliant APIs, and enabling analytical workflows on top of it.

> **Status:** Active development — core ingestion, data model, STAC API, tile serving, and analysis modules are built;
> some areas (EDR data-retrieval plane, generic analysis-plugin framework) are still in progress. See
> the [Architecture Design Document](docs/architecture/README.md) for the as-built design and open discussion points.

---

## What It Does

- **Ingest** gridded data from multiple sources via plugin apps or by dropping files into a MinIO directory
- **Process** data into cloud-optimized formats (COG, encoded PNG) through an async Celery pipeline
- **Index** everything as STAC-compliant Catalogs, Collections, and Items with time-series optimized storage
- **Serve** tiles and data through a STAC API, Titiler tile server, and encoded PNGs for browser-side rendering
- **Analyze** data using pluggable modules that integrate with the Xarray-compatible scientific Python ecosystem
- **Visualize** with modern browser-side rendering (WeatherLayers GL), moving beyond legacy WMS

## Architecture at a Glance

![High-Level Architecture Diagram](docs/images/georiva-architecture.png)

For the detailed architecture with diagrams, data model, and design decisions,
see [docs/architecture/README.md](docs/architecture/README.md).

---

## Tech Stack

| Component        | Technology                                            |
|------------------|-------------------------------------------------------|
| Core Framework   | Django 5.x + Wagtail 7.x                              |
| Database         | PostgreSQL 18 + TimescaleDB + PostGIS (via PgBouncer) |
| Object Storage   | MinIO (S3-compatible), multi-bucket                   |
| Task Queue       | Celery + Redis (two queues)                           |
| Tile Servers     | Titiler (raster COGs) + Martin (vector/MVT)           |
| Discovery APIs   | STAC API + OGC API – EDR                              |
| Data Formats     | COG, virtual Zarr (kerchunk), Encoded PNG             |
| Event Bus        | MinIO → Redis list → `minio-consumer`                 |
| Containerization | Docker Compose                                        |

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) (v2+)
- Git

### Quick Start (Production)

1. **Clone the repository**

   ```bash
   git clone https://github.com/wmo-raf/georiva.git
   cd georiva
   ```

2. **Configure environment variables**

   ```bash
   cp .env.sample .env
   ```

   Edit `.env` and set the required values. At minimum, you need to set:

    - `SECRET_KEY` — Django secret key
    - `GEORIVA_DB_USER`, `GEORIVA_DB_NAME`, `GEORIVA_DB_PASSWORD` — database credentials
    - `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` — MinIO credentials
    - `ALLOWED_HOSTS` — comma-separated list of allowed hostnames
    - `CSRF_TRUSTED_ORIGINS` — comma-separated list of trusted origins

   See `.env.sample` for the full list of options. (MinIO events are delivered to a Redis list and drained by the
   `minio-consumer` service — there is no webhook endpoint or token to configure.)

3. **Start the stack**

   ```bash
   docker compose up -d
   ```

   On first run, the entrypoint automatically handles database migrations and static file collection.

4. **Access GeoRiva**

   Open [http://localhost](http://localhost) in your browser.

   Additional services:
    - **STAC Browser:** [http://localhost/stac-browser/](http://localhost/stac-browser/)

### Development Setup

The dev setup uses a compose override that mounts your source code for hot reloading.

1. **Follow steps 1–2 from Quick Start above.**

2. **Start with the dev override**

   ```bash
   docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
   ```

   This gives you:

    - **Django dev server** with auto-reload on code changes
    - **Celery worker** with auto-reload
    - **Source code mounted** from `./georiva` into the container

   Optionally, create a shortcut in your shell:

   ```bash
   alias dc-dev="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
   ```

3. **Useful commands**

   ```bash
   # View logs
   docker compose logs -f georiva

   # Run management commands
   docker compose exec georiva python manage.py createsuperuser
   docker compose exec georiva python manage.py shell

   # Rebuild after dependency changes
   docker compose build georiva

   # Restart a single service
   docker compose restart georiva-celery-worker
   ```

### Installing Plugins

Plugins can be installed at build time or at runtime.

**Build time** — declare plugins in `plugins.toml` (copy `plugins.toml.sample` and edit), then build:

```bash
docker compose build georiva
```

**Runtime** — set `GEORIVA_PLUGIN_URLS` in your `.env` file and restart. Ensure
`GEORIVA_DISABLE_PLUGIN_INSTALL_ON_STARTUP` is not set to `"true"`.

## Project Structure

```
georiva/src/georiva/      # Main Django/Wagtail application
├── config/               # Settings (base/dev/production), URLs, Celery, WSGI/ASGI
├── core/                 # STAC-aligned data models, multi-bucket storage, filename conventions
├── ingestion/            # Async ingestion pipeline, IngestionLog, MinIO event consumer
├── formats/              # Format handler plugins (GRIB, NetCDF, GeoTIFF) + registry
├── sources/              # Source-plugin framework (DataSource, LoaderProfile, fetch strategies)
├── stac/                 # STAC API
├── edr/                  # OGC API – EDR (metadata plane)
├── analysis/             # Time-series + zonal-statistics modules
├── virtual_zarr/         # Per-Variable virtual Zarr (kerchunk) manifests
├── visualization/        # Wagtail admin hooks for map/tile config
└── pages/                # Wagtail CMS pages

titiler-app/              # Custom Titiler tile server (FastAPI)
source-plugin-boilerplate/# Cookiecutter template for new source plugins
deploy/                   # Nginx, Martin, plugin install scripts
docs/                     # Architecture, data model, plugin, and storage docs
```

For a deeper map of conventions and patterns, see [`docs/`](docs/README.md).

---

## Contributing

GeoRiva is in its early stages and contributions are welcome — especially feedback on the architecture.

**Where to start:**

1. Read the [Architecture Design Document](docs/architecture/README.md) to understand the system design
2. Check the [Open Questions](docs/architecture/README.md#9-open-questions--discussion-points) section for areas where
   input is needed
3. See [docs/contributing.md](docs/contributing.md) for development setup and guidelines

**Ways to contribute:**

- Review and comment on the architecture
- Build a source plugin for a data provider you know well
- Build an analysis module for your domain
- Improve documentation
- Report bugs and suggest features via issues

---

## Documentation

Start at the [documentation index](docs/README.md), which ties everything together. Key documents:

| Document                                                                         | Description                                                |
|----------------------------------------------------------------------------------|------------------------------------------------------------|
| [Documentation Index](docs/README.md)                                            | Map of all docs and a suggested reading order              |
| [Architecture Design Document](docs/architecture/README.md)                      | Full system architecture, data model, and design decisions |
| [Data Model Guide](docs/georiva-data-model-guide.md)                             | How to organize data into Catalogs, Collections, Variables |
| [Format Plugin System](docs/format-plugins.md)                                   | Reading GRIB/NetCDF/GeoTIFF; writing a new format plugin   |
| [Storage & Ingestion Architecture](docs/plugins/georiva-storage-architecture.md) | Buckets, event-driven ingestion, IngestionLog              |
| [Download Deduplication](docs/architecture/download-dedup.md)                    | Multi-collection feeds and download dedup                  |
| [Plugin Parameter Contract](docs/architecture/plugin-parameter-contract.md)      | Proposed declarative parameter manifest (RFC)              |
| [Contributing Guide](docs/contributing.md)                                       | How to set up a dev environment and contribute             |

---

## License

[TBD]
