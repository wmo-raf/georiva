# GeoRiva

**Geospatial Raster Ingestion, Visualization & Analysis**

GeoRiva is a geospatial backend platform for automated ingestion, processing, visualization, and analysis of gridded
raster data. Built on Django/Wagtail, it provides a plugin-driven architecture for pulling data from diverse sources,
serving it through modern standards-compliant APIs, and enabling analytical workflows on top of it.

> **Status:** Early development — architecture phase. See
> the [Architecture Design Document](docs/architecture/README.md) for the full system design and open discussion points.

---

## What It Does

- **Ingest** gridded data from multiple sources via plugin apps or by dropping files into a MinIO directory
- **Process** data into cloud-optimized formats (COG, encoded PNG) through an async Celery pipeline
- **Index** everything as STAC-compliant Catalogs, Collections, and Items with time-series optimized storage
- **Serve** tiles and data through a STAC API, Titiler tile server, and encoded PNGs for browser-side rendering
- **Analyze** data using pluggable modules that integrate with the Xarray-compatible scientific Python ecosystem
- **Visualize** with modern browser-side rendering (WeatherLayers GL), moving beyond legacy WMS

## Architecture at a Glance

![High-Level Architecture Diagram](docs/images/architecture-overview.png)

For the detailed architecture with diagrams, data model, and design decisions,
see [docs/architecture/README.md](docs/architecture/README.md).

---

## Tech Stack

| Component        | Technology                            |
|------------------|---------------------------------------|
| Core Framework   | Django 5.x + Wagtail                  |
| Database         | PostgreSQL 16 + TimescaleDB + PostGIS |
| Object Storage   | MinIO (S3-compatible)                 |
| Task Queue       | Celery + Redis                        |
| Tile Server      | Titiler                               |
| Data Formats     | COG, Zarr, Encoded PNG                |
| Messaging        | Mosquitto (MQTT)                      |
| Containerization | Docker Compose                        |

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Git

### Setup

TODO


---

## Project Structure

TODO

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

| Document                                                    | Description                                                |
|-------------------------------------------------------------|------------------------------------------------------------|
| [Architecture Design Document](docs/architecture/README.md) | Full system architecture, data model, and design decisions |
| [Contributing Guide](docs/contributing.md)                  | How to set up a dev environment and contribute             |

---

## License

[TBD]
