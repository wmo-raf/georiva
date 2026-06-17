# {{ cookiecutter.project_name }}

{{ cookiecutter.project_description }}

This is a **GeoRiva source plugin** — a standalone Django/Wagtail app that plugs
a remote data provider into a [GeoRiva](https://github.com/wmo-raf/georiva)
instance. It is installed into a running GeoRiva stack (not run on its own).

## What's in this plugin

| File | Purpose |
| --- | --- |
| `src/{{ cookiecutter.project_module }}/models.py` | The `DataFeed` subclass — operator config + the `COLLECTIONS` spec (what collections/variables it creates) |
| `src/{{ cookiecutter.project_module }}/source.py` | The `BaseDataSource` subclass — what files to fetch and how to build each `FileRequest` |
| `src/{{ cookiecutter.project_module }}/apps.py` | Plain `AppConfig` — **no registration code needed** |
| `src/{{ cookiecutter.project_module }}/wagtail_hooks.py` | Optional Wagtail admin customisation |

GeoRiva **auto-discovers** your `DataFeed` subclass and builds its admin form and
setup wizard. You do not register anything by hand — just define the `DataFeed`
and `BaseDataSource`, and make sure the package is on the plugin path.

## The plugin contract

1. **`DataFeed` subclass** (`models.py`) — a Wagtail snippet holding operator
   configuration. Implement:
   - `get_collection_definitions()` → built from the `COLLECTIONS` dict via
     `parse_collection_defs()`
   - `get_catalog_defaults()` → pre-fills the wizard's catalog step
   - `data_source_cls` → returns your `BaseDataSource` class
   - `get_loader_config()` → feed-wide settings passed to the data source
2. **`BaseDataSource` subclass** (`source.py`) — set `type` and `label`, accept a
   `fetch_strategy` (e.g. the built-in `HTTPFetchStrategy`), and implement
   `generate_requests(start_time, end_time, variables=None)` to yield one
   `FileRequest` per file. Optionally override `get_latest_available()` and
   `post_process_fetched_file()`.

For full reference implementations, see the standalone
[`georiva-source-chirps`](https://github.com/wmo-raf/georiva-source-chirps) (GeoTIFF /
derived) and [`georiva-source-ecmwf`](https://github.com/wmo-raf/georiva-source-ecmwf)
(GRIB / forecast) plugins, and the
[Source Plugin Contract](https://github.com/wmo-raf/georiva/blob/main/docs/architecture/plugin-parameter-contract.md).

## Getting started

### Prerequisites

- Docker and Docker Compose.
- A locally built GeoRiva core image (`georiva:latest`). Follow the
  [GeoRiva core repository](https://github.com/wmo-raf/georiva) to build it.

### Build & run this plugin

`dev.Dockerfile` uses the `georiva:latest` image as its base and installs this
plugin into it. Source is bind-mounted, so edits hot-reload the dev server.

1. Clone and enter the plugin:

   ```bash
   git clone https://github.com/wmo-raf/{{ cookiecutter.project_slug }}.git
   cd {{ cookiecutter.project_slug }}
   ```

2. Create the `.env` file and set the build UID/GID to your user:

   ```bash
   cp .env.sample .env
   # set PLUGIN_BUILD_UID=$(id -u) and PLUGIN_BUILD_GID=$(id -g), plus DB values
   ```

3. Build and start:

   ```bash
   docker compose build      # add DOCKER_BUILDKIT=0 if you hit a base-image pull error
   docker compose up
   ```

4. Create a superuser and run this plugin's migrations:

   ```bash
   docker compose exec georiva georiva createsuperuser
   docker compose exec georiva georiva makemigrations {{ cookiecutter.project_module }}
   docker compose exec georiva georiva migrate
   ```

   (`georiva` is shorthand for `python manage.py`.)

The app is at `http://localhost:8000` (change via `PORT` in `.env`). In the
GeoRiva admin, open **Automated Sources → Set up wizard** and choose your feed.

### Developing against an existing GeoRiva stack

Alternatively, bind-mount this plugin's package into a running GeoRiva dev stack
by adding it to that repo's `docker-compose.override.yml`:

```yaml
- ../{{ cookiecutter.project_slug }}/plugins/{{ cookiecutter.project_module }}:/georiva/dev-plugins/{{ cookiecutter.project_module }}
```

GeoRiva installs every folder under `/georiva/dev-plugins` as an editable package
and adds it to `INSTALLED_APPS` automatically.
