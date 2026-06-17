# {{ cookiecutter.project_name }}

{{ cookiecutter.project_description }}

This is a **GeoRiva source plugin** — a standalone Python package (a Django/Wagtail
app) that plugs a remote data provider into a
[GeoRiva](https://github.com/wmo-raf/georiva) instance. It is **not a service you
run on its own**: it is installed into a running GeoRiva stack, which provides the
database, Redis, MinIO, Titiler, and the workers.

## What's in this plugin

| File | Purpose |
| --- | --- |
| `src/{{ cookiecutter.project_module }}/models.py` | The `DataFeed` subclass — operator config + the `COLLECTIONS` spec (what collections/variables it creates) |
| `src/{{ cookiecutter.project_module }}/source.py` | The `BaseDataSource` subclass — what files to fetch and how to build each `FileRequest` |
| `src/{{ cookiecutter.project_module }}/apps.py` | Plain `AppConfig` — **no registration code needed** |
| `src/{{ cookiecutter.project_module }}/wagtail_hooks.py` | Optional Wagtail admin customisation |
| `georiva_plugin_info.json` | Plugin metadata (name, version, declared `requires_env`) |

GeoRiva **auto-discovers** your `DataFeed` subclass and builds its admin form and
setup wizard. You do not register anything by hand — just define the `DataFeed`
and `BaseDataSource`.

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

## Developing against the GeoRiva dev stack

A plugin is developed by **bind-mounting it into the core GeoRiva dev stack** —
that stack provides the database, Redis, MinIO and workers your plugin needs.
There is no plugin-only Docker image to build.

1. **Get the core dev stack running.** Clone
   [GeoRiva](https://github.com/wmo-raf/georiva) and build/run it per its README
   (`make dev-build`).

2. **Clone this plugin** somewhere the core stack can reach by relative path — by
   convention, a sibling of the core repo:

   ```bash
   git clone https://github.com/wmo-raf/{{ cookiecutter.project_slug }}.git
   ```

3. **Bind-mount the package** into the core stack. In the core repo's
   `docker-compose.override.yml` (copy it from `docker-compose.override.sample.yml`,
   which has a commented template), add this plugin's package to the dev-plugin
   volumes for every backend service:

   ```yaml
   - ../{{ cookiecutter.project_slug }}/plugins/{{ cookiecutter.project_module }}:/georiva/dev-plugins/{{ cookiecutter.project_module }}
   ```

   GeoRiva installs every folder under `/georiva/dev-plugins` as an editable
   package (`pip install -e`, pulling your `requirements/base.txt`) and adds it to
   `INSTALLED_APPS` automatically — edits hot-reload.

4. **Start the stack with the override and run your migrations:**

   ```bash
   make dev-up OV=1
   make dev-makemigrations          # generates this plugin's migrations
   make dev-migrate
   ```

5. In the GeoRiva admin, open **Automated Sources → Set up wizard** and choose
   your feed.

## Configuration

If your source needs credentials or other runtime settings, read them from the
process environment (`os.environ` / your data source's config) and **declare the
variable names** in `georiva_plugin_info.json`:

```json
"requires_env": [
  {"name": "EXAMPLE_API_KEY", "required": true,
   "description": "API key for the upstream provider (https://example.com/account)"},
  {"name": "EXAMPLE_API_URL", "required": false,
   "description": "Override the default endpoint"}
]
```

`requires_env` is **declaration only** — it lists the variable *names* a plugin
needs, never their values. Operators supply the actual values in the **GeoRiva
stack's `.env`** (or the service `environment`), so they are available to the
web and worker processes at runtime. Leave `requires_env` as `[]` if your plugin
needs none.

## Installing in production

Operators install this plugin by declaring it in their GeoRiva `plugins.toml`
(baked into the image at build time):

```toml
[[plugins]]
name = "{{ cookiecutter.project_name }}"
git  = "https://github.com/wmo-raf/{{ cookiecutter.project_slug }}.git"
tag  = "1.0.0"
```

Then they set any `requires_env` values in their stack `.env`, rebuild, and run
this plugin's migrations.
