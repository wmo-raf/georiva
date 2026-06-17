# Plugin Installation Guide

This guide covers how to install and develop GeoRiva source plugins — for both production deployments
and local development against a running stack.

---

## Table of Contents

- [How the plugin system works](#how-the-plugin-system-works)
- [Production installation](#production-installation)
  - [Method 1: Build-time (plugins.toml)](#method-1-build-time-pluginstoml)
  - [Method 2: Runtime (GEORIVA_PLUGIN_URLS)](#method-2-runtime-georiva_plugin_urls)
- [Local development](#local-development)
- [Creating a new plugin](#creating-a-new-plugin)
- [Reference: environment variables](#reference-environment-variables)

---

## How the plugin system works

A GeoRiva plugin is a standard Python package that is also a Django/Wagtail app. The lifecycle has
two phases:

**Install phase** — the package is installed into the virtualenv (via `pip install`). This makes
the Python code importable. It is handled by `deploy/plugins/install_plugin.sh` (build-time) or
`startup_plugin_setup` in `deploy/plugins/utils.sh` (runtime).

**Discovery phase** — Django needs to know the app label so it can load models, migrations, signals,
and admin hooks. `settings/base.py` scans the directories listed in `GEORIVA_PLUGIN_DIRS` (default:
`/georiva/plugins`) and automatically adds every subdirectory name to `INSTALLED_APPS`. The dev
plugin directory (`GEORIVA_DEV_PLUGIN_DIR`, default `/georiva/dev-plugins`) is also scanned
automatically when it exists.

---

## Production installation

### Method 1: Build-time (plugins.toml)

Plugins declared in `plugins.toml` are baked into the Docker image at build time. This is the
recommended approach for production — the image is self-contained and container startup is fast.

**Step 1** — copy the sample manifest and edit it:

```bash
cp plugins.toml.sample plugins.toml
```

**Step 2** — declare your plugins. Supported source types:

```toml
# From a GitHub release tag (recommended for production)
[[plugins]]
name = "GeoRiva CDS Plugin"
git  = "https://github.com/wmo-raf/georiva-cds-plugin.git"
tag  = "1.2.0"

# From any Git repository (latest default branch)
[[plugins]]
name = "My Plugin"
git  = "https://github.com/org/my-plugin.git"

# From a direct tarball URL
[[plugins]]
name = "My Plugin"
url  = "https://example.com/my-plugin-1.0.tar.gz"
hash = "abc123def456"   # optional SHA-1 integrity check

# Disabled — kept in file but skipped during install
[[plugins]]
name    = "Old Plugin"
git     = "https://github.com/org/old-plugin.git"
enabled = false
```

**Step 3** — build and start:

```bash
docker compose build georiva
docker compose up -d
```

The `GEORIVA_DISABLE_PLUGIN_INSTALL_ON_STARTUP` flag is set to `"true"` for the production service
in `docker-compose.yml` — plugins are already baked in, so startup skips the install phase.

---

### Method 2: Runtime (GEORIVA_PLUGIN_URLS)

For deployments where rebuilding the image is impractical, plugins can be downloaded and installed
when the container starts. Add to your `.env`:

```bash
GEORIVA_PLUGIN_URLS=https://example.com/plugin1.tar.gz,https://example.com/plugin2.tar.gz
```

Remove or set to `false`:

```bash
GEORIVA_DISABLE_PLUGIN_INSTALL_ON_STARTUP=false
```

Then restart:

```bash
docker compose restart georiva georiva-celery-default-worker georiva-celery-ingestion-worker georiva-celery-beat
```

> **Note:** Runtime installation adds to container startup time and requires network access on every
> cold start. Build-time baking is preferred for production.

---

## Local development

The dev workflow lets you work on plugin source code with hot-reload, without rebuilding the image.
Plugin source is bind-mounted into the container and installed as an editable package.

**Step 1** — copy the override template:

```bash
cp docker-compose.override.sample.yml docker-compose.override.yml
```

**Step 2** — edit `docker-compose.override.yml` to bind-mount your plugin source. Mount the same
path into every backend service so workers and the beat scheduler also see the plugin:

```yaml
services:
  georiva:
    volumes:
      - ../georiva-my-plugin/plugins/georiva_my_plugin:/georiva/dev-plugins/georiva_my_plugin

  georiva-celery-default-worker:
    volumes:
      - ../georiva-my-plugin/plugins/georiva_my_plugin:/georiva/dev-plugins/georiva_my_plugin

  georiva-celery-ingestion-worker:
    volumes:
      - ../georiva-my-plugin/plugins/georiva_my_plugin:/georiva/dev-plugins/georiva_my_plugin

  georiva-celery-beat:
    volumes:
      - ../georiva-my-plugin/plugins/georiva_my_plugin:/georiva/dev-plugins/georiva_my_plugin
```

**Step 3** — declare the plugin in `plugins.toml` with `dev = true`:

```toml
[[plugins]]
name   = "My Plugin (dev)"
folder = "/georiva/dev-plugins/georiva_my_plugin"
dev    = true
```

The `dev = true` flag tells `install_plugin.sh` to use `pip install -e` (editable install). If the
folder is absent at build time (which it will be — it's a bind-mount), the script skips gracefully.
At container startup, `startup_plugin_setup` runs `pip install -e` against the live bind-mount, so
changes to the plugin source are reflected immediately.

Settings auto-discovers plugins in `/georiva/dev-plugins/` and adds them to `INSTALLED_APPS` — no
manual configuration needed.

**Step 4** — start the stack with the override:

```bash
make dev-up OV=1
```

Or without the Makefile:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.override.yml up -d
```

**Step 5** — run migrations for your plugin's models:

```bash
make dev-migrate
# or: docker compose exec georiva georiva migrate
```

> **Tip:** After adding new models to your plugin, run `make dev-makemigrations` inside the container
> to create the migration files, then `make dev-migrate` to apply them.

---

## Creating a new plugin

Use the cookiecutter template in `source-plugin-boilerplate/` to scaffold a new plugin:

```bash
cd source-plugin-boilerplate
pip install cookiecutter
cookiecutter .
```

The generated plugin is a standard Django/Wagtail app. The key files to implement:

| File | Purpose |
|------|---------|
| `models.py` | `DataFeed` subclass — operator config, scheduling, collection factory |
| `sources.py` | `BaseDataSource` subclass — fetch logic, file construction |
| `collection_definitions.py` | `CollectionDefinition` list — what collections and variables the plugin creates |
| `wagtail_hooks.py` | Wagtail admin registration |
| `migrations/` | Django migrations for your models |

See the [Source Plugin Contract](../architecture/plugin-parameter-contract.md) for the full component
specification. Reference implementations: `georiva/src/georiva/sample_plugins/chirps/` and
`georiva/src/georiva/sample_plugins/ecmwf_opendata_source/`.

### Plugin package layout

GeoRiva supports two layouts:

**Flat layout** (package at repo root's `plugins/` subdirectory):
```
georiva-my-plugin/
└── plugins/
    └── georiva_my_plugin/       ← the Python package (also the Django app label)
        ├── __init__.py
        ├── apps.py
        ├── models.py
        └── ...
```

**Src layout** (package under `src/`):
```
georiva-my-plugin/
└── plugins/
    └── georiva_my_plugin/
        └── src/
            └── georiva_my_plugin/
                ├── __init__.py
                └── ...
```

`install_plugin.sh` expects exactly one subdirectory under `plugins/` in the repo. The subdirectory
name becomes the Django app label and must be a valid Python identifier (lowercase, underscores only
— no hyphens).

### Distributing your plugin

Once the plugin is ready for production use:

1. Push to a GitHub repository following the layout above
2. Create a release tag (e.g. `1.0.0`)
3. Operators declare it in their `plugins.toml`:

```toml
[[plugins]]
name = "My Plugin"
git  = "https://github.com/org/georiva-my-plugin.git"
tag  = "1.0.0"
```

---

## Reference: environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GEORIVA_PLUGIN_DIR` | `/georiva/plugins` | Where baked and runtime plugins are copied on install |
| `GEORIVA_PLUGIN_DIRS` | `/georiva/plugins` | Comma-separated list of dirs settings scans for `INSTALLED_APPS` |
| `GEORIVA_DEV_PLUGIN_DIR` | `/georiva/dev-plugins` | Dir for bind-mounted dev plugins; auto-added to `GEORIVA_PLUGIN_DIRS` when it exists |
| `GEORIVA_PLUGIN_URLS` | _(empty)_ | Comma-separated tarball URLs to download and install at startup |
| `GEORIVA_DISABLE_PLUGIN_INSTALL_ON_STARTUP` | _(unset)_ | Set to `"true"` to skip all startup plugin installation (used in prod where plugins are baked in) |
