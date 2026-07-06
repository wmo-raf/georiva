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
`/georiva/plugins`) and, for each plugin folder, derives the app's import name from the package it
contains (the package under `src/`) rather than from the folder name — so a plugin checkout can use
any directory name. The derived names are added to `INSTALLED_APPS`. The dev plugin directory
(`GEORIVA_DEV_PLUGIN_DIR`, default `/georiva/dev-plugins`) is also scanned automatically when it exists.

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
git  = "https://github.com/wmo-raf/georiva-source-cds.git"
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

The dev workflow lets you work on plugin source with hot-reload, without rebuilding the image. Your
plugin repos are checked out into `dev-plugins/`, the whole folder is bind-mounted into the stack, and
every plugin inside it is installed as an editable package at container startup.

**Step 1** — copy the dev override template (it already mounts the whole `dev-plugins/` folder):

```bash
cp docker-compose.override.sample.yml docker-compose.override.yml
```

The template bind-mounts `./dev-plugins:/georiva/dev-plugins` into every backend service — there are
no per-plugin mount lines to add.

**Step 2** — clone the plugin repos you want to work on into `dev-plugins/`:

```bash
mkdir -p dev-plugins
git clone https://github.com/wmo-raf/georiva-source-cds.git dev-plugins/georiva-source-cds
```

The checkout directory name does not matter — discovery derives the import/app name from the package
under `src/`. `dev-plugins/` is gitignored, so these checkouts stay out of the core repo. You do **not**
need to declare dev plugins in `plugins.toml`; every subdirectory of `dev-plugins/` is auto-discovered.

**Step 3** — start the stack with the override:

```bash
make dev-up OV=1
# or: docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.override.yml up -d
```

At startup, `startup_plugin_setup` runs `pip install -e --no-build-isolation` against each plugin in
the live bind-mount (building with the setuptools already in the image — no network needed for the
build itself, only to fetch a plugin's own new dependencies). Settings auto-discovers the plugins and
adds them to `INSTALLED_APPS`, so edits to plugin source are reflected immediately.

**Step 4** — run migrations for your plugin's models:

```bash
make dev-makemigrations   # create migration files for new models
make dev-migrate          # apply them
```

> **Local (non-Docker) tooling:** a plugin checked out under `dev-plugins/` is also a member of the
> repo's uv workspace overlay. Run `uv sync --all-packages` at the repo root to install core and every
> checked-out plugin editable into one shared virtualenv — handy for IDE autocomplete, type-checking,
> and running tests outside Docker.

---

## Creating a new plugin

Use the cookiecutter template in `source-plugin-boilerplate/` to scaffold a new plugin. It generates a
flat repo (`georiva-source-<provider>/` with `pyproject.toml` + `src/<module>/` at its root):

```bash
uvx cookiecutter source-plugin-boilerplate
```

The generated plugin is a standard Django/Wagtail app. The key files to implement:

| File | Purpose |
|------|---------|
| `models.py` | **Required.** Your `DataFeed` subclass — operator config + the `COLLECTIONS` spec (collections/variables it creates, via `parse_collection_defs`) |
| `source.py` | **Required.** Your `BaseDataSource` subclass — what files to fetch and how to build each `FileRequest` |
| `apps.py` | A plain `AppConfig`. **No registration code** — GeoRiva auto-discovers every `DataFeed` subclass and builds its admin form + setup wizard |
| `migrations/` | Django migrations for your `DataFeed` model (run `makemigrations`) |
| `wagtail_hooks.py` | Optional — extra Wagtail admin customisation only; feed registration is automatic |

See the [Source Plugin Contract](../architecture/plugin-parameter-contract.md) for the full component
specification. Reference implementations: the standalone
[`georiva-source-chirps`](https://github.com/wmo-raf/georiva-source-chirps) and
[`georiva-source-ecmwf`](https://github.com/wmo-raf/georiva-source-ecmwf) plugins.

### Plugin package layout

A plugin is a flat Python package: its repository root **is** the package project. `pyproject.toml`
(PEP 621) and `src/<module>/` live at the repo root — there is no nested `plugins/<module>` directory,
no `setup.py`, and no `requirements/` directory.

```
georiva-my-plugin/            ← git repo root = package project
├── pyproject.toml            ← [project] metadata + [project.dependencies]
├── georiva_plugin_info.json
└── src/
    └── georiva_my_plugin/    ← the Python package (the Django app)
        ├── __init__.py
        ├── apps.py
        ├── models.py
        └── ...
```

The import/app name is the package directory under `src/` (`georiva_my_plugin` above) — a valid Python
identifier (lowercase, underscores, no hyphens). The **checkout directory** name is free: discovery
reads the name from the package, not the folder, so cloning the repo under its natural
`georiva-my-plugin` name works fine.

A plugin declares only its own extra dependencies in `[project.dependencies]`; it does **not** depend on
`georiva` — the core package is provided by the runtime environment (the image, or the dev workspace).

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
