# Plugin Installation Guide

This guide covers how to install and develop GeoRiva source plugins — on a server, in an image you build
yourself, and in local development against a running stack.

**Which section do I want?** A server runs the images CI publishes to `ghcr.io/wmo-raf/` ("published images")
and builds nothing — that is the normal way to run GeoRiva, and its plugins go in `deploy/plugins.toml`. Only
a site that builds its own image (a fork, or its own CI) bakes plugins in with the repo-root `plugins.toml`.

---

## Table of Contents

- [How the plugin system works](#how-the-plugin-system-works)
- [Servers (published images)](#servers-published-images)
- [Building your own image](#building-your-own-image)
- [Deprecated: runtime install (GEORIVA_PLUGIN_URLS)](#deprecated-runtime-install-georiva_plugin_urls)
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

## Servers (published images)

A server running the published `ghcr.io/wmo-raf/georiva` images (see `deploy/README.md`) has no
`plugins.toml` baked in and builds nothing. It installs plugins once, into a volume every service shares ([ADR 0035](../adr/0035-source-plugins-install-once-into-a-shared-volume.md)):

```bash
cd deploy
cp plugins.toml.sample plugins.toml   # then list your plugins (git + tag, or url)
./deploy.sh v0.5.0                    # or: docker compose up -d
```

The one-shot `georiva-plugins` service installs the plugins and their dependencies, pinned to the versions core
ships, then exits; the other services wait for it. Re-running is instant while `plugins.toml` and the release are
unchanged; editing the manifest or upgrading reinstalls. `docker compose run --rm georiva-plugins install-plugins --force`
reinstalls regardless.

Limits: it needs internet at install time (no air-gapped mode yet); dependencies need a prebuilt linux/amd64
wheel (the image has no compilers); and `build.sh` / `runtime_setup.sh` must be empty of commands, as they are not
run. A plugin's `requires_env` variables must be passed to the services in `docker-compose.override.yml`; the
installer warns when one is unset in its own container.

## Building your own image

For sites that fork the repo or run their own CI. Plugins declared in the repo-root `plugins.toml` are baked
into the Docker image at build time, so the image is self-contained and starts fast. A server running the
published images does not do this — see [Servers](#servers-published-images).

**Step 1** — copy the sample manifest and edit it:

```bash
cp plugins.toml.sample plugins.toml
```

**Step 2** — declare your plugins. Supported source types:

```toml
# From a GitHub release tag (recommended: reproducible)
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
hash = "abc123def456"   # optional SHA-1 of the plugin contents (on a server, hash is the sha256 of the .tar.gz)

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

The `GEORIVA_DISABLE_PLUGIN_INSTALL_ON_STARTUP` flag is set to `"true"` for the services
in `docker-compose.yml` — plugins are already baked in, so startup skips the install phase.

---

## Deprecated: runtime install (GEORIVA_PLUGIN_URLS)

> **Deprecated — will be removed in the next release.** Setting `GEORIVA_PLUGIN_URLS` installs the plugins in
> every container, on every start, from PyPI with unpinned dependencies; nothing is shared and nothing survives
> the container being recreated. Use [Servers (published images)](#servers-published-images) instead: list the
> same tarball in `deploy/plugins.toml` as a `url = "..."` entry.

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
just up    # picks up docker-compose.override.yml automatically when it exists
# or: docker compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.override.yml up -d
```

At startup, `startup_plugin_setup` runs `pip install -e --no-build-isolation` against each plugin in
the live bind-mount (building with the setuptools already in the image — no network needed for the
build itself, only to fetch a plugin's own new dependencies). Settings auto-discovers the plugins and
adds them to `INSTALLED_APPS`, so edits to plugin source are reflected immediately.

**Step 4** — run migrations for your plugin's models:

```bash
just makemigrations   # create migration files for new models
just migrate          # apply them
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
| `GEORIVA_PLUGIN_DIRS` | `/georiva/plugins,/georiva/site-plugins/src` | Comma-separated list of dirs settings scans for `INSTALLED_APPS` |
| `GEORIVA_DEV_PLUGIN_DIR` | `/georiva/dev-plugins` | Dir for bind-mounted dev plugins; auto-added to `GEORIVA_PLUGIN_DIRS` when it exists |
| `GEORIVA_PLUGIN_URLS` | _(empty)_ | **Deprecated.** Comma-separated tarball URLs to download and install at startup |
| `GEORIVA_DISABLE_PLUGIN_INSTALL_ON_STARTUP` | _(unset)_ | **Deprecated.** Set to `"true"` to skip all startup plugin installation |
| `GEORIVA_STRICT_PLUGIN_ENV` | `false` | When `true`, a plugin's missing required `requires_env` variable is a `manage.py check` error instead of a warning |
