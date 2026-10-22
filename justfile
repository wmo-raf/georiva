# GeoRiva dev tasks. Dev only: production uses plain `docker compose`.
# Run `just` to list recipes. Needs just >= 1.58 (https://just.systems).

set dotenv-load
set shell := ["bash", "-euo", "pipefail", "-c"]

# docker-compose.override.yml is gitignored and per-developer (e.g. it mounts
# dev-plugins/); it joins the stack automatically when the file exists.
override := if path_exists(justfile_directory() / "docker-compose.override.yml") == "true" { " -f docker-compose.override.yml" } else { "" }
dc := "docker compose -f docker-compose.yml -f docker-compose.dev.yml" + override

# Short names for compose services. Anything not listed passes through as is,
# so `just logs georiva-db` works too.
svc_fn := '''
svc() {
  case "$1" in
    app) echo georiva ;;
    default) echo georiva-celery-default-worker ;;
    ingestion) echo georiva-celery-ingestion-worker ;;
    processing) echo georiva-celery-processing-worker ;;
    beat) echo georiva-celery-beat ;;
    titiler) echo georiva-titiler-app ;;
    *) echo "$1" ;;
  esac
}
'''

# Resolve the aliases in $@ into a `targets` array.
resolve := svc_fn + '''
targets=()
for s in "$@"; do targets+=("$(svc "$s")"); done
'''

_default:
    @just --list

# ======================
# Stack
# ======================

# Start the stack; extra args go to compose, e.g. `just up -d`
up *args:
    {{ dc }} up {{ args }}

# Stop and remove the stack
down *args:
    {{ dc }} down {{ args }}

# Stop containers without removing them
stop *args:
    {{ dc }} stop {{ args }}

# Restart all services, or the named ones: `just restart ingestion beat`
[positional-arguments]
restart *services:
    #!/usr/bin/env bash
    set -euo pipefail
    {{ resolve }}
    {{ dc }} restart ${targets[@]+"${targets[@]}"}

# Build images; extra args go to compose
build *args:
    {{ dc }} build {{ args }}

# List containers
ps:
    {{ dc }} ps

# Print the resolved compose config
config:
    {{ dc }} config

# Follow logs for all services, or the named ones: `just logs ingestion`
[positional-arguments]
logs *services:
    #!/usr/bin/env bash
    set -euo pipefail
    {{ resolve }}
    {{ dc }} logs -f --tail 100 ${targets[@]+"${targets[@]}"}

# Shell into a service (default: app): `just shell ingestion`
[positional-arguments]
shell service="app":
    #!/usr/bin/env bash
    set -euo pipefail
    {{ svc_fn }}
    {{ dc }} exec "$(svc "$1")" bash

# ======================
# Django
# ======================

migrate:
    {{ dc }} exec georiva georiva migrate

makemigrations:
    {{ dc }} exec georiva georiva makemigrations

# Django tests bypass PgBouncer (it cannot CREATE DATABASE) and connect straight
# to georiva-db with the credentials from .env. They also get their own Redis
# database (15): the stack's cache and broker live in 0, and a test run that
# shared it left `testserver` in Wagtail's cached site root URLs and could hand
# test-enqueued tasks to the live workers.
# e.g. `just test georiva.ingestion.tests -v 2`
test *args:
    {{ dc }} exec \
      -e "DATABASE_URL=timescalegis://${GEORIVA_DB_USER}:${GEORIVA_DB_PASSWORD}@georiva-db:5432/${GEORIVA_DB_NAME}" \
      -e "REDIS_URL=redis://georiva-redis:6379/15" \
      georiva georiva test --keepdb {{ args }}

# Titiler tests run on the host (the image ships no tests): pytest at the HTTP
# boundary with Redis, Django and storage faked, see titiler-app/tests/.
# e.g. `just titiler-test tests/test_wmts_kvp_gettile.py -v`
titiler-test *args:
    cd titiler-app && uv run --no-project --with-requirements requirements-dev.txt pytest {{ args }}

# ======================
# Core dependencies (uv, on the host, not in Docker)
# ======================
# Core's deps live in georiva/pyproject.toml + georiva/uv.lock, a standalone uv
# project. These recipes edit them from the core dir, then refresh the
# integrated dev venv (core + any plugins under dev-plugins/). Commit
# georiva/pyproject.toml + georiva/uv.lock afterwards (the root uv.lock is
# gitignored). Quote the package when it has a version specifier or extras.

core_dir := "georiva"

# --no-sync updates pyproject.toml + uv.lock without creating a separate core
# venv; the overlay `uv sync --all-packages` is the env.

# Add a runtime package: `just uv-add "wagtail-foo>=1.2"`
uv-add pkg:
    cd {{ core_dir }} && uv add --no-sync "{{ pkg }}"
    uv sync --all-packages

# Add a dev-only package: `just uv-add-dev pytest`
uv-add-dev pkg:
    cd {{ core_dir }} && uv add --dev --no-sync "{{ pkg }}"
    uv sync --all-packages

# Remove a package: `just uv-remove wagtail-foo`
uv-remove pkg:
    cd {{ core_dir }} && uv remove --no-sync "{{ pkg }}"
    uv sync --all-packages

# Re-lock core (e.g. after hand-editing georiva/pyproject.toml), then refresh
uv-lock:
    cd {{ core_dir }} && uv lock
    uv sync --all-packages

# Refresh the integrated dev venv (core + all checked-out plugins)
uv-sync:
    uv sync --all-packages

# ======================
# Lint & format (ruff, on the host, not in Docker)
# ======================
# Rules live in ./ruff.toml and cover core and the Titiler app together. The
# version is pinned: unpinned, a ruff release changes CI's mind about code
# nobody touched. CI runs `just lint`, so this is the only place it is set.

ruff := "ruff@0.15.20"

# What CI runs. Reports, changes nothing.
lint:
    uvx {{ ruff }} check .
    uvx {{ ruff }} format --check .

# What to run before committing: sorts imports, applies safe fixes, formats.
format:
    uvx {{ ruff }} check --fix .
    uvx {{ ruff }} format .
