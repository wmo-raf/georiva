# ======================
# Compose Definitions
# ======================

PROD = -f docker-compose.yml
DEV  = -f docker-compose.yml -f docker-compose.dev.yml

# Optional override: make <target> OV=1
# Includes docker-compose.override.yml in DEV commands
ifdef OV
DEV += -f docker-compose.override.yml
endif

DC     = docker compose $(PROD)
DEV_DC = docker compose $(DEV)

# Main services (match your docker-compose.yml)
APP ?= georiva
WORKER_DEFAULT ?= georiva-celery-default-worker
WORKER_INGESTION ?= georiva-celery-ingestion-worker
WORKER_PROCESSING ?= georiva-celery-processing-worker
BEAT ?= georiva-celery-beat
TITILER ?= georiva-titiler-app

LOG_ARGS ?= --tail 100

.PHONY: \
	up down stop restart build ps logs \
	app-logs worker-logs beat-logs \
	shell worker-shell beat-shell \
	migrate makemigrations \
	dev-up dev-up-d dev-down dev-stop dev-restart dev-build dev-ps dev-config dev-logs \
	dev-app-logs dev-worker-default-logs dev-worker-ingestion-logs dev-beat-logs dev-titiler-logs \
	dev-shell dev-worker-default-shell dev-worker-ingestion-shell dev-beat-shell dev-titiler-shell \
	dev-migrate dev-makemigrations dev-test \
	uv-add uv-add-dev uv-remove uv-lock uv-sync

# ======================
# PROD (default)
# ======================

up:
	$(DC) up -d

down:
	$(DC) down

stop:
	$(DC) stop

restart:
	$(DC) restart

build:
	$(DC) build

ps:
	$(DC) ps

logs:
	$(DC) logs -f $(LOG_ARGS)

app-logs:
	$(DC) logs -f $(APP) $(LOG_ARGS)

worker-logs:
	$(DC) logs -f $(WORKER) $(LOG_ARGS)

beat-logs:
	$(DC) logs -f $(BEAT) $(LOG_ARGS)

shell:
	$(DC) exec $(APP) bash

worker-shell:
	$(DC) exec $(WORKER) bash

beat-shell:
	$(DC) exec $(BEAT) bash

migrate:
	$(DC) exec $(APP) georiva migrate

makemigrations:
	$(DC) exec $(APP) georiva makemigrations


# ======================
# DEV
# ======================

dev-up:
	$(DEV_DC) up

dev-up-d:
	$(DEV_DC) up -d

dev-down:
	$(DEV_DC) down

dev-stop:
	$(DEV_DC) stop

dev-restart:
	$(DEV_DC) restart

dev-build:
	$(DEV_DC) build

dev-ps:
	$(DEV_DC) ps

dev-config:
	$(DEV_DC) config

dev-logs:
	$(DEV_DC) logs -f $(LOG_ARGS)

dev-app-logs:
	$(DEV_DC) logs -f $(APP) $(LOG_ARGS)

dev-worker-default-logs:
	$(DEV_DC) logs -f $(WORKER_DEFAULT)

dev-worker-ingestion-logs:
	$(DEV_DC) logs -f $(WORKER_INGESTION)

dev-worker-processing-logs:
	$(DEV_DC) logs -f $(WORKER_PROCESSING)

dev-beat-logs:
	$(DEV_DC) logs -f $(BEAT) $(LOG_ARGS)

dev-titiler-logs:
	$(DEV_DC) logs -f $(TITILER) $(LOG_ARGS)

dev-shell:
	$(DEV_DC) exec $(APP) bash

dev-worker-default-shell:
	$(DEV_DC) exec $(WORKER_DEFAULT) bash

dev-worker-ingestion-shell:
	$(DEV_DC) exec $(WORKER_INGESTION) bash

dev-worker-processing-shell:
	$(DEV_DC) exec $(WORKER_PROCESSING) bash

dev-beat-shell:
	$(DEV_DC) exec $(BEAT) bash

dev-titiler-shell:
	$(DEV_DC) exec $(TITILER) bash

dev-migrate:
	$(DEV_DC) exec $(APP) georiva migrate

dev-makemigrations:
	$(DEV_DC) exec $(APP) georiva makemigrations

# Run tests bypassing PgBouncer (which cannot CREATE DATABASE).
# Reads DB credentials from .env and connects directly to georiva-db.
# Usage: make dev-test TEST_ARGS="georiva.ingestion.tests -v 2"
# Usage with override: make dev-test OV=1 TEST_ARGS="georiva.ingestion.tests -v 2"
dev-test:
	@export $$(grep -v '^[[:space:]]*#' .env | grep -v '^[[:space:]]*$$' | grep -v '^UID=' | xargs) && \
	$(DEV_DC) exec \
	  -e "DATABASE_URL=timescalegis://$$GEORIVA_DB_USER:$$GEORIVA_DB_PASSWORD@georiva-db:5432/$$GEORIVA_DB_NAME" \
	  $(APP) georiva test --keepdb $(TEST_ARGS)


# ======================
# Core dependencies (uv, on the host — not in Docker)
# ======================
# Core's deps live in georiva/pyproject.toml + georiva/uv.lock (a standalone uv
# project). These targets manage them from the core dir, then refresh the
# integrated dev venv (core + any plugins under dev-plugins/).
# Commit georiva/pyproject.toml + georiva/uv.lock afterwards (the root uv.lock is
# gitignored). Quote pkg when it has a version specifier or extras.

CORE_DIR ?= georiva

# Add a runtime package.  Usage: make uv-add pkg="wagtail-foo>=1.2"
# --no-sync updates georiva/pyproject.toml + georiva/uv.lock without creating a
# separate core venv; the overlay `uv sync --all-packages` below is the env.
uv-add:
	@test -n "$(pkg)" || { echo 'Usage: make uv-add pkg="<package>[==version]"'; exit 1; }
	cd $(CORE_DIR) && uv add --no-sync "$(pkg)"
	uv sync --all-packages

# Add a dev-only package.  Usage: make uv-add-dev pkg=pytest
uv-add-dev:
	@test -n "$(pkg)" || { echo 'Usage: make uv-add-dev pkg="<package>"'; exit 1; }
	cd $(CORE_DIR) && uv add --dev --no-sync "$(pkg)"
	uv sync --all-packages

# Remove a package.  Usage: make uv-remove pkg=wagtail-foo
uv-remove:
	@test -n "$(pkg)" || { echo 'Usage: make uv-remove pkg="<package>"'; exit 1; }
	cd $(CORE_DIR) && uv remove --no-sync "$(pkg)"
	uv sync --all-packages

# Re-lock core (e.g. after hand-editing georiva/pyproject.toml), then refresh.
uv-lock:
	cd $(CORE_DIR) && uv lock
	uv sync --all-packages

# Refresh the integrated dev venv (core + all checked-out plugins).
uv-sync:
	uv sync --all-packages