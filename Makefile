# ======================
# Compose Definitions
# ======================

PROD = -f docker-compose.yml
DEV  = -f docker-compose.yml -f docker-compose.dev.yml

DC     = docker compose $(PROD)
DEV_DC = docker compose $(DEV)

# Main services (match your docker-compose.yml)
APP ?= georiva
WORKER ?= georiva-celery-worker
BEAT ?= georiva-celery-beat

LOG_ARGS ?= --tail 100

.PHONY: \
	up down stop restart build ps logs \
	app-logs worker-logs beat-logs \
	shell worker-shell beat-shell \
	migrate makemigrations \
	dev-up dev-down dev-stop dev-restart dev-build dev-ps dev-logs \
	dev-app-logs dev-worker-logs dev-beat-logs \
	dev-shell dev-worker-shell dev-beat-shell \
	dev-migrate dev-makemigrations

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
	$(DC) exec $(APP) georivamigrate

makemigrations:
	$(DC) exec $(APP) georiva makemigrations


# ======================
# DEV
# ======================

dev-up:
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

dev-logs:
	$(DEV_DC) logs -f $(LOG_ARGS)

dev-app-logs:
	$(DEV_DC) logs -f $(APP) $(LOG_ARGS)

dev-worker-logs:
	$(DEV_DC) logs -f $(WORKER)

dev-beat-logs:
	$(DEV_DC) logs -f $(BEAT) $(LOG_ARGS)

dev-shell:
	$(DEV_DC) exec $(APP) bash

dev-worker-shell:
	$(DEV_DC) exec $(WORKER) bash

dev-beat-shell:
	$(DEV_DC) exec $(BEAT) bash

dev-migrate:
	$(DEV_DC) exec $(APP) georiva migrate

dev-makemigrations:
	$(DEV_DC) exec $(APP) georiva makemigrations
