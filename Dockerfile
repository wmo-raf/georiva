# syntax=docker/dockerfile:1.5
ARG UID=9999
ARG GID=9999

# =============================================================================
# Builder — install dependencies and compile everything
# =============================================================================
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.1 AS builder

ARG UID
ARG GID

# Create group and user
RUN if getent group $GID > /dev/null; then \
        existing_group=$(getent group $GID | cut -d: -f1); \
        if [ "$existing_group" != "georiva_docker_group" ]; then \
            groupmod -n georiva_docker_group "$existing_group"; \
        fi; \
    else \
        groupadd -g $GID georiva_docker_group; \
    fi && \
    useradd --shell /bin/bash -u $UID -g $GID -o -c "" -m georiva -l || exit 0

# Install build-time dependencies
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/sources.list.d/apache-arrow* && \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    gosu \
    libgeos-dev \
    libpq-dev \
    python3-dev \
    python3-pip \
    python3-venv

# uv — pinned
COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /usr/local/bin/uv

# Create /georiva owned by the build user before dropping privileges.
RUN mkdir -p /georiva/app && chown -R $UID:$GID /georiva

USER $UID:$GID
WORKDIR /georiva/app

# Seed the venv with pip/setuptools/wheel. uv installs into this venv via
# UV_PROJECT_ENVIRONMENT, and `uv sync --inexact` below preserves these tools —
# the plugin installer (deploy/plugins/install_plugin.sh) relies on venv/bin/pip.
RUN python3 -m venv /georiva/venv \
    && /georiva/venv/bin/pip install --upgrade pip setuptools wheel

ENV PIP_CACHE_DIR=/tmp/georiva_pip_cache \
    UV_CACHE_DIR=/tmp/georiva_uv_cache \
    UV_PROJECT_ENVIRONMENT=/georiva/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

# Install ONLY core's locked dependencies first, reproducibly
# from georiva/uv.lock. Bind-mounting lock+pyproject keeps this expensive layer
# cached until dependencies actually change.
RUN --mount=type=cache,mode=777,target=$UV_CACHE_DIR,uid=$UID,gid=$GID \
    --mount=type=bind,source=georiva/uv.lock,target=/georiva/app/uv.lock \
    --mount=type=bind,source=georiva/pyproject.toml,target=/georiva/app/pyproject.toml \
    uv sync --frozen --no-dev --no-install-project --inexact

# Copy core source and install core itself (deps already satisfied), still frozen
# to the lock. --no-editable installs it as a regular package into the venv.
COPY --chown=$UID:$GID ./georiva /georiva/app
RUN --mount=type=cache,mode=777,target=$UV_CACHE_DIR,uid=$UID,gid=$GID \
    uv sync --frozen --no-dev --inexact --no-editable

# Install plugins at build time
COPY --chown=$UID:$GID ./deploy/plugins/*.sh /georiva/plugins/
COPY --chown=$UID:$GID ./deploy/plugins/parse_plugins_toml.py /georiva/plugins/

# Optionally bake in a plugins.toml manifest (glob trick: no-op if file absent in build context)
COPY --chown=$UID:$GID plugins.tom[l] /georiva/

# Drop back to root so install_plugin.sh can chown + gosu
USER root

# Install any plugins declared in plugins.toml
ARG GEORIVA_PLUGIN_LIST_FILE=""
RUN --mount=type=cache,mode=777,target=$PIP_CACHE_DIR,uid=$UID,gid=$GID \
    manifest="${GEORIVA_PLUGIN_LIST_FILE:-/georiva/plugins.toml}"; \
    if [ -f "$manifest" ]; then \
        echo "Installing plugins from manifest: $manifest"; \
        tmpfile=$(mktemp); \
        /georiva/venv/bin/python3 /georiva/plugins/parse_plugins_toml.py "$manifest" > "$tmpfile" || { rm -f "$tmpfile"; exit 1; }; \
        while IFS= read -r args_line; do \
            [ -z "$args_line" ] && continue; \
            echo "Processing: $args_line"; \
            echo "$args_line" | xargs /georiva/plugins/install_plugin.sh || { rm -f "$tmpfile"; exit 1; }; \
        done < "$tmpfile"; \
        rm -f "$tmpfile"; \
    else \
        echo "No plugins manifest found at $manifest, skipping."; \
    fi

# Restore non-root user for the remainder of the builder stage
USER $UID:$GID

# =============================================================================
# Runtime base — shared between prod and dev
# Sets up OS-level runtime dependencies, user, and tools.
# =============================================================================
FROM ghcr.io/osgeo/gdal:ubuntu-full-3.12.1 AS runtime-base

ARG UID
ARG GID

ENV POSTGRES_VERSION=18 \
    DOCKER_USER=georiva

# Create matching group and user
RUN if getent group $GID > /dev/null; then \
        existing_group=$(getent group $GID | cut -d: -f1); \
        if [ "$existing_group" != "georiva_docker_group" ]; then \
            groupmod -n georiva_docker_group "$existing_group"; \
        fi; \
    else \
        groupadd -g $GID georiva_docker_group; \
    fi && \
    useradd --shell /bin/bash -u $UID -g $GID -o -c "" -m georiva -l || exit 0

RUN mkdir -p /var/tmp/georiva && chown $UID:$GID /var/tmp/georiva

# Install runtime dependencies (no compilers, no -dev headers)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    rm -f /etc/apt/sources.list.d/apache-arrow* && \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    lsb-release \
    libgeos-c1v5 \
    libpq5 \
    gosu \
    git \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl --silent -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
        https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client-$POSTGRES_VERSION

# Install docker-compose wait
ARG DOCKER_COMPOSE_WAIT_VERSION=2.12.1
ARG DOCKER_COMPOSE_WAIT_PLATFORM_SUFFIX=""

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$DOCKER_COMPOSE_WAIT_VERSION/wait${DOCKER_COMPOSE_WAIT_PLATFORM_SUFFIX} /wait
RUN chmod +x /wait

ENV PATH="/georiva/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1


# =============================================================================
# Production target
# =============================================================================
FROM runtime-base AS prod

ARG UID
ARG GID

USER $UID:$GID

# Copy the fully built venv, app code, and plugin scripts from the builder
COPY --from=builder --chown=$UID:$GID /georiva/venv /georiva/venv
COPY --from=builder --chown=$UID:$GID /georiva/app /georiva/app
COPY --from=builder --chown=$UID:$GID /georiva/plugins /georiva/plugins

WORKDIR /georiva/app/src/georiva

COPY --chown=$UID:$GID ./docker-entrypoint.sh /georiva/docker-entrypoint.sh

ENV DJANGO_SETTINGS_MODULE='georiva.config.settings.production'

ENTRYPOINT ["/georiva/docker-entrypoint.sh"]
CMD ["gunicorn-wsgi"]


# =============================================================================
# Development target
# Expects source code to be bind-mounted at /georiva/app.
# Includes dev tools and auto-reload support.
# =============================================================================
FROM runtime-base AS dev

ARG UID
ARG GID

USER $UID:$GID

COPY --from=builder --chown=$UID:$GID /georiva/venv /georiva/venv
COPY --from=builder --chown=$UID:$GID /georiva/plugins /georiva/plugins

ENV PIP_CACHE_DIR=/tmp/georiva_pip_cache
RUN --mount=type=cache,mode=777,target=$PIP_CACHE_DIR,uid=$UID,gid=$GID \
    /georiva/venv/bin/pip install --no-cache-dir watchfiles

# Source is bind-mounted — add it to PYTHONPATH directly
ENV PYTHONPATH="/georiva/app/src:$PYTHONPATH"

WORKDIR /georiva/app/src/georiva

COPY --chown=$UID:$GID ./docker-entrypoint.sh /georiva/docker-entrypoint.sh

ENV DJANGO_SETTINGS_MODULE='georiva.config.settings.dev'

ENTRYPOINT ["/georiva/docker-entrypoint.sh"]
CMD ["django-dev"]