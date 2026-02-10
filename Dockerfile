# syntax=docker/dockerfile:1.5
ARG UID=9999
ARG GID=9999

# =============================================================================
# Builder — install dependencies and compile everything
# =============================================================================
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.1 AS builder

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

# Install build-time dependencies (includes compilers, headers, etc.)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libgeos-dev \
    libpq-dev \
    python3-dev \
    python3-pip \
    python3-venv

USER $UID:$GID

# Create venv and install Python dependencies
COPY --chown=$UID:$GID ./georiva/requirements.txt /georiva/requirements.txt
RUN python3 -m venv /georiva/venv \
    && /georiva/venv/bin/pip install --upgrade pip setuptools wheel

ENV PIP_CACHE_DIR=/tmp/georiva_pip_cache
RUN --mount=type=cache,mode=777,target=$PIP_CACHE_DIR,uid=$UID,gid=$GID \
    /georiva/venv/bin/pip install -r /georiva/requirements.txt

# Copy app code and install the package
COPY --chown=$UID:$GID ./georiva /georiva/app
RUN /georiva/venv/bin/pip install --no-cache-dir /georiva/app/

# Install plugins at build time
COPY --chown=$UID:$GID ./deploy/plugins/*.sh /georiva/plugins/

ARG GEORIVA_PLUGIN_GIT_REPOS=""
RUN --mount=type=cache,mode=777,target=$PIP_CACHE_DIR,uid=$UID,gid=$GID \
    if [ -n "$GEORIVA_PLUGIN_GIT_REPOS" ]; then \
        echo "Baking in plugins: $GEORIVA_PLUGIN_GIT_REPOS"; \
        plugin_list=$(echo "$GEORIVA_PLUGIN_GIT_REPOS" | tr ',' ' '); \
        for repo in $plugin_list; do \
            echo "Processing: $repo"; \
            /bin/bash /georiva/plugins/install_plugin.sh --git "$repo" || exit 1; \
        done \
    else \
        echo "No plugins specified for build."; \
    fi


# =============================================================================
# Runtime base — shared between prod and dev
# Sets up OS-level runtime dependencies, user, and tools.
# =============================================================================
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.1 AS runtime-base

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

# Install runtime dependencies (no compilers, no -dev headers)
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
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