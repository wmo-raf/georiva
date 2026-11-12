FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.1

ARG UID
ENV UID=${UID:-9999}
ARG GID
ENV GID=${GID:-9999}

# Create or rename group to ttl_docker_group with desired GID
RUN if getent group $GID > /dev/null; then \
        existing_group=$(getent group $GID | cut -d: -f1); \
        if [ "$existing_group" != "ttl_docker_group" ]; then \
            groupmod -n ttl_docker_group "$existing_group"; \
        fi; \
    else \
        groupadd -g $GID ttl_docker_group; \
    fi
RUN useradd --shell /bin/bash -u $UID -g $GID -o -c "" -m ttl_docker_group -l || exit 0
ENV DOCKER_USER=ttl_docker_group

# install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    lsb-release \
    ca-certificates \
    curl \
    python3-pip --fix-missing \
    python3-dev \
    python3-venv \
    && apt-get autoclean \
    && apt-get clean \
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/*

USER $UID:$GID

WORKDIR /app

# uv — pinned, the same release core's image uses
COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /usr/local/bin/uv

# The venv uses the image's own Python 3.12, and uv
# installs exactly uv.lock into it: no resolution at build time, so two builds
# of one commit serve tiles from the same rio-tiler / titiler.core (#621).
RUN python3 -m venv /app/venv

ENV PATH="/app/venv/bin:$PATH" \
    UV_CACHE_DIR=/tmp/ttl_uv_cache \
    UV_PROJECT_ENVIRONMENT=/app/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

RUN --mount=type=cache,mode=777,target=$UV_CACHE_DIR,uid=$UID,gid=$GID \
    --mount=type=bind,source=uv.lock,target=/app/uv.lock \
    --mount=type=bind,source=pyproject.toml,target=/app/pyproject.toml \
    uv sync --frozen --no-dev --no-install-project

COPY --chown=$UID:$GID ./app ./app
COPY --chown=$UID:$GID ./docker-entrypoint.sh /app/docker-entrypoint.sh

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["gunicorn"]
