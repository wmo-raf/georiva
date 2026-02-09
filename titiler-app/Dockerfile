FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.1

ARG UID
ENV UID=${UID:-9999}
ARG GID
ENV GID=${GID:-9999}

# Create or rename group to adl_docker_group with desired GID
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

COPY --chown=$UID:$GID ./requirements.txt /app/requirements.txt
RUN python3 -m venv /app/venv

ENV PATH="/app/venv/bin:$PATH"

ENV PIP_CACHE_DIR=/tmp/ttl_pip_cache
RUN --mount=type=cache,mode=777,target=$PIP_CACHE_DIR,uid=$UID,gid=$GID . /app/venv/bin/activate && pip3 install  -r /app/requirements.txt

COPY --chown=$UID:$GID ./app ./app

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]