#!/bin/bash
set -euo pipefail

TTL_PORT="${TTL_PORT:-8000}"
TTL_WORKERS="${TTL_WORKERS:-4}"
TTL_LOG_LEVEL="${TTL_LOG_LEVEL:-info}"
TTL_MAX_REQUESTS="${TTL_MAX_REQUESTS:-2000}"
TTL_MAX_REQUESTS_JITTER="${TTL_MAX_REQUESTS_JITTER:-200}"
TTL_TIMEOUT="${TTL_TIMEOUT:-120}"
TTL_ROOT_PATH="${TTL_ROOT_PATH:-/titiler}"

show_help() {
    echo """
GeoRiva TiTiler — available commands:

SERVICE COMMANDS:
  gunicorn    : Start TiTiler with gunicorn + uvicorn workers (production)
  uvicorn     : Start TiTiler with plain uvicorn (single worker, no process management)
  uvicorn-dev : Start TiTiler with uvicorn --reload (development, requires source bind mount)

OPTIONS (via environment variables):
  TTL_PORT                  Listen port (default: 8000)
  TTL_WORKERS               Gunicorn worker count (default: 4)
  TTL_LOG_LEVEL             Log level: debug|info|warning|error (default: info)
  TTL_MAX_REQUESTS          Gunicorn max requests before worker restart (default: 2000)
  TTL_MAX_REQUESTS_JITTER   Jitter for max requests (default: 200)
  TTL_TIMEOUT               Gunicorn worker timeout in seconds (default: 120)
  TTL_ROOT_PATH             ASGI root_path for reverse proxy prefix (default: empty)

GDAL TUNING (via environment variables):
  GDAL_CACHEMAX             Block cache in MB (default: unset)
  CPL_VSIL_CURL_CACHE_SIZE  HTTP response cache in bytes (default: unset)

  help : Show this message
"""
}

build_root_path_args() {
    if [[ -n "$TTL_ROOT_PATH" ]]; then
        echo "--root-path" "$TTL_ROOT_PATH"
    fi
}

# ======================================================
# COMMANDS
# ======================================================

if [[ -z "${1:-}" ]]; then
    echo "Must provide a command to docker-entrypoint.sh"
    show_help
    exit 1
fi

source /app/venv/bin/activate

case "$1" in
gunicorn)
    echo "Starting TiTiler (gunicorn + uvicorn workers) on 0.0.0.0:${TTL_PORT}"
    exec gunicorn app.main:app \
        -k uvicorn.workers.UvicornWorker \
        -w "$TTL_WORKERS" \
        -b "0.0.0.0:${TTL_PORT}" \
        --worker-tmp-dir "${TMPDIR:-/dev/shm}" \
        --timeout "$TTL_TIMEOUT" \
        --max-requests "$TTL_MAX_REQUESTS" \
        --max-requests-jitter "$TTL_MAX_REQUESTS_JITTER" \
        --log-file=- \
        --access-logfile=- \
        --log-level="$TTL_LOG_LEVEL" \
        --forwarded-allow-ips="*" \
        $(build_root_path_args) \
        "${@:2}"
    ;;
uvicorn)
    echo "Starting TiTiler (uvicorn) on 0.0.0.0:${TTL_PORT}"
    exec python -m uvicorn app.main:app \
        --host 0.0.0.0 \
        --port "$TTL_PORT" \
        --log-level "$TTL_LOG_LEVEL" \
        --proxy-headers \
        --forwarded-allow-ips "*" \
        $(build_root_path_args) \
        "${@:2}"
    ;;
uvicorn-dev)
    echo "Starting TiTiler (uvicorn --reload) on 0.0.0.0:${TTL_PORT}"
    exec python -m uvicorn app.main:app \
        --host 0.0.0.0 \
        --port "$TTL_PORT" \
        --log-level debug \
        --reload \
        --reload-dir /app/app \
        --proxy-headers \
        --forwarded-allow-ips "*" \
        $(build_root_path_args) \
        "${@:2}"
    ;;
help)
    show_help
    ;;
*)
    echo "Unknown command: $*"
    show_help
    exit 1
    ;;
esac
