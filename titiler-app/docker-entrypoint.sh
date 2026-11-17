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
  uvicorn-dev : Start TiTiler with uvicorn --reload (development, requires source bind mount).
                A worker killed by a signal (an OOM kill) stops the container, so
                its restart policy brings it back (app/devserver.py, #699)

OPTIONS (via environment variables):
  TTL_PORT                  Listen port (default: 8000)
  TTL_WORKERS               Gunicorn worker count (default: 4)
  TTL_LOG_LEVEL             Log level: debug|info|warning|error (default: info)
  TTL_MAX_REQUESTS          Gunicorn max requests before worker restart (default: 2000)
  TTL_MAX_REQUESTS_JITTER   Jitter for max requests (default: 200)
  TTL_TIMEOUT               Gunicorn worker timeout in seconds (default: 120)
  TTL_ROOT_PATH             ASGI root_path for reverse proxy prefix (default: empty)

GDAL TUNING (via environment variables):
  GDAL_CACHEMAX                 Block cache in MB (default: unset)
  CPL_VSIL_CURL_CACHE_SIZE      HTTP response cache in bytes (default: unset)
  GDAL_DISABLE_READDIR_ON_OPEN  Sidecar lookup on open (app pins EMPTY_DIR; see
                                app/reader.py before overriding it)

REMOTE ASSETS (range-read from their upstream, ADR 0043; no byte cache):
  GDAL_HTTP_CONNECTTIMEOUT            Seconds to wait for a connection (compose: 5)
  GDAL_HTTP_TIMEOUT                   Seconds one HTTP request may take (compose: 30)
  GDAL_HTTP_MERGE_CONSECUTIVE_RANGES  Fetch adjacent blocks in one request (compose: YES)
  GDAL_INGESTED_BYTES_AT_OPEN         Bytes read on open, to take a COG's header in
                                      one request (compose: 32768)

UPSTREAM STORES (read from the store itself, ADR 0044):
  GEORIVA_STORE_READ_TIMEOUT     Seconds to open a store or read one field (compose: 30)
  GEORIVA_STORE_CONNECT_TIMEOUT  Seconds to connect to a store (compose: 5)
  GEORIVA_STORE_MISS_TTL         Seconds a step still missing after a reopen is remembered (compose: 60)
  GEORIVA_STORE_FIELD_CACHE_MB   Megabytes of decoded fields each worker keeps (compose: 128)
  GEORIVA_STORE_WARM             Open the published stores as each worker starts, in the
                                 background (compose: 1; 0 is off)

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
    exec python -m app.devserver app.main:app \
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
