#!/bin/bash
# Bash strict mode: http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

MIGRATE_ON_STARTUP=${MIGRATE_ON_STARTUP:-true}
COLLECT_STATICFILES_ON_STARTUP=${COLLECT_STATICFILES_ON_STARTUP:-true}

GEORIVA_GUNICORN_NUM_OF_WORKERS=${GEORIVA_GUNICORN_NUM_OF_WORKERS:-}
GEORIVA_CELERY_BEAT_DEBUG_LEVEL=${GEORIVA_CELERY_BEAT_DEBUG_LEVEL:-INFO}
GEORIVA_CELERY_WORKER_LOG_LEVEL=${GEORIVA_CELERY_WORKER_LOG_LEVEL:-INFO}

# The one name every worker command reads for its pool size, prod and dev alike.
# It is deliberately queue-agnostic: compose maps the operator-facing per-queue
# setting (GEORIVA_CELERY_{DEFAULT,INGESTION,PROCESSING}_WORKER_CONCURRENCY) onto
# it per service, so the entrypoint never has to know which queue it is serving,
# and the per-queue defaults stay in the one file that does. Reading a per-queue
# name in here reads a variable that does not reach the container — the bug of
# issue #399. Empty means unset: no --concurrency, celery's own default.
GEORIVA_CELERY_WORKER_CONCURRENCY=${GEORIVA_CELERY_WORKER_CONCURRENCY:-}

GEORIVA_LOG_LEVEL=${GEORIVA_LOG_LEVEL:-INFO}

GEORIVA_PORT="${GEORIVA_PORT:-8000}"

show_help() {
    echo """
The available GeoRiva related commands and services are shown below:

ADMIN COMMANDS:
manage          : Manage GeoRiva and its database
shell           : Start a Django Python shell
install-plugin  : Installs a plugin (append --help for more info).
uninstall-plugin: Un-installs a plugin (append --help for more info).
list-plugins    : Lists currently installed plugins.
help            : Show this message

SERVICE COMMANDS:
gunicorn              : Start GeoRiva django using a prod ready gunicorn server:
                           * Waits for the postgres database to be available first.
                           * Automatically migrates the database on startup.
                           * Binds to 0.0.0.0
celery-default-worker   : Start the default celery worker (scheduled tasks, pruning, sweeps
celery-ingestion-worker : Start the ingestion celery worker (heavy GRIB/raster processing)
celery-processing-worker: Start the processing celery worker (derivation units, zonal stats)
celery-default-worker-dev       : Start the default celery worker with auto-reload on code changes
                          (requires the dev build target).
celery-ingestion-worker-dev     : Start the ingestion celery worker with auto-reload on code changes
                          (requires the dev build target).
celery-processing-worker-dev    : Start the processing celery worker with auto-reload on code changes
                          (requires the dev build target).
celery-beat             : Start the celery beat service used to schedule periodic jobs

DEV COMMANDS:
django-dev      : Start a normal GeoRiva backend django development server, performs
                  the same checks and setup as the gunicorn command above.
"""
}

run_setup_commands_if_configured(){
  startup_plugin_setup
  if [ "$MIGRATE_ON_STARTUP" = "true" ] ; then
    echo "python /georiva/app/src/georiva/manage.py migrate"
    /georiva/app/src/georiva/manage.py migrate
  fi

  # collect staticfiles
  if [ "$COLLECT_STATICFILES_ON_STARTUP" = "true" ] ; then
    echo "python /georiva/app/src/georiva/manage.py collectstatic --noinput"
    /georiva/app/src/georiva/manage.py collectstatic --noinput
  fi

  # setup minio
  echo "Setting up MinIO..."
  /georiva/app/src/georiva/manage.py setup_minio

  # warm the palette cache
  echo "Warming palette cache..."
  /georiva/app/src/georiva/manage.py warm_palette

  echo "Creating martin boundary stats function..."
  /georiva/app/src/georiva/manage.py create_martin_function
}

# The pool-size flag, or nothing when the operator set no size — in which case
# celery picks its own default. Both worker commands below, prod and dev, get
# the flag from here, so the two cannot end up reading different names again.
# The size itself is never decided here: compose owns the per-queue defaults.
celery_concurrency_flag() {
    if [[ -n "$GEORIVA_CELERY_WORKER_CONCURRENCY" ]]; then
        echo "--concurrency=$GEORIVA_CELERY_WORKER_CONCURRENCY"
    fi
}

# Args: queue, worker node name, then any extra celery args.
start_celery_worker() {
    local queue="$1" node_name="$2"
    shift 2

    startup_plugin_setup

    local concurrency_flag
    concurrency_flag="$(celery_concurrency_flag)"

    exec celery -A georiva worker \
        -Q "$queue" \
        -n "$node_name" \
        ${concurrency_flag:+"$concurrency_flag"} \
        -l "${GEORIVA_CELERY_WORKER_LOG_LEVEL}" \
        "$@"
}

# The dev counterpart: the same worker wrapped in watchfiles, so a code change
# restarts it. watchfiles takes the command as one string rather than an argv,
# which is why this builds its own line instead of delegating above — and why
# it takes no extra celery args.
#
# Args: queue, worker node name.
start_celery_worker_dev() {
    local queue="$1" node_name="$2"

    startup_plugin_setup

    local concurrency_flag
    concurrency_flag="$(celery_concurrency_flag)"

    exec watchfiles \
        --filter python \
        "celery -A georiva worker -Q ${queue} -n ${node_name} -l ${GEORIVA_CELERY_WORKER_LOG_LEVEL} ${concurrency_flag}" \
        /georiva/app/src/
}

# Lets devs attach to this container running the passed command, press ctrl-c and only
# the command will stop. Additionally they will be able to use bash history to
# re-run the containers command after they have done what they want.
attachable_exec(){
    echo "$@"
    exec bash --init-file <(echo "history -s $*; $*")
}

run_server() {
    run_setup_commands_if_configured

    if [[ "$1" = "wsgi" ]]; then
        STARTUP_ARGS=(georiva.config.wsgi:application)
    elif [[ "$1" = "asgi" ]]; then
        STARTUP_ARGS=(-k uvicorn.workers.UvicornWorker georiva.config.asgi:application)
    else
        echo -e "\e[31mUnknown run_server argument $1 \e[0m" >&2
        exit 1
    fi


    # Gunicorn args explained in order:
    #
    # 1. See https://docs.gunicorn.org/en/stable/faq.html#blocking-os-fchmod for
    #    why we set worker-tmp-dir to /dev/shm by default.
    # 2. Log to stdout
    # 3. Log requests to stdout
    exec gunicorn --workers="$GEORIVA_GUNICORN_NUM_OF_WORKERS" \
        --worker-tmp-dir "${TMPDIR:-/dev/shm}" \
        --log-file=- \
        --access-logfile=- \
        --capture-output \
        -b "0.0.0.0":"${GEORIVA_PORT}" \
        --log-level="${GEORIVA_LOG_LEVEL}" \
        "${STARTUP_ARGS[@]}" \
        "${@:2}"
}

# ======================================================
# COMMANDS
# ======================================================

if [[ -z "${1:-}" ]]; then
    echo "Must provide arguments to docker-entrypoint.sh"
    show_help
    exit 1
fi

source /georiva/venv/bin/activate

# wait for required services to be available, using docker-compose-wait
/wait

source /georiva/plugins/utils.sh

case "$1" in
django-dev)
    run_setup_commands_if_configured
    echo "Running Development Server on 0.0.0.0:${GEORIVA_PORT}"
    echo "Press CTRL-p CTRL-q to close this session without stopping the container."
    attachable_exec python /georiva/app/src/georiva/manage.py runserver "0.0.0.0:${GEORIVA_PORT}"
    ;;
django-dev-no-attach)
    run_setup_commands_if_configured
    echo "Running Development Server on 0.0.0.0:${GEORIVA_PORT}"
    python /georiva/app/src/georiva/manage.py runserver "0.0.0.0:${GEORIVA_PORT}"
    ;;
gunicorn)
    run_server asgi "${@:2}"
    ;;
gunicorn-wsgi)
    run_server wsgi "${@:2}"
    ;;
manage)
    exec python /georiva/app/src/georiva/manage.py "${@:2}"
    ;;
shell)
    exec python /georiva/app/src/georiva/manage.py shell
    ;;
celery-default-worker)
    start_celery_worker georiva-default default-worker@%h "${@:2}"
    ;;
celery-ingestion-worker)
    start_celery_worker georiva-ingestion ingestion-worker@%h "${@:2}"
    ;;
celery-processing-worker)
    start_celery_worker georiva-processing processing-worker@%h "${@:2}"
    ;;
celery-default-worker-dev)
    start_celery_worker_dev georiva-default default-worker@%h
    ;;
celery-ingestion-worker-dev)
    start_celery_worker_dev georiva-ingestion ingestion-worker@%h
    ;;
celery-processing-worker-dev)
    start_celery_worker_dev georiva-processing processing-worker@%h
    ;;
celery-beat)
    exec celery -A georiva beat -l "${GEORIVA_CELERY_BEAT_DEBUG_LEVEL}" -S django_celery_beat.schedulers:DatabaseScheduler "${@:2}"
    ;;
minio-consumer)
      exec python /georiva/app/src/georiva/manage.py minio_event_consumer "${@:2}"
    ;;
staging-consumer)
      exec python /georiva/app/src/georiva/manage.py staging_event_consumer "${@:2}"
    ;;
install-plugin)
    exec /georiva/plugins/install_plugin.sh --runtime "${@:2}"
    ;;
uninstall-plugin)
    exec /georiva/plugins/uninstall_plugin.sh "${@:2}"
    ;;
list-plugins)
    exec /georiva/plugins/list_plugins.sh "${@:2}"
    ;;
*)
    echo "Command given was $*"
    show_help
    exit 1
    ;;
esac