#!/bin/bash
# Bash strict mode: http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

MIGRATE_ON_STARTUP=${MIGRATE_ON_STARTUP:-true}
COLLECT_STATICFILES_ON_STARTUP=${COLLECT_STATICFILES_ON_STARTUP:-true}

GEORIVA_GUNICORN_NUM_OF_WORKERS=${GEORIVA_GUNICORN_NUM_OF_WORKERS:-}
GEORIVA_CELERY_BEAT_DEBUG_LEVEL=${GEORIVA_CELERY_BEAT_DEBUG_LEVEL:-INFO}
GEORIVA_CELERY_WORKER_LOG_LEVEL=${GEORIVA_CELERY_WORKER_LOG_LEVEL:-INFO}

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
gunicorn            : Start GeoRiva django using a prod ready gunicorn server:
                         * Waits for the postgres database to be available first.
                         * Automatically migrates the database on startup.
                         * Binds to 0.0.0.0
celery-worker       : Start the celery worker queue which runs important async tasks
celery-worker-dev : Start the celery worker with auto-reload on code changes
                    (requires the dev build target).
celery-beat         : Start the celery beat service used to schedule periodic jobs

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
}

start_celery_worker() {
    startup_plugin_setup

    EXTRA_CELERY_ARGS=()

    if [[ -n "$GEORIVA_GUNICORN_NUM_OF_WORKERS" ]]; then
        EXTRA_CELERY_ARGS+=(--concurrency "$GEORIVA_GUNICORN_NUM_OF_WORKERS")
    fi
    exec celery -A georiva worker "${EXTRA_CELERY_ARGS[@]}" -l "${GEORIVA_CELERY_WORKER_LOG_LEVEL}" "$@"
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
    exec python3 /georiva/app/src/georiva/manage.py "${@:2}"
    ;;
shell)
    exec python3 /georiva/app/src/georiva/manage.py shell
    ;;
celery-worker)
    start_celery_worker -Q celery -n default-worker@%h "${@:2}"
    ;;
celery-worker-dev)
    startup_plugin_setup
    exec watchfiles \
        --filter python \
        "celery -A georiva worker -Q celery -n default-worker@%h -l ${GEORIVA_CELERY_WORKER_LOG_LEVEL}" \
        /georiva/app/src/
    ;;
celery-beat)
    exec celery -A georiva beat -l "${GEORIVA_CELERY_BEAT_DEBUG_LEVEL}" -S django_celery_beat.schedulers:DatabaseScheduler "${@:2}"
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