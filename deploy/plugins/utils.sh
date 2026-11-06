# Ensure looping over globs doesn't match nothing
shopt -s nullglob

if [ -t 0 ]; then
  TPUTTERM=()
else
  # if we are in a non-interactive environment set a default -T for tput so it doesn't
  # crash
  TPUTTERM=(-T xterm-256color)
fi

safe_tput(){
  if [[ -z "${NO_COLOR:-}" ]]; then
    tput "${TPUTTERM[@]}" "$@"
  fi
}

# shellcheck disable=SC2034
RED=$(safe_tput setaf 1)
# shellcheck disable=SC2034
GREEN=$(safe_tput setaf 2)
# shellcheck disable=SC2034
YELLOW=$(safe_tput setaf 3)
# shellcheck disable=SC2034
BLUE=$(safe_tput setaf 4)
# shellcheck disable=SC2034
PURPLE=$(safe_tput setaf 5)
# shellcheck disable=SC2034
CYAN=$(safe_tput setaf 6)
# shellcheck disable=SC2034
BOLD="$(safe_tput bold)"

NC=$(safe_tput sgr0) # No Color

GEORIVA_PLUGIN_DIR=${GEORIVA_PLUGIN_DIR:-/georiva/plugins}

simple_log(){
  echo -e "${BLUE}[PLUGIN] $*${NC}"
}
log(){
  echo -e "${BLUE}[PLUGIN][${plugin_name:-SETUP}] $*${NC}"
}
log_success(){
  echo -e "${GREEN}[PLUGIN][${plugin_name:-SETUP}] $*${NC}"
}
error(){
  echo -e "${RED}[PLUGIN][${plugin_name:-SETUP}] ERROR: $*${NC}"
}


install_dev_plugins(){
  if [[ -z "${GEORIVA_PLUGIN_SETUP_ALREADY_RUN:-}" ]]; then
    GEORIVA_DEV_PLUGIN_DIR="${GEORIVA_DEV_PLUGIN_DIR:-/georiva/dev-plugins}"
    if [[ -d "$GEORIVA_DEV_PLUGIN_DIR" ]]; then
      for plugin_dir in "$GEORIVA_DEV_PLUGIN_DIR"/*/; do
        if [[ -d "$plugin_dir" ]]; then
          plugin_name="$(basename -- "$plugin_dir")"
          log "Installing dev plugin '$plugin_name' from $plugin_dir as editable..."
          /georiva/venv/bin/pip install -e "$plugin_dir"
        fi
      done
    fi

    # Ensure we don't run this function multiple times in the same shell.
    export GEORIVA_PLUGIN_SETUP_ALREADY_RUN="yes"
  fi
}
