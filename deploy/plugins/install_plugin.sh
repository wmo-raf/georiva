#!/bin/bash
# Bash strict mode: http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

show_help(){
    echo """
Usage: install_plugin.sh [-d] [-f <plugin folder>]
  -f, --folder <plugin folder>        The folder where the plugin to install is located.
  -g, --git <https git repo url>      An url to a git repo containing the plugin to install.
  -u, --url <plugin url>              An url to a .tar.gz file containing the plugin to install.
      --hash <plugin hash>            If provided the plugin's contents will be hashed and checked against this hash, if they do not match the install will fail.
  -d, --dev                           Install the plugin for development.
  -r, --runtime                       If provided any runtime plugin setup scripts will be run if found. Should never be set if being called from a Dockerfile.
  -o, --overwrite                     If provided any existing plugin of the same name will be overwritten and force re-installed, built and/or setup.
  -h, --help                          Show this help message and exit.

A GeoRiva plugin is a flat PEP 621 package: a pyproject.toml at its root with the
Python package under src/<module>/.
"""
}

source /georiva/plugins/utils.sh

# Resolve the canonical name of a flat PEP 621 plugin from the package under src/.
#
# Django discovery (config/settings/base.py) derives the app name from the package
# *contents* under src/, never from the checkout folder name — and for --git/--url
# installs the checkout folder is a mktemp dir, so its basename is meaningless. Use
# the src/<module> name for the install dir and container markers too, so the two
# agree.
#
# Sets the global `resolved_plugin_name`. Returns non-zero on failure — note that
# error() in utils.sh writes to stdout, so this must NOT be called in a command
# substitution or the message would be swallowed into the captured value.
resolve_plugin_name(){
    local plugin_dir="$1"
    local source_desc="$2"
    local pkgs=() pkg

    if [[ ! -f "$plugin_dir/pyproject.toml" ]]; then
        error "$source_desc does not look like a GeoRiva plugin: no pyproject.toml at its root."
        return 1
    fi

    # nullglob is set in utils.sh, so this expands to nothing when src/ is absent.
    for pkg in "$plugin_dir"/src/*/; do
        pkg="$(basename -- "$pkg")"
        case "$pkg" in
            __pycache__|*.egg-info|*.dist-info|.*) continue ;;
        esac
        [[ -f "$plugin_dir/src/$pkg/__init__.py" ]] || continue
        pkgs+=("$pkg")
    done

    if [[ "${#pkgs[@]}" -ne 1 ]]; then
        error "$source_desc does not look like a GeoRiva plugin: src/ must contain exactly one Python package, found ${#pkgs[@]}."
        return 1
    fi

    resolved_plugin_name="${pkgs[0]}"
}

# The builder stage doesn't export DOCKER_USER (only runtime-base does), so
# default it to the user created in both stages.
DOCKER_USER="${DOCKER_USER:-georiva}"

# First parse the args using getopt
VALID_ARGS=$(getopt -o u:dhf:rg:o --long hash:,url:,git:,help,dev,folder:,runtime,overwrite -- "$@")
if [[ $? -ne 0 ]]; then
    error "Incorrect options provided."
    show_help
    exit 1;
fi
eval set -- "$VALID_ARGS"

if [[ "$*" == "--" ]]; then
    error "No arguments provided."
    show_help
    exit 1;
fi

# Next loop over the user provided args and set flags accordingly.
dev=false
url=
folder=
hash=
git=
exclusive_flag_count=0
runtime=
overwrite=
resolved_plugin_name=
# shellcheck disable=SC2078
while [ : ]; do
  case "$1" in
    -d | --dev)
        log "Installing plugin in dev mode."
        dev=true
        shift
        ;;
    -f | --folder)
        folder="$2"
        shift 2
        exclusive_flag_count=$((exclusive_flag_count+1))
        ;;
    --hash)
        hash="$2"
        shift 2
        ;;
    -u | --url)
        url="$2"
        shift 2
        exclusive_flag_count=$((exclusive_flag_count+1))
        ;;
    -g | --git)
        git="$2"
        shift 2
        exclusive_flag_count=$((exclusive_flag_count+1))
        ;;
    -r | --runtime)
        runtime="true"
        shift
        ;;
    -o | --overwrite)
        overwrite="true"
        shift
        ;;
   -h | --help)
        show_help
        exit 0;
        ;;
    --)
        shift
        break
        ;;
  esac
done

if [[ "$exclusive_flag_count" -eq "0" ]]; then
    error "You must provide one of the following flags: --folder, --url or --git"
    show_help
    exit 1;
fi

if [[ "$exclusive_flag_count" -gt "1" ]]; then
    echo "You must provide only one of the following flags: --folder, --url or --git"
    show_help
    exit 1;
fi

# --git was provided, support either plain repo or a GitHub repo with #TAG (via Releases)
if [[ -n "$git" ]]; then
    log "Processing --git source: $git"
    temp_work_dir=$(mktemp -d)

    # Split URL and optional #tag
    repo_url="${git%%#*}"
    tag=""
    if [[ "$git" == *"#"* ]]; then
        tag="${git##*#}"
    fi

    if [[ "$repo_url" == https://github.com/* ]]; then
        # Parse owner/repo from GitHub URL, strip optional .git
        gh_path="${repo_url#https://github.com/}"
        gh_path="${gh_path%.git}"
        gh_owner="${gh_path%%/*}"
        gh_repo="${gh_path#*/}"

        if [[ -n "$tag" ]]; then
            # Use the GitHub Releases "Source code (tar.gz)" link for the given tag
            # This URL lives under /archive/refs/tags and redirects appropriately.
            archive_url="https://github.com/$gh_owner/$gh_repo/archive/refs/tags/$tag.tar.gz"
            log "Downloading GitHub release source tarball: $gh_owner/$gh_repo@$tag"
            # -f: fail on HTTP errors; -L: follow redirects; -sS: silent but show errors
            curl -fLsS "$archive_url" | tar xz --strip-components=1 -C "$temp_work_dir"
        else
            # No tag: shallow clone default branch for speed
            log "Cloning Git repo (no tag): $repo_url"
            git clone --depth 1 "$repo_url" "$temp_work_dir"
        fi
    else
        # Non-GitHub repos: just clone
        log "Cloning non-GitHub repo: $repo_url"
        git clone "$repo_url" "$temp_work_dir"
    fi

    # The repo root IS the plugin (flat PEP 621 layout).
    folder="$temp_work_dir"
    resolve_plugin_name "$folder" "$git" || exit 1

    # Don't bake the git checkout into the image — the whole repo root gets copied
    # into GEORIVA_PLUGIN_DIR below.
    rm -rf "${folder:?}/.git"
fi

# --url was set, download the url, untar it to a temp dir, and verify it only has one
# sub dir.
if [[ -n "$url" ]]; then
    log "Downloading and extracting plugin from $url."
    temp_work_dir=$(mktemp -d)
    curl -Ls "$url" | tar xz -C "$temp_work_dir"

    # A source tarball unpacks to a single top-level directory which is the plugin
    # root (flat PEP 621 layout).
    dirs=("$temp_work_dir"/*/)
    num_dirs=${#dirs[@]}
    if [[ "$num_dirs" -ne 1 ]]; then
        error "$url does not look like a GeoRiva plugin. The archive must contain exactly one top-level directory, found $num_dirs."
        exit 1;
    fi
    folder=${dirs[0]}
    resolve_plugin_name "$folder" "$url" || exit 1
fi

# Dev folder plugins may only exist at runtime (bind-mounted). Skip gracefully at build time.
if [[ "$dev" == true && -n "${folder:-}" && ! -d "$folder" ]]; then
    log "Dev plugin folder '$folder' not found — skipping (expected to be a runtime bind-mount)."
    exit 0
fi

# copy the plugin at the folder location into the plugin dir if it has not been already.
# --git/--url resolved the name from src/<module>; --folder installs keep using the
# folder's own name, which is already meaningful for a bind-mounted dev checkout.
plugin_name="${resolved_plugin_name:-$(basename -- "$folder")}"
plugin_install_dir="$GEORIVA_PLUGIN_DIR/$plugin_name"
if [[ ! "$folder" -ef "$plugin_install_dir" ]]; then
  if [[ ! -d "$plugin_install_dir" || "$overwrite" == "true" ]]; then
    log "Copying plugin $plugin_name into plugins folder at $plugin_install_dir."
    mkdir -p "$GEORIVA_PLUGIN_DIR"
    rm -rf "$plugin_install_dir"
    cp -Tr "$folder" "$plugin_install_dir"
  else
    log "Found an existing plugin installed at $plugin_install_dir, not overwriting it
        as the --overwrite flag was not provided to this script."
  fi
  folder="$GEORIVA_PLUGIN_DIR/$plugin_name"
fi
chown -R "$DOCKER_USER": "$folder"

# Now we've copied the plugin into the plugin dir we can delete the tmp download dir
# if we used it.
if [[ -n "${temp_work_dir:-}" ]]; then
  rm -rf "$temp_work_dir"
fi

# --hash was set, hash the plugin folder and check it matches.
if [[ -n "$hash" ]]; then
  plugin_hash=$(find "$folder" -type f -print0 | sort -z | xargs -0 sha1sum | sha1sum | cut -d " " -f 1 )
  if [[ "$plugin_hash" != "$hash" ]]; then
    error "Plugin $plugin_name does not match the provided hash. This could mean it has been maliciously modified and it is not safe to install."
    error "The plugins hash was: $plugin_hash"
    error "Instead we expected : $hash"
    exit 1;
  else
    log "Plugin ${plugin_name}'s hash matches provided hash."
  fi
fi

check_and_run_script(){
    if [[ -f "$1/$2" ]]; then
        log "Running ${plugin_name}'s custom $2 script"
        bash "$1/$2"
    fi
}

run_as_docker_user(){
  CURRENT_USER=$(whoami)
  if [[ "$CURRENT_USER" != "$DOCKER_USER" ]]; then
    gosu "$DOCKER_USER" "$@"
  else
    "$@"
  fi
}

# Make sure we create the container markers folder which we will use to check if a
# plugin has been installed or not already inside this container.
mkdir -p /georiva/container_markers

# Install plugin
if [[ -d "$folder" ]]; then
    BUILT_MARKER=/georiva/container_markers/$plugin_name.built
    if [[ ! -f "$BUILT_MARKER" || "$overwrite" == "true" ]]; then
      log "Building ${plugin_name}."

      cd /georiva

      VENV_PIP="/georiva/venv/bin/pip"

      # --no-build-isolation: build the plugin using the setuptools/wheel already
      # in the venv instead of an isolated build env that re-downloads them from
      # PyPI (needs network; breaks offline / on DNS issues). The venv is seeded
      # with pip/setuptools/wheel at image build.
      if [[ "$dev" == true ]]; then
          run_as_docker_user "$VENV_PIP" install --no-build-isolation -e "$folder"
      else
          run_as_docker_user "$VENV_PIP" install --no-build-isolation "$folder"
      fi

      check_and_run_script "$folder" build.sh
      touch "$BUILT_MARKER"
    else
      log "Skipping install of ${plugin_name} as it is already installed."
    fi

    PLUGIN_RUNTIME_SETUP_MARKER=/georiva/container_markers/$plugin_name.runtime-setup
    if [[ ( ! -f "$PLUGIN_RUNTIME_SETUP_MARKER" || "$overwrite" == "true" ) && $runtime == "true" ]]; then
      check_and_run_script "$folder" runtime_setup.sh
      touch "$PLUGIN_RUNTIME_SETUP_MARKER"
    else
      log "Skipping runtime setup of ${plugin_name}."
    fi
fi

log "Fixing ownership of plugins from $(id -u) to $DOCKER_USER in $GEORIVA_PLUGIN_DIR"
chown -R "$DOCKER_USER": "$GEORIVA_PLUGIN_DIR"
chown -R "$DOCKER_USER": /georiva/container_markers/
log_success "Finished setting up ${plugin_name} successfully."
