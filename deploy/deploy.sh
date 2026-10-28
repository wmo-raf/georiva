#!/usr/bin/env bash
# Install or upgrade this server to a GeoRiva release.
#
#   ./deploy.sh v0.5.0        a release tag (what production should run)
#   ./deploy.sh main          the tip of main — a moving target, for staging
#   ./deploy.sh sha-abc1234   one commit of main, as CI tagged its images
#
# Checks out that version of deploy/, writes GEORIVA_TAG into .env, pulls the
# images and restarts the stack. If the pull fails nothing has restarted, so the
# checkout and tag are put back as they were. Rollback is the same command with
# the older version. See README.md.
#
# Everything runs from main(), called on the last line: the checkout below can
# replace this very file, and bash reads a script as it goes, so it must have
# read all of it before anything changes.

set -euo pipefail

# The published georiva image's user, and the timescaledb-ha image's.
readonly APP_UID=9999
readonly DB_UID=1000
readonly IMAGE_PREFIX=ghcr.io/wmo-raf/georiva

die() { echo "deploy: $*" >&2; exit 1; }
warn() { echo "deploy: warning: $*" >&2; }
info() { echo "deploy: $*"; }

usage() {
    sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

# The value of KEY in .env, or empty. Last assignment wins, as in compose.
env_value() {
    sed -n "s/^$1=//p" .env | tail -n 1
}

# Set KEY=VALUE in .env in place, appending the key if it is absent.
set_env_value() {
    local key="$1" value="$2" tmp
    tmp="$(mktemp)"
    if grep -q "^$key=" .env; then
        awk -v k="$key" -v v="$value" 'index($0, k "=") == 1 { print k "=" v; next } { print }' .env >"$tmp"
    else
        cat .env >"$tmp"
        printf '%s=%s\n' "$key" "$value" >>"$tmp"
    fi
    # cat, not mv: keeps .env's own owner and mode.
    cat "$tmp" >.env
    rm -f "$tmp"
}

owner_uid() {
    stat -c %u "$1" 2>/dev/null || stat -f %u "$1"
}

# A bind-mounted data directory must exist and belong to the container user,
# or Docker creates it as root and the container cannot write to it. A value
# with no slash is a named volume, which Docker owns, so it is not checked.
check_data_dir() {
    local var="$1" default="$2" uid="$3" dir
    dir="$(env_value "$var")"
    dir="${dir:-$default}"
    [[ "$dir" == */* ]] || return 0
    [[ "$dir" == /* ]] || dir="$PWD/${dir#./}"
    if [[ ! -d "$dir" ]] || [[ "$(owner_uid "$dir")" != "$uid" ]]; then
        bad_dirs+=("$dir")
        bad_owners+=("$uid")
    fi
}

check_data_dirs() {
    bad_dirs=()
    bad_owners=()
    check_data_dir GEORIVA_STATIC_VOLUME ./docker/static "$APP_UID"
    check_data_dir GEORIVA_MEDIA_VOLUME ./docker/media "$APP_UID"
    check_data_dir GEORIVA_BACKUP_VOLUME ./docker/backup "$APP_UID"
    check_data_dir GEORIVA_DB_VOLUME ./docker/db_data "$DB_UID"

    if ((${#bad_dirs[@]})); then
        echo "deploy: these data directories are missing or have the wrong owner:" >&2
        local i
        for i in "${!bad_dirs[@]}"; do
            echo "  ${bad_dirs[$i]}  (needs uid ${bad_owners[$i]})" >&2
        done
        echo "deploy: create them with:" >&2
        for i in "${!bad_dirs[@]}"; do
            echo "  sudo install -d -o ${bad_owners[$i]} -g ${bad_owners[$i]} ${bad_dirs[$i]}" >&2
        done
        exit 1
    fi
}

# Keys the sample lists that this .env does not: new settings a release added.
# Only a warning — a blank falls back to compose's default, and the ones with
# no default stop `docker compose` with their own message.
warn_missing_keys() {
    local sample=../.env.sample missing
    [[ -f "$sample" ]] || { warn "$sample not found; skipping the .env key check"; return 0; }
    missing="$(comm -23 \
        <(sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' "$sample" | sort -u) \
        <(sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' .env | sort -u))"
    if [[ -n "$missing" ]]; then
        warn "keys in .env.sample that .env does not set (copy any you need from ../.env.sample):"
        while read -r key; do echo "  $key" >&2; done <<<"$missing"
    fi
}

# The commit a version names, after fetching it.
resolve_commit() {
    local version="$1"
    case "$version" in
        main)
            git fetch --quiet origin main
            git rev-parse --verify --quiet "origin/main^{commit}"
            ;;
        sha-*)
            git fetch --quiet origin main
            git rev-parse --verify --quiet "${version#sha-}^{commit}"
            ;;
        *)
            git fetch --quiet --force origin "refs/tags/$version:refs/tags/$version"
            git rev-parse --verify --quiet "refs/tags/$version^{commit}"
            ;;
    esac
}

# Remove GeoRiva images other than the version now running and the one it
# replaced, so a one-step rollback needs no download. Third-party images are
# compose's business and are left alone.
prune_images() {
    local keep_new="$1" keep_old="$2" image tag
    while read -r image; do
        tag="${image##*:}"
        [[ "$tag" == "$keep_new" || "$tag" == "$keep_old" ]] && continue
        docker image rm "$image" >/dev/null 2>&1 || true
    done < <(docker image ls --format '{{.Repository}}:{{.Tag}}' |
        grep -E "^${IMAGE_PREFIX}(-titiler-app|-stac-browser)?:" || true)
    docker image prune -f >/dev/null
}

main() {
    [[ $# -eq 1 ]] || usage 1
    case "$1" in -h | --help) usage ;; esac
    local version="$1"

    [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ||
        "$version" == main ||
        "$version" =~ ^sha-[0-9a-f]{7,40}$ ]] ||
        die "'$version' is not a version: use a release tag (v0.5.0), main, or sha-<short>"

    cd "$(dirname "$0")"
    [[ -f .env ]] || die "no .env here. First install: cp ../.env.sample .env, fill it in, then re-run (README.md)"

    command -v docker >/dev/null || die "docker is not installed"
    docker compose version >/dev/null 2>&1 || die "the docker compose plugin is not installed"

    check_data_dirs

    if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
        git status --short --untracked-files=no >&2
        die "tracked files under this checkout are modified. Site changes go in docker-compose.override.yml and .env; restore the rest with: git checkout -- ."
    fi

    [[ "$version" == main ]] && warn "main moves with every merge: re-running this pulls whatever main is now. Use a release tag in production."

    local commit
    commit="$(resolve_commit "$version")" || die "no such version: $version"

    local old_commit old_tag
    old_commit="$(git rev-parse HEAD)"
    old_tag="$(env_value GEORIVA_TAG)"

    info "checking out $version (${commit:0:12})"
    git -c advice.detachedHead=false checkout --quiet "$commit"
    set_env_value GEORIVA_TAG "$version"

    # Nothing has restarted yet, so any failure from here to `up` can put the
    # server back exactly as it was.
    restore() {
        warn "nothing was restarted. Putting the checkout and GEORIVA_TAG back."
        git -c advice.detachedHead=false checkout --quiet "$old_commit"
        set_env_value GEORIVA_TAG "$old_tag"
    }

    # Against the sample this version ships, not the one it replaced.
    warn_missing_keys

    # A release can add a value with no default; compose refuses the file
    # without it. Ask before pulling, so that reads as what it is.
    local config_error
    if ! config_error="$(docker compose config --quiet 2>&1)"; then
        restore
        die "$version's docker-compose.yml does not accept this .env: ${config_error}"
    fi

    info "pulling images for $version"
    if ! docker compose pull; then
        restore
        die "could not pull $version. If it was just released, CI may still be publishing it: check the repo's Actions tab, then re-run."
    fi

    info "starting $version"
    docker compose up -d --remove-orphans

    prune_images "$version" "$old_tag"

    info "now running $version. Migrations run as the georiva container starts: docker compose logs -f georiva"
}

main "$@"
