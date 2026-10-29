#!/usr/bin/env bash
# Hold deploy/docker-compose.yml to the root docker-compose.yml.
#
# The root file is the dev/build setup; deploy/ is the image-only copy servers
# run. Both are rendered by compose itself against one throwaway .env, the
# differences deploy/ is meant to have are normalised away, and whatever is
# left is drift: a change made to one file and not the other. Those intended
# differences, and nothing else:
#
#   - deploy/ builds nothing, and pulls ghcr.io/wmo-raf/<image>:${GEORIVA_TAG}
#   - deploy/ publishes no host port but the web proxy's
#   - deploy/ uses compose's own network and names its project explicitly
#   - deploy/'s paths are relative to deploy/
#   - deploy/ requires the site values the root file defaults for dev — the
#     throwaway .env sets them, so both render the same
#
# Run from anywhere: .github/scripts/check-compose-drift.sh

set -euo pipefail

repo="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# The sample, with every blank that compose refuses filled in.
cp .env.sample "$work/env"
cat >>"$work/env" <<'EOF'
GEORIVA_TAG=drift-check
SECRET_KEY=drift-check
GEORIVA_DB_USER=georiva
GEORIVA_DB_PASSWORD=drift-check
GEORIVA_DB_NAME=georiva
MINIO_ROOT_USER=georiva
MINIO_ROOT_PASSWORD=drift-check
GEORIVA_BASE_DOMAIN=example.org
MINIO_PUBLIC_ENDPOINT=s3.example.org
GEORIVA_NETWORK_NAME=drift-check
UID=1000
GID=1000
EOF

# Nothing from the caller's shell may leak into the interpolation.
render() {
    env -i PATH="$PATH" HOME="$HOME" docker compose -f "$1" --env-file "$work/env" config --format json
}

render docker-compose.yml >"$work/root.json"
render deploy/docker-compose.yml >"$work/deploy.json"

if jq -e '[.services[] | select(has("build"))] | length > 0' "$work/deploy.json" >/dev/null; then
    echo "deploy/docker-compose.yml must build nothing; these services have build:" >&2
    jq -r '.services | to_entries[] | select(.value | has("build")) | "  " + .key' "$work/deploy.json" >&2
    exit 1
fi

# Both files' paths, rooted at the repo — the root file names deploy/nginx/...
# where deploy/ says ./nginx/..., and deploy/'s ./docker/ is deploy/docker/.
# shellcheck disable=SC2016  # a jq program: $repo and $root are jq's
normalise='
  def published($svc):
    {"georiva": "georiva", "georiva-titiler-app": "georiva-titiler-app",
     "georiva-stac-browser": "georiva-stac-browser"}[$svc] as $name
    | if $name then "ghcr.io/wmo-raf/\($name):drift-check" else null end;
  del(.name, .networks)
  | with_entries(select(.key | startswith("x-") | not))
  | walk(if type == "string" then gsub("^" + $repo + "/deploy/"; $repo + "/") else . end)
  | .services |= with_entries(
      .key as $svc
      | .value |= (
          del(.build)
          # Every service sharing the georiva image is published as georiva.
          | if $root and (.image == "georiva") then .image = published("georiva")
            elif $root and published($svc) then .image = published($svc)
            else . end
          | if $root and $svc != "georiva-web-proxy" then del(.ports) else . end
        )
    )
'

jq -S --arg repo "$repo" --argjson root true "$normalise" "$work/root.json" >"$work/root.norm.json"
jq -S --arg repo "$repo" --argjson root false "$normalise" "$work/deploy.json" >"$work/deploy.norm.json"

if ! diff -u --label docker-compose.yml --label deploy/docker-compose.yml \
    "$work/root.norm.json" "$work/deploy.norm.json"; then
    cat >&2 <<'EOF'

deploy/docker-compose.yml has drifted from docker-compose.yml (rendered and
normalised; - is the root file, + is deploy/). Make the same change in both,
or, if the difference is one deploy/ is meant to have, teach this script it
(.github/scripts/check-compose-drift.sh).
EOF
    exit 1
fi

echo "deploy/docker-compose.yml matches docker-compose.yml"
