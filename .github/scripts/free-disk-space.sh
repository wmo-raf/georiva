#!/usr/bin/env bash
# Free disk on a GitHub-hosted runner before an image build.
#
# The dev image sits on gdal:ubuntu-full and is several gigabytes. `load: true`
# exports it from buildx into the runner's Docker store as a tarball, so the
# image briefly exists twice on a disk that starts with only ~14 GB free — and
# some runners fail the export with "no space left on device" while others,
# running the identical build, pass. The toolchains removed here (.NET, Android
# SDK, GHC, CodeQL) are preinstalled and used by nothing in this workflow.
set -euo pipefail

echo "before:"
df -h / | tail -1

sudo rm -rf \
  /usr/share/dotnet \
  /usr/local/lib/android \
  /opt/ghc \
  /opt/hostedtoolcache/CodeQL \
  /usr/local/.ghcup \
  /usr/share/swift

echo "after:"
df -h / | tail -1
