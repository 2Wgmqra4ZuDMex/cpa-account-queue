#!/usr/bin/env bash
# Builds the plugin library and, from the same source, a second library with
# different bytes for tests/host_upgrade.py: the host maps byte-identical
# libraries to one image, which would not be a replacement.
# Usage: scripts/build_for_host_tests.sh <directory>
set -euo pipefail
out="$1"
cd "$(dirname "$0")/.."
mkdir -p "$out"
CGO_ENABLED=1 go build -trimpath -buildmode=c-shared -o "$out/cpa-account-queue.so" ./cmd/plugin
# Without -trimpath the build directory is embedded, so the bytes differ.
CGO_ENABLED=1 go build -buildmode=c-shared -o "$out/cpa-account-queue-upgrade-target.so" ./cmd/plugin
if cmp -s "$out/cpa-account-queue.so" "$out/cpa-account-queue-upgrade-target.so"; then
  echo "the upgrade target is byte-identical to the plugin build" >&2
  exit 1
fi
