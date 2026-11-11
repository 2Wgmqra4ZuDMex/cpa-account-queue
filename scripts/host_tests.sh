#!/usr/bin/env bash
# Runs every real-host test against one CPA binary and reports all failures.
# Usage: scripts/host_tests.sh HOST PLUGIN.so UPGRADE_TARGET.so [PUBLISHED_0.2.0-dev.so]
#
# UPGRADE_TARGET.so is the same plugin built to different bytes; the host maps
# byte-identical libraries to one image, which would not be a replacement.
# With the published 0.2.0-dev library the upgrade defect it had is reproduced
# as well; leave it out for a host version the reproduction was not written for.
# PYTHON selects the interpreter (needs PyYAML).
set -uo pipefail
host="$1"
plugin="$2"
target="$3"
published="${4:-}"
py="${PYTHON:-python3}"
cd "$(dirname "$0")/.."

failed=()
run() {
  echo "::group::$*"
  if ! timeout 300 "$py" "$@"; then
    failed+=("$*")
  fi
  echo "::endgroup::"
}

run tests/host_integration.py "$host" "$plugin"
run tests/host_priority.py "$host" "$plugin"
run tests/host_affinity.py "$host" "$plugin"
run tests/host_selection.py "$host" "$plugin"
run tests/host_quota.py "$host" "$plugin"
run tests/host_quota.py "$host" "$plugin" --stream
run tests/host_quota.py "$host" "$plugin" --host-selection
run tests/host_quota.py "$host" "$plugin" --host-selection --stream
run tests/host_upgrade.py "$host" "$plugin" "$target"
run tests/host_upgrade.py "$host" "$plugin" "$target" --between-requests
if [ -n "$published" ]; then
  run tests/host_upgrade.py "$host" "$published" "$plugin" --expect-unregistered
fi

if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED host tests on $host:"
  printf '  %s\n' "${failed[@]}"
  exit 1
fi
echo "All host tests passed."
