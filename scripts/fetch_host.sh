#!/usr/bin/env bash
# Downloads one CPA release for linux/amd64, verifies it against that
# release's checksums.txt, unpacks it and prints the path of the host binary.
# Usage: scripts/fetch_host.sh <version, with or without a leading v> <directory>
set -euo pipefail
version="${1#v}"
out="$2"
mkdir -p "$out"
tarball="CLIProxyAPI_${version}_linux_amd64.tar.gz"
base="https://github.com/router-for-me/CLIProxyAPI/releases/download/v${version}"
curl -fsSL --retry 3 "$base/$tarball" -o "$out/$tarball"
curl -fsSL --retry 3 "$base/checksums.txt" -o "$out/checksums.txt"
(cd "$out" && grep " ${tarball}\$" checksums.txt | sha256sum -c - >&2)
tar -xzf "$out/$tarball" -C "$out"
binary="$(find "$out" -maxdepth 2 -type f -iname 'cli*proxy*' ! -name '*.tar.gz' | head -n 1)"
test -n "$binary"
echo "$binary"
