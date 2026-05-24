#!/usr/bin/env bash
# Issue #744: SBOM snapshot CI gate.
# Diffs the detected dependency list against the committed baseline.
# SKIPs when syft is not on PATH or baseline file is absent.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
baseline="$repo_root/sbom/snapshot.txt"

# SKIP if syft not available
if ! command -v syft >/dev/null 2>&1; then
    echo "check-sbom-snapshot: SKIP: syft not on PATH" >&2
    exit 0
fi

# SKIP if baseline file doesn't exist (first time; regenerate via scripts/release/generate-sbom.sh)
if [ ! -f "$baseline" ]; then
    echo "check-sbom-snapshot: SKIP: baseline missing: $baseline" >&2
    echo "  first-time: run scripts/release/generate-sbom.sh <build_root>" >&2
    exit 0
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

# Extract current dependency list as "name@version:license"
syft dir:"$repo_root" -o syft-json 2>/dev/null \
  | jq -r '.artifacts[] | "\(.name)@\(.version // "unknown"):\((.licenses // [{}])[0].value // "NOASSERTION")"' \
  | sort > "$tmpdir/current.txt"

# Compare against baseline
if diff -u "$baseline" "$tmpdir/current.txt"; then
    n=$(wc -l < "$baseline")
    echo "check-sbom-snapshot: OK; $n packages in snapshot match baseline"
    exit 0
else
    echo "check-sbom-snapshot: FAIL: dependency snapshot differs from baseline" >&2
    echo "  To regenerate: scripts/release/generate-sbom.sh <build_root>" >&2
    exit 1
fi
