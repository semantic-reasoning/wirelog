#!/usr/bin/env bash
# Issue #744: SBOM snapshot CI gate.
# Diffs the detected dependency list against the committed baseline.
# SKIPs (exit 77) when syft is not on PATH.  A missing baseline is a
# FAILURE, not a skip: sbom/snapshot.txt is committed, so its absence
# means it was deleted (#1301).

set -euo pipefail

# Meson reads exit 77 as SKIP and exit 0 as a pass, so a branch that cannot
# check anything must exit 77 or the gate is recorded as having asserted
# something it never looked at (#1301).  Matches SKIP_EXIT in
# check-clang-tidy-backlog-monotonic.sh and SKIP in run-doop-perf-gate.sh.
#
# 77 is only safe under meson: a bare workflow `run:` step uses `bash -e`,
# where 77 fails the job.  Do not invoke this from one.
SKIP_EXIT=77

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
baseline="$repo_root/sbom/snapshot.txt"

# SKIP if syft not available
if ! command -v syft >/dev/null 2>&1; then
    echo "check-sbom-snapshot: SKIP: syft not on PATH" >&2
    exit "$SKIP_EXIT"
fi

# NOT a skip: the baseline is committed, so its absence means deletion.
if [ ! -f "$baseline" ]; then
    # Not a skip: sbom/snapshot.txt is committed, so its absence means it was
    # deleted, not that this build cannot answer.
    echo "check-sbom-snapshot: FAIL: baseline missing: $baseline" >&2
    echo "  it is committed, so this means it was deleted." >&2
    echo "  first-time: run scripts/release/generate-sbom.sh <build_root>" >&2
    exit 1
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

# Extract current dependency list as "name@version:license"
syft dir:"$repo_root" -o syft-json 2>/dev/null \
  | jq -r '.artifacts[] | "\(.name)@\(.version // "unknown"):\((.licenses // [{}])[0].value // "NOASSERTION")"' \
  | LC_ALL=C sort > "$tmpdir/current.txt"

# Compare against baseline (exclude comment lines starting with #)
grep -v '^#' "$baseline" | LC_ALL=C sort > "$tmpdir/baseline_clean.txt"

# Compare against baseline
if diff -u "$tmpdir/baseline_clean.txt" "$tmpdir/current.txt"; then
    n=$(wc -l < "$tmpdir/baseline_clean.txt")
    echo "check-sbom-snapshot: OK; $n packages in snapshot match baseline"
    exit 0
else
    echo "check-sbom-snapshot: FAIL: dependency snapshot differs from baseline" >&2
    echo "  To regenerate: scripts/release/generate-sbom.sh <build_root>" >&2
    exit 1
fi
