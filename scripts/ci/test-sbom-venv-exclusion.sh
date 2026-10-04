#!/usr/bin/env bash
# Issue #1919: the SBOM snapshot scan must ignore the repository-root .venv.
#
# AGENTS.md tells contributors to create a uv environment at the repository
# root, and syft catalogs the Python packages installed there (meson and ninja
# among them) as if they were wirelog dependencies, so a normal local setup
# failed sbom_snapshot. The gate and the generator now exclude `./.venv/**`.
#
# This runs the REAL gate and the real syft against a throwaway fixture tree,
# because the property is about what syft catalogs, which a stub cannot show:
#   - with no root .venv the gate accepts the fixture's baseline;
#   - with a populated root .venv (meson, ninja metadata) it still does;
#   - the exclusion is that one directory only: a .venv nested elsewhere, a
#     tracked requirements manifest and a workflow declaration all stay in the
#     baseline, and a change to the manifest still fails the gate.
#
# The gate derives repo_root from its own location, so a copy of it inside the
# fixture scans the fixture and never the real tree.
set -euo pipefail

skip() {
    if [ "${WIRELOG_SBOM_REQUIRED:-0}" = "1" ]; then
        echo "test-sbom-venv-exclusion: FAIL: $* (WIRELOG_SBOM_REQUIRED=1 forbids skipping)" >&2
        exit 1
    fi
    echo "test-sbom-venv-exclusion: SKIP: $*"
    exit 77
}
command -v syft >/dev/null 2>&1 || skip "syft not on PATH"
command -v jq >/dev/null 2>&1 || skip "jq not on PATH"

root=$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-sbom-venv.XXXXXX")
case "$tmp" in ""|"/") echo "FATAL: refusing sandbox '$tmp'" >&2; exit 2 ;; esac
trap 'rm -rf "$tmp"' EXIT
repo="$tmp/repo"
mkdir -p "$repo/scripts/ci" "$repo/sbom" "$repo/.github/workflows"
cp "$root/scripts/ci/check-sbom-snapshot.sh" "$repo/scripts/ci/"

cat > "$repo/.github/workflows/ci.yml" <<'EOF'
name: ci
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
EOF
printf 'requests==2.31.0\n' > "$repo/requirements.txt"

# Installed-package metadata in the layout uv and pip produce.
dist_info() {
    local dir="$1/lib/python3.14/site-packages/$2-$3.dist-info"
    mkdir -p "$dir"
    printf 'Metadata-Version: 2.1\nName: %s\nVersion: %s\nLicense: MIT\n' \
        "$2" "$3" > "$dir/METADATA"
    printf '%s/__init__.py,,\n' "$2" > "$dir/RECORD"
}
dist_info "$repo/tools/.venv" pyyaml 6.0.1

cat > "$repo/sbom/snapshot.txt" <<'EOF'
# fixture baseline
actions/checkout@v4:NOASSERTION
pyyaml@6.0.1:MIT
requests@2.31.0:NOASSERTION
EOF

failures=0
gate() { "$repo/scripts/ci/check-sbom-snapshot.sh" >"$tmp/out" 2>&1; }
# $3, when given, must appear in the gate's output: a failure for the wrong
# reason (say, the .venv packages leaking in) must not satisfy a case.
expect() {
    local what="$1" want="$2" needle="${3:-}" rc=0
    gate || rc=$?
    if [ "$rc" -eq "$want" ] \
        && { [ -z "$needle" ] || grep -Fq -- "$needle" "$tmp/out"; }; then
        echo "ok: $what"
    else
        echo "FAIL: $what (exit $rc, wanted $want)" >&2
        sed 's/^/    /' "$tmp/out" >&2
        failures=$((failures + 1))
    fi
}

expect 'without a root .venv the gate keeps the nested .venv, manifest and workflow' 0

dist_info "$repo/.venv" meson 1.12.0
dist_info "$repo/.venv" ninja 1.13.2
expect 'a populated root .venv does not change the snapshot' 0

printf 'urllib3==2.0.7\n' >> "$repo/requirements.txt"
expect 'a tracked manifest change still fails the gate with .venv present' 1 \
    '+urllib3@2.0.7'
if grep -Eq '^\+(meson|ninja)@' "$tmp/out"; then
    echo "FAIL: the root .venv packages appear in the gate's diff" >&2
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    echo "test-sbom-venv-exclusion: $failures case(s) failed" >&2
    exit 1
fi
echo "test-sbom-venv-exclusion: all cases passed"
