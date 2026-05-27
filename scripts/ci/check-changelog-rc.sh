#!/usr/bin/env bash
# check-changelog-rc.sh - Issue #747 RC changelog freeze gate.
#
# Enforces the RC freeze policy only for PRs targeting branch `1.0`.
# For non-1.0 PRs, SKIPs cleanly with exit 0 so `main` remains unaffected.
#
# Active checks (base-ref == 1.0):
#   1) meson.build project version must be 1.0.0 or 1.0.0-* (e.g. 1.0.0-rc1)
#   2) CHANGELOG.md edits are restricted to the [1.0.0] section only, and
#      [Unreleased] must match the base branch byte-for-byte.
#
# Usage:
#   scripts/ci/check-changelog-rc.sh [--base-ref REF]
#                                    [--base-changelog PATH]
#                                    [--head-changelog PATH]
#                                    [--meson-file PATH]
#
# Exit codes:
#   0 - OK or SKIP.
#   1 - policy violation or hard failure.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

base_ref="${GITHUB_BASE_REF:-main}"
base_changelog_override=""
head_changelog="$repo_root/CHANGELOG.md"
meson_file="$repo_root/meson.build"

usage() {
    cat <<EOF
usage: check-changelog-rc.sh [--base-ref REF] [--base-changelog PATH] [--head-changelog PATH] [--meson-file PATH]
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --base-ref)
            base_ref="${2:-}"
            shift 2
            ;;
        --base-changelog)
            base_changelog_override="${2:-}"
            shift 2
            ;;
        --head-changelog)
            head_changelog="${2:-}"
            shift 2
            ;;
        --meson-file)
            meson_file="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "check-changelog-rc: FAIL: unknown argument '$1'" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [ "$base_ref" != "1.0" ]; then
    echo "check-changelog-rc: SKIP: base ref is '$base_ref' (policy only enforces on '1.0')"
    exit 0
fi

if [ ! -f "$meson_file" ]; then
    echo "check-changelog-rc: FAIL: meson file not found: $meson_file" >&2
    exit 1
fi

if [ ! -f "$head_changelog" ]; then
    echo "check-changelog-rc: FAIL: head changelog not found: $head_changelog" >&2
    exit 1
fi

project_version="$(sed -nE "s/^[[:space:]]*version:[[:space:]]*'([^']+)'.*/\1/p" "$meson_file" | head -n1)"
if [ -z "$project_version" ]; then
    echo "check-changelog-rc: FAIL: could not parse project version from $meson_file" >&2
    exit 1
fi

case "$project_version" in
    1.0.0|1.0.0-*)
        ;;
    *)
        echo "check-changelog-rc: FAIL: project version '$project_version' is invalid for base '1.0'" >&2
        echo "  expected: 1.0.0 or 1.0.0-* (e.g. 1.0.0-rc1)" >&2
        exit 1
        ;;
esac

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

base_changelog="$workdir/base-CHANGELOG.md"

if [ -n "$base_changelog_override" ]; then
    if [ ! -f "$base_changelog_override" ]; then
        echo "check-changelog-rc: FAIL: base changelog not found: $base_changelog_override" >&2
        exit 1
    fi
    cp "$base_changelog_override" "$base_changelog"
else
    if ! git -C "$repo_root" fetch --no-tags origin \
        "+refs/heads/$base_ref:refs/remotes/origin/$base_ref" >/dev/null 2>&1; then
        echo "check-changelog-rc: FAIL: could not fetch origin/$base_ref" >&2
        exit 1
    fi
    if ! git -C "$repo_root" show "origin/$base_ref:CHANGELOG.md" > "$base_changelog"; then
        echo "check-changelog-rc: FAIL: could not read CHANGELOG.md from origin/$base_ref" >&2
        exit 1
    fi
fi

extract_section() {
    # $1 = section key (inside brackets), $2 = input file, $3 = output file
    local section="$1"
    local infile="$2"
    local outfile="$3"

    awk -v section="$section" '
BEGIN {
    target = "## [" section "]"
    in_section = 0
    found = 0
}
{
    is_target = ($0 == target || index($0, target " ") == 1)
    is_h2 = ($0 ~ /^## \[/)

    if (is_target) {
        in_section = 1
        found = 1
    } else if (in_section && is_h2) {
        exit
    }

    if (in_section) {
        print
    }
}
END {
    if (!found) {
        exit 3
    }
}
' "$infile" > "$outfile"
}

strip_section() {
    # $1 = section key (inside brackets), $2 = input file, $3 = output file
    local section="$1"
    local infile="$2"
    local outfile="$3"

    awk -v section="$section" '
BEGIN {
    target = "## [" section "]"
    in_section = 0
}
{
    is_target = ($0 == target || index($0, target " ") == 1)
    is_h2 = ($0 ~ /^## \[/)

    if (is_target) {
        in_section = 1
        next
    }
    if (in_section && is_h2) {
        in_section = 0
    }
    if (!in_section) {
        print
    }
}
' "$infile" > "$outfile"
}

base_unreleased="$workdir/base-unreleased.md"
head_unreleased="$workdir/head-unreleased.md"
extract_section "Unreleased" "$base_changelog" "$base_unreleased" || {
    echo "check-changelog-rc: FAIL: base CHANGELOG missing '## [Unreleased]'" >&2
    exit 1
}
extract_section "Unreleased" "$head_changelog" "$head_unreleased" || {
    echo "check-changelog-rc: FAIL: head CHANGELOG missing '## [Unreleased]'" >&2
    exit 1
}

if ! cmp -s "$base_unreleased" "$head_unreleased"; then
    echo "check-changelog-rc: FAIL: CHANGELOG [Unreleased] is frozen on base '1.0'" >&2
    diff -u "$base_unreleased" "$head_unreleased" >&2 || true
    exit 1
fi

base_without_100="$workdir/base-without-1.0.0.md"
head_without_100="$workdir/head-without-1.0.0.md"
strip_section "1.0.0" "$base_changelog" "$base_without_100"
strip_section "1.0.0" "$head_changelog" "$head_without_100"

if ! cmp -s "$base_without_100" "$head_without_100"; then
    echo "check-changelog-rc: FAIL: CHANGELOG edits must be confined to '## [1.0.0]' on base '1.0'" >&2
    diff -u "$base_without_100" "$head_without_100" >&2 || true
    exit 1
fi

echo "check-changelog-rc: OK: base '1.0' policy satisfied (version=$project_version; [Unreleased] frozen; edits scoped to [1.0.0])"
exit 0
