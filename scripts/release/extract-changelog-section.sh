#!/usr/bin/env bash
# extract-changelog-section.sh - emit one CHANGELOG section to stdout.
#
# Usage:
#   scripts/release/extract-changelog-section.sh <version> [path]
#
# Where <version> is a Semver string (e.g. 1.0.0) and the optional
# [path] defaults to <repo_root>/CHANGELOG.md.
#
# Emits the lines between `## [<version>] - YYYY-MM-DD` (inclusive)
# and the next `## [...]` heading (exclusive), so the result is the
# verbatim release-section content.  Designed to feed `gh release
# create --notes-file` per docs/RELEASE_PROCESS.md §2.5 and to back
# the CI gate at scripts/ci/check-release-template.sh.

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <version> [path/to/CHANGELOG.md]" >&2
    exit 1
fi

version="$1"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
path="${2:-$repo_root/CHANGELOG.md}"

if [ ! -f "$path" ]; then
    echo "extract-changelog-section: $path does not exist" >&2
    exit 1
fi

# Stream the lines starting at `## [<version>] - ...` and stop at the
# next `## [` line (the next versioned or `[Unreleased]` heading).
awk -v v="$version" '
    BEGIN { active = 0 }
    /^## \[/ {
        if (active) { exit }
        if ($0 ~ "^## \\[" v "\\] - ") { active = 1; print; next }
    }
    active { print }
' "$path"
