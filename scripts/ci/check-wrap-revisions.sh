#!/usr/bin/env bash
# check-wrap-revisions.sh - Issue #715 release dependency pin guard.
#
# Reject Meson wrap files that depend on floating git branch names. Release
# dependency wraps must be pinned to immutable revisions or checksum-verified
# archives.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

fail=0
for wrap in "$repo_root"/subprojects/*.wrap; do
    [ -e "$wrap" ] || continue
    matches=$(mktemp)
    if grep -nEi '^[[:space:]]*revision[[:space:]]*=[[:space:]]*(main|master)([[:space:]]*(#.*)?)?$' \
            "$wrap" >"$matches"; then
        if [ "$fail" -eq 0 ]; then
            echo "check-wrap-revisions: FAIL: floating wrap revisions found" >&2
        fi
        while IFS= read -r match; do
            echo "  ${wrap#$repo_root/}:$match" >&2
        done <"$matches"
        fail=1
    fi
    rm -f "$matches"
done

if [ "$fail" -ne 0 ]; then
    echo "" >&2
    echo "Pin release dependency wraps to immutable commits or use" >&2
    echo "checksum-verified wrap-file archives." >&2
    exit 1
fi

echo "check-wrap-revisions: OK; no revision=main/master in subprojects/*.wrap"
