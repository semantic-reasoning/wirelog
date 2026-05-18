#!/usr/bin/env bash
# scripts/ci/check-phase-labels.sh - Issue #737.
#
# Forbid new unanchored "Phase NX" labels in source (.c / .h under
# wirelog/).  A "Phase NX" label is a token of the form:
#
#     Phase <digit(s)><uppercase letter>[trailing chars]
#
# Examples: "Phase 2A", "Phase 3C", "Phase 3C-001-Ext", "Phase 4B".
# These are internal commit-series labels per the global rule in
# CLAUDE.md and are not meaningful to external readers.
#
# The gate is a ratchet:
#
#   - Any unanchored Phase NX label must either have a public-issue
#     cross-reference on the same line ("#N" / "Issue N" / "US-N-NNN"),
#     or be present in scripts/ci/phase-label-allowlist.txt as a
#     historical grandfathered entry.
#   - The allowlist is keyed on `file|content` (line numbers stripped)
#     so it survives reformatting that moves lines around.
#   - The allowlist may shrink as cleanup PRs land; that is expected.
#     It must NEVER grow without an explicit decision in the PR
#     description.
#
# Exit codes:
#   0 - no new unanchored labels detected
#   1 - new unanchored labels detected, or the allowlist file is
#       missing / unreadable.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
allowlist="$script_dir/phase-label-allowlist.txt"

if [ ! -f "$allowlist" ]; then
    echo "check-phase-labels: FAIL: allowlist missing at $allowlist" >&2
    exit 1
fi

# Collect current unanchored Phase NX matches in source.
# Filter out lines that already cite a public issue / Souffle US- task.
# Use relative paths (cd into repo_root) so the allowlist is portable
# across checkout locations.
current=$(cd "$repo_root" && \
          grep -rEHn "Phase [0-9]+[A-Z][A-Za-z0-9.-]*" wirelog \
            --include='*.c' --include='*.h' 2>/dev/null \
          | grep -vE "#[0-9]+|Issue [0-9]+|US-[0-9.A-Za-z-]+" \
          | sed -E 's/^([^:]+):[0-9]+:(.*)$/\1|\2/' \
          | sort -u \
          || true)

fail=0
while IFS= read -r entry; do
    [ -z "$entry" ] && continue
    if ! grep -Fxq "$entry" "$allowlist"; then
        if [ "$fail" -eq 0 ]; then
            echo "check-phase-labels: FAIL: new unanchored 'Phase NX' label(s) detected" >&2
            echo "" >&2
            echo "Each match below must either (a) have an '(see #N)' / 'Issue N' / 'US-N-NNN'" >&2
            echo "cross-reference on the same line, or (b) be appended to:" >&2
            echo "  $allowlist" >&2
            echo "" >&2
            echo "If you legitimately need a new historical reference, append the" >&2
            echo "match and explain it in the PR description.  Otherwise reword" >&2
            echo "the comment to remove the bare phase label." >&2
            echo "" >&2
            echo "New matches:" >&2
        fi
        echo "  $entry" >&2
        fail=1
    fi
done <<< "$current"

if [ "$fail" -ne 0 ]; then
    exit 1
fi

# Soft warning: stale allowlist entries that no longer appear in source.
# Not a failure -- cleanup PRs will naturally orphan entries when they
# remove the underlying labels.  The maintainer-side workflow is to
# prune the allowlist as a follow-up.
stale_count=0
while IFS= read -r line; do
    [ -z "$line" ] && continue
    if ! grep -Fxq "$line" <(echo "$current"); then
        stale_count=$((stale_count + 1))
    fi
done < "$allowlist"

if [ "$stale_count" -gt 0 ]; then
    echo "check-phase-labels: OK (with $stale_count stale allowlist entr$([ $stale_count -eq 1 ] && echo y || echo ies); prune via follow-up PR)"
else
    echo "check-phase-labels: OK"
fi
exit 0
