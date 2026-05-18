#!/usr/bin/env bash
# scripts/ci/check-semantics-future.sh - Issue #742.
#
# Asserts that every `Status: Future` entry in `docs/SEMANTICS.md`
# is anchored to a public milestone or issue.  A `Status: Future`
# declaration without a schedule is an unbounded liability for the
# 1.0 stable contract: external embedders cannot estimate when the
# Future entry becomes Current.
#
# Rule:
#   Within an 8-line window starting at each `Status: Future` heading,
#   the document MUST cite at least one of:
#     - a GitHub issue reference (`#NNN`)
#     - a milestone marker (`milestone:` / `Milestone:` / `Milestone NNN`
#       / `v0.NN` / `v1.0.NN`)
#
#   The 8-line window is wide enough to span "## Heading (Status: Future)"
#   followed by a "Tracked under #NNN" or "Target: milestone v0.42" line.
#
# Exit codes:
#   0 - every Future entry has an anchor, or no Future entries exist
#   1 - at least one Future entry is unanchored
#   1 - docs/SEMANTICS.md is missing
#
# Intended to register under `meson test --suite abi:semantics_future`.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
doc="$repo_root/docs/SEMANTICS.md"

if [ ! -f "$doc" ]; then
    echo "check-semantics-future: FAIL: $doc is missing" >&2
    exit 1
fi

# Find each Status: Future heading and capture its line number.
future_lines=$(grep -nE 'Status: *Future' "$doc" || true)

if [ -z "$future_lines" ]; then
    echo "check-semantics-future: OK (no Status: Future entries)"
    exit 0
fi

fail=0
while IFS= read -r match; do
    [ -z "$match" ] && continue
    line=$(echo "$match" | cut -d: -f1)
    text=$(echo "$match" | cut -d: -f2-)

    # Read the 8-line window starting at the heading line.
    start=$line
    end=$((line + 7))
    window=$(sed -n "${start},${end}p" "$doc")

    # Require an issue reference or a milestone marker within the window.
    if ! echo "$window" \
            | grep -qE '#[0-9]+|[mM]ilestone[: ]|v[0-9]+\.[0-9]+'; then
        if [ "$fail" -eq 0 ]; then
            echo "check-semantics-future: FAIL: unanchored Status: Future entry/entries detected" >&2
            echo "" >&2
            echo "Each Status: Future entry MUST cite a milestone or issue link" >&2
            echo "within the 8-line window starting at the heading.  Use either:" >&2
            echo "  - 'Tracked under #NNN' / '(see #NNN)'" >&2
            echo "  - 'Milestone v0.NN' / 'Target: milestone v0.NN'" >&2
            echo "" >&2
            echo "Unanchored entries:" >&2
        fi
        echo "  $doc:$line: $text" >&2
        fail=1
    fi
done <<< "$future_lines"

if [ "$fail" -ne 0 ]; then
    exit 1
fi

count=$(echo "$future_lines" | wc -l | tr -d ' ')
echo "check-semantics-future: OK ($count Status: Future entr$([ "$count" -eq 1 ] && echo y || echo ies), all anchored)"
exit 0
