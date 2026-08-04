#!/usr/bin/env bash
# check-doop-catalogue.sh - keep the four DOOP fact catalogues in agreement.
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
#
# The set of DOOP .facts files is written down in four places:
#
#   bench/data/doop/download.sh        REQUIRED     (what is extracted)
#   bench/bench_flowlog.c              doop_edbs[]  (what is loaded)
#   tests/test_option2_doop.c          doop_fact_files[]
#   scripts/run_doop_validation.sh     the expected file count
#
# Issue #950 happened because these drifted apart silently: the archive
# switched from .csv to .facts, one catalogue was updated and the others
# were not, and the test that should have caught it skipped itself instead.
# Issue #952 added this check so the next drift fails a build rather than
# waiting to be noticed.
#
# Source-only: parses the four files and needs no dataset, so it runs in CI
# on hosts that will never download the 740 MB archive.
#
# Usage: scripts/ci/check-doop-catalogue.sh [project_root]

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

DOWNLOAD="$ROOT/bench/data/doop/download.sh"
BENCH="$ROOT/bench/bench_flowlog.c"
TEST="$ROOT/tests/test_option2_doop.c"
VALIDATE="$ROOT/scripts/run_doop_validation.sh"

for f in "$DOWNLOAD" "$BENCH" "$TEST" "$VALIDATE"; do
    if [[ ! -f "$f" ]]; then
        echo "ERROR: missing $f" >&2
        exit 1
    fi
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# download.sh lists bare relation names between REQUIRED=" and the closing
# quote; the .facts suffix is appended at the use site.
sed -n '/^REQUIRED="/,/"$/p' "$DOWNLOAD" \
    | sed 's/^REQUIRED="//; s/"$//' \
    | tr ' \t' '\n\n' \
    | sed '/^$/d; s/$/.facts/' \
    | sort -u > "$tmp/download"

# The two C catalogues are the only quoted "*.facts" literals in their files.
grep -o '"[A-Za-z0-9_-]*\.facts"' "$BENCH" | tr -d '"' | sort -u > "$tmp/bench"
grep -o '"[A-Za-z0-9_-]*\.facts"' "$TEST" | tr -d '"' | sort -u > "$tmp/test"

# A parse that silently yields nothing would make every set trivially equal --
# the exact shape of failure this check exists to catch.  Demand a plausible
# floor rather than merely non-empty.
for name in download bench test; do
    n=$(wc -l < "$tmp/$name")
    if [[ "$n" -lt 30 ]]; then
        echo "ERROR: extracted only $n .facts names from the '$name' catalogue." >&2
        echo "       The catalogue shrank drastically or this parser broke;" >&2
        echo "       either way do not treat the comparison below as meaningful." >&2
        exit 1
    fi
done

rc=0

if ! diff -u "$tmp/download" "$tmp/bench" > "$tmp/d1"; then
    echo "ERROR: download.sh REQUIRED and bench_flowlog.c doop_edbs[] disagree:" >&2
    sed 's/^/    /' "$tmp/d1" >&2
    rc=1
fi

if ! diff -u "$tmp/download" "$tmp/test" > "$tmp/d2"; then
    echo "ERROR: download.sh REQUIRED and test_option2_doop.c doop_fact_files[]" >&2
    echo "       disagree:" >&2
    sed 's/^/    /' "$tmp/d2" >&2
    rc=1
fi

EXPECTED=$(wc -l < "$tmp/download")

# DOOP_NRELS sizes doop_edbs[]; a mismatch is a compile error there, but it is
# still worth naming here so the count appears in one report.
NRELS=$(sed -n 's/^#define DOOP_NRELS \([0-9]*\).*/\1/p' "$BENCH" | head -1)
if [[ "$NRELS" != "$EXPECTED" ]]; then
    echo "ERROR: DOOP_NRELS is $NRELS but the catalogue has $EXPECTED files." >&2
    rc=1
fi

# run_doop_validation.sh gates on the file count before it runs anything.
COUNT=$(sed -n 's/^DOOP_FACT_COUNT_EXPECTED=\([0-9]*\).*/\1/p' "$VALIDATE" | head -1)
if [[ -z "$COUNT" ]]; then
    echo "ERROR: run_doop_validation.sh has no DOOP_FACT_COUNT_EXPECTED." >&2
    echo "       It gates on the fact-file count, so that count must be" >&2
    echo "       greppable from here or this check silently stops covering it." >&2
    rc=1
elif [[ "$COUNT" != "$EXPECTED" ]]; then
    echo "ERROR: run_doop_validation.sh expects $COUNT fact files," >&2
    echo "       but the catalogue has $EXPECTED." >&2
    rc=1
fi

if [[ "$rc" -eq 0 ]]; then
    echo "DOOP catalogue check: OK ($EXPECTED .facts files agreed across 4 sources)"
fi

exit "$rc"
