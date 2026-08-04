#!/usr/bin/env bash
# scripts/run_doop_validation.sh - Full DOOP benchmark validation (US-010)
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
#
# Runs the DOOP Java points-to analysis (zxing dataset, ~740MB, 35 .facts
# files) using the bench_flowlog binary and captures wall time, peak RSS,
# tuple count, and iteration count.  Reports PASS/FAIL based on completion
# plus tuple- and iteration-count oracles when they are provided.
#
# Usage:
#   scripts/run_doop_validation.sh [--build-dir DIR] [--workers N]
#                                  [--repeat N] [--baseline FILE]
#                                  [--expected-tuples N]
#                                  [--expected-iterations N] [--save-results]
#
# Options:
#   --build-dir DIR   Meson build directory (default: build)
#   --workers N       Worker thread count (default: 1)
#   --repeat N        Benchmark repeat count (default: 1)
#   --baseline FILE   TSV file with baseline metrics for comparison
#   --expected-tuples N
#                     Expected DOOP output tuple count. Defaults to
#                     DOOP_EXPECTED_TUPLES when set; otherwise defaults to
#                     6069774 for the bundled bench/data/doop zxing dataset.
#   --expected-iterations N
#                     Expected fixpoint iteration count. Defaults to
#                     DOOP_EXPECTED_ITERATIONS when set; otherwise defaults
#                     to 28 for the bundled dataset.  See "Oracles" below.
#   --save-results    Write a validation TSV under docs/performance
#
# Environment:
#   DOOP_DATA_DIR     Override for the DOOP .facts data directory
#                     (default: bench/data/doop relative to project root)
#   DOOP_EXPECTED_TUPLES
#                     Expected DOOP output tuple count
#   DOOP_EXPECTED_ITERATIONS
#                     Expected fixpoint iteration count
#
# Oracles:
#   Both defaults describe the archive with sha256
#   154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7,
#   measured on 2026-08-04 (issue #952).  Supply --expected-tuples on any
#   other dataset; the tuple count is dataset-specific.
#
#   The iteration count is checked as a cheap second signal, not as a
#   sensitive one.  It does catch some rule defects the tuple total hides:
#   the heap-typing bug fixed in #951 ran 70 iterations while its tuple
#   count stayed within 3% of correct.  But it is insensitive in the other
#   direction -- 28 is pinned by the depth of the points-to fixpoint and
#   survives configurations in which the entire type hierarchy derives
#   zero rows.  Do not read a matching iteration count as evidence that
#   the analysis is right.
#
# Output:
#   Benchmark output plus validation summary printed to stdout.
#   Exit code 0 = DOOP completed successfully (PASS).
#   Exit code 1 = DOOP failed or binary not found.
#
# Example:
#   cd /path/to/wirelog
#   meson compile -C build bench_flowlog
#   scripts/run_doop_validation.sh --workers 1 --repeat 3

set -euo pipefail

# -------------------------------------------------------------------------
# Defaults
# -------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUILD_DIR="${BUILD_DIR:-build}"
WORKERS="${WORKERS:-1}"
REPEAT="${REPEAT:-1}"
BASELINE_FILE=""
EXPECTED_TUPLES="${DOOP_EXPECTED_TUPLES:-}"
EXPECTED_ITERATIONS="${DOOP_EXPECTED_ITERATIONS:-}"
SAVE_RESULTS=0

# Number of .facts files the bundled dataset must contain.  Kept in step with
# download.sh, doop_edbs[], and doop_fact_files[] by
# scripts/ci/check-doop-catalogue.sh -- that check greps this exact name.
DOOP_FACT_COUNT_EXPECTED=35

# -------------------------------------------------------------------------
# Argument parsing
# -------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --repeat)
            REPEAT="$2"
            shift 2
            ;;
        --baseline)
            BASELINE_FILE="$2"
            shift 2
            ;;
        --expected-tuples)
            EXPECTED_TUPLES="$2"
            shift 2
            ;;
        --expected-iterations)
            EXPECTED_ITERATIONS="$2"
            shift 2
            ;;
        --save-results)
            SAVE_RESULTS=1
            shift
            ;;
        -h|--help)
            # Print the whole leading comment block, however long it grows;
            # a fixed head -N silently truncated the options list once.
            awk 'NR > 1 { if (!/^#/) exit; sub(/^# ?/, ""); print }' "$0"
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1'" >&2
            exit 1
            ;;
    esac
done

# -------------------------------------------------------------------------
# Resolve paths
# -------------------------------------------------------------------------

BENCH_BIN="$PROJECT_ROOT/$BUILD_DIR/bench/bench_flowlog"
DOOP_DATA="${DOOP_DATA_DIR:-$PROJECT_ROOT/bench/data/doop}"
DEFAULT_DOOP_DATA="$PROJECT_ROOT/bench/data/doop"

# -------------------------------------------------------------------------
# Pre-flight checks
# -------------------------------------------------------------------------

echo "=== DOOP Validation (US-010) ==="
echo "Project root : $PROJECT_ROOT"
echo "Build dir    : $BUILD_DIR"
echo "Workers      : $WORKERS"
echo "Repeat       : $REPEAT"
echo "DOOP data    : $DOOP_DATA"
echo "Save results : $([[ "$SAVE_RESULTS" -eq 1 ]] && echo yes || echo no)"
echo ""

# Check bench binary
if [[ ! -x "$BENCH_BIN" ]]; then
    echo "ERROR: bench_flowlog not found at $BENCH_BIN"
    echo "Build it first:"
    echo "  meson compile -C $BUILD_DIR bench_flowlog"
    exit 1
fi

# Check DOOP data directory
if [[ ! -d "$DOOP_DATA" ]]; then
    echo "ERROR: DOOP data directory not found: $DOOP_DATA"
    echo "Set DOOP_DATA_DIR to the zxing .facts directory, or run"
    echo "  bench/data/doop/download.sh"
    exit 1
fi

# Count fact files
FACT_COUNT=$(find "$DOOP_DATA" -maxdepth 1 -name "*.facts" | wc -l | tr -d ' ')
echo "DOOP fact files found: $FACT_COUNT (expected: $DOOP_FACT_COUNT_EXPECTED)"
if [[ "$FACT_COUNT" -ne "$DOOP_FACT_COUNT_EXPECTED" ]]; then
    echo "ERROR: expected $DOOP_FACT_COUNT_EXPECTED .facts files, found $FACT_COUNT"
    if [[ "$FACT_COUNT" -eq 0 ]]; then
        echo "Fetch the dataset first:"
        echo "  bench/data/doop/download.sh"
    fi
    exit 1
fi

if [[ -z "$EXPECTED_TUPLES" && "$DOOP_DATA" == "$DEFAULT_DOOP_DATA" ]]; then
    EXPECTED_TUPLES=6069774
fi
if [[ -n "$EXPECTED_TUPLES" && ! "$EXPECTED_TUPLES" =~ ^[0-9]+$ ]]; then
    echo "ERROR: expected tuple count must be a non-negative integer"
    exit 1
fi
echo "Expected tuples: ${EXPECTED_TUPLES:-not supplied}"

if [[ -z "$EXPECTED_ITERATIONS" && "$DOOP_DATA" == "$DEFAULT_DOOP_DATA" ]]; then
    EXPECTED_ITERATIONS=28
fi
if [[ -n "$EXPECTED_ITERATIONS" && ! "$EXPECTED_ITERATIONS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: expected iteration count must be a non-negative integer"
    exit 1
fi
echo "Expected iterations: ${EXPECTED_ITERATIONS:-not supplied}"

# Spot-check a key file
ACTPARAM="$DOOP_DATA/ActualParam.facts"
if [[ ! -s "$ACTPARAM" ]]; then
    echo "ERROR: ActualParam.facts is missing or empty"
    exit 1
fi

TOTAL_BYTES=$(du -sb "$DOOP_DATA" 2>/dev/null | awk '{print $1}' || \
              find "$DOOP_DATA" -maxdepth 1 -name "*.facts" -exec stat -f%z {} \; 2>/dev/null | \
              awk '{s+=$1} END{print s}')
TOTAL_MB=$(( TOTAL_BYTES / 1048576 ))
echo "Dataset size : ~${TOTAL_MB} MB"
echo ""

# -------------------------------------------------------------------------
# Run benchmark
# -------------------------------------------------------------------------

echo "Running DOOP benchmark..."
echo ""

# Run and capture output
set +e
BENCH_OUTPUT=$("$BENCH_BIN" \
    --workload doop \
    --data-doop "$DOOP_DATA" \
    --workers "$WORKERS" \
    --repeat "$REPEAT" \
    2>&1)
BENCH_RC=$?
set -e

echo "$BENCH_OUTPUT"
echo ""

# -------------------------------------------------------------------------
# Parse results
# -------------------------------------------------------------------------

# Expected TSV columns (from bench_flowlog print_header):
# workload nodes edges workers repeat min_ms median_ms max_ms peak_rss_kb tuples iterations status

DOOP_ROWS=$(echo "$BENCH_OUTPUT" | awk -F '\t' '$1 == "doop" { print }')
DOOP_ROW_COUNT=$(printf '%s\n' "$DOOP_ROWS" | sed '/^$/d' | wc -l | tr -d ' ')
if [[ "$DOOP_ROW_COUNT" -ne 1 ]]; then
    echo "RESULT: FAIL - expected exactly one doop TSV row, found $DOOP_ROW_COUNT"
    exit 1
fi
DOOP_ROW="$DOOP_ROWS"
FIELD_COUNT=$(awk -F '\t' '{ print NF }' <<< "$DOOP_ROW")
if [[ "$FIELD_COUNT" -ne 12 ]]; then
    echo "RESULT: FAIL - expected 12 TSV fields, found $FIELD_COUNT"
    echo "Row: $DOOP_ROW"
    exit 1
fi

NODES=$(awk -F '\t' '{ print $2 }' <<< "$DOOP_ROW")
EDGES=$(awk -F '\t' '{ print $3 }' <<< "$DOOP_ROW")
ROW_WORKERS=$(awk -F '\t' '{ print $4 }' <<< "$DOOP_ROW")
ROW_REPEAT=$(awk -F '\t' '{ print $5 }' <<< "$DOOP_ROW")
MIN_MS=$(awk -F '\t' '{ print $6 }' <<< "$DOOP_ROW")
MEDIAN_MS=$(awk -F '\t' '{ print $7 }' <<< "$DOOP_ROW")
MAX_MS=$(awk -F '\t' '{ print $8 }' <<< "$DOOP_ROW")
PEAK_RSS_KB=$(awk -F '\t' '{ print $9 }' <<< "$DOOP_ROW")
TUPLES=$(awk -F '\t' '{ print $10 }' <<< "$DOOP_ROW")
ITERATIONS=$(awk -F '\t' '{ print $11 }' <<< "$DOOP_ROW")
STATUS=$(awk -F '\t' '{ print $12 }' <<< "$DOOP_ROW")

# -------------------------------------------------------------------------
# Report metrics
# -------------------------------------------------------------------------

echo "=== DOOP Validation Results ==="
echo ""
echo "Status      : ${STATUS:-UNKNOWN}"
echo "Exit code   : $BENCH_RC"
echo "Wall time   : min=${MIN_MS:-?}ms  median=${MEDIAN_MS:-?}ms  max=${MAX_MS:-?}ms"
echo "Peak RSS    : ${PEAK_RSS_KB:-?} KB"
echo "Output tuples: ${TUPLES:-?}"
echo "Iterations  : ${ITERATIONS:-?}"
echo ""

# -------------------------------------------------------------------------
# Correctness check
# -------------------------------------------------------------------------

if [[ "$BENCH_RC" -ne 0 || "${STATUS:-FAIL}" != "OK" ]]; then
    echo "RESULT: FAIL - DOOP did not complete successfully"
    echo ""
    echo "This means the evaluator timed out or produced an error on DOOP."
    echo "Option 2 + CSE must enable DOOP to complete (currently DNF)."
    exit 1
fi

if [[ -n "$EXPECTED_TUPLES" ]]; then
    if [[ "$TUPLES" != "$EXPECTED_TUPLES" ]]; then
        echo "RESULT: FAIL - tuple count mismatch (got $TUPLES, expected $EXPECTED_TUPLES)"
        exit 1
    fi
    echo "Tuple count: MATCH ($TUPLES)"
else
    echo "Tuple count: not checked (no oracle supplied)"
fi

if [[ -n "$EXPECTED_ITERATIONS" ]]; then
    if [[ "$ITERATIONS" != "$EXPECTED_ITERATIONS" ]]; then
        echo "RESULT: FAIL - iteration count mismatch (got $ITERATIONS, expected $EXPECTED_ITERATIONS)"
        exit 1
    fi
    echo "Iterations: MATCH ($ITERATIONS)"
else
    echo "Iterations: not checked (no oracle supplied)"
fi

echo "RESULT: PASS - DOOP completed successfully"
echo ""

# -------------------------------------------------------------------------
# Baseline comparison (optional)
# -------------------------------------------------------------------------

if [[ -n "$BASELINE_FILE" && -f "$BASELINE_FILE" ]]; then
    echo "=== Baseline Comparison ==="
    BASE_ROWS=$(awk -F '\t' '$1 == "doop" { print }' "$BASELINE_FILE")
    BASE_ROW_COUNT=$(printf '%s\n' "$BASE_ROWS" | sed '/^$/d' | wc -l | tr -d ' ')
    if [[ "$BASE_ROW_COUNT" -ne 1 ]]; then
        echo "RESULT: FAIL - expected exactly one doop baseline row, found $BASE_ROW_COUNT"
        exit 1
    fi
    BASE_ROW="$BASE_ROWS"
    BASE_FIELD_COUNT=$(awk -F '\t' '{ print NF }' <<< "$BASE_ROW")
    BASE_MEDIAN=$(awk -F '\t' '{ print $7 }' <<< "$BASE_ROW")
    BASE_TUPLES=$(awk -F '\t' '{ print $10 }' <<< "$BASE_ROW")
    if [[ "$BASE_FIELD_COUNT" -ge 12 ]]; then
        BASE_ITERATIONS=$(awk -F '\t' '{ print $11 }' <<< "$BASE_ROW")
        BASE_STATUS=$(awk -F '\t' '{ print $12 }' <<< "$BASE_ROW")
    elif [[ "$BASE_FIELD_COUNT" -eq 11 ]]; then
        BASE_ITERATIONS=""
        BASE_STATUS=$(awk -F '\t' '{ print $11 }' <<< "$BASE_ROW")
    else
        echo "RESULT: FAIL - expected 11 or more baseline fields, found $BASE_FIELD_COUNT"
        exit 1
    fi

    echo "Baseline status  : ${BASE_STATUS:-?}"
    echo "Baseline median  : ${BASE_MEDIAN:-?}ms"
    echo "Baseline tuples  : ${BASE_TUPLES:-?}"
    echo "Baseline iterations: ${BASE_ITERATIONS:-not recorded}"
    echo ""

    # Tuple correctness: must match baseline exactly
    if [[ -n "$BASE_TUPLES" && -n "$TUPLES" && "$BASE_TUPLES" != "?" ]]; then
        if [[ "$TUPLES" == "$BASE_TUPLES" ]]; then
            echo "Tuple count: MATCH ($TUPLES)"
        else
            echo "Tuple count: MISMATCH (got $TUPLES, expected $BASE_TUPLES)"
            echo "RESULT: FAIL - Output tuple count does not match baseline"
            exit 1
        fi
    fi
    if [[ -n "$BASE_ITERATIONS" && -n "$ITERATIONS" && "$BASE_ITERATIONS" != "?" ]]; then
        if [[ "$ITERATIONS" == "$BASE_ITERATIONS" ]]; then
            echo "Iterations: MATCH ($ITERATIONS)"
        else
            echo "Iterations: MISMATCH (got $ITERATIONS, expected $BASE_ITERATIONS)"
            echo "RESULT: FAIL - Iteration count does not match baseline"
            exit 1
        fi
    fi

    # Performance: report speedup (informational, not a pass/fail gate)
    if [[ -n "$BASE_MEDIAN" && -n "$MEDIAN_MS" ]]; then
        echo "Performance: ${MEDIAN_MS}ms vs baseline ${BASE_MEDIAN}ms"
    fi
    echo ""
fi

# -------------------------------------------------------------------------
# Save results for future baseline comparison
# -------------------------------------------------------------------------

RESULTS_DIR="$PROJECT_ROOT/docs/performance"
if [[ "$SAVE_RESULTS" -eq 1 ]]; then
    mkdir -p "$RESULTS_DIR"
    RESULT_FILE="$RESULTS_DIR/doop-validation-$(date +%Y-%m-%d).tsv"
    printf 'workload\tnodes\tedges\tworkers\trepeat\tmin_ms\tmedian_ms\tmax_ms\tpeak_rss_kb\ttuples\titerations\tstatus\texpected_tuples\n' \
        > "$RESULT_FILE"
    printf 'doop\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$NODES" "$EDGES" "$ROW_WORKERS" "$ROW_REPEAT" "$MIN_MS" \
        "$MEDIAN_MS" "$MAX_MS" "$PEAK_RSS_KB" "$TUPLES" "$ITERATIONS" \
        "$STATUS" "${EXPECTED_TUPLES:-}" >> "$RESULT_FILE"
    echo "Results saved to: $RESULT_FILE"
fi

echo "=== DOOP validation complete ==="
exit 0
