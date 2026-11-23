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
#   scripts/run_doop_validation.sh --calibration [--evidence-dir DIR]
#                                  --memory-limit-kb N [--wall-timeout SECONDS]
#                                  [--oracle-file FILE]
#
# Options:
#   --build-dir DIR   Meson build directory (default: build)
#   --workers N       Worker thread count (default: 1)
#   --repeat N        Benchmark repeat count (default: 1)
#   --baseline FILE   TSV file with baseline metrics for comparison
#   --expected-tuples N
#                     Expected DOOP output tuple count. Defaults to
#                     DOOP_EXPECTED_TUPLES when set; otherwise defaults to
#                     14096448 for the bundled bench/data/doop zxing
#                     dataset at --workers 1.  W>1 does not complete at all
#                     (#959); see "Oracles" below.
#   --expected-iterations N
#                     Expected fixpoint iteration count. Defaults to
#                     DOOP_EXPECTED_ITERATIONS when set; otherwise defaults
#                     to 153 for the bundled dataset.  See "Oracles" below.
#   --save-results    Write a validation TSV under docs/performance
#   --calibration     Run the fixed W=8, repeat=5 calibration with evidence
#   --evidence-dir DIR
#                     Preserve calibration logs and metadata in DIR
#   --wall-timeout N  Calibration wall timeout in seconds (default: 7200)
#   --memory-limit-kb N
#                     Required positive Linux cgroup-v2 memory.max bound
#   --oracle-file FILE
#                     Calibration oracle TSV (default: release manifest)
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
#   measured at --workers 1 on 2026-08-05 (issues #952, #955, #956).
#   The run takes about 23 minutes and peaks near 40 GB.  Supply
#   --expected-tuples on any other dataset; the tuple count is
#   dataset-specific.  An explicitly supplied --expected-tuples, and a
#   --baseline TSV, are both applied at any width: comparing against a
#   baseline captured at a different width is the caller's call, and the
#   run warns rather than refusing.
#
#   The default tuple oracle is a post-#957 FINGERPRINT of current code,
#   not a statement about what DOOP should compute.  The current evaluator
#   uses set semantics for single-rule IDBs, so duplicate derivations are
#   consolidated and this total represents distinct tuples.  Historical
#   pre-#957 measurements counted duplicate rows; do not compare those row
#   counts with this set-semantic oracle.
#   It is W=1-only because W>1 does not complete -- the per-worker
#   join-output cap is the session cap divided by the worker count
#   (#959) -- and because W>1 totals varied run to run when they did
#   complete (#958).
#
#   #955 is fixed as of 2026-08-05.  Before it, this workload derived
#   VarPointsTo = 5,266 against a true value near 4.1 million; any oracle
#   recorded before that date describes a broken analysis.
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
WORKERS_SET=0
REPEAT_SET=0
BASELINE_FILE=""
EXPECTED_TUPLES="${DOOP_EXPECTED_TUPLES:-}"
EXPECTED_ITERATIONS="${DOOP_EXPECTED_ITERATIONS:-}"
SAVE_RESULTS=0
CALIBRATION=0
EVIDENCE_DIR=""
WALL_TIMEOUT_SECONDS="${DOOP_CALIBRATION_WALL_TIMEOUT_SECONDS:-7200}"
MEMORY_LIMIT_KB=""
ORACLE_FILE=""

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
            WORKERS_SET=1
            shift 2
            ;;
        --repeat)
            REPEAT="$2"
            REPEAT_SET=1
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
        --calibration)
            CALIBRATION=1
            shift
            ;;
        --evidence-dir)
            EVIDENCE_DIR="$2"
            shift 2
            ;;
        --wall-timeout)
            WALL_TIMEOUT_SECONDS="$2"
            shift 2
            ;;
        --memory-limit-kb)
            MEMORY_LIMIT_KB="$2"
            shift 2
            ;;
        --oracle-file)
            ORACLE_FILE="$2"
            shift 2
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

if [[ "$CALIBRATION" -eq 1 ]]; then
    if [[ "$WORKERS_SET" -eq 0 ]]; then WORKERS=8; fi
    if [[ "$REPEAT_SET" -eq 0 ]]; then REPEAT=5; fi
    if [[ "$WORKERS" != 8 || "$REPEAT" != 5 ]]; then
        echo "ERROR: calibration requires --workers 8 --repeat 5" >&2
        exit 1
    fi
    if [[ ! "$WALL_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: calibration wall timeout must be a positive integer" >&2
        exit 1
    fi
    if [[ ! "$MEMORY_LIMIT_KB" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: calibration requires a positive --memory-limit-kb" >&2
        exit 1
    fi
fi

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
echo "Calibration  : $([[ "$CALIBRATION" -eq 1 ]] && echo yes || echo no)"
if [[ "$CALIBRATION" -eq 1 ]]; then
    echo "Wall timeout : ${WALL_TIMEOUT_SECONDS}s"
fi
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

# The default tuple oracle is W=1-specific: W>1 varies run to run (#958),
# so applying it there would report a spurious FAIL.  An explicitly supplied
# --expected-tuples is still honoured at any width -- that is the caller's
# call to make, not ours.
if [[ -z "$EXPECTED_TUPLES" && "$DOOP_DATA" == "$DEFAULT_DOOP_DATA" ]]; then
    if [[ "$WORKERS" -eq 1 ]]; then
        EXPECTED_TUPLES=14096448
    else
        echo "note: no default tuple oracle at --workers $WORKERS;" \
             "W>1 is not reproducible (#958)"
    fi
fi
if [[ -n "$EXPECTED_TUPLES" && ! "$EXPECTED_TUPLES" =~ ^[0-9]+$ ]]; then
    echo "ERROR: expected tuple count must be a non-negative integer"
    exit 1
fi
echo "Expected tuples: ${EXPECTED_TUPLES:-not supplied}"

if [[ -z "$EXPECTED_ITERATIONS" && "$DOOP_DATA" == "$DEFAULT_DOOP_DATA" ]]; then
    EXPECTED_ITERATIONS=153
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

facts_manifest() {
    ( CDPATH= cd -- "$1" && sha256sum -- *.facts | LC_ALL=C sort -k2 |
        sha256sum | awk '{print $1}' )
}

if [[ "$CALIBRATION" -eq 1 ]]; then
    if [[ -z "$EVIDENCE_DIR" ]]; then
        EVIDENCE_DIR="${DOOP_EVIDENCE_DIR:-$PROJECT_ROOT/docs/performance/doop-calibration-$(date +%Y-%m-%d-%H%M%S)}"
    fi
    mkdir -p "$EVIDENCE_DIR"
    PROGRESS_FILE="$EVIDENCE_DIR/repeat-progress.tsv"
    BENCH_LOG="$EVIDENCE_DIR/bench-output.log"
    METADATA_FILE="$EVIDENCE_DIR/metadata.tsv"
    FINAL_FILE="$EVIDENCE_DIR/final.tsv"
    FINAL_TMP="$EVIDENCE_DIR/final.tsv.tmp.$$"
    LAUNCH_GATE="$EVIDENCE_DIR/launch-gate.$$"
    CGROUP_ROOT="${DOOP_CGROUP_ROOT:-/sys/fs/cgroup}"
    CGROUP_DIR=""
    CGROUP_EVENTS_RECORDED=0
    CGROUP_CLEANUP_FAILED=0
    : > "$PROGRESS_FILE"
    : > "$BENCH_LOG"
    rm -f "$FINAL_FILE" "$FINAL_TMP"
    facts_manifest=$(facts_manifest "$DOOP_DATA")
    {
        printf 'field\tvalue\n'
        printf 'host_uname\t%s\nstarted_at\t%s\n' "$(uname -a)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf 'workload\tdoop\nworkers\t%s\nrepeat\t%s\n' "$WORKERS" "$REPEAT"
        printf 'wall_timeout_seconds\t%s\n' "$WALL_TIMEOUT_SECONDS"
        printf 'memory_limit_kb\t%s\n' "$MEMORY_LIMIT_KB"
        printf 'data_dir\t%s\nfacts_manifest_sha256\t%s\n' "$DOOP_DATA" "$facts_manifest"
        printf 'expected_tuples\t%s\nexpected_iterations\t%s\n' \
            "${EXPECTED_TUPLES:-}" "${EXPECTED_ITERATIONS:-}"
        printf 'rss\tobservation_only\n'
    } > "$METADATA_FILE"

    ORACLE_FILE="${ORACLE_FILE:-$PROJECT_ROOT/scripts/release/downstream-matrix-oracles.tsv}"
    oracle_header='# schema=2 workload tuple_oracle iteration_oracle data_path data_manifest_sha256 provenance_id acquisition_command'
    if [[ ! -r "$ORACLE_FILE" ]] ||
        [[ "$(awk 'NF { print; exit }' "$ORACLE_FILE")" != "$oracle_header" ]]; then
        printf 'oracle_failure\tmissing_or_invalid_header\n' >> "$METADATA_FILE"
        exit 1
    fi
    oracle_rows=$(awk -F '\t' '$1 == "doop" { print }' "$ORACLE_FILE")
    if [[ "$(printf '%s\n' "$oracle_rows" | sed '/^$/d' | wc -l | tr -d ' ')" -ne 1 ]]; then
        printf 'oracle_failure\texpected_one_doop_row\n' >> "$METADATA_FILE"
        exit 1
    fi
    IFS=$'\t' read -r oracle_workload oracle_tuples oracle_iterations oracle_data_path \
        oracle_manifest oracle_provenance oracle_acquisition <<< "$oracle_rows"
    if [[ "$oracle_workload" != doop || ! "$oracle_tuples" =~ ^[0-9]+$ ||
        ! "$oracle_iterations" =~ ^[0-9]+$ || "$oracle_data_path" != bench/data/doop ||
        ! "$oracle_manifest" =~ ^archive:[0-9a-f]{64}\;files:[0-9a-f]{64}$ ]]; then
        printf 'oracle_failure\tmalformed_doop_row\n' >> "$METADATA_FILE"
        exit 1
    fi
    EXPECTED_TUPLES="$oracle_tuples"
    EXPECTED_ITERATIONS="$oracle_iterations"
    EXPECTED_FILES_MANIFEST="${oracle_manifest#*;files:}"
    actual_facts_manifest=$(facts_manifest "$DOOP_DATA")
    printf 'oracle_tuples\t%s\noracle_iterations\t%s\n' \
        "$EXPECTED_TUPLES" "$EXPECTED_ITERATIONS" >> "$METADATA_FILE"
    printf 'facts_manifest_expected\t%s\nfacts_manifest_actual\t%s\n' \
        "$EXPECTED_FILES_MANIFEST" "$actual_facts_manifest" >> "$METADATA_FILE"
    if [[ "$actual_facts_manifest" != "$EXPECTED_FILES_MANIFEST" ]]; then
        printf 'oracle_failure\tfacts_manifest_mismatch\n' >> "$METADATA_FILE"
        exit 1
    fi

    cleanup_cgroup() {
        if [[ -n "$CGROUP_DIR" && "$CGROUP_EVENTS_RECORDED" -eq 0 &&
            -r "$CGROUP_DIR/memory.events" ]]; then
            printf 'memory_events_oom\t%s\n' \
                "$(awk '$1 == "oom" { print $2 + 0 }' "$CGROUP_DIR/memory.events")" \
                >> "$METADATA_FILE"
            printf 'memory_events_oom_kill\t%s\n' \
                "$(awk '$1 == "oom_kill" { print $2 + 0 }' "$CGROUP_DIR/memory.events")" \
                >> "$METADATA_FILE"
            CGROUP_EVENTS_RECORDED=1
        fi
        if [[ -n "$CGROUP_DIR" && -d "$CGROUP_DIR" ]]; then
            if [[ -n "${DOOP_CGROUP_ROOT:-}" ]]; then
                rm -f "$CGROUP_DIR/memory.max" "$CGROUP_DIR/cgroup.procs" \
                    "$CGROUP_DIR/memory.events"
            fi
            if ! rmdir "$CGROUP_DIR" 2>/dev/null || [[ -d "$CGROUP_DIR" ]]; then
                printf 'cgroup_cleanup_failure\t%s\n' "$CGROUP_DIR" >> "$METADATA_FILE"
                CGROUP_CLEANUP_FAILED=1
            fi
        fi
    }
    cleanup_calibration_artifacts() {
        local status=$?
        rm -f "$FINAL_TMP" "$LAUNCH_GATE"
        cleanup_cgroup
        if [[ "$status" -eq 0 && "$CGROUP_CLEANUP_FAILED" -ne 0 ]]; then
            status=1
        fi
        return "$status"
    }
    trap cleanup_calibration_artifacts EXIT

    memory_limit_bytes=$(awk -v kb="$MEMORY_LIMIT_KB" \
        'BEGIN { printf "%.0f\n", kb * 1024 }')
    memory_bound_refused() {
        local reason=$1
        printf 'memory_bound_refused\t%s\n' "$reason" >> "$METADATA_FILE"
        exit 1
    }
    if [[ ! -d "$CGROUP_ROOT" || ! -r "$CGROUP_ROOT" || ! -w "$CGROUP_ROOT" ]]; then
        memory_bound_refused "cgroup_root_unavailable"
    fi
    if [[ ! -r "$CGROUP_ROOT/cgroup.controllers" ]] ||
        ! grep -Eq '(^|[[:space:]])memory([[:space:]]|$)' "$CGROUP_ROOT/cgroup.controllers" ||
        [[ ! -e "$CGROUP_ROOT/cgroup.procs" || ! -w "$CGROUP_ROOT/cgroup.procs" ]]; then
        memory_bound_refused "memory_controller_unavailable"
    fi
    CGROUP_DIR="$CGROUP_ROOT/wirelog-doop-$$"
    if ! mkdir "$CGROUP_DIR" 2>/dev/null; then
        memory_bound_refused "cgroup_create_failed"
    fi
    # An injected root is a regular-filesystem fixture; real cgroup-v2 creates
    # these files with the directory. Never synthesize them for the real root.
    if [[ -n "${DOOP_CGROUP_ROOT:-}" ]]; then
        : > "$CGROUP_DIR/memory.max"
        : > "$CGROUP_DIR/cgroup.procs"
        printf 'oom 0\noom_kill 0\n' > "$CGROUP_DIR/memory.events"
    fi
    if [[ ! -w "$CGROUP_DIR/memory.max" || ! -w "$CGROUP_DIR/cgroup.procs" ||
        ! -r "$CGROUP_DIR/memory.events" ]] ||
        ! printf '%s\n' "$memory_limit_bytes" > "$CGROUP_DIR/memory.max"; then
        memory_bound_refused "cgroup_setup_unavailable"
    fi
    memory_oom_before=$(awk '$1 == "oom" { print $2 + 0 }' "$CGROUP_DIR/memory.events")
    memory_oom_kill_before=$(awk '$1 == "oom_kill" { print $2 + 0 }' "$CGROUP_DIR/memory.events")
    printf 'memory_events_oom_before\t%s\nmemory_events_oom_kill_before\t%s\n' \
        "$memory_oom_before" "$memory_oom_kill_before" >> "$METADATA_FILE"
    printf 'memory_bound\tcgroup_v2_memory.max\n' >> "$METADATA_FILE"
fi

# -------------------------------------------------------------------------
# Run benchmark
# -------------------------------------------------------------------------

echo "Running DOOP benchmark..."
echo ""

# Run and capture output. Calibration runs keep the process in its own
# session so timeout and interruption cleanup reaches stubborn descendants.
if [[ "$CALIBRATION" -eq 1 ]]; then
    calibration_pid=""
    calibration_pgid=""
    interrupted=0
    leader_early=0
    # Only a group the launched child leads, in a session of its own, is ever
    # signalled; calibration_pgid stays empty until the launch has proved that.
    group_exists() {
        [[ "$calibration_pgid" =~ ^[0-9]+$ && "$calibration_pgid" != 1 &&
            "$calibration_pgid" == "$calibration_pid" ]] &&
            kill -0 -- "-$calibration_pgid" 2>/dev/null
    }
    stop_calibration_group() {
        if group_exists; then
            kill -TERM -- "-$calibration_pgid" 2>/dev/null || true
            for _ in 1 2 3 4 5; do
                group_exists || break
                sleep 1
            done
            if group_exists; then
                kill -KILL -- "-$calibration_pgid" 2>/dev/null || true
                for _ in 1 2 3 4 5; do
                    group_exists || break
                    sleep 1
                done
            fi
        fi
    }
    calibration_interrupt() {
        interrupted=1
        stop_calibration_group
    }
    # Launch isolation is polled every 0.05 s, 200 times (10 s) by default.
    isolate_polls=${DOOP_CALIBRATION_ISOLATE_POLLS:-200}
    # Zero polls would refuse before the child can have run setsid, and such
    # a refusal can be lost in the fork, leaving its launch-gate loop behind.
    # Six digits keep the count inside bash arithmetic.
    if [[ ! "$isolate_polls" =~ ^[1-9][0-9]{0,5}$ ]]; then
        echo "ERROR: DOOP_CALIBRATION_ISOLATE_POLLS must be an integer from 1 to 999999" >&2
        exit 1
    fi
    export WIRELOG_DOOP_CGROUP_DIR="$CGROUP_DIR"
    export WIRELOG_DOOP_CGROUP_MARKER="${DOOP_CGROUP_MARKER:-}"
    trap calibration_interrupt INT TERM HUP
    rm -f "$LAUNCH_GATE"
    setsid --wait bash -c \
        'while [[ ! -e "$1" ]]; do sleep 0.05; done; shift; exec "$@"' \
        bash "$LAUNCH_GATE" "$BENCH_BIN" \
        --workload doop \
        --data-doop "$DOOP_DATA" \
        --workers "$WORKERS" \
        --repeat "$REPEAT" \
        --repeat-progress "$PROGRESS_FILE" \
        >"$BENCH_LOG" 2>&1 &
    calibration_pid=$!
    # Without job control the child starts in this shell's process group and
    # setsid(1) moves it out only once it runs, so a look right after the
    # fork can still name this shell's own group, and signalling that group
    # kills this script and whatever started it.  The launch gate holds the
    # benchmark back, so poll until the child leads both its group and its
    # session; a setsid that forked instead (job control) never gets there
    # and is refused.
    for ((poll = 0; poll < isolate_polls; poll++)); do
        child_ids=$(ps -o pgid=,sid= -p "$calibration_pid" 2>/dev/null) || child_ids=""
        read -r child_pgid child_sid _ <<< "$child_ids" || true
        if [[ "${child_pgid:-}" == "$calibration_pid" && "${child_sid:-}" == "$calibration_pid" ]]; then
            calibration_pgid=$calibration_pid
            break
        fi
        kill -0 "$calibration_pid" 2>/dev/null || break
        sleep 0.05
    done
    if [[ -z "$calibration_pgid" ]]; then
        echo "ERROR: could not isolate calibration process group" >&2
        # Never signal a group here: none has been shown to be the child's.
        kill -TERM "$calibration_pid" 2>/dev/null || true
        wait "$calibration_pid" 2>/dev/null || true
        exit 1
    fi
    if ! printf '%s\n' "$calibration_pid" > "$CGROUP_DIR/cgroup.procs"; then
        printf 'memory_bound_refused\tcgroup_attach_failed\n' >> "$METADATA_FILE"
        stop_calibration_group
        wait "$calibration_pid" 2>/dev/null || true
        exit 1
    fi
    : > "$LAUNCH_GATE"
    deadline=$((SECONDS + WALL_TIMEOUT_SECONDS))
    timed_out=0
    while group_exists; do
        if ! kill -0 "$calibration_pid" 2>/dev/null; then
            leader_early=1
            stop_calibration_group
            break
        fi
        if (( SECONDS >= deadline )); then
            timed_out=1
            stop_calibration_group
            break
        fi
        sleep 1
    done
    set +e
    wait "$calibration_pid"
    BENCH_RC=$?
    # The reaped pid can be reused; forgetting the group narrows the window
    # in which a late trap could signal someone else's.
    calibration_pgid=""
    set -e
    trap - INT TERM HUP
    memory_oom_after=$(awk '$1 == "oom" { print $2 + 0 }' "$CGROUP_DIR/memory.events")
    memory_oom_kill_after=$(awk '$1 == "oom_kill" { print $2 + 0 }' "$CGROUP_DIR/memory.events")
    memory_oom_delta=$(( memory_oom_after - memory_oom_before ))
    memory_oom_kill_delta=$(( memory_oom_kill_after - memory_oom_kill_before ))
    printf 'memory_events_oom_after\t%s\nmemory_events_oom_kill_after\t%s\n' \
        "$memory_oom_after" "$memory_oom_kill_after" >> "$METADATA_FILE"
    printf 'memory_events_oom_delta\t%s\nmemory_events_oom_kill_delta\t%s\n' \
        "$memory_oom_delta" "$memory_oom_kill_delta" >> "$METADATA_FILE"
    CGROUP_EVENTS_RECORDED=1
    if [[ "$memory_oom_delta" -gt 0 || "$memory_oom_kill_delta" -gt 0 ]]; then
        BENCH_RC=137
        oom_reason=memory_oom
    else
        oom_reason=""
    fi
    if [[ "$timed_out" -eq 1 ]]; then
        BENCH_RC=124
        printf 'calibration_timeout\tseconds=%s\n' "$WALL_TIMEOUT_SECONDS" >> "$BENCH_LOG"
    elif [[ "$interrupted" -eq 1 ]]; then
        BENCH_RC=130
        printf 'calibration_interrupted\n' >> "$BENCH_LOG"
    elif [[ "$leader_early" -eq 1 ]]; then
        BENCH_RC=125
        printf 'calibration_leader_exit\n' >> "$BENCH_LOG"
    elif [[ -n "$oom_reason" ]]; then
        printf 'calibration_oom\n' >> "$BENCH_LOG"
    fi
    BENCH_OUTPUT=$(<"$BENCH_LOG")
else
    set +e
    BENCH_OUTPUT=$("$BENCH_BIN" \
        --workload doop \
        --data-doop "$DOOP_DATA" \
        --workers "$WORKERS" \
        --repeat "$REPEAT" \
        2>&1)
    BENCH_RC=$?
    set -e
fi

echo "$BENCH_OUTPUT"
echo ""

if [[ "$CALIBRATION" -eq 1 ]]; then
    synthesize_failure_progress() {
        local reason=$1 rc=$2 starts completes failed next_rep
        failed=$(awk -F '\t' '$1 == "repeat_complete" && $9 == "status=FAIL" { n++ } END { print n + 0 }' "$PROGRESS_FILE")
        if [[ "$failed" -eq 0 ]]; then
            starts=$(awk -F '\t' '$1 == "repeat_start" { n = $3; sub("repetition=", "", n); last = n } END { print last + 0 }' "$PROGRESS_FILE")
            completes=$(awk -F '\t' '$1 == "repeat_complete" { n = $3; sub("repetition=", "", n); last = n } END { print last + 0 }' "$PROGRESS_FILE")
            if [[ "$starts" -gt "$completes" ]]; then
                next_rep=$starts
            else
                next_rep=$((completes + 1))
            fi
            if [[ "$next_rep" -le "$REPEAT" ]]; then
                if [[ "$starts" -le "$completes" ]]; then
                    printf 'repeat_start\tworkload=doop\trepetition=%s\trepeat=%s\tworkers=8\n' \
                        "$next_rep" "$REPEAT" >> "$PROGRESS_FILE"
                fi
                printf 'repeat_complete\tworkload=doop\trepetition=%s\trepeat=%s\tworkers=8\tduration_ms=0\trss_kb=0\trc=%s\tstatus=FAIL\tfailure_reason=%s\n' \
                    "$next_rep" "$REPEAT" "$rc" "$reason" >> "$PROGRESS_FILE"
            fi
        fi
        printf 'calibration_failure\tworkload=doop\trepeat=%s\tworkers=8\trc=%s\tstatus=FAIL\tfailure_reason=%s\n' \
            "$REPEAT" "$rc" "$reason" >> "$PROGRESS_FILE"
    }
    if [[ "$BENCH_RC" -ne 0 ]]; then
        failure_reason=process_exit
        [[ "$timed_out" -eq 1 ]] && failure_reason=timeout
        [[ "$interrupted" -eq 1 ]] && failure_reason=signal
        [[ "$leader_early" -eq 1 ]] && failure_reason=leader_exit
        [[ "${oom_reason:-}" == memory_oom ]] && failure_reason=memory_oom
        synthesize_failure_progress "$failure_reason" "$BENCH_RC"
    elif [[ "${oom_reason:-}" == memory_oom ]]; then
        synthesize_failure_progress memory_oom 137
    fi
    validate_progress() {
        awk -F '\t' -v want_repeat="$REPEAT" '
            function field(i, expected, pattern, pair) {
                split($i, pair, "=");
                return pair[1] == expected && pair[2] ~ pattern && pair[2] != "";
            }
            BEGIN { next_rep = 1; started = 0; complete = 0; failed = 0; in_rep = 0; done = 0; bad = 0 }
            $1 == "repeat_start" {
                if (NF != 5 || !field(2, "workload", "^doop$") ||
                    !field(3, "repetition", "^[1-9][0-9]*$") ||
                    !field(4, "repeat", "^[1-9][0-9]*$") ||
                    !field(5, "workers", "^[1-9][0-9]*$") ||
                    $3 != "repetition=" next_rep || $4 != "repeat=" want_repeat ||
                    $5 != "workers=8" || in_rep) bad = 1;
                started++;
                in_rep = 1;
                next;
            }
            $1 == "repeat_complete" {
                if (NF == 10 && field(9, "status", "^FAIL$") &&
                    field(2, "workload", "^doop$") &&
                    field(3, "repetition", "^[1-9][0-9]*$") &&
                    field(4, "repeat", "^[1-9][0-9]*$") &&
                    field(5, "workers", "^[1-9][0-9]*$") &&
                    field(6, "duration_ms", "^[0-9]+([.][0-9]+)?$") &&
                    field(7, "rss_kb", "^[0-9]+$") &&
                    field(8, "rc", "^-?[0-9]+$") && $8 != "rc=0" &&
                    field(10, "failure_reason", "^.+$")) {
                    if ($3 != "repetition=" next_rep || $4 != "repeat=" want_repeat ||
                        $5 != "workers=8" || !in_rep) bad = 1;
                    failed++;
                    next_rep++;
                    complete++;
                    in_rep = 0;
                    next;
                }
                if (NF != 11 || !field(2, "workload", "^doop$") ||
                    !field(3, "repetition", "^[1-9][0-9]*$") ||
                    !field(4, "repeat", "^[1-9][0-9]*$") ||
                    !field(5, "workers", "^[1-9][0-9]*$") ||
                    !field(6, "duration_ms", "^[0-9]+([.][0-9]+)?$") ||
                    !field(7, "rss_kb", "^[0-9]+$") ||
                    !field(8, "rc", "^[0-9]+$") ||
                    !field(9, "status", "^OK$") ||
                    !field(10, "tuples", "^[0-9]+$") ||
                    !field(11, "iterations", "^[0-9]+$") ||
                    $3 != "repetition=" next_rep || $4 != "repeat=" want_repeat ||
                    $5 != "workers=8" || $8 != "rc=0" || !in_rep) bad = 1;
                next_rep++;
                complete++;
                in_rep = 0;
                next;
            }
            $1 == "calibration_failure" {
                if (NF != 7 || !field(2, "workload", "^doop$") ||
                    !field(3, "repeat", "^[1-9][0-9]*$") ||
                    !field(4, "workers", "^[1-9][0-9]*$") ||
                    !field(5, "rc", "^-?[0-9]+$") || $5 == "rc=0" ||
                    !field(6, "status", "^FAIL$") ||
                    !field(7, "failure_reason", "^.+$")) bad = 1;
                failed++;
                next;
            }
            $1 == "DONE" {
                if (NF != 5 || !field(2, "workload", "^doop$") ||
                    !field(3, "repeat", "^[1-9][0-9]*$") ||
                    !field(4, "workers", "^[1-9][0-9]*$") ||
                    !field(5, "status", "^OK$") ||
                    $3 != "repeat=" want_repeat || $4 != "workers=8" || done || in_rep ||
                    complete != want_repeat) bad = 1;
                done++;
                next;
            }
            { bad = 1 }
            END {
                if (failed == 0 && (next_rep != want_repeat + 1 || started != want_repeat ||
                    complete != want_repeat || in_rep || done != 1)) bad = 1;
                if (bad) exit 1;
                if (failed > 0) exit 2;
                exit 0;
            }
        ' "$PROGRESS_FILE"
    }
    progress_status=0
    if [[ ! -s "$PROGRESS_FILE" ||
        "$(tail -c 1 "$PROGRESS_FILE" | od -An -t x1 | tr -d ' ')" != "0a" ]]; then
        progress_status=1
    elif validate_progress; then
        progress_status=0
    else
        progress_status=$?
    fi
    if [[ "$progress_status" -eq 2 ]]; then
        echo "RESULT: FAIL - calibration recorded a failed repetition"
        printf 'exit_reason\trepetition_failed\n' >> "$METADATA_FILE"
        exit 1
    elif [[ "$progress_status" -ne 0 ]]; then
        echo "RESULT: FAIL - calibration progress is incomplete or malformed"
        printf 'exit_reason\tprogress_invalid\n' >> "$METADATA_FILE"
        exit 1
    fi
fi

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
if [[ "$CALIBRATION" -eq 1 ]]; then
    if [[ "$BENCH_RC" -ne 0 ]]; then
        echo "RESULT: FAIL - calibration process exited with code $BENCH_RC"
        printf 'exit_reason\tprocess_exit_%s\n' "$BENCH_RC" >> "$METADATA_FILE"
        exit 1
    fi
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

if [[ "$CALIBRATION" -eq 1 &&
    ( "$ROW_WORKERS" != 8 || "$ROW_REPEAT" != 5 ) ]]; then
    echo "RESULT: FAIL - calibration row is not W=8 repeat=5"
    printf 'exit_reason\trow_configuration_mismatch\n' >> "$METADATA_FILE"
    exit 1
fi

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

if [[ "$CALIBRATION" -eq 1 ]]; then
    # Keep the candidate off the evidence path until all correctness and
    # optional baseline/oracle checks below have passed.
    printf '%s\n' "$DOOP_ROW" > "$FINAL_TMP"
fi

echo "RESULT: PASS - DOOP completed successfully"
echo ""

if [[ "$CALIBRATION" -eq 1 ]]; then
    printf 'exit_reason\tcomplete_success\n' >> "$METADATA_FILE"
fi

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
    if [[ "$WORKERS" -ne 1 ]]; then
        echo "warning: comparing tuple counts against a baseline at" \
             "--workers $WORKERS; W>1 is not reproducible (#958), so a" \
             "mismatch here may be nondeterminism rather than a regression"
    fi
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

if [[ "$CALIBRATION" -eq 1 ]]; then
    mv -f "$FINAL_TMP" "$FINAL_FILE"
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
