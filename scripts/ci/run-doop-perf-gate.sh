#!/usr/bin/env bash
# DOOP W=8 performance gate (Issue #748).
#
# The benchmark driver owns the DOOP catalogue and rules.  This runner only
# enforces the perf-host contract and validates its structured result, so the
# gate cannot silently drift from bench_flowlog.
set -euo pipefail

SKIP=77
# The target is deliberately supplied by the provisioned runner: the checked-
# in README DOOP row is repeat=1, not the required 5-repetition calibration.
# A runner must record its 5-rep median and pass median*1.05 here.
WL_DOOP_PERF_GATE_TARGET_MS="${WL_DOOP_PERF_GATE_TARGET_MS:-}"
EXPECTED_TUPLES=6276657
EXPECTED_ITERS="${WL_DOOP_PERF_GATE_EXPECTED_ITERS:-28}"
WORKERS=8
REPEAT=5

skip() { echo "doop_w8_gate: SKIP: $*" >&2; exit "$SKIP"; }
fail() { echo "doop_w8_gate: FAIL: $*" >&2; exit 1; }

[[ "${WIRELOG_PERF_GATE:-0}" == 1 ]] || \
    skip "set WIRELOG_PERF_GATE=1 to run on dedicated performance hardware"

# The shipped stability contract is Linux cpufreq-specific.  In particular,
# do not turn a Windows release build's inherited PERF_REQUIRE setting into a
# false log-ceiling failure when there is no governor to validate.
case "$(uname -s 2>/dev/null || printf unknown)" in
    Linux*) ;;
    *) skip "cpufreq governor stability gate is Linux-only" ;;
esac

if [[ "${WIRELOG_PERF_LOG_COMPILE_MAX_LEVEL:-1}" -gt 1 ]]; then
    if [[ "${WIRELOG_PERF_REQUIRE:-0}" == 1 ]]; then
        fail "WIRELOG_PERF_REQUIRE=1 but log compile ceiling is above ERROR"
    fi
    skip "log compile ceiling is above ERROR"
fi

if [[ "${WIRELOG_PERF_REQUIRE:-0}" == 1 ]]; then
    governor_file=/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
    if [[ ! -r "$governor_file" ]]; then
        fail "WIRELOG_PERF_REQUIRE=1 but cpufreq governor is unavailable"
    fi
    governor=$(<"$governor_file")
    [[ "$governor" == performance ]] || \
        fail "WIRELOG_PERF_REQUIRE=1 but cpufreq governor is '$governor'"
else
    governor_file=/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
    [[ -r "$governor_file" ]] || skip "cpufreq governor is unavailable"
    [[ "$(<"$governor_file")" == performance ]] || \
        skip "cpufreq governor is not 'performance'"
fi

data_dir="${WIRELOG_DOOP_DATA_DIR:-bench/data/doop}"
bench_bin="${WIRELOG_DOOP_BENCH_BIN:-bench/bench_flowlog}"
[[ -d "$data_dir" ]] || skip "DOOP data directory not found: $data_dir"
[[ -x "$bench_bin" ]] || fail "bench_flowlog not found or not executable: $bench_bin"

[[ -n "$WL_DOOP_PERF_GATE_TARGET_MS" ]] || \
    skip "no calibrated 5-repetition W=8 target; set WL_DOOP_PERF_GATE_TARGET_MS"

# The 6,276,657 oracle belongs to the recovered zxing artifact.  Require the
# provisioned runner to pin its sorted .facts manifest so a refreshed upstream
# archive cannot silently turn this into a different benchmark.
expected_manifest="${WIRELOG_DOOP_DATASET_MANIFEST_SHA256:-}"
[[ -n "$expected_manifest" ]] || \
    skip "DOOP dataset manifest is not pinned; set WIRELOG_DOOP_DATASET_MANIFEST_SHA256"
# LC_ALL=C: en_US collation ignores the hyphen in Method-Modifier.facts and
# reorders it against MethodHandleConstant.facts, so an unpinned sort yields a
# different manifest on a non-C runner and this fails as if the dataset had
# been substituted. See #1294.
actual_manifest=$(sha256sum "$data_dir"/*.facts | LC_ALL=C sort -k2 | sha256sum | awk '{print $1}')
[[ "$actual_manifest" == "$expected_manifest" ]] || \
    fail "DOOP dataset manifest $actual_manifest != pinned $expected_manifest"

# Validate the complete catalogue before starting a multi-minute run.
for fact in \
    DirectSuperclass DirectSuperinterface MainClass FormalParam ComponentType \
    AssignReturnValue ActualParam Method-Modifier Var-Type ClassType ArrayType \
    InterfaceType Var-DeclaringMethod ApplicationClass ThisVar NormalHeap \
    StringConstant AssignHeapAllocation AssignLocal AssignCast Field \
    StaticMethodInvocation SpecialMethodInvocation VirtualMethodInvocation Method \
    StoreInstanceField LoadInstanceField StoreStaticField LoadStaticField \
    StoreArrayIndex LoadArrayIndex Return ClassHeap MethodHandleConstant \
    MethodTypeConstant; do
    [[ -s "$data_dir/$fact.facts" ]] || \
        skip "missing or empty DOOP input: $data_dir/$fact.facts"
done

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
set +e
"$bench_bin" --workload doop --data-doop "$data_dir" \
    --workers "$WORKERS" --repeat "$REPEAT" >"$tmp" 2>&1
status=$?
set -e
(( status == 0 )) || fail "bench_flowlog DOOP run failed (exit $status); see output below\n$(<"$tmp")"

row=$(awk -F '\t' '$1 == "doop" { print; exit }' "$tmp")
[[ -n "$row" ]] || fail "bench_flowlog produced no DOOP result row"
[[ "$(awk -F '\t' '{print NF}' <<<"$row")" == 12 ]] || \
    fail "DOOP result row has an unexpected field count"
IFS=$'\t' read -r workload _ facts workers repeat min_ms median_ms max_ms rss tuples iters result <<<"$row"

[[ "$workers" == "$WORKERS" && "$repeat" == "$REPEAT" ]] || \
    fail "DOOP result reports workers=$workers repeat=$repeat, expected $WORKERS/$REPEAT"

# Correctness sentinels intentionally precede any timing assertion.
[[ "$tuples" == "$EXPECTED_TUPLES" ]] || \
    fail "tuple count $tuples != sentinel $EXPECTED_TUPLES"
[[ "$iters" == "$EXPECTED_ITERS" ]] || \
    fail "iteration count $iters != sentinel $EXPECTED_ITERS"

[[ "$result" == OK ]] || fail "DOOP result status is '$result'"
if ! awk -v value="$median_ms" -v target="$WL_DOOP_PERF_GATE_TARGET_MS" \
    'BEGIN { exit !(value + 0 <= target + 0) }'; then
    fail "median ${median_ms} ms exceeds target ${WL_DOOP_PERF_GATE_TARGET_MS} ms"
fi

echo "doop_w8_gate OK: tuples=$tuples iterations=$iters median_ms=$median_ms target_ms=$WL_DOOP_PERF_GATE_TARGET_MS"
