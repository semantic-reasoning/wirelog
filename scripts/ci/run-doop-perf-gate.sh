#!/usr/bin/env bash
# DOOP W=8 performance gate (Issue #748).
#
# The benchmark driver owns the DOOP catalogue and rules.  This runner only
# enforces the perf-host contract and validates its structured result, so the
# gate cannot silently drift from bench_flowlog.
set -euo pipefail

SKIP=77
script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# The target is deliberately supplied by the provisioned runner: the checked-
# in README DOOP row is repeat=1, not the required 5-repetition calibration.
# A runner must record its 5-rep median and pass median*1.05 here.
WL_DOOP_PERF_GATE_TARGET_MS="${WL_DOOP_PERF_GATE_TARGET_MS:-}"
WIRELOG_DOOP_PERF_MODE="${WIRELOG_DOOP_PERF_MODE:-strict-stable}"
WORKERS=8
REPEAT=5

skip() { echo "doop_w8_gate: SKIP: $*" >&2; exit "$SKIP"; }
fail() { echo "doop_w8_gate: FAIL: $*" >&2; exit 1; }
required_or_skip() {
    if [[ "${WIRELOG_PERF_REQUIRE:-0}" == 1 ]]; then
        fail "$*"
    fi
    skip "$*"
}

[[ "${WIRELOG_PERF_GATE:-0}" == 1 ]] || \
    skip "set WIRELOG_PERF_GATE=1 to run on dedicated performance hardware"

case "$WIRELOG_DOOP_PERF_MODE" in
    required-hosted|strict-stable) ;;
    *) fail "unknown DOOP performance mode: $WIRELOG_DOOP_PERF_MODE" ;;
esac

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

if [[ "$WIRELOG_DOOP_PERF_MODE" == strict-stable ]]; then
    governor_file="${WIRELOG_DOOP_PERF_GATE_GOVERNOR_FILE:-/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor}"
    if [[ "${WIRELOG_PERF_REQUIRE:-0}" == 1 ]]; then
        if [[ ! -r "$governor_file" ]]; then
            fail "WIRELOG_PERF_REQUIRE=1 but cpufreq governor is unavailable"
        fi
        governor=$(<"$governor_file")
        [[ "$governor" == performance ]] || \
            fail "WIRELOG_PERF_REQUIRE=1 but cpufreq governor is '$governor'"
    else
        [[ -r "$governor_file" ]] || skip "cpufreq governor is unavailable"
        [[ "$(<"$governor_file")" == performance ]] || \
            skip "cpufreq governor is not 'performance'"
    fi
else
    governor_file="${WIRELOG_DOOP_PERF_GATE_GOVERNOR_FILE:-/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor}"
fi

data_dir="${WIRELOG_DOOP_DATA_DIR:-bench/data/doop}"
bench_bin="${WIRELOG_DOOP_BENCH_BIN:-bench/bench_flowlog}"
[[ -d "$data_dir" ]] || \
    required_or_skip "DOOP data directory not found: $data_dir"
[[ -x "$bench_bin" ]] || fail "bench_flowlog not found or not executable: $bench_bin"

if [[ "$WIRELOG_DOOP_PERF_MODE" == strict-stable ]]; then
    [[ -n "$WL_DOOP_PERF_GATE_TARGET_MS" ]] || \
        required_or_skip "no calibrated 5-repetition W=8 target; set WL_DOOP_PERF_GATE_TARGET_MS"
fi

oracle_file="$script_dir/../release/downstream-matrix-oracles.tsv"
[[ -r "$oracle_file" ]] || \
    required_or_skip "DOOP oracle file not found or unreadable: $oracle_file"

expected_header='# schema=2 workload tuple_oracle iteration_oracle data_path data_manifest_sha256 provenance_id acquisition_command'
actual_header=$(awk 'NF { print; exit }' "$oracle_file")
[[ "$actual_header" == "$expected_header" ]] || \
    fail "DOOP oracle file does not use the expected schema 2 header"

if ! awk -F '\t' '
    /^[[:space:]]*$/ || /^#/ { next }
    NF != 7 { exit 1 }
' "$oracle_file"; then
    fail "DOOP oracle file contains a row without exactly seven fields"
fi

doop_rows=0
EXPECTED_TUPLES=
EXPECTED_ITERS=
expected_manifest=
manifest_re='^archive:[0-9a-f]{64};files:[0-9a-f]{64}$'
while IFS=$'\t' read -r workload tuples iterations oracle_data_path \
        oracle_manifest provenance acquisition; do
    [[ -z "$workload" || "$workload" == \#* ]] && continue
    [[ "$workload" == doop ]] || continue

    doop_rows=$((doop_rows + 1))
    [[ "$doop_rows" -eq 1 ]] || \
        fail "DOOP oracle file contains more than one doop row"
    [[ -n "$tuples" && -n "$iterations" && -n "$oracle_data_path" && \
       -n "$oracle_manifest" && -n "$provenance" && -n "$acquisition" ]] || \
        fail "DOOP oracle row contains an empty field"
    [[ "$tuples" =~ ^[0-9]+$ && "$iterations" =~ ^[0-9]+$ ]] || \
        fail "DOOP tuple and iteration oracles must be unsigned decimal integers"
    [[ "$oracle_data_path" == bench/data/doop ]] || \
        fail "DOOP oracle data path is '$oracle_data_path', expected bench/data/doop"
    [[ "$oracle_manifest" =~ $manifest_re ]] || \
        fail "DOOP oracle manifest must contain archive and files SHA256 values"

    EXPECTED_TUPLES=$tuples
    EXPECTED_ITERS=$iterations
    expected_manifest=${oracle_manifest#*;files:}
done < "$oracle_file"

[[ "$doop_rows" -eq 1 ]] || fail "DOOP oracle file contains no doop row"
[[ "$expected_manifest" =~ ^[0-9a-f]{64}$ ]] || \
    fail "DOOP oracle files manifest is not a lowercase SHA256"

# The schema 2 DOOP row records both the downloaded archive and the sorted
# .facts manifest.  The performance gate consumes only the files: component:
# the benchmark reads extracted facts, not the archive that transported them.
# The manifest must be a function of the dataset -- the file names and their
# contents -- and of nothing else.  Two things that are not the dataset used to
# leak into it, and both surfaced as the same message below, which reads as
# tampering:
#
#   LC_ALL=C   en_US collation ignores the hyphen in Method-Modifier.facts and
#              reorders it against MethodHandleConstant.facts, so an unpinned
#              sort yields a different manifest on a non-C runner (#1294).
#
#   the path   sha256sum prints "<hash>  <path>", and the path is whatever
#              $data_dir expanded to, so relative, absolute and trailing-slash
#              spellings of one directory gave three different hashes (#1297).
#              This cd's into the directory instead of stripping the prefix
#              afterwards: stripping handled the common spellings but not a
#              backslash, which GNU sha256sum escapes by prefixing the whole
#              line, so two spellings of a backslash-containing directory still
#              disagreed.  Never embedding the path removes the class rather
#              than the instances.
#
#              This is also what makes the result agree with manifest_for_doop
#              in scripts/release/run-downstream-matrix.sh ON A CANONICAL PATH.
#              The two do NOT agree in general -- manifest_for_doop is not
#              trailing-slash invariant, counts dotfiles this glob skips, and
#              excludes symlinks this glob follows.  Those are properties of
#              that function, which this change does not touch; the divergences
#              are pinned as assertions in scripts/ci/test-doop-manifest.sh so
#              a shared-helper refactor has to address them deliberately.
#
# A function, not an inline expression, so scripts/ci/test-doop-manifest.sh can
# lift and exercise it -- the inline form could not be tested without running
# the whole benchmark.
doop_dataset_manifest() {
    # CDPATH= is load-bearing, not hygiene.  `cd` consults $CDPATH for a
    # RELATIVE argument -- which $data_dir is by default -- and on a hit it
    # both prints the resolved path to stdout (landing inside this command
    # substitution, so the manifest becomes a path plus a hash and can never
    # match a pin) and, worse, silently reaches a DIFFERENT directory.  With
    # CDPATH pointing at anything that also contains a `doop`, this returned
    # that directory's hash with exit 0: a manifest that exists to prove the
    # benchmark ran on the pinned data, certifying data it never read.
    #
    # `--` on both commands so an option-shaped NAME is treated as a name:
    # `cd -- -P` and `sha256sum -- -x.facts` work where the bare forms exit
    # "invalid option".  It does NOT protect a bare `-`: bash converts that to
    # $OLDPWD after option parsing, so `cd -- -` still jumps and still prints.
    # A directory literally named `-` is therefore still mishandled; it passes
    # the `[[ -d "$data_dir" ]]` check above and is out of scope here.
    #
    # The subshell keeps the cd from leaking, and under `set -e` a failed cd
    # aborts rather than hashing whatever the caller was sitting in.
    ( CDPATH= cd -- "$1" && sha256sum -- *.facts | LC_ALL=C sort -k2 \
        | sha256sum | awk '{print $1}' )
}

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
        required_or_skip "missing or empty DOOP input: $data_dir/$fact.facts"
done

actual_manifest=$(doop_dataset_manifest "$data_dir")
[[ "$actual_manifest" == "$expected_manifest" ]] || \
    fail "DOOP dataset files manifest $actual_manifest != oracle $expected_manifest"

tmp=$(mktemp)
progress=$(mktemp)
trap 'rm -f "$tmp" "$progress"' EXIT
set +e
"$bench_bin" --workload doop --data-doop "$data_dir" \
    --workers "$WORKERS" --repeat "$REPEAT" \
    --repeat-progress "$progress" >"$tmp" 2>&1
status=$?
set -e
(( status == 0 )) || fail "bench_flowlog DOOP run failed (exit $status); see output below\n$(<"$tmp")"

cat "$tmp"
cat "$progress"
[[ "$(grep -c '^repeat_start.*repeat=5.*workers=8$' "$progress")" == 5 ]] || \
    fail "DOOP repetition evidence does not contain five starts"
[[ "$(grep -c '^repeat_complete.*status=OK' "$progress")" == 5 ]] || \
    fail "DOOP repetition evidence does not contain five successful completions"
grep -Eq '^DONE.*repeat=5.*workers=8.*status=OK$' "$progress" || \
    fail "DOOP repetition evidence has no successful DONE record"

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
timing_status=enforced
if [[ "$WIRELOG_DOOP_PERF_MODE" == strict-stable ]]; then
    if ! awk -v value="$median_ms" -v target="$WL_DOOP_PERF_GATE_TARGET_MS" \
        'BEGIN { exit !(value + 0 <= target + 0) }'; then
        fail "median ${median_ms} ms exceeds target ${WL_DOOP_PERF_GATE_TARGET_MS} ms"
    fi
else
    timing_status=advisory
    [[ -n "$WL_DOOP_PERF_GATE_TARGET_MS" ]] || \
        WL_DOOP_PERF_GATE_TARGET_MS=unconfigured
fi

echo "doop_w8_gate OK: tuples=$tuples iterations=$iters workers=$workers repeat=$repeat median_ms=$median_ms target_ms=$WL_DOOP_PERF_GATE_TARGET_MS timing=$timing_status mode=$WIRELOG_DOOP_PERF_MODE"
