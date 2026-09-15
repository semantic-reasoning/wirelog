#!/usr/bin/env bash
# Focused contract fixture for the DOOP W=8 gate's schema 2 oracle.
set -euo pipefail

case "$(uname -s 2>/dev/null || printf unknown)" in
    Linux*) ;;
    *) echo "test-doop-perf-gate-oracle: SKIP: needs Linux"; exit 77 ;;
esac

root=${1:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)}
source_gate="$root/scripts/ci/run-doop-perf-gate.sh"
tmp=$(CDPATH= cd -- "$(mktemp -d "${TMPDIR:-/tmp}/wirelog-doop-gate.XXXXXX")" && pwd)
trap 'rm -rf "$tmp"' EXIT

fixture_root="$tmp/root"
fixture_gate="$fixture_root/scripts/ci/run-doop-perf-gate.sh"
oracle="$fixture_root/scripts/release/downstream-matrix-oracles.tsv"
data="$tmp/doop"
bench="$tmp/bench_flowlog"
governor="$tmp/scaling_governor"
mkdir -p "$fixture_root/scripts/ci" "$fixture_root/scripts/release" "$data"
cp "$source_gate" "$fixture_gate"

for fact in \
    DirectSuperclass DirectSuperinterface MainClass FormalParam ComponentType \
    AssignReturnValue ActualParam Method-Modifier Var-Type ClassType ArrayType \
    InterfaceType Var-DeclaringMethod ApplicationClass ThisVar NormalHeap \
    StringConstant AssignHeapAllocation AssignLocal AssignCast Field \
    StaticMethodInvocation SpecialMethodInvocation VirtualMethodInvocation Method \
    StoreInstanceField LoadInstanceField StoreStaticField LoadStaticField \
    StoreArrayIndex LoadArrayIndex Return ClassHeap MethodHandleConstant \
    MethodTypeConstant; do
    printf 'fixture for %s\n' "$fact" > "$data/$fact.facts"
done

printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'progress=' \
    'while (($#)); do [[ "$1" == --repeat-progress ]] && progress=$2 && shift; shift; done' \
    'for n in 1 2 3 4 5; do printf '\''repeat_start\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\n'\'' "$n" >> "$progress"; printf '\''repeat_complete\tworkload=doop\trepetition=%s\trepeat=5\tworkers=8\tstatus=OK\n'\'' "$n" >> "$progress"; done' \
    'printf '\''DONE\tworkload=doop\trepeat=5\tworkers=8\tstatus=OK\n'\'' >> "$progress"' \
    'printf '\''doop\tfixture\t35\t8\t5\t1\t1\t1\t1\t%s\t%s\tOK\n'\'' "${FAKE_TUPLES:?}" "${FAKE_ITERS:?}"' \
    > "$bench"
chmod +x "$bench"
printf 'performance\n' > "$governor"

manifest_function=$(sed -n '/^doop_dataset_manifest()/,/^}/p' "$fixture_gate")
printf '%s\n' "$manifest_function" | bash -n
eval "$manifest_function"
files_manifest=$(doop_dataset_manifest "$data")
[[ "$files_manifest" =~ ^[0-9a-f]{64}$ ]]

header='# schema=2 workload tuple_oracle iteration_oracle data_path data_manifest_sha256 provenance_id acquisition_command'
archive_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
write_oracle() {
    local manifest=$1 tuples=${2:-13828835} iterations=${3:-153}
    printf '%s\n' "$header" > "$oracle"
    printf 'doop\t%s\t%s\tbench/data/doop\t%s\t' "$tuples" "$iterations" "$manifest" >> "$oracle"
    printf '%s\t%s\n' 'huggingface:NemoYuu/flowlog_benchmark@da9e91b3ff75d94604f57ba2b21ef3aa97e241ec#dataset/csv/zxing.zip' 'download-doop-zxing' >> "$oracle"
}

expect_gate() {
    local name=$1 want=$2 needle=$3 status
    shift 3
    if env -u WL_DOOP_PERF_GATE_TARGET_MS \
           -u WIRELOG_DOOP_DATASET_MANIFEST_SHA256 \
           WIRELOG_PERF_GATE=1 \
           WIRELOG_PERF_LOG_COMPILE_MAX_LEVEL=1 \
           WIRELOG_DOOP_PERF_GATE_GOVERNOR_FILE="$governor" \
           WIRELOG_DOOP_DATA_DIR="$data" \
           WIRELOG_DOOP_BENCH_BIN="$bench" \
           "$@" "$fixture_gate" > "$tmp/out" 2>&1; then
        status=0
    else
        status=$?
    fi
    if [[ "$status" != "$want" ]]; then
        printf 'test-doop-perf-gate-oracle: FAIL %s (want exit %s, got %s)\n' \
            "$name" "$want" "$status" >&2
        sed 's/^/    /' "$tmp/out" >&2
        exit 1
    fi
    if ! grep -Fq -- "$needle" "$tmp/out"; then
        printf 'test-doop-perf-gate-oracle: FAIL %s (missing %s)\n' \
            "$name" "$needle" >&2
        sed 's/^/    /' "$tmp/out" >&2
        exit 1
    fi
    printf 'test-doop-perf-gate-oracle: ok %s\n' "$name"
}

valid_manifest="archive:$archive_sha;files:$files_manifest"
write_oracle "$valid_manifest"
expect_gate 'stale tuple and iteration sentinels are rejected' 1 \
    'tuple count 6276657 != sentinel 13828835' \
    WIRELOG_PERF_REQUIRE=1 WL_DOOP_PERF_GATE_TARGET_MS=10 \
    FAKE_TUPLES=6276657 FAKE_ITERS=28
expect_gate 'schema 2 tuple, iteration, and files manifest pass' 0 \
    'doop_w8_gate OK: tuples=13828835 iterations=153' \
    WIRELOG_PERF_REQUIRE=1 WL_DOOP_PERF_GATE_TARGET_MS=10 \
    FAKE_TUPLES=13828835 FAKE_ITERS=153
expect_gate 'hosted required mode accepts correctness without a stable target' 0 \
    'timing=advisory mode=required-hosted' \
    WIRELOG_PERF_REQUIRE=1 WIRELOG_DOOP_PERF_MODE=required-hosted \
    FAKE_TUPLES=13828835 FAKE_ITERS=153
expect_gate 'missing target fails required mode' 1 \
    'no calibrated 5-repetition W=8 target' \
    WIRELOG_PERF_REQUIRE=1 FAKE_TUPLES=13828835 FAKE_ITERS=153
expect_gate 'missing target skips non-required mode' 77 \
    'doop_w8_gate: SKIP: no calibrated 5-repetition W=8 target' \
    WIRELOG_PERF_REQUIRE=0 FAKE_TUPLES=13828835 FAKE_ITERS=153

write_oracle ''
expect_gate 'missing manifest fails required mode' 1 \
    'DOOP oracle row contains an empty field' \
    WIRELOG_PERF_REQUIRE=1 WL_DOOP_PERF_GATE_TARGET_MS=10 \
    FAKE_TUPLES=13828835 FAKE_ITERS=153

write_oracle "$valid_manifest"
printf 'doop\t13828835\t153\tbench/data/doop\t%s\tduplicate\tdownload-doop-zxing\n' \
    "$valid_manifest" >> "$oracle"
expect_gate 'duplicate doop oracle row fails' 1 \
    'more than one doop row' \
    WIRELOG_PERF_REQUIRE=1 WL_DOOP_PERF_GATE_TARGET_MS=10 \
    FAKE_TUPLES=13828835 FAKE_ITERS=153

write_oracle "archive:$archive_sha;files:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
expect_gate 'files manifest mismatch fails' 1 \
    'DOOP dataset files manifest' \
    WIRELOG_PERF_REQUIRE=1 WL_DOOP_PERF_GATE_TARGET_MS=10 \
    FAKE_TUPLES=13828835 FAKE_ITERS=153

write_oracle "$valid_manifest"
mv "$data" "$tmp/doop-saved"
expect_gate 'missing data fails required mode' 1 \
    'DOOP data directory not found' \
    WIRELOG_PERF_REQUIRE=1 WL_DOOP_PERF_GATE_TARGET_MS=10 \
    FAKE_TUPLES=13828835 FAKE_ITERS=153
mv "$tmp/doop-saved" "$data"

mv "$data/Method.facts" "$tmp/Method.facts"
expect_gate 'missing fact fails required mode' 1 \
    'missing or empty DOOP input' \
    WIRELOG_PERF_REQUIRE=1 WL_DOOP_PERF_GATE_TARGET_MS=10 \
    FAKE_TUPLES=13828835 FAKE_ITERS=153

printf 'test-doop-perf-gate-oracle: all cases passed\n'
