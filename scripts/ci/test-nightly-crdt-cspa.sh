#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
test_home=${HOME:?HOME must be set}
tmp_root="$test_home/.tmp"
mkdir -p "$tmp_root"
tmp_dir=$(mktemp -d "$tmp_root/nightly-perf-test.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

gate_block() {
    local index=$1 name=$2 result=$3 marker=$4 evidence=$5
    cat <<EOF
=================================== $index/2 ===================================
test:         perf - wirelog:$name
result:       exit status $result
----------------------------------- stdout -----------------------------------
$marker
$evidence
==============================================================================
EOF
}

good_crdt='test_crdt_perf_gate: raw_ms = 1 2 3 4 5 6 7 8 9
  result     = 104851 (expected 104851)
  median_ms  = 5 (target 38120)
  stdev_ms   = 0.05 (CoV 1.000%)'
good_cspa='test_cspa_perf_gate: raw_ms = 1 2 3 4 5 6 7 8 9
  tuples     = 20381/20,381
  iterations = 6/6
  median_ms  = 5 (target 2050)
  stdev_ms   = 0.05 (CoV 1.000%)'
write_valid() {
    gate_block 1 crdt_perf_gate 0 'test_crdt_perf_gate OK' "$good_crdt" > "$1"
    gate_block 2 cspa_w1_gate 0 'test_cspa_perf_gate OK' "$good_cspa" >> "$1"
}

write_valid "$tmp_dir/valid.log"
"$script_dir/check-nightly-crdt-cspa.sh" "$tmp_dir/valid.log"

gate_block 1 crdt_perf_gate 1 \
    'test_crdt_perf_gate: FAIL: median 40000.0 ms exceeds target 38120 ms (regression)' \
    "$good_crdt" > "$tmp_dir/target-miss.log"
gate_block 2 cspa_w1_gate 0 'test_cspa_perf_gate OK' "$good_cspa" >> "$tmp_dir/target-miss.log"
if ! "$script_dir/check-nightly-crdt-cspa.sh" "$tmp_dir/target-miss.log" \
    > "$tmp_dir/target-miss.result"; then
    echo 'complete target-miss fixture did not preserve acquisition status' >&2; exit 1
fi
if ! grep -Fxq 'performance_verdict=fail' "$tmp_dir/target-miss.result"; then
    echo 'complete target-miss fixture did not preserve performance verdict' >&2; exit 1
fi

gate_block 1 crdt_perf_gate 77 'test_crdt_perf_gate: SKIP: noisy' "$good_crdt" > "$tmp_dir/skip.log"
gate_block 2 cspa_w1_gate 0 'test_cspa_perf_gate OK' "$good_cspa" >> "$tmp_dir/skip.log"
if "$script_dir/check-nightly-crdt-cspa.sh" "$tmp_dir/skip.log"; then
    echo 'skip fixture unexpectedly passed' >&2; exit 1
fi

gate_block 1 crdt_perf_gate 1 'test_crdt_perf_gate: FAIL: target miss' "$good_crdt" > "$tmp_dir/miss.log"
gate_block 2 cspa_w1_gate 0 'test_cspa_perf_gate OK' "$good_cspa" >> "$tmp_dir/miss.log"
if "$script_dir/check-nightly-crdt-cspa.sh" "$tmp_dir/miss.log"; then
    echo 'target miss fixture unexpectedly passed' >&2; exit 1
fi

gate_block 1 crdt_perf_gate 0 'test_crdt_perf_gate OK' "$good_crdt" > "$tmp_dir/missing.log"
if "$script_dir/check-nightly-crdt-cspa.sh" "$tmp_dir/missing.log"; then
    echo 'missing CSPA fixture unexpectedly passed' >&2; exit 1
fi

gate_block 1 crdt_perf_gate 0 'test_crdt_perf_gate OK' "${good_crdt/raw_ms = 1 2 3 4 5 6 7 8 9/raw_ms = 1 2 3}" > "$tmp_dir/incomplete.log"
gate_block 2 cspa_w1_gate 0 'test_cspa_perf_gate OK' "$good_cspa" >> "$tmp_dir/incomplete.log"
if "$script_dir/check-nightly-crdt-cspa.sh" "$tmp_dir/incomplete.log"; then
    echo 'incomplete sample fixture unexpectedly passed' >&2; exit 1
fi

cat > "$tmp_dir/docker-missing" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$DOCKER_CALL_LOG"
exit 1
EOF
chmod +x "$tmp_dir/docker-missing"
if HOME="$test_home" PERF_ARTIFACT_DIR="$tmp_dir/missing-image" \
    DOCKER_CALL_LOG="$tmp_dir/docker-calls" DOCKER_BIN="$tmp_dir/docker-missing" \
    "$script_dir/run-perf-nightly-linux.sh"; then
    echo 'missing-image fixture unexpectedly passed' >&2; exit 1
fi
grep -Fq 'image inspect semantic-reasoning:ubuntu26' "$tmp_dir/docker-calls"
! grep -Fq 'run --pull=never' "$tmp_dir/docker-calls"

write_telemetry() {
    local path=$1 psi=$2 throttled=$3 load=${4:-0.01}
    printf 'requested_affinity=0\n' > "$path"
    printf 'affinity=0\n' >> "$path"
    printf 'kernel=Linux test\n' >> "$path"
    printf 'cpu_model=Test CPU\n' >> "$path"
    printf 'loadavg=%s 0.02 0.03 1/2 3\n' "$load" >> "$path"
    printf 'cpuset=0-3\n' >> "$path"
    printf 'governor=performance\n' >> "$path"
    printf 'cur_freq=3000000\n' >> "$path"
    printf 'min_freq=1000000\n' >> "$path"
    printf 'max_freq=4000000\n' >> "$path"
    printf 'cpuinfo_cur_freq=unavailable\n' >> "$path"
    printf 'psi_cpu=some avg10=%s avg60=0.00 avg300=0.00 total=1;full avg10=0.00 avg60=0.00 avg300=0.00 total=1;\n' \
        "$psi" >> "$path"
    printf 'cpu_stat=usage_usec=2 user_usec=1 system_usec=1 nr_periods=1 nr_throttled=0 throttled_usec=%s\n' \
        "$throttled" >> "$path"
}
write_telemetry "$tmp_dir/host-before" 0.00 10
write_telemetry "$tmp_dir/host-after" 0.10 10
"$script_dir/check-nightly-host-telemetry.sh" \
    "$tmp_dir/host-before" "$tmp_dir/host-after"
write_telemetry "$tmp_dir/host-after" 2.00 10
if "$script_dir/check-nightly-host-telemetry.sh" \
    "$tmp_dir/host-before" "$tmp_dir/host-after"; then
    echo 'high-pressure telemetry fixture unexpectedly passed' >&2; exit 1
fi
write_telemetry "$tmp_dir/host-after" 0.10 11
if "$script_dir/check-nightly-host-telemetry.sh" \
    "$tmp_dir/host-before" "$tmp_dir/host-after"; then
    echo 'throttled telemetry fixture unexpectedly passed' >&2; exit 1
fi
sed '/^governor=/d' "$tmp_dir/host-after" > "$tmp_dir/host-after.tmp"
mv "$tmp_dir/host-after.tmp" "$tmp_dir/host-after"
if "$script_dir/check-nightly-host-telemetry.sh" \
    "$tmp_dir/host-before" "$tmp_dir/host-after"; then
    echo 'missing telemetry fixture unexpectedly passed' >&2; exit 1
fi
write_telemetry "$tmp_dir/host-after" 0.10 10 5.0
if "$script_dir/check-nightly-host-telemetry.sh" \
    "$tmp_dir/host-before" "$tmp_dir/host-after"; then
    echo 'high-load fixture unexpectedly passed' >&2; exit 1
fi

# Exercise the real snapshot producer with Linux cpu.stat's multiline,
# whitespace-separated format, then feed its output to the real checker.
mkdir -p "$tmp_dir/cpufreq"
cat > "$tmp_dir/taskset" <<'EOF'
#!/usr/bin/env bash
printf "pid %s's current affinity list: 0-3\n" "${2:-1}"
EOF
chmod +x "$tmp_dir/taskset"
printf 'model name : Fixture CPU\n' > "$tmp_dir/cpuinfo"
printf '0.01 0.02 0.03 1/2 3\n' > "$tmp_dir/loadavg"
printf '0-3\n' > "$tmp_dir/cpuset"
printf 'performance\n' > "$tmp_dir/cpufreq/scaling_governor"
printf '3000000\n' > "$tmp_dir/cpufreq/scaling_cur_freq"
printf '1000000\n' > "$tmp_dir/cpufreq/scaling_min_freq"
printf '4000000\n' > "$tmp_dir/cpufreq/scaling_max_freq"
cat > "$tmp_dir/psi" <<'EOF'
some avg10=0.00 avg60=0.00 avg300=0.00 total=1
full avg10=0.00 avg60=0.00 avg300=0.00 total=1
EOF
write_kernel_cpu_stat() {
    local throttled=$1 variant=${2:-valid}
    printf 'usage_usec 20\n' > "$tmp_dir/kernel-cpu.stat"
    printf 'user_usec 10\n' >> "$tmp_dir/kernel-cpu.stat"
    printf 'system_usec 1\n' >> "$tmp_dir/kernel-cpu.stat"
    printf 'nice_usec 0\n' >> "$tmp_dir/kernel-cpu.stat"
    case $variant in
        malformed_key) printf 'core-sched.force_idle_usec 0\n' >> "$tmp_dir/kernel-cpu.stat" ;;
        malformed_value) printf 'core_sched.force_idle_usec invalid\n' >> "$tmp_dir/kernel-cpu.stat" ;;
        *) printf 'core_sched.force_idle_usec 0\n' >> "$tmp_dir/kernel-cpu.stat" ;;
    esac
    printf 'nr_periods 2\n' >> "$tmp_dir/kernel-cpu.stat"
    if [[ $variant != missing_required ]]; then
        printf 'nr_throttled 0\n' >> "$tmp_dir/kernel-cpu.stat"
    fi
    printf 'throttled_usec %s\n' "$throttled" >> "$tmp_dir/kernel-cpu.stat"
    printf 'nr_bursts 0\n' >> "$tmp_dir/kernel-cpu.stat"
    printf 'burst_usec 0\n' >> "$tmp_dir/kernel-cpu.stat"
}
capture_fixture() {
    CPUFREQ_ROOT="$tmp_dir/cpufreq" CPU_PSI_PATH="$tmp_dir/psi" \
        CPU_STAT_PATH="$tmp_dir/kernel-cpu.stat" LOADAVG_PATH="$tmp_dir/loadavg" \
        CPUSET_PATH="$tmp_dir/cpuset" CPUINFO_PATH="$tmp_dir/cpuinfo" \
        TASKSET_BIN="$tmp_dir/taskset" \
        "$script_dir/capture-nightly-perf-host.sh" "$1"
}
write_kernel_cpu_stat 15
capture_fixture "$tmp_dir/captured-before"
capture_fixture "$tmp_dir/captured-after"
grep -Fxq 'cpu_stat=usage_usec=20 user_usec=10 system_usec=1 nice_usec=0 core_sched.force_idle_usec=0 nr_periods=2 nr_throttled=0 throttled_usec=15 nr_bursts=0 burst_usec=0' \
    "$tmp_dir/captured-before"
"$script_dir/check-nightly-host-telemetry.sh" \
    "$tmp_dir/captured-before" "$tmp_dir/captured-after"
write_kernel_cpu_stat 16
capture_fixture "$tmp_dir/captured-after-throttled"
if "$script_dir/check-nightly-host-telemetry.sh" \
    "$tmp_dir/captured-before" "$tmp_dir/captured-after-throttled"; then
    echo 'captured kernel throttling delta unexpectedly passed' >&2; exit 1
fi
for variant in malformed_key malformed_value missing_required; do
    write_kernel_cpu_stat 15 "$variant"
    capture_fixture "$tmp_dir/captured-$variant"
    if "$script_dir/check-nightly-host-telemetry.sh" \
        "$tmp_dir/captured-before" "$tmp_dir/captured-$variant"; then
        echo "$variant captured kernel cpu.stat unexpectedly passed" >&2; exit 1
    fi
done

echo 'nightly CRDT/CSPA checker fixtures: OK'
