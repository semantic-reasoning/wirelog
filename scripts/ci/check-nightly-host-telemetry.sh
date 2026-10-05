#!/usr/bin/env bash
set -euo pipefail

if (( $# != 2 )); then echo "usage: $0 BEFORE AFTER" >&2; exit 2; fi
before=$1 after=$2
fail() { echo "nightly host telemetry ineligible: $*" >&2; exit 1; }

# Eligibility readiness rule: CPU PSI avg10 must stay at or below 1.0%,
# cgroup throttling must not increase, and one-minute load must not exceed the
# number of logical CPUs allowed by the runner's effective cpuset.

for file in "$before" "$after"; do
    [[ -s $file ]] || fail "missing snapshot: $file"
    for key in affinity kernel cpu_model loadavg cpuset governor cur_freq min_freq \
        max_freq psi_cpu cpu_stat; do
        grep -Eq "^${key}=.+$" "$file" || fail "missing $key in $file"
        ! grep -Eq "^${key}=unavailable$" "$file" || fail "$key unavailable in $file"
    done
    grep -Eq '^governor=performance$' "$file" || fail "performance governor was not active"
    for key in cur_freq min_freq max_freq; do
        grep -Eq "^${key}=[1-9][0-9]*$" "$file" || fail "invalid $key"
    done
    grep -Eq '^cpuinfo_cur_freq=(unavailable|[1-9][0-9]*)$' "$file" \
        || fail "invalid optional cpuinfo_cur_freq"
    grep -Eq '^cpu_model=.+$' "$file" || fail "CPU model is missing"
    grep -Eq '^cpuset=[0-9,-]+$' "$file" || fail "CPU set is malformed"
    grep -Eq '^affinity=[0-9,-]+$' "$file" || fail "CPU affinity is malformed"
    grep -Eq '^loadavg=[0-9.]+ [0-9.]+ [0-9.]+ .+$' "$file" \
        || fail "load average is malformed"
    grep -Eq '^psi_cpu=some avg10=[0-9.]+ avg60=[0-9.]+ avg300=[0-9.]+ total=[0-9]+' "$file" \
        || fail "CPU PSI data is malformed"
    for metric in usage_usec nr_throttled throttled_usec; do
        value=$(awk -v wanted="$metric" '
            /^cpu_stat=/ {
                for (i = 1; i <= NF; i++) {
                    token = $i
                    sub(/^cpu_stat=/, "", token)
                    split(token, pair, "=")
                    if (pair[1] == wanted) { print pair[2]; exit }
                }
            }' "$file")
        [[ $value =~ ^[0-9]+$ ]] || fail "cgroup CPU statistic $metric is malformed"
    done
done

check_load() {
    local file=$1 load1 cpuset allowed
    load1=$(sed -n 's/^loadavg=\([0-9.]*\).*/\1/p' "$file")
    cpuset=$(sed -n 's/^cpuset=//p' "$file")
    allowed=$(awk -v cpus="$cpuset" 'BEGIN {
        count = split(cpus, ranges, ",")
        total = 0
        for (i = 1; i <= count; i++) {
            parts = split(ranges[i], bounds, "-")
            if (parts == 1) total++
            else total += bounds[2] - bounds[1] + 1
        }
        print total
    }')
    [[ $allowed =~ ^[1-9][0-9]*$ ]] || fail "allowed CPU count is malformed"
    awk -v load1="$load1" -v cpus="$allowed" \
        'BEGIN { if (load1 > cpus) exit 1 }' \
        || fail "one-minute load exceeded the allowed logical CPU count"
}
check_load "$before"
check_load "$after"

before_psi=$(sed -n 's/^psi_cpu=some avg10=\([0-9.]*\).*/\1/p' "$before")
after_psi=$(sed -n 's/^psi_cpu=some avg10=\([0-9.]*\).*/\1/p' "$after")
[[ $before_psi =~ ^[0-9]+([.][0-9]+)?$ && $after_psi =~ ^[0-9]+([.][0-9]+)?$ ]] \
    || fail "CPU PSI avg10 is malformed"
awk -v before="$before_psi" -v after="$after_psi" \
    'BEGIN { if (before > 1.0 || after > 1.0) exit 1 }' \
    || fail "CPU PSI avg10 exceeded 1.0%"

throttled_usec() {
    awk '
        /^cpu_stat=/ {
            for (i = 1; i <= NF; i++) {
                token = $i
                sub(/^cpu_stat=/, "", token)
                split(token, pair, "=")
                if (pair[1] == "throttled_usec") { print pair[2]; exit }
            }
        }' "$1"
}
before_throttled=$(throttled_usec "$before")
after_throttled=$(throttled_usec "$after")
[[ $before_throttled =~ ^[0-9]+$ && $after_throttled =~ ^[0-9]+$ ]] \
    || fail "cgroup CPU throttling telemetry is malformed"
(( after_throttled == before_throttled )) || fail "cgroup throttling occurred during timing"
echo 'telemetry_eligibility=eligible'
