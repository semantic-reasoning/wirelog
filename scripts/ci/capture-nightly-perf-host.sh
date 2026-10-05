#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then echo "usage: $0 OUTPUT" >&2; exit 2; fi
out=$1
cpufreq=${CPUFREQ_ROOT:-/sys/devices/system/cpu/cpu0/cpufreq}
psi=${CPU_PSI_PATH:-/proc/pressure/cpu}
cgroup=${CPU_STAT_PATH:-/sys/fs/cgroup/cpu.stat}
loadavg=${LOADAVG_PATH:-/proc/loadavg}
cpuset=${CPUSET_PATH:-/sys/fs/cgroup/cpuset.cpus.effective}
cpuinfo=${CPUINFO_PATH:-/proc/cpuinfo}
taskset_bin=${TASKSET_BIN:-taskset}
{
    printf 'requested_affinity=0\n'
    printf 'affinity=%s\n' "$($taskset_bin -pc $$ 2>&1 | sed 's/.*: //')"
    printf 'kernel=%s\n' "$(uname -srmo)"
    printf 'cpu_model=%s\n' "$(awk -F: '/^model name[[:space:]]*:/ { sub(/^[[:space:]]+/, "", $2); print $2; exit }' "$cpuinfo")"
    printf 'loadavg=%s\n' "$(cat "$loadavg" 2>/dev/null || echo unavailable)"
    printf 'cpuset=%s\n' "$(cat "$cpuset" 2>/dev/null || echo unavailable)"
    printf 'governor=%s\n' "$(cat "$cpufreq/scaling_governor" 2>/dev/null || echo unavailable)"
    printf 'cur_freq=%s\n' "$(cat "$cpufreq/scaling_cur_freq" 2>/dev/null || echo unavailable)"
    printf 'min_freq=%s\n' "$(cat "$cpufreq/scaling_min_freq" 2>/dev/null || echo unavailable)"
    printf 'max_freq=%s\n' "$(cat "$cpufreq/scaling_max_freq" 2>/dev/null || echo unavailable)"
    printf 'cpuinfo_cur_freq=%s\n' "$(cat "$cpufreq/cpuinfo_cur_freq" 2>/dev/null || echo unavailable)"
    printf 'psi_cpu=%s\n' "$(tr '\n' ';' < "$psi" 2>/dev/null || echo unavailable)"
    cpu_stat=$(awk '
        NF != 2 || $1 !~ /^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)*$/ \
            || $2 !~ /^[0-9]+$/ { bad = 1; next }
        { printf "%s%s=%s", separator, $1, $2; separator = " " }
        END { if (bad || separator == "") exit 1; print "" }
    ' "$cgroup" 2>/dev/null) || cpu_stat=unavailable
    printf 'cpu_stat=%s\n' "$cpu_stat"
} > "$out"
