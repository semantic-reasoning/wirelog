#!/usr/bin/env bash
# Temporary #1913 diagnostic. Run unchanged performance gates on two pinned
# source trees in a predeclared, balanced order on one hosted runner.
set -u -o pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 BASE_TREE CANDIDATE_TREE ARTIFACT_DIR" >&2
    exit 2
fi

base_tree=$1
candidate_tree=$2
artifact_dir=$3
mkdir -p "$artifact_dir/runs"

run_gate() {
    local variant=$1
    local workload=$2
    local block=$3
    local attempt=$4
    local source_tree
    local target
    local seed
    local data_dir
    local data_env
    local source_sha
    local binary
    local log
    local rc

    if [ "$variant" = base ]; then
        source_tree=$base_tree
    else
        source_tree=$candidate_tree
    fi
    if [ "$workload" = crdt ]; then
        target=38120
        seed=23
        data_dir="$source_tree/bench/data/crdt"
        data_env="WIRELOG_CRDT_DATA_DIR=$data_dir"
        binary="$source_tree/build-perf/tests/test_crdt_perf_gate"
    else
        target=2050
        seed=160
        data_dir="$source_tree/bench/data/cspa"
        data_env="WIRELOG_CSPA_DATA_DIR=$data_dir"
        binary="$source_tree/build-perf/tests/test_cspa_perf_gate"
    fi
    log="$artifact_dir/runs/${workload}-block${block}-${variant}-attempt${attempt}.log"
    source_sha=$(git -C "$source_tree" rev-parse HEAD)

    if [ "$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)" != performance ]; then
        echo "cpu0 governor changed during measurement; aborting" >&2
        return 90
    fi
    taskset -c 0 true || return 90
    printf 'variant=%s source_sha=%s workload=%s block=%s attempt=%s seed=%s target_ms=%s governor=%s affinity=0\n' \
        "$variant" "$source_sha" "$workload" "$block" "$attempt" "$seed" "$target" \
        "$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)" \
        > "$log"
    sleep 15

    taskset -c 0 env \
        -u WIRELOG_PERF_REQUIRE \
        -u WIRELOG_GATE_CORRECTNESS_ONLY \
        WIRELOG_PERF_GATE=1 \
        MALLOC_PERTURB_="$seed" \
        "$data_env" \
        bash -c 'printf "child MALLOC_PERTURB_=%s\\n" "$MALLOC_PERTURB_"; exec "$1"' \
            issue-1913-gate "$binary" >> "$log" 2>&1
    rc=$?
    cat "$log"
    printf 'exit_status=%s\n' "$rc" | tee -a "$log"

    if [ "$rc" -eq 77 ]; then
        return 77
    fi
    if [ "$rc" -eq 0 ]; then
        return 0
    fi
    if grep -q 'FAIL: median .* exceeds target' "$log"; then
        return 1
    fi
    echo "gate failed for a reason other than a stable target miss; see $log" >&2
    return 90
}

run_checked() {
    local variant=$1
    local workload=$2
    local block=$3
    local result

    run_gate "$variant" "$workload" "$block" 1
    result=$?
    if [ "$result" -eq 77 ]; then
        if ! grep -q 'SKIP: baseline CoV .* exceeds' \
            "$artifact_dir/runs/${workload}-block${block}-${variant}-attempt1.log"; then
            echo "non-noise SKIP; aborting as inconclusive without retry" >&2
            return 90
        fi
        echo "retrying only noisy CoV skip: $workload block $block $variant" >&2
        run_gate "$variant" "$workload" "$block" 2
        result=$?
        if [ "$result" -eq 77 ]; then
            echo "inconclusive: repeated noisy CoV skip" >&2
            return 3
        fi
    fi
    printf '%s\n' "$result" > "$artifact_dir/runs/${workload}-block${block}-${variant}.result"
    return "$result"
}

declare -A candidate_failures=([crdt]=0 [cspa]=0)

# Qualify both baseline workloads before measuring any candidate workload.
# A stable target miss on main invalidates attribution to the candidate.
for workload in crdt cspa; do
    run_checked base "$workload" preflight
    result=$?
    if [ "$result" -eq 3 ] || [ "$result" -eq 90 ]; then
        exit "$result"
    fi
    if [ "$result" -eq 1 ]; then
        echo "baseline stably exceeds the $workload target; stopping before candidate measurements" >&2
        exit 4
    fi
done

# Fixed schedule: AB, BA, AB. A=main baseline; B=PR candidate.
for workload in crdt cspa; do
    for block in 1 2 3; do
        if [ "$block" -eq 2 ]; then
            order=(candidate base)
        else
            order=(base candidate)
        fi
        for variant in "${order[@]}"; do
            run_checked "$variant" "$workload" "$block"
            result=$?
            if [ "$result" -eq 3 ] || [ "$result" -eq 90 ]; then
                exit "$result"
            fi
            if [ "$variant" = base ] && [ "$result" -eq 1 ]; then
                echo "baseline stably exceeds the gate target; stopping diagnosis before further candidate measurements" >&2
                exit 4
            fi
            if [ "$variant" = candidate ] && [ "$result" -eq 1 ]; then
                candidate_failures[$workload]=1
            fi
        done
    done
done

for workload in crdt cspa; do
    if [ "${candidate_failures[$workload]}" -ne 0 ]; then
        echo "candidate stably exceeded the $workload target in one or more paired blocks" >&2
    else
        echo "candidate passed all $workload invocations" >&2
    fi
done

if [ "${candidate_failures[crdt]}" -ne 0 ] || [ "${candidate_failures[cspa]}" -ne 0 ]; then
    exit 1
fi
