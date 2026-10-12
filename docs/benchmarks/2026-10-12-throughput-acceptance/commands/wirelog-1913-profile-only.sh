#!/usr/bin/env bash
set -euo pipefail
diagnostic_root="$HOME/.cache/wirelog-1913-current-69111650"
cd "$diagnostic_root/source"
source "$diagnostic_root/venv/bin/activate"
for workload in cspa crdt; do
    scripts/ci/capture-nightly-perf-host.sh "$diagnostic_root/evidence/$workload-before.txt"
    set +e
    if [[ $workload == cspa ]]; then
        perf record -e cycles:u -F399 --call-graph dwarf,8192 \
            -o "$diagnostic_root/evidence/cspa.perf.data" -- taskset -c 0 \
            env WIRELOG_GATE_CORRECTNESS_ONLY=1 WIRELOG_CSPA_DATA_DIR="$PWD/bench/data/cspa" \
            "$diagnostic_root/build/tests/test_cspa_perf_gate" \
            > "$diagnostic_root/evidence/cspa-profile.stdout" \
            2> "$diagnostic_root/evidence/cspa-profile.stderr"
    else
        perf record -e cycles:u -F399 --call-graph dwarf,8192 \
            -o "$diagnostic_root/evidence/crdt.perf.data" -- taskset -c 0 \
            env WIRELOG_CRDT_PROBE=1 WIRELOG_CRDT_DATA_DIR="$PWD/bench/data/crdt" \
            "$diagnostic_root/build/tests/test_crdt_perf_gate" \
            > "$diagnostic_root/evidence/crdt-profile.stdout" \
            2> "$diagnostic_root/evidence/crdt-profile.stderr"
    fi
    profile_rc=$?
    set -e
    printf '%s\n' "$profile_rc" > "$diagnostic_root/evidence/$workload-profile.exit"
    scripts/ci/capture-nightly-perf-host.sh "$diagnostic_root/evidence/$workload-after.txt"
    if (( profile_rc != 0 )); then
        echo "diagnosis_status=incomplete" > "$diagnostic_root/evidence/$workload-status.txt"
        continue
    fi
    echo "diagnosis_status=recorded" > "$diagnostic_root/evidence/$workload-status.txt"
    perf report -i "$diagnostic_root/evidence/$workload.perf.data" \
        --stdio --no-children --call-graph none --sort symbol \
        > "$diagnostic_root/evidence/$workload-self-report.txt" \
        2> "$diagnostic_root/evidence/$workload-report.stderr"
    perf report -i "$diagnostic_root/evidence/$workload.perf.data" \
        --stdio --children --sort symbol \
        > "$diagnostic_root/evidence/$workload-callgraph-report.txt" 2>> "$diagnostic_root/evidence/$workload-report.stderr"
    perf script -i "$diagnostic_root/evidence/$workload.perf.data" \
        --show-lost-events > "$diagnostic_root/evidence/$workload-events.txt" \
        2> "$diagnostic_root/evidence/$workload-events.stderr"
done
printf 'diagnostic_root=%s\n' "$diagnostic_root"
