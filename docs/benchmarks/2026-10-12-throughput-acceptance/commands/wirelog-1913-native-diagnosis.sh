#!/usr/bin/env bash
set -euo pipefail
repo=/srv/home/joykim/git/wirelog
diagnostic_root="$HOME/.cache/wirelog-1913-current-69111650"
mkdir -p "$diagnostic_root/tmp" "$diagnostic_root/evidence"
export TMPDIR="$diagnostic_root/tmp"
if [[ ! -d "$diagnostic_root/source" ]]; then
    git -C "$repo" worktree add --detach "$diagnostic_root/source" 691116500ac45407b73244850641c646877fdb43
fi
cd "$diagnostic_root/source"
test "$(git rev-parse HEAD)" = 691116500ac45407b73244850641c646877fdb43
test -z "$(git status --porcelain=v1 --untracked-files=all)"
uv venv "$diagnostic_root/venv"
source "$diagnostic_root/venv/bin/activate"
uv pip install meson==1.12.0 ninja
gcc --version > "$diagnostic_root/evidence/gcc-version.txt"
perf --version > "$diagnostic_root/evidence/perf-version.txt"
git rev-parse HEAD HEAD^{tree} > "$diagnostic_root/evidence/source.txt"
meson setup "$diagnostic_root/build" --buildtype=release \
    -Doptimization=s -Db_lto=true -Ddebug=true -Dtests=true \
    -DmbedTLS=disabled -Dwirelog_log_max_level=error \
    > "$diagnostic_root/evidence/configure.log" 2>&1
meson compile -C "$diagnostic_root/build" -j8 \
    test_crdt_perf_gate test_cspa_perf_gate test_memory_governor \
    test_memory_admission_relation test_memory_admission_join \
    test_memory_admission_arena test_memory_admission_delta \
    test_memory_admission_compound test_memory_admission_intern \
    > "$diagnostic_root/evidence/build.log" 2>&1
meson configure "$diagnostic_root/build" > "$diagnostic_root/evidence/meson-config.txt"
meson introspect "$diagnostic_root/build" --compilers > "$diagnostic_root/evidence/compilers.json"
sha256sum "$diagnostic_root/build/tests/test_crdt_perf_gate" \
    "$diagnostic_root/build/tests/test_cspa_perf_gate" \
    bench/data/crdt/Insert_input.csv bench/data/crdt/Remove_input.csv \
    bench/data/cspa/assign.csv bench/data/cspa/dereference.csv \
    > "$diagnostic_root/evidence/sha256.txt"
set +e
meson test -C "$diagnostic_root/build" --no-rebuild --num-processes 1 \
    --print-errorlogs memory_governor memory_admission_relation \
    memory_admission_join memory_admission_arena memory_admission_delta \
    memory_admission_compound memory_admission_intern \
    > "$diagnostic_root/evidence/admission-tests.txt" 2>&1
admission_rc=$?
set -e
printf '%s\n' "$admission_rc" > "$diagnostic_root/evidence/admission-tests.exit"
cp "$diagnostic_root/build/meson-logs/testlog.txt" \
    "$diagnostic_root/evidence/admission-testlog.txt"
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
