#!/usr/bin/env bash
set -euo pipefail

image=semantic-reasoning:ubuntu26
docker_bin=${DOCKER_BIN:-docker}
repo_root=$(git rev-parse --show-toplevel)
artifact_dir=${PERF_ARTIFACT_DIR:-"$repo_root/perf-artifacts/linux-suite"}
mkdir -p "$artifact_dir"

if [[ ${1:-} == --inside-image ]]; then
    artifact_mount=${2:?artifact mount required}
    tmp_root=${TMPDIR:?TMPDIR must be set under HOME}
    mkdir -p "$tmp_root"
    export UV_CACHE_DIR="$tmp_root/uv-cache"
    python_env="$tmp_root/meson-venv"
    uv venv --python python3 "$python_env"
    uv pip install --python "$python_env/bin/python" 'meson==1.12.0' ninja
    export PATH="$python_env/bin:$PATH"
    test "$(meson --version)" = 1.12.0
    rm -rf build-perf-nightly build-perf-nightly-trace
    gcc --version > "$artifact_mount/gcc-version.txt"
    meson --version > "$artifact_mount/meson-version.txt"
    ninja --version > "$artifact_mount/ninja-version.txt"
    uv --version > "$artifact_mount/uv-version.txt"
    meson setup build-perf-nightly --buildtype=release \
        -Dwirelog_log_max_level=error -Dtests=true -DmbedTLS=disabled \
        > "$artifact_mount/configure.log" 2>&1
    meson compile -C build-perf-nightly test_crdt_perf_gate test_cspa_perf_gate \
        > "$artifact_mount/build.log" 2>&1
    cooldown_seconds=0
    for _ in 1 2 3 4 5 6; do
        psi_avg10=$(sed -n 's/.*some avg10=\([0-9.]*\).*/\1/p' \
            /proc/pressure/cpu 2>/dev/null | head -n 1)
        if [[ $psi_avg10 =~ ^[0-9]+([.][0-9]+)?$ ]] \
            && awk -v psi="$psi_avg10" 'BEGIN { exit !(psi <= 1.0) }'; then
            break
        fi
        sleep 5
        cooldown_seconds=$((cooldown_seconds + 5))
    done
    echo "cooldown_seconds=$cooldown_seconds" > "$artifact_mount/cooldown.txt"
    scripts/ci/capture-nightly-perf-host.sh "$artifact_mount/gate-before.txt"
    set +e
    taskset -c 0 env WIRELOG_PERF_GATE=1 WIRELOG_PERF_REQUIRE=1 \
        meson test -C build-perf-nightly crdt_perf_gate cspa_w1_gate \
        --no-rebuild --print-errorlogs --num-processes 1 \
        > "$artifact_mount/meson-test.stdout" \
        2> "$artifact_mount/meson-test.stderr"
    test_rc=$?
    set -e
    echo "$test_rc" > "$artifact_mount/meson-test.exit"
    scripts/ci/capture-nightly-perf-host.sh "$artifact_mount/gate-after.txt"
    [[ -f build-perf-nightly/meson-logs/testlog.txt ]] && \
        cp build-perf-nightly/meson-logs/testlog.txt "$artifact_mount/testlog.txt"
    meson configure build-perf-nightly > "$artifact_mount/meson-config.txt"
    meson introspect build-perf-nightly --compilers > "$artifact_mount/compilers.json"
    sha256sum build-perf-nightly/tests/test_crdt_perf_gate \
        build-perf-nightly/tests/test_cspa_perf_gate \
        build-perf-nightly/meson-info/intro-buildoptions.json \
        bench/data/crdt/Insert_input.csv bench/data/crdt/Remove_input.csv \
        bench/data/cspa/assign.csv bench/data/cspa/dereference.csv \
        > "$artifact_mount/sha256.txt"
    set +e
    scripts/ci/check-nightly-crdt-cspa.sh "$artifact_mount/testlog.txt" \
        > "$artifact_mount/qualification-status.txt"
    qualification_rc=$?
    scripts/ci/check-nightly-host-telemetry.sh \
        "$artifact_mount/gate-before.txt" "$artifact_mount/gate-after.txt" \
        > "$artifact_mount/telemetry-status.txt"
    telemetry_rc=$?
    set -e
    if (( qualification_rc == 0 )); then
        acquisition_status=complete
        performance_verdict=$(awk -F= '$1 == "performance_verdict" { print $2 }' \
            "$artifact_mount/qualification-status.txt")
    else
        acquisition_status=incomplete
        performance_verdict=unknown
    fi
    evidence_eligibility=ineligible
    (( telemetry_rc == 0 )) && evidence_eligibility=eligible
    if { (( test_rc == 0 )) && [[ $performance_verdict != pass ]]; } \
        || { (( test_rc == 1 )) && [[ $performance_verdict != fail ]]; } \
        || (( test_rc > 1 )); then
        acquisition_status=incomplete
        evidence_eligibility=ineligible
    fi
    printf 'acquisition_status=%s\nevidence_eligibility=%s\nperformance_verdict=%s\nmeson_exit=%s\n' \
        "$acquisition_status" "$evidence_eligibility" "$performance_verdict" "$test_rc" \
        > "$artifact_mount/campaign-status.txt"

    if [[ ${PERF_STRICT_SMOKE:-false} == true ]]; then
        echo 'coverage_status=partial_smoke' >> "$artifact_mount/campaign-status.txt"
        echo 'scope=CRDT-CSPA-strict-only' > "$artifact_mount/legacy-coverage-status.txt"
        echo 'trace_test=not_run' >> "$artifact_mount/legacy-coverage-status.txt"
        echo 'portfolio=not_run' >> "$artifact_mount/legacy-coverage-status.txt"
        if (( qualification_rc == 0 && telemetry_rc == 0 && test_rc == 0 )) \
            && [[ $performance_verdict == pass && $evidence_eligibility == eligible ]]; then
            exit 0
        fi
        exit 1
    fi

    # Preserve the broader nightly workload separately from the strict ERROR
    # profile that owns CRDT/CSPA qualification.
    set +e
    meson setup build-perf-nightly-trace --buildtype=release \
        -Dwirelog_log_max_level=trace -Dtests=true -DmbedTLS=disabled \
        > "$artifact_mount/trace-configure.log" 2>&1
    trace_setup_rc=$?
    if (( trace_setup_rc == 0 )); then
        mapfile -t perf_targets < <(python scripts/ci/list_perf_suite_targets.py \
            build-perf-nightly-trace)
        if (( ${#perf_targets[@]} == 0 )); then
            trace_build_rc=1
            echo 'no perf suite build targets found' > "$artifact_mount/trace-build.log"
        else
            meson compile -C build-perf-nightly-trace "${perf_targets[@]}" \
                > "$artifact_mount/trace-build.log" 2>&1
            trace_build_rc=$?
        fi
    else
        trace_build_rc=1
    fi
    if (( trace_build_rc == 0 )); then
        env WIRELOG_PERF_GATE=1 WIRELOG_PERF_NIGHTLY=1 \
            meson test -C build-perf-nightly-trace --suite perf \
            --no-rebuild --print-errorlogs --num-processes 1 \
            > "$artifact_mount/trace-test.stdout" \
            2> "$artifact_mount/trace-test.stderr"
        trace_test_rc=$?
        [[ -f build-perf-nightly-trace/meson-logs/testlog.txt ]] && \
            cp build-perf-nightly-trace/meson-logs/testlog.txt \
                "$artifact_mount/trace-testlog.txt"
    else
        trace_test_rc=1
    fi
    mkdir -p "$artifact_mount/portfolio"
    if (( trace_build_rc == 0 )); then
        mkdir -p /workspace/perf-artifacts/portfolio
        python scripts/perf/run-flowlog-portfolio.py \
            --bench build-perf-nightly-trace/bench/bench_flowlog \
            --tier "${PORTFOLIO_TIER:-readme-full}" \
            --workers "${PORTFOLIO_WORKERS:-1}" \
            --repeat "${PORTFOLIO_REPEAT:-1}" \
            --out-dir /workspace/perf-artifacts/portfolio \
            > "$artifact_mount/portfolio.stdout" 2> "$artifact_mount/portfolio.stderr"
        portfolio_rc=$?
    else
        portfolio_rc=1
    fi
    set -e
    printf 'scope=full\ntrace_setup=%s\ntrace_build=%s\ntrace_test=%s\nportfolio=%s\n' \
        "$trace_setup_rc" "$trace_build_rc" "$trace_test_rc" "$portfolio_rc" \
        > "$artifact_mount/legacy-coverage-status.txt"
    if (( qualification_rc == 0 && telemetry_rc == 0 && test_rc == 0 \
          && trace_setup_rc == 0 && trace_build_rc == 0 \
          && trace_test_rc == 0 && portfolio_rc == 0 )) \
        && [[ $performance_verdict == pass && $evidence_eligibility == eligible ]]; then
        exit 0
    fi
    exit 1
fi

home_tmp=${HOME:?HOME must be set}/.tmp
mkdir -p "$home_tmp" "$artifact_dir"
export TMPDIR="$home_tmp"
if ! "$docker_bin" image inspect "$image" > "$artifact_dir/image-inspect.json"; then
    echo 'image_status=missing_locally' > "$artifact_dir/image-status.txt"
    echo 'performance_verdict=unknown' >> "$artifact_dir/image-status.txt"
    exit 1
fi
image_id=$(jq -r '.[0].Id // empty' "$artifact_dir/image-inspect.json")
[[ $image_id =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "invalid inspected Docker image ID: $image_id" >&2
    exit 1
}
printf 'tag=%s\nimage_id=%s\n' "$image" "$image_id" \
    > "$artifact_dir/image-identity.txt"
{
    echo "commit=$(git rev-parse HEAD)"
    echo "source_tree=$(git rev-parse HEAD^{tree})"
    echo "runner=${RUNNER_NAME:-unknown} os=${RUNNER_OS:-unknown} arch=${RUNNER_ARCH:-unknown}"
    echo 'runner_labels=self-hosted,Linux,X64,perf'
    echo "image_tag=$image image_id=$image_id pull=never"
    echo 'requested_affinity=0'
    taskset -pc $$
    echo "kernel=$(uname -srmo)"
    echo "cpu_model=$(awk -F: '/^model name[[:space:]]*:/ { sub(/^[[:space:]]+/, "", $2); print $2; exit }' /proc/cpuinfo)"
    echo "boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo unavailable)"
    echo "loadavg=$(cat /proc/loadavg 2>/dev/null || echo unavailable)"
    for file in /sys/fs/cgroup/cpuset.cpus.effective /sys/fs/cgroup/cpu.stat /proc/pressure/cpu; do
        echo "[$file]"
        cat "$file" 2>/dev/null || echo unavailable
    done
    for name in scaling_governor scaling_cur_freq scaling_min_freq scaling_max_freq cpuinfo_cur_freq; do
        file="/sys/devices/system/cpu/cpu0/cpufreq/$name"
        if [[ -r $file ]]; then echo "$name=$(cat "$file")"; else echo "$name=unavailable"; fi
    done
} > "$artifact_dir/host.txt"

"$docker_bin" run --pull=never --rm --user "$(id -u):$(id -g)" \
    -e HOME=/home/perf -e TMPDIR=/home/perf/tmp \
    -e PORTFOLIO_TIER="${PORTFOLIO_TIER:-readme-full}" \
    -e PORTFOLIO_WORKERS="${PORTFOLIO_WORKERS:-1}" \
    -e PORTFOLIO_REPEAT="${PORTFOLIO_REPEAT:-1}" \
    -e PERF_STRICT_SMOKE="${PERF_STRICT_SMOKE:-false}" \
    -v "$repo_root:/workspace" -v "$artifact_dir:/artifacts" \
    -v "$home_tmp:/home/perf" -w /workspace "$image_id" \
    bash scripts/ci/run-perf-nightly-linux.sh --inside-image /artifacts
