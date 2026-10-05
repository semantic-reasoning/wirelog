#!/usr/bin/env bash
set -euo pipefail

image=semantic-reasoning:ubuntu26
docker_bin=${DOCKER_BIN:-docker}
repo_root=$(git rev-parse --show-toplevel)
artifacts="$repo_root/perf-artifacts/doop"
mkdir -p "$artifacts"

if [[ ${1:-} == --inside-image ]]; then
    mkdir -p "$HOME/.tmp"
    export TMPDIR="$HOME/.tmp"
    export UV_CACHE_DIR="$TMPDIR/uv-cache"
    python_env="$TMPDIR/meson-venv"
    uv venv --python python3 "$python_env"
    uv pip install --python "$python_env/bin/python" 'meson==1.12.0' ninja
    export PATH="$python_env/bin:$PATH"
    rm -rf build-perf-error
    bench/data/doop/download.sh > /artifacts/doop-download.log 2>&1
    meson setup build-perf-error --buildtype=release \
        -Dwirelog_log_max_level=error -Dtests=true -DmbedTLS=disabled \
        > /artifacts/configure.log 2>&1
    meson compile -C build-perf-error test_crdt_perf_gate test_cspa_perf_gate \
        test_cspa_correctness bench_flowlog \
        > /artifacts/build.log 2>&1
    gcc --version > /artifacts/gcc-version.txt
    meson --version > /artifacts/meson-version.txt
    ninja --version > /artifacts/ninja-version.txt
    uv --version > /artifacts/uv-version.txt
    meson configure build-perf-error > /artifacts/meson-config.txt
    meson introspect build-perf-error --compilers > /artifacts/compilers.json

    set +e
    env WIRELOG_PERF_GATE=1 meson test -C build-perf-error \
        crdt_correctness_full cspa_correctness --no-rebuild \
        --print-errorlogs --num-processes 1 \
        > /artifacts/correctness.stdout 2> /artifacts/correctness.stderr
    correctness_rc=$?
    affinity=$(taskset -pc $$ | sed 's/.*: //')
    taskset -c "$affinity" env WIRELOG_PERF_GATE=1 WIRELOG_PERF_REQUIRE=1 \
        WIRELOG_DOOP_PERF_MODE=strict-tagged \
        WL_DOOP_PERF_GATE_TARGET_MS="${WL_DOOP_PERF_GATE_TARGET_MS:-}" \
        meson test -C build-perf-error doop_w8_gate --logbase doop-testlog \
        --no-rebuild --print-errorlogs --num-processes 1 \
        > /artifacts/doop.stdout 2> /artifacts/doop.stderr
    doop_rc=$?
    set -e
    [[ -f build-perf-error/meson-logs/testlog.txt ]] && \
        cp build-perf-error/meson-logs/testlog.txt /artifacts/testlog.txt
    [[ -f build-perf-error/meson-logs/doop-testlog.txt ]] && \
        cp build-perf-error/meson-logs/doop-testlog.txt /artifacts/doop-testlog.txt
    if [[ -f build-perf-error/meson-logs/doop-testlog.txt ]]; then
        cat build-perf-error/meson-logs/doop-testlog.txt >> build-perf-error/meson-logs/testlog.txt
    fi
    set +e
    scripts/ci/check-doop-perf-gate-execution.sh \
        build-perf-error/meson-logs/doop-testlog.txt \
        > /artifacts/doop-check.txt 2>&1
    check_rc=$?
    set -e
    if (( doop_rc != 0 )); then
        exit "$doop_rc"
    fi
    if (( check_rc != 0 )); then exit "$check_rc"; fi
    exit "$correctness_rc"
fi

home_tmp="$HOME/.tmp"
mkdir -p "$home_tmp"
if ! "$docker_bin" image inspect "$image" > "$artifacts/image-inspect.json"; then
    echo 'image_status=missing_locally' > "$artifacts/image-status.txt"
    exit 1
fi
image_id=$(jq -r '.[0].Id // empty' "$artifacts/image-inspect.json")
[[ $image_id =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "invalid inspected Docker image ID: $image_id" >&2
    exit 1
}
printf 'tag=%s\nimage_id=%s\n' "$image" "$image_id" \
    > "$artifacts/image-identity.txt"
"$docker_bin" run --pull=never --rm --user "$(id -u):$(id -g)" \
    -e HOME=/home/perf -e TMPDIR=/home/perf/.tmp \
    -e WL_DOOP_PERF_GATE_TARGET_MS="${WL_DOOP_PERF_GATE_TARGET_MS:-}" \
    -v "$repo_root:/workspace" -v "$artifacts:/artifacts" \
    -v "$home_tmp:/home/perf" -w /workspace "$image_id" \
    bash scripts/ci/run-perf-stable-linux.sh --inside-image
