#!/usr/bin/env bash
set -euo pipefail
controlled_root="$HOME/.cache/wirelog-1913-controlled-69111650"
for block in base-1 candidate-1 candidate-2 base-2; do
    variant=${block%-*}
    cd "$controlled_root/$variant"
    artifact="$controlled_root/timings/$block"
    mkdir -p "$artifact"
    git rev-parse HEAD > "$artifact/source-head.txt"
    git status --porcelain --untracked-files=no > "$artifact/source-dirty.txt"
    sha256sum wirelog/columnar/relation.c tests/test_memory_admission_relation.c > "$artifact/measured-source.sha256"
    cp "$controlled_root/predeclared-plan.json" "$artifact/predeclared-plan.json"
    date -u +%FT%TZ > "$artifact/started-utc.txt"
    set +e
    PERF_ARTIFACT_DIR="$artifact" PERF_STRICT_SMOKE=true RUNNER_NAME=atlas-perf RUNNER_OS=Linux RUNNER_ARCH=X64 scripts/ci/run-perf-nightly-linux.sh > "$artifact/wrapper.stdout" 2> "$artifact/wrapper.stderr"
    wrapper_rc=$?
    set -e
    printf '%s\n' "$wrapper_rc" > "$artifact/wrapper.exit"
    date -u +%FT%TZ > "$artifact/finished-utc.txt"
    sha256sum -c "$artifact/measured-source.sha256" > "$artifact/source-integrity-after.txt"
    printf '%s: wrapper_exit=%s\n' "$block" "$wrapper_rc"
    if [[ -f "$artifact/campaign-status.txt" ]]; then cat "$artifact/campaign-status.txt"; fi
 done
