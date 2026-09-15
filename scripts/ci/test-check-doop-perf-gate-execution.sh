#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-doop-execution.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

block_start='test:         perf - wirelog:doop_w8_gate'
block_end='============================================='
write_log() {
    local path=$1 status=$2 marker=$3
    {
        printf '%s\n' "$block_start"
        printf '%s\n' "$marker"
        printf 'result:             exit status %s\n' "$status"
        printf '%s\n' "$block_end"
    } > "$path"
}

write_log "$tmp/good.log" 0 \
    'doop_w8_gate OK: tuples=13828835 iterations=153 workers=8 repeat=5'
"$script_dir/check-doop-perf-gate-execution.sh" "$tmp/good.log"

write_log "$tmp/skipped.log" 77 \
    'doop_w8_gate: SKIP: no calibrated 5-repetition W=8 target workers=8 repeat=5'
if "$script_dir/check-doop-perf-gate-execution.sh" "$tmp/skipped.log"; then
    echo 'skipped DOOP fixture unexpectedly passed' >&2
    exit 1
fi

write_log "$tmp/wrong-workers.log" 0 \
    'doop_w8_gate OK: tuples=13828835 iterations=153 workers=1 repeat=5'
if "$script_dir/check-doop-perf-gate-execution.sh" "$tmp/wrong-workers.log"; then
    echo 'wrong-worker fixture unexpectedly passed' >&2
    exit 1
fi

if "$script_dir/check-doop-perf-gate-execution.sh" "$tmp/missing.log"; then
    echo 'missing-log fixture unexpectedly passed' >&2
    exit 1
fi

echo 'check-doop-perf-gate-execution fixtures: OK'
