#!/usr/bin/env bash
# Run one bounded downstream-matrix phase and append its result to a TSV.
set -euo pipefail

if [[ $# -lt 6 || "$5" != -- ]]; then
    echo 'usage: run-downstream-phase.sh PHASE TIMEOUT LOG PHASES_TSV -- COMMAND [ARG...]' >&2
    exit 2
fi
phase=$1
timeout_seconds=$2
log=$3
phases_tsv=$4
shift 5
[[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
    echo "phase timeout must be a positive integer: $timeout_seconds" >&2
    exit 2
}
command -v setsid >/dev/null || { echo 'setsid is required' >&2; exit 2; }

local_status=FAIL
return_code=0
setsid --wait "$@" >"$log" 2>&1 &
child_pid=$!
deadline=$((SECONDS + timeout_seconds))
timed_out=0
while kill -0 "$child_pid" 2>/dev/null; do
    child_state=$(ps -o stat= -p "$child_pid" 2>/dev/null || true)
    [[ "$child_state" == Z* ]] && break
    if (( SECONDS >= deadline )); then
        timed_out=1
        kill -TERM -- "-$child_pid" 2>/dev/null || true
        for _ in 1 2 3 4 5; do
            kill -0 "$child_pid" 2>/dev/null || break
            sleep 1
        done
        kill -KILL -- "-$child_pid" 2>/dev/null || true
        break
    fi
    sleep 0.1
done
wait "$child_pid" || return_code=$?
if kill -0 -- "-$child_pid" 2>/dev/null; then
    kill -TERM -- "-$child_pid" 2>/dev/null || true
    sleep 0.1
    kill -KILL -- "-$child_pid" 2>/dev/null || true
fi
if [[ "$timed_out" == 1 ]]; then
    return_code=124
    local_status=TIMEOUT
elif [[ "$return_code" == 0 ]]; then
    local_status=PASS
fi
printf '%s\t%s\t%s\t%s\t%s\n' "$phase" "$local_status" "$timeout_seconds" "$return_code" "$(basename "$log")" >> "$phases_tsv"
if [[ "$local_status" != PASS ]]; then
    echo "$phase failed; see $log" >&2
    exit "$return_code"
fi
