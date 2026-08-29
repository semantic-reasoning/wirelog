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
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
[[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
    echo "phase timeout must be a positive integer: $timeout_seconds" >&2
    exit 2
}
windows_shell=0
case "${OSTYPE:-}" in
    msys*|cygwin*) windows_shell=1 ;;
esac

if (( windows_shell )); then
    command -v pwsh.exe >/dev/null 2>&1 || {
        echo 'pwsh.exe (PowerShell 7) is required on Windows' >&2
        exit 2
    }
    windows_launcher=$script_dir/run-downstream-phase-windows.ps1
    [[ -f "$windows_launcher" ]] || {
        echo "Windows launcher is missing: $windows_launcher" >&2
        exit 2
    }
    windows_launcher_path=$(cygpath -w -- "$windows_launcher")
    process_group_runner=windows
elif command -v setsid >/dev/null 2>&1; then
    process_group_runner=setsid
else
    command -v perl >/dev/null 2>&1 || {
        echo 'setsid or perl is required' >&2
        exit 2
    }
    process_group_runner=perl
fi

local_status=FAIL
return_code=0
if [[ "$process_group_runner" == setsid ]]; then
    setsid --wait "$@" >"$log" 2>&1 &
elif [[ "$process_group_runner" == windows ]]; then
    # Let MSYS convert phase paths for native Windows commands. The launcher
    # path is converted explicitly above because this invocation is otherwise
    # intentionally passed through without special argument rewriting.
    pwsh.exe -NoProfile -NonInteractive \
        -File "$windows_launcher_path" "$log" "$timeout_seconds" "$@" >"$log" 2>&1 &
else
    perl -MPOSIX -e 'setpgid(0, 0) or die "$!\n"; exec @ARGV' -- "$@" >"$log" 2>&1 &
fi
child_pid=$!

deadline=$((SECONDS + timeout_seconds))
timed_out=0
while kill -0 "$child_pid" 2>/dev/null; do
    child_state=$(ps -o stat= -p "$child_pid" 2>/dev/null || true)
    [[ "$child_state" == Z* ]] && break
    if (( SECONDS >= deadline )); then
        timed_out=1
        if (( ! windows_shell )); then
            kill -TERM -- "-$child_pid" 2>/dev/null || true
        fi
        for _ in 1 2 3 4 5; do
            kill -0 "$child_pid" 2>/dev/null || break
            sleep 1
        done
        if (( ! windows_shell )); then
            kill -KILL -- "-$child_pid" 2>/dev/null || true
        else
            # pwsh is a native process under MSYS; terminate the launcher
            # itself if its own cleanup did not return within the grace time.
            kill -KILL "$child_pid" 2>/dev/null || true
        fi
        break
    fi
    sleep 0.1
done
wait "$child_pid" || return_code=$?
if (( ! windows_shell )) && kill -0 -- "-$child_pid" 2>/dev/null; then
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
