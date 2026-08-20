#!/usr/bin/env bash
set -euo pipefail

if (( $# != 2 )); then
    echo "usage: $0 TRACE_TESTLOG ERROR_TESTLOG" >&2
    exit 2
fi

trace_log=$1
error_log=$2

check_gate() {
    local log=$1
    local test_name=$2
    local marker=$3
    local block

    [[ -f $log ]] || {
        echo "perf gate execution check: missing log: $log" >&2
        return 1
    }

    block=$(awk -v wanted="test:         perf - wirelog:${test_name}" '
        $0 == wanted { capture = 1; count++; next }
        capture && /^===================================/ { capture = 0; closed = 1 }
        capture { print }
        END { if (count != 1 || closed != 1) exit 2 }
    ' "$log") || {
        echo "perf gate execution check: expected one test block for ${test_name} in ${log}" >&2
        return 1
    }

    grep -Eq '^result:[[:space:]]+exit status 0$' <<<"$block" || {
        echo "perf gate execution check: ${test_name} did not pass in ${log}" >&2
        return 1
    }
    grep -Fq "$marker" <<<"$block" || {
        echo "perf gate execution check: ${test_name} has no timing success marker in ${log}" >&2
        return 1
    }
    ! grep -Fq 'SKIP:' <<<"$block" || {
        echo "perf gate execution check: ${test_name} was skipped in ${log}" >&2
        return 1
    }
}

# log_perf_gate is intentionally isolated in the trace-ceiling build.
check_gate "$trace_log" log_perf_gate 'test_log_perf_gate OK'

# The three evaluator gates are intentionally isolated in the stripped build.
check_gate "$error_log" crdt_perf_gate 'test_crdt_perf_gate OK'
check_gate "$error_log" cspa_w1_gate 'test_cspa_perf_gate OK'
check_gate "$error_log" sub_ms_graph_perf_gate 'test_sub_ms_graph_perf_gate OK'

echo "perf gate execution check: all four timing gates executed successfully"
