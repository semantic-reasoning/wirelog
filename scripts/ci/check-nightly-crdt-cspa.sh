#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
    echo "usage: $0 MESON_TESTLOG" >&2
    exit 2
fi

log=$1
[[ -f $log ]] || { echo "missing Meson testlog: $log" >&2; exit 1; }

check_gate() {
    local name=$1 marker=$2 block
    block=$(awk -v wanted="test:         perf - wirelog:${name}" '
        $0 == wanted { capture = 1; count++; next }
        capture && /^===================================/ { capture = 0; closed = 1 }
        capture { print }
        END { if (count != 1 || closed != 1) exit 2 }
    ' "$log") || {
        echo "missing or duplicate perf test block: $name" >&2
        return 1
    }
    if grep -Eq '^result:[[:space:]]+exit status 0$' <<<"$block" \
        && grep -Fq "$marker" <<<"$block"; then
        printf '%s=pass\n' "$name"
    elif grep -Eq '^result:[[:space:]]+exit status 1$' <<<"$block" \
        && grep -Eq 'FAIL: median [0-9]+([.][0-9]+)? ms exceeds target [0-9]+ ms \(regression\)' <<<"$block"; then
        printf '%s=target_miss\n' "$name"
    else
        echo "$name did not produce a pass or eligible target-miss result" >&2
        return 1
    fi
    ! grep -Fq 'SKIP:' <<<"$block" || {
        echo "$name was skipped" >&2; return 1;
    }
    grep -Eq 'raw_ms =([[:space:]]+[0-9]+([.][0-9]+)?){9}[[:space:]]*$' <<<"$block" || {
        echo "$name does not contain exactly nine raw samples" >&2; return 1;
    }
    grep -Eq 'median_ms[[:space:]]*=' <<<"$block" || {
        echo "$name has no median" >&2; return 1;
    }
    grep -Eq 'CoV [0-9]+([.][0-9]+)?%' <<<"$block" || {
        echo "$name has no CoV" >&2; return 1;
    }
    cov=$(grep -Eo 'CoV [0-9]+([.][0-9]+)?%' <<<"$block" | tail -n 1 | awk '{print $2}' | tr -d '%')
    awk -v cov="$cov" 'BEGIN { if (cov + 0 > 3.0) exit 1 }' || {
        echo "$name CoV exceeds the 3% eligibility limit" >&2; return 1;
    }
    if [[ $name == crdt_perf_gate ]]; then
        grep -Eq 'result[[:space:]]*=[[:space:]]*104851 \(expected 104851\)' <<<"$block" || {
            echo "$name correctness count is invalid" >&2; return 1;
        }
    else
        grep -Eq 'tuples[[:space:]]*=[[:space:]]*20381/20,381' <<<"$block" || {
            echo "$name correctness count is invalid" >&2; return 1;
        }
        grep -Eq 'iterations[[:space:]]*=[[:space:]]*6/6' <<<"$block" || {
            echo "$name iteration count is invalid" >&2; return 1;
        }
    fi
}

crdt=$(check_gate crdt_perf_gate 'test_crdt_perf_gate OK')
cspa=$(check_gate cspa_w1_gate 'test_cspa_perf_gate OK')
echo 'acquisition_status=complete'
if [[ $crdt == *target_miss* || $cspa == *target_miss* ]]; then
    echo 'performance_verdict=fail'
else
    echo 'performance_verdict=pass'
fi
printf '%s\n%s\n' "$crdt" "$cspa"
