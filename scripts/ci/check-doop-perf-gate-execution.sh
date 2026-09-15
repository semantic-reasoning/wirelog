#!/usr/bin/env bash
set -euo pipefail

if (( $# != 1 )); then
    echo "usage: $0 DOOP_TESTLOG" >&2
    exit 2
fi

log=$1
[[ -f $log ]] || {
    echo "DOOP execution check: missing log: $log" >&2
    exit 1
}

block=$(awk -v wanted="test:         perf - wirelog:doop_w8_gate" '
    $0 == wanted { capture = 1; count++; next }
    capture && /^===================================/ { capture = 0; closed = 1 }
    capture { print }
    END { if (count != 1 || closed != 1) exit 2 }
' "$log") || {
    echo "DOOP execution check: expected one doop_w8_gate block in $log" >&2
    exit 1
}

grep -Eq '^result:[[:space:]]+exit status 0$' <<<"$block" || {
    echo "DOOP execution check: doop_w8_gate did not pass" >&2
    exit 1
}
grep -Fq 'doop_w8_gate OK' <<<"$block" || {
    echo "DOOP execution check: missing DOOP success marker" >&2
    exit 1
}
! grep -Fq 'SKIP:' <<<"$block" || {
    echo "DOOP execution check: DOOP was skipped" >&2
    exit 1
}
grep -Eq 'workers=8([^[:alnum:]]|$)' <<<"$block" || {
    echo "DOOP execution check: result does not prove workers=8" >&2
    exit 1
}
grep -Eq 'repeat=5([^[:alnum:]]|$)' <<<"$block" || {
    echo "DOOP execution check: result does not prove repeat=5" >&2
    exit 1
}

echo "DOOP execution check: W=8/repeat=5 completed successfully"
