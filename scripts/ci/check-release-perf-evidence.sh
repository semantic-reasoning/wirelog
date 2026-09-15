#!/usr/bin/env bash
# Validate the hosted release performance/RSS evidence contract (#1359).
#
# Hosted runners cannot promise a cpufreq governor, so timing blocks may be
# SKIP records and remain advisory. Correctness, RSS, and the host contract are
# release requirements and must never be absent, skipped, or unsuccessful.
set -euo pipefail

if (( $# != 4 )); then
    echo "usage: $0 CORRECTNESS_LOG RSS_LOG TIMING_LOG HOST_FILE" >&2
    exit 2
fi

correctness_log=$1
rss_log=$2
timing_log=$3
host_file=$4

fail() {
    echo "release performance evidence: FAIL: $*" >&2
    exit 1
}

for file in "$correctness_log" "$rss_log" "$timing_log" "$host_file"; do
    [[ -s "$file" ]] || fail "missing evidence file: $file"
done

grep -Fxq 'mode=github-hosted-advisory-timing-required-correctness' "$host_file" || \
    fail "host mode contract is missing or incorrect"
grep -Eq '^sha=[0-9a-f]{40}$' "$host_file" || \
    fail "host evidence does not identify an immutable tag SHA"
grep -Eq '^expected_sha=[0-9a-f]{40}$' "$host_file" || \
    fail "host evidence does not identify the expected tag SHA"
test "$(grep '^sha=' "$host_file" | cut -d= -f2)" = \
    "$(grep '^expected_sha=' "$host_file" | cut -d= -f2)" || \
    fail "checked-out and expected tag SHAs differ"
grep -Eq '^cpus=[0-9]+$' "$host_file" || fail "host CPU evidence is missing"
grep -Eq '^memory_kb=[0-9]+$' "$host_file" || fail "host memory evidence is missing"
grep -Eq '^affinity=.+$' "$host_file" || fail "host affinity evidence is missing"

block_for() {
    local log=$1 token=$2
    awk -v token="$token" '
        index($0, "test:") == 1 && index($0, token) > 0 {
            pos = index($0, token) + length(token)
            next_char = substr($0, pos, 1)
            if (next_char ~ /[[:alnum:]_]/)
                next
            if (found) exit
            found = 1
        }
        found { print }
        found && /^===================================/ { exit }
    ' "$log"
}

require_gate() {
    local log=$1 token=$2 label=$3 marker=$4 block count
    count=$(grep -Ec "^test:[[:space:]].*wirelog:${token}([[:space:]]|$)" "$log")
    [[ "$count" == 1 ]] || fail "$label must have exactly one test block (found $count)"
    block=$(block_for "$log" "wirelog:$token")
    [[ -n "$block" ]] || fail "$label block is missing"
    grep -Eq '^result:[[:space:]]+exit status 0$' <<<"$block" || \
        fail "$label did not pass"
    ! grep -Fq 'SKIP:' <<<"$block" || fail "$label was skipped"
    grep -Fq "$marker" <<<"$block" || fail "$label has no success evidence"
}

require_gate "$correctness_log" crdt_correctness_full CRDT-correctness \
    'test_crdt_perf_gate: correctness OK'
require_gate "$correctness_log" cspa_correctness CSPA-correctness \
    'test_cspa_perf_gate: correctness OK'
require_gate "$correctness_log" sub_ms_graph_correctness graph-correctness \
    'test_sub_ms_graph_perf_gate OK'
require_gate "$rss_log" rss_bounded_release RSS 'PASS (R='

require_advisory_gate() {
    local log=$1 token=$2 block count
    count=$(grep -Ec "^test:[[:space:]].*wirelog:${token}([[:space:]]|$)" "$log")
    [[ "$count" == 1 ]] || fail "timing gate $token must have exactly one test block (found $count)"
    block=$(block_for "$log" "wirelog:$token")
    [[ -n "$block" ]] || fail "timing evidence block is missing: $token"
    grep -Eq '^result:[[:space:]]+exit status [0-9]+$' <<<"$block" || \
        fail "timing evidence has an unexpected result: $token"
    if grep -Fq 'SKIP:' <<<"$block"; then
        echo "release performance evidence: advisory timing skipped: $token"
    elif ! grep -Eq '^result:[[:space:]]+exit status 0$' <<<"$block"; then
        echo "release performance evidence: advisory timing failed: $token" >&2
    else
        echo "release performance evidence: advisory timing passed: $token"
    fi
}

for token in log_perf_gate crdt_perf_gate cspa_w1_gate sub_ms_graph_perf_gate; do
    require_advisory_gate "$timing_log" "$token"
done

echo "release performance evidence: required correctness/RSS and hosted contract passed"
