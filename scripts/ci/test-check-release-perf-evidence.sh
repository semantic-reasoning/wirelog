#!/usr/bin/env bash
# Fixtures for check-release-perf-evidence.sh (#1359).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-release-perf-evidence.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

host="$tmp/host.txt"
cat > "$host" <<'EOF'
mode=github-hosted-advisory-timing-required-correctness
runner=Linux
image=ubuntu-latest
sha=0123456789012345678901234567890123456789
expected_sha=0123456789012345678901234567890123456789
cpus=4
memory_kb=16777216
affinity=0-3
EOF

write_log() {
    local file=$1 test_name=$2 result=$3 body=$4
    cat > "$file" <<EOF
==================================== 1/1 =====================================
test:         wirelog:$test_name
result:       exit status $result
------------------------------------ stdout -----------------------------------
$body
==============================================================================
EOF
}

write_log "$tmp/correctness.log" crdt_correctness_full 0 'test_crdt_perf_gate: correctness OK'
cat >> "$tmp/correctness.log" <<'EOF'
==================================== 2/3 =====================================
test:         wirelog:cspa_correctness
result:       exit status 0
------------------------------------ stdout -----------------------------------
test_cspa_perf_gate: correctness OK
==============================================================================
==================================== 3/3 =====================================
test:         wirelog:sub_ms_graph_correctness
result:       exit status 0
------------------------------------ stdout -----------------------------------
test_sub_ms_graph_perf_gate OK
==============================================================================
EOF
write_log "$tmp/rss.log" rss_bounded_release 0 'PASS (R=1000, ledger stable @ 0 bytes)'
write_log "$tmp/timing.log" log_perf_gate 77 'test_log_perf_gate: SKIP: hosted timing is advisory'
cat >> "$tmp/timing.log" <<'EOF'
==================================== 2/4 =====================================
test:         wirelog:crdt_perf_gate
result:       exit status 77
------------------------------------ stdout -----------------------------------
test_crdt_perf_gate: SKIP: hosted timing is advisory
==============================================================================
EOF
cat >> "$tmp/timing.log" <<'EOF'
==================================== 3/4 =====================================
test:         wirelog:cspa_w1_gate
result:       exit status 0
------------------------------------ stdout -----------------------------------
cspa timing OK
==============================================================================
EOF
cat >> "$tmp/timing.log" <<'EOF'
==================================== 4/4 =====================================
test:         wirelog:sub_ms_graph_perf_gate
result:       exit status 77
------------------------------------ stdout -----------------------------------
sub-ms timing SKIP: hosted timing is advisory
==============================================================================
EOF

gate="$root/scripts/ci/check-release-perf-evidence.sh"
"$gate" "$tmp/correctness.log" "$tmp/rss.log" "$tmp/timing.log" "$host"

# Neither edit uses `sed -i`: BSD sed requires a suffix argument for -i, so the
# GNU spelling makes macOS read the expression as the suffix and the filename as
# the script. `0,/re/` is a GNU address extension on top of that, which BSD sed
# rejects outright -- hence awk for the first-occurrence-only replacement.
awk '!done && sub(/exit status 0/, "exit status 1") { done = 1 } { print }' \
    "$tmp/timing.log" >"$tmp/timing.log.new"
mv "$tmp/timing.log.new" "$tmp/timing.log"
"$gate" "$tmp/correctness.log" "$tmp/rss.log" "$tmp/timing.log" "$host"

sed 's/result:       exit status 0/result:       exit status 77/' \
    "$tmp/rss.log" >"$tmp/rss.log.new"
mv "$tmp/rss.log.new" "$tmp/rss.log"
if "$gate" "$tmp/correctness.log" "$tmp/rss.log" "$tmp/timing.log" "$host"; then
    echo 'expected RSS failure was not detected' >&2
    exit 1
fi

echo 'check-release-perf-evidence fixtures: OK'
