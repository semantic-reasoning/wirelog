#!/usr/bin/env bash
# Static and negative fixtures for the GA downstream matrix contract.
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=${1:-$(cd "$script_dir/../.." && pwd)}
oracle="$root/scripts/release/downstream-matrix-oracles.tsv"
runner="$root/scripts/release/run-downstream-matrix.sh"
download="$root/bench/data/doop/download.sh"

bash -n "$runner" "$download"
"$runner" --help >/dev/null 2>&1
set +e
"$runner" >/dev/null 2>&1
missing_args_rc=$?
set -e
test "$missing_args_rc" = 2

count=$(awk -F '\t' '!/^#/ && NF { count++ } END { print count + 0 }' "$oracle")
test "$count" = 6
awk -F '\t' '!/^#/ && NF != 5 { bad++ } END { exit bad + 0 }' "$oracle"
expected='cspa-fast galen polonius ddisasm crdt doop'
actual=$(awk -F '\t' '!/^#/ && NF { print $1 }' "$oracle" | paste -sd ' ' -)
test "$actual" = "$expected"
grep -Fq '154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7' "$oracle"
grep -Fq 'MATRIX_REPEAT=1' "$runner"
grep -Fq -- '--repeat "$MATRIX_REPEAT"' "$runner"

set +e
DOOP_ZXING_URL=file:///etc/hosts "$download" >/tmp/wirelog-doop-checksum-negative.log 2>&1
mismatch_rc=$?
set -e
test "$mismatch_rc" -ne 0
grep -Fq 'checksum mismatch' /tmp/wirelog-doop-checksum-negative.log

echo 'downstream matrix contract fixtures: OK'
