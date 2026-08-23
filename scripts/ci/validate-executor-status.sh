#!/usr/bin/env bash
# validate-executor-status.sh - validate the accepted unsupported result.
set -euo pipefail

(($# == 7)) || {
    echo 'usage: validate-executor-status.sh STATUS LINK_LOG OLD_COMMIT CANDIDATE_COMMIT CANDIDATE_OUTPUT SYMBOLS ABI_REPORT' >&2
    exit 2
}
status=$1
link_log=$2
old_commit=$3
candidate_commit=$4
candidate_output=$5
symbols=$6
abi_report=$7
[[ -s "$status" && -s "$link_log" && -s "$candidate_output" && -s "$symbols" && -s "$abi_report" ]] || {
    echo 'executor status: evidence file is missing or empty' >&2
    exit 1
}
grep -Fq '"status":"EXPECTED_UNSUPPORTED"' "$status"
grep -Fq "\"old_commit\":\"$old_commit\"" "$status"
grep -Fq "\"candidate_commit\":\"$candidate_commit\"" "$status"
grep -Fq '"candidate_output":"reach-count 3"' "$status"
declared_csv=$(paste -sd, "$symbols")
grep -Fq $'declared\t'"$declared_csv" "$abi_report"
grep -Fq $'missing\t'"$declared_csv" "$abi_report"
missing_json=$(paste -sd, <(sed 's/^/"/; s/$/"/' "$symbols"))
grep -Fq '"missing_symbols":['"$missing_json"']' "$status"
digest=$(sha256sum "$link_log" | awk '{print $1}')
grep -Fq "\"linker_diagnostic_sha256\":\"$digest\"" "$status"
[[ "$(cat "$candidate_output")" == 'reach-count 3' ]] || {
    echo 'executor status: candidate output mismatch' >&2
    exit 1
}
while IFS= read -r symbol; do
    [[ -n "$symbol" ]] || continue
    grep -Fq "\"$symbol\"" "$status" || {
        echo "executor status: symbol missing from JSON: $symbol" >&2
        exit 1
    }
    grep -Fq "$symbol" "$link_log" || {
        echo "executor status: symbol missing from linker diagnostic: $symbol" >&2
        exit 1
    }
done <"$symbols"
