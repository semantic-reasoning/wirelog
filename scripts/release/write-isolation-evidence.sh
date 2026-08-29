#!/usr/bin/env bash
# Write one escaped, machine-readable runner-isolation evidence row.
set -euo pipefail

[[ $# -eq 10 ]] || {
    echo 'usage: write-isolation-evidence.sh OUTPUT CANDIDATE WORKFLOW RUN STARTED RUNNER GROUP CONTROL ASSERTION SOURCE' >&2
    exit 2
}
output=$1 candidate_sha=$2 workflow=$3 run_id=$4 started_at=$5 runner_name=$6
runner_group=$7 control=$8 assertion=$9 source=${10}

tsv_escape() {
    local value=$1
    value=${value//$'\\'/\\\\}
    value=${value//$'\t'/\\t}
    value=${value//$'\r'/\\r}
    value=${value//$'\n'/\\n}
    printf '%s' "$value"
}

escaped=()
for value in "$candidate_sha" "$workflow" "$run_id" "$started_at" "$runner_name" \
    "$runner_group" "$control" "$assertion" "$source"; do
    escaped+=("$(tsv_escape "$value")")
done
if [[ "$runner_group" == wirelog-ga && "$control" != unverified \
    && "$assertion" == dedicated-runner-group ]]; then
    result=PASS
else
    result=UNVERIFIED
fi
printf 'schema\tcandidate_sha\tworkflow\trun_id\tstarted_at\trunner_name\trunner_group\tdeclared_labels\tisolation_control\tisolation_assertion\tassertion_result\tisolation_source\tconcurrency_group\tcleanup_policy\n' > "$output"
printf '1\t%s\t%s\t%s\t%s\t%s\t%s\tself-hosted,linux,x64,wirelog-ga\t%s\t%s\t%s\t%s\twirelog-ga-downstream\tpost-upload-remove-workspace\n' \
    "${escaped[0]}" "${escaped[1]}" "${escaped[2]}" "${escaped[3]}" "${escaped[4]}" \
    "${escaped[5]}" "${escaped[6]}" "${escaped[7]}" "$result" "${escaped[8]}" >> "$output"
awk -F '\t' 'NR == 1 { if (NF != 14) bad = 1; next } { if (NF != 14) bad = 1; rows++ } END { if (rows != 1) bad = 1; exit bad + 0 }' "$output" >/dev/null || {
    echo 'isolation evidence has an invalid TSV row' >&2
    exit 1
}
