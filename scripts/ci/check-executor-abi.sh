#!/usr/bin/env bash
# check-executor-abi.sh - verify the deliberate v0.30.0 executor break.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: check-executor-abi.sh <public-header> <shared-library> <report> [symbols-file]

The v0.30.0 executor policy is EXPECTED_UNSUPPORTED: every executor/result
function declared by the selected public header must be absent from the
selected shared library. The report is a stable, machine-readable TSV file.
EOF
}

(($# == 3 || $# == 4)) || { usage; exit 2; }
header=$1
library=$2
report=$3
symbols_file=${4:-}
[[ -r "$header" && -r "$library" ]] || {
    echo 'executor ABI check: header and library must be readable' >&2
    exit 2
}
command -v nm >/dev/null 2>&1 || {
    echo 'executor ABI check: nm is required' >&2
    exit 2
}

# The API sections are deliberately bounded: utility/parser names mentioned in
# comments must not become part of the executor compatibility contract.
declared=$(
    awk '
        /Executor API/ { in_executor = 1; next }
        /Result API/ { in_result = 1 }
        /Utility API/ { in_executor = 0; in_result = 0 }
        (in_executor || in_result) { print }
    ' "$header" |
    grep -oE 'wirelog_(executor_[A-Za-z0-9_]+|load_facts_from_csv|evaluate|result_[A-Za-z0-9_]+)\(' |
    sed 's/($//' | LC_ALL=C sort -u
)
[[ -n "$declared" ]] || {
    echo 'executor ABI check: no executor/result declarations found' >&2
    exit 1
}

exported=$(nm -D --defined-only "$library" |
    awk '{
        name = $NF
        sub(/@@?.*$/, "", name)
        if (name ~ /^wirelog_(executor_|load_facts_from_csv$|evaluate$|result_)/)
            print name
    }' |
    LC_ALL=C sort -u)

unexpected=$(comm -12 <(printf '%s\n' "$declared") <(printf '%s\n' "$exported"))
if [[ -n "$unexpected" ]]; then
    echo 'executor ABI check: unsupported baseline unexpectedly exports executor symbols:' >&2
    printf '  %s\n' $unexpected >&2
    exit 1
fi

mkdir -p "$(dirname "$report")"
{
    printf 'status\tEXPECTED_UNSUPPORTED\n'
    printf 'declared\t%s\n' "$(tr '\n' ',' <<<"$declared" | sed 's/,$//')"
    printf 'exported\t%s\n' "$(tr '\n' ',' <<<"$exported" | sed 's/,$//')"
    printf 'missing\t%s\n' "$(tr '\n' ',' <<<"$declared" | sed 's/,$//')"
} >"$report"

if [[ -n "$symbols_file" ]]; then
    mkdir -p "$(dirname "$symbols_file")"
    printf '%s\n' "$declared" >"$symbols_file"
fi
printf 'EXPECTED_UNSUPPORTED: %s declarations absent from %s\n' \
    "$(wc -l <<<"$declared")" "$library"
