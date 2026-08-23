#!/usr/bin/env bash
# check-threading-doc.sh - Issue #734 backstop.
#
# Asserts that the atomics audit table in `docs/THREADING.md` has one
# row for every `atomic_*` call site in `wirelog/` production sources
# (`.c` and `.h` under the project root, excluding MSVC shim
# definitions in `mem_ledger.h` and the macro-definition block at the
# top of `lockfree_queue.c`).
#
# A row is a line in `docs/THREADING.md` whose first cell starts with
# a backtick-quoted file:line reference under `wirelog/`.  Rows are
# pattern-matched as: `| \`file:line\` | ...`.
#
# Exit codes:
#   0 - row count and every cited source location match the code.
#   1 - mismatch (atomic_* added or removed; doc needs an audit
#       refresh), an unresolved citation, or other hard failure.
#
# Intended to register under `meson test --suite abi:threading_doc`.

set -euo pipefail

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="${WIRELOG_THREADING_DOC_ROOT:-$(cd "$script_dir/../.." && pwd)}"
doc="$repo_root/docs/THREADING.md"

if [ ! -f "$doc" ]; then
    echo "check-threading-doc: FAIL: docs/THREADING.md missing" >&2
    exit 1
fi

# Count atomic_* call sites in wirelog/ production sources.
# Restricted to .c and .h.  The MSVC shim block in mem_ledger.h
# (lines containing `#define atomic_`) is excluded; ditto the
# WL_ATOMIC_* macro definitions inside lockfree_queue.c (the
# definitions are at lines 22-37 and 47-57; the actual atomic_*
# tokens are inside the macro bodies and are the contract sites we
# do want to count as "atomic operations on the SPSC ring").
sites=$(grep -rEn '(^|[^[:alnum:]_])atomic_(load|store|fetch_add|fetch_sub|fetch_or|fetch_and|fetch_xor|compare_exchange_weak|compare_exchange_strong|exchange|init|thread_fence)(_explicit)?[[:space:]]*\(' \
        "$repo_root/wirelog" \
        --include='*.c' --include='*.h' \
    | grep -v '#define atomic_' \
    | grep -v '#define WL_ATOMIC' \
    | wc -l)

# Count audit-table rows in docs/THREADING.md.  Rows begin with
# `| \`wirelog/...` or `| \`mem_ledger.c:...` (file:line ref in the
# first table cell).  Pattern: a line starting with `| \`` followed
# by an identifier and a colon and a digit, ending the first cell
# with a backtick.
rows=$(grep -cE '^\| `[A-Za-z_./-]+:[0-9]+`' "$doc" || true)

# Return success only when the cited physical line and its backslash
# continuations contain an atomic_* call. Status 2 means the line is past EOF.
logical_line_has_atomic() {
    local source=$1
    local line=$2
    local operation=$3
    awk -v wanted="$line" -v operation="$operation" '
        NR == wanted {
            logical = $0
            while (logical ~ /\\$/ && getline > 0)
                logical = logical "\n" $0
            found = logical ~ ("(^|[^[:alnum:]_])" operation "[[:space:]]*\\(")
            seen = 1
            exit
        }
        END {
            if (!seen)
                exit 2
            if (!found)
                exit 1
        }
    ' "$source"
}

atomic_token_line() {
    local source=$1
    local line=$2
    local operation=$3
    awk -v wanted="$line" -v operation="$operation" '
        NR == wanted {
            physical = NR
            while (1) {
                if ($0 ~ ("(^|[^[:alnum:]_])" operation "[[:space:]]*\\(")) {
                    print physical
                    found = 1
                    exit
                }
                if ($0 !~ /\\$/ || getline <= 0)
                    exit
                physical++
            }
        }
        END {
            if (!found)
                exit 1
        }
    ' "$source"
}

# Resolve basename references used by the audit table. The document predates
# repo-relative citations, so candidates are selected by the cited logical
# line; a tie fails loudly rather than depending on `find` ordering.
resolve_source() {
    local reference=$1
    local operation=$2
    local basename=${reference%%:*}
    local line=${reference##*:}
    local -a candidates
    local -a matches=()
    local candidate
    candidates=()
    while IFS= read -r candidate; do
        candidates+=("$candidate")
    done < <(find "$repo_root/wirelog" -type f -name "$basename" -print | sort)
    if [ "${#candidates[@]}" -eq 0 ]; then
        echo "check-threading-doc: FAIL: $reference has no matching source; fix the audit row to name an existing wirelog file" >&2
        return 1
    fi
    for candidate in "${candidates[@]}"; do
        if logical_line_has_atomic "$candidate" "$line" "$operation"; then
            matches+=("$candidate")
        fi
    done
    if [ "${#matches[@]}" -eq 1 ]; then
        printf '%s\n' "${matches[0]}"
        return 0
    fi
    if [ "${#matches[@]}" -eq 0 ]; then
        echo "check-threading-doc: FAIL: $reference does not contain $operation on its logical line; fix the file:line or operation citation" >&2
    else
        printf 'check-threading-doc: FAIL: %s is ambiguous; matching candidates:\n' "$reference" >&2
        printf '  %s\n' "${matches[@]}" >&2
        echo "  Fix the audit row to use a uniquely identifying file:line citation." >&2
    fi
    return 1
}

# Check the complete logical preprocessor line, not only its first physical
# line. The intern.c audit deliberately cites macro definitions whose
# atomic_* token is on the continuation line.
check_citation() {
    local reference=$1
    local operation=$2
    local source
    source=$(resolve_source "$reference" "$operation") || return 1
    return 0
}

# Validate every audit-table citation after the count check. Keep the first
# cell deliberately narrow so prose references elsewhere do not become rows.
validated_rows=0
while IFS=$'\t' read -r reference operation; do
    if [[ ! "$operation" =~ ^atomic_[A-Za-z0-9_]+$ ]]; then
        echo "check-threading-doc: FAIL: $reference has invalid operation '$operation'; fix the audit row" >&2
        exit 1
    fi
    check_citation "$reference" "$operation" || exit 1
    validated_rows=$((validated_rows + 1))
done < <(sed -nE 's/^\| `([A-Za-z_./-]+:[0-9]+)` \| [^|]* \| `([^`]*)` \|.*/\1\t\2/p' "$doc")

if [ "$validated_rows" -ne "$rows" ]; then
    echo "check-threading-doc: FAIL: $rows audit rows found but only $validated_rows rows have a valid citation shape" >&2
    exit 1
fi

if [ "$sites" -ne "$rows" ]; then
    echo "check-threading-doc: FAIL:" >&2
    echo "  atomic_* call sites in wirelog/: $sites" >&2
    echo "  audit table rows in $doc: $rows" >&2
    echo "" >&2
    echo "Drift detected. Either update docs/THREADING.md §5 to add or remove the matching row." >&2
    exit 1
fi

# Compare the normalized source-site inventory with the audit references. The
# count check alone cannot detect a duplicated row that hides an omitted site.
source_sites="$tmp_dir/source-sites"
audit_sites="$tmp_dir/audit-sites"
grep -rEn '(^|[^[:alnum:]_])atomic_(load|store|fetch_add|fetch_sub|fetch_or|fetch_and|fetch_xor|compare_exchange_weak|compare_exchange_strong|exchange|init|thread_fence)(_explicit)?[[:space:]]*\(' \
        "$repo_root/wirelog" \
        --include='*.c' --include='*.h' \
    | grep -v '#define atomic_' \
    | grep -v '#define WL_ATOMIC' \
    | awk -F: '{print $1 ":" $2}' \
    | while IFS=: read -r source line; do
        file=$source
        file=${file##*/}
        printf '%s:%s\n' "$file" "$line"
    done \
    | sort > "$source_sites"
while IFS=$'\t' read -r reference operation; do
    source=$(resolve_source "$reference" "$operation") || exit 1
    token_line=$(atomic_token_line "$source" "${reference##*:}" "$operation") || exit 1
    file=${source##*/}
    printf '%s:%s\n' "$file" "$token_line"
done < <(sed -nE 's/^\| `([A-Za-z_./-]+:[0-9]+)` \| [^|]* \| `([^`]*)` \|.*/\1\t\2/p' "$doc") \
    | sort > "$audit_sites"
if ! cmp -s "$source_sites" "$audit_sites"; then
    echo "check-threading-doc: FAIL: audit citations do not cover the exact atomic-site inventory" >&2
    echo "  source-only or duplicate audit entries:" >&2
    comm -3 "$source_sites" "$audit_sites" >&2
    exit 1
fi

# Validate line and range references in prose as well as audit rows. Ranges
# are bounds checks only; the operation-specific check above remains the
# authoritative validation for table rows.
resolve_reference_source() {
    local basename=$1
    local start=$2
    local end=$3
    local -a candidates=()
    local -a matches=()
    local candidate line_count
    if [[ "$basename" == wirelog/* ]]; then
        candidates=("$repo_root/$basename")
    else
        while IFS= read -r candidate; do
            candidates+=("$candidate")
        done < <(find "$repo_root/wirelog" -type f -name "$basename" -print | sort)
    fi
    for candidate in "${candidates[@]}"; do
        [ -f "$candidate" ] || continue
        line_count=$(wc -l < "$candidate")
        if [ "$start" -ge 1 ] && [ "$end" -ge "$start" ] && [ "$end" -le "$line_count" ]; then
            matches+=("$candidate")
        fi
    done
    [ "${#matches[@]}" -eq 1 ]
}

while IFS= read -r citation; do
    citation=${citation#\`}
    citation=${citation%\`}
    basename=${citation%%:*}
    locations=${citation#*:}
    IFS=',' read -ra location_list <<< "$locations"
    for location in "${location_list[@]}"; do
        if [[ "$location" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            start=${BASH_REMATCH[1]}
            end=${BASH_REMATCH[2]}
        elif [[ "$location" =~ ^[0-9]+$ ]]; then
            start=$location
            end=$location
        else
            echo "check-threading-doc: FAIL: malformed prose citation '$citation'" >&2
            exit 1
        fi
        if ! resolve_reference_source "$basename" "$start" "$end"; then
            echo "check-threading-doc: FAIL: prose citation '$basename:$location' does not resolve to an in-range source" >&2
            exit 1
        fi
    done
done < <(grep -oE '`[A-Za-z0-9_./-]+\.(c|h):[0-9]+([,-][0-9]+)*`' "$doc" || true)

echo "check-threading-doc: OK; $sites atomic_* call sites and $rows resolved audit rows match in docs/THREADING.md"
exit 0
