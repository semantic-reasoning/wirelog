#!/usr/bin/env bash
# Verify the symbol-anchored atomics audit and preserve its exact inventory.
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="${WIRELOG_THREADING_DOC_ROOT:-$(cd "$script_dir/../.." && pwd)}"
doc="$repo_root/docs/THREADING.md"
helper="$script_dir/threading_doc_anchors.py"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

[ -f "$doc" ] || { echo "check-threading-doc: FAIL: docs/THREADING.md missing" >&2; exit 1; }
if command -v uv >/dev/null 2>&1; then
    uv run python "$helper" "$repo_root" --dump >"$tmp_dir/inventory"
else
    python3 "$helper" "$repo_root" --dump >"$tmp_dir/inventory"
fi

rows="$tmp_dir/rows"
sed -nE 's/^\| `([^`]+:[A-Za-z_][A-Za-z0-9_]*(#[0-9]+)?)` \| [^|]* \| `([^`]*)` \|.*/\1\t\3/p' "$doc" >"$rows"
row_count=$(wc -l <"$rows")
expected_rows="${WIRELOG_THREADING_EXPECTED_ROWS:-44}"
[ "$row_count" -eq "$expected_rows" ] || {
    echo "check-threading-doc: FAIL: expected $expected_rows audit rows, found $row_count" >&2
    exit 1
}

audit="$tmp_dir/audit"
: >"$tmp_dir/keys"
: >"$audit"
while IFS=$'\t' read -r anchor operation; do
    [[ "$operation" =~ ^atomic_[A-Za-z0-9_]+$ ]] || {
        echo "check-threading-doc: FAIL: $anchor has invalid operation '$operation'" >&2
        exit 1
    }
    matches=$(awk -F '\t' -v anchor="$anchor" -v operation="$operation" \
        '$5 == anchor && $4 == operation' "$tmp_dir/inventory")
    count=$(printf '%s\n' "$matches" | sed '/^$/d' | wc -l)
    [ "$count" -eq 1 ] || {
        echo "check-threading-doc: FAIL: $anchor does not resolve uniquely to $operation" >&2
        exit 1
    }
    key="$anchor:$operation"
    if grep -Fqx "$key" "$tmp_dir/keys"; then
        echo "check-threading-doc: FAIL: duplicate audit anchor $key" >&2
        exit 1
    fi
    printf '%s\n' "$key" >>"$tmp_dir/keys"
    printf '%s\n' "$matches" >>"$audit"
done <"$rows"

cut -f1,2,4 "$tmp_dir/inventory" | sort >"$tmp_dir/source-sites"
cut -f1,2,4 "$audit" | sort >"$tmp_dir/audit-sites"
if ! cmp -s "$tmp_dir/source-sites" "$tmp_dir/audit-sites"; then
    echo "check-threading-doc: FAIL: audit citations do not cover the exact atomic-site inventory" >&2
    comm -3 "$tmp_dir/source-sites" "$tmp_dir/audit-sites" >&2
    exit 1
fi

resolve_reference_source() {
    local basename=$1 start=$2 end=$3 candidate line_count
    local -a candidates=() matches=()
    if [[ "$basename" == wirelog/* ]]; then
        candidates=("$repo_root/$basename")
    else
        while IFS= read -r candidate; do candidates+=("$candidate"); done \
            < <(find "$repo_root/wirelog" -type f -name "$basename" -print | sort)
    fi
    for candidate in "${candidates[@]}"; do
        [ -f "$candidate" ] || continue
        line_count=$(wc -l <"$candidate")
        if [ "$start" -ge 1 ] && [ "$end" -ge "$start" ] \
            && [ "$end" -le "$line_count" ]; then
            matches+=("$candidate")
        fi
    done
    [ "${#matches[@]}" -eq 1 ]
}

while IFS= read -r citation; do
    citation=${citation#\`}; citation=${citation%\`}
    basename=${citation%%:*}; locations=${citation#*:}
    IFS=',' read -ra location_list <<<"$locations"
    for location in "${location_list[@]}"; do
        if [[ "$location" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            start=${BASH_REMATCH[1]}; end=${BASH_REMATCH[2]}
        elif [[ "$location" =~ ^[0-9]+$ ]]; then
            start=$location; end=$location
        else
            echo "check-threading-doc: FAIL: malformed prose citation '$citation'" >&2
            exit 1
        fi
        if ! resolve_reference_source "$basename" "$start" "$end"; then
            echo "check-threading-doc: FAIL: prose citation '$basename:$location' does not resolve" >&2
            exit 1
        fi
    done
done < <(grep -oE '`[A-Za-z0-9_./-]+\.(c|h):[0-9]+([,-][0-9]+)*`' "$doc" || true)

echo "check-threading-doc: OK; $row_count audit rows cover the exact atomic-site inventory"
