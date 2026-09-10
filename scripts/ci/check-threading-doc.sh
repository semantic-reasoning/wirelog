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
expected_rows="${WIRELOG_THREADING_EXPECTED_ROWS:-110}"
[ "$row_count" -eq "$expected_rows" ] || {
    echo "check-threading-doc: FAIL: expected $expected_rows audit rows, found $row_count" >&2
    exit 1
}

audit="$tmp_dir/audit"
# One awk pass replaces a per-row shell loop that ran awk, sed, wc, cut and grep
# for every audit row -- nine processes per row counting the command
# substitutions' own subshells, about 820 in all for the 91 rows.  Process
# creation is cheap on Linux and expensive on Windows, so that loop was ~85% of
# this gate's runtime and it grew with the audit inventory: the gate timed out
# at 30.09s against meson's 30s default on the Windows msvc runner (#1462).
# awk brings its own associative arrays, so the anchor and duplicate maps need
# no bash 4 `declare -A` and the bash 3.2 floor this script targets is
# unaffected.  The four checks below are emitted in the same order as the loop
# they replace and stop at the first offending row, so the diagnostic text and
# the exit behaviour are unchanged.  The order decides which diagnostic a row
# failing more than one check reports, not whether it fails; the duplicate
# check's position is pinned by scripts/ci/test-threading-doc.sh with two
# duplicate rows, one that is also invalid and one that is merely documented
# wrong.
#
# `FILENAME == ARGV[1]` rather than `NR == FNR`: an empty inventory would make
# the first row line look like an inventory line.  Comparing against ARGV[1]
# rather than a -v variable also avoids awk's escape processing on the assigned
# value, which would mangle a $TMPDIR containing backslashes.
#
# Diagnostics go to /dev/stderr.  gawk, mawk and BWK awk all special-case that
# path and write to the inherited fd 2 rather than opening the file, so the
# regular-file stderr redirection the selftest uses is appended to, not
# truncated.
awk -F '\t' '
FILENAME == ARGV[1] {
    count[$5]++
    resolved[$5] = $4
    site[$5] = $0
    next
}
{
    anchor = $1
    operation = $2
    if (operation !~ /^atomic_[A-Za-z0-9_]+$/) {
        printf("check-threading-doc: FAIL: %s has invalid operation \047%s\047\n",
            anchor, operation) > "/dev/stderr"
        exit 1
    }
    if (count[anchor] != 1) {
        printf("check-threading-doc: FAIL: %s does not resolve uniquely to %s\n",
            anchor, operation) > "/dev/stderr"
        exit 1
    }
    if (resolved[anchor] != operation) {
        printf("check-threading-doc: FAIL: %s resolves to %s, documented %s\n",
            anchor, resolved[anchor], operation) > "/dev/stderr"
        exit 1
    }
    if (anchor in seen) {
        printf("check-threading-doc: FAIL: duplicate audit anchor %s\n",
            anchor) > "/dev/stderr"
        exit 1
    }
    seen[anchor] = 1
    print site[anchor]
}' "$tmp_dir/inventory" "$rows" >"$audit" || exit 1

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
    # Return before iterating when nothing matched. On bash 3.2 -- which the
    # macOS CI runners ship, and this script is suite-registered with no
    # platform gate -- `for x in "${arr[@]}"` over an EMPTY array is an
    # unbound-variable error under `set -u`; bash 4.4 changed that, 3.2
    # predates it. Without this the script aborts with `candidates[@]: unbound
    # variable` instead of letting the caller report the citation that did not
    # resolve. `${#arr[@]}` on an assigned array is safe at 3.2, so the guard
    # itself is portable.
    [ "${#candidates[@]}" -gt 0 ] || return 1
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
    # Same bash 3.2 hazard as resolve_reference_source above: `read -ra` over an
    # empty string yields an assigned-empty array, and iterating one under
    # `set -u` aborts there. Unreachable today -- the grep below guarantees a
    # non-empty post-colon part -- but it is one pattern edit away from being
    # live, and the guard costs a line.
    [ "${#location_list[@]}" -gt 0 ] || {
        echo "check-threading-doc: FAIL: prose citation '$citation' has no locations" >&2
        exit 1
    }
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
