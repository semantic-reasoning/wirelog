#!/usr/bin/env bash
# Verify the symbol-anchored ownership matrix in docs/MEMORY.md (#1384).
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="${WIRELOG_OWNERSHIP_DOC_ROOT:-$(cd "$script_dir/../.." && pwd)}"
doc="$repo_root/docs/MEMORY.md"
helper="$script_dir/ownership_doc_anchors.py"
heading='### Pin acquisition and release sites'
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

[ -f "$doc" ] || { echo "check-ownership-matrix: FAIL: docs/MEMORY.md missing" >&2; exit 1; }

# Structural assertions.  A row-extraction regex that matches nothing is
# indistinguishable from a document with nothing to match, so the shape of the
# section is asserted before its contents (#1464's vacuity lesson).
grep -qF "$heading" "$doc" || {
    echo "check-ownership-matrix: FAIL: heading '$heading' missing from docs/MEMORY.md" >&2
    exit 1
}

# The subsection runs from its heading to the next heading of any level.
section="$tmp_dir/section"
awk -v h="$heading" '
    index($0, h) == 1 { inside = 1; next }
    inside && /^#{2,4} / { exit }
    inside { print }
' "$doc" >"$section"

for column in Class Owner Acquire "Last reader" Invalidation Destruction Rebuildable Read; do
    grep -qF "| $column |" "$section" || {
        echo "check-ownership-matrix: FAIL: column header '$column' missing from the matrix" >&2
        exit 1
    }
done

rows="$tmp_dir/rows"
# Only the first contiguous table after the heading.  The subsection ends at
# the next heading, but §10 continues with further prose and tables, so a
# heading-to-heading window would sweep those in and the count would police the
# wrong rows.
awk '
    /^\|/ { started = 1; print; next }
    started { exit }
' "$section" | grep -vE '^\|[ -]*-[ -|-]*\|' | grep -vF '| Class |' >"$rows" || true
row_count=$(wc -l <"$rows")
expected_rows="${WIRELOG_OWNERSHIP_EXPECTED_ROWS:-16}"
[ "$row_count" -eq "$expected_rows" ] || {
    echo "check-ownership-matrix: FAIL: expected exactly $expected_rows matrix rows, found $row_count" >&2
    exit 1
}

read_rows=$(grep -cE '\| read \|$' "$rows" || true)
expected_read="${WIRELOG_OWNERSHIP_EXPECTED_READ:-16}"
[ "$read_rows" -eq "$expected_read" ] || {
    echo "check-ownership-matrix: FAIL: expected exactly $expected_read rows marked read, found $read_rows" >&2
    exit 1
}

if command -v uv >/dev/null 2>&1; then
    uv run python "$helper" "$repo_root" --dump >"$tmp_dir/inventory"
elif command -v python3 >/dev/null 2>&1 && python3 -c 'import sys' >/dev/null 2>&1; then
    python3 "$helper" "$repo_root" --dump >"$tmp_dir/inventory"
elif command -v python >/dev/null 2>&1 && python -c 'import sys' >/dev/null 2>&1; then
    python "$helper" "$repo_root" --dump >"$tmp_dir/inventory"
else
    echo "check-ownership-matrix: FAIL: python interpreter not found" >&2
    exit 1
fi

anchors="$tmp_dir/anchors"
grep -oE '`[A-Za-z0-9_.-]+\.(c|h):[A-Za-z_][A-Za-z0-9_]*`' "$section" \
    | tr -d '`' | sort -u >"$anchors" || true
anchor_count=$(wc -l <"$anchors")
[ "$anchor_count" -gt 0 ] || {
    echo "check-ownership-matrix: FAIL: the section cites no symbol anchors; the section or the extraction pattern has changed shape" >&2
    exit 1
}

# One awk pass over inventory + anchors, per the #1462 structure: a per-anchor
# shell loop spawning python or grep is what timed this gate's sibling out at
# 30.09s on the Windows runner.  A duplicate *definition* (two bodies for one
# symbol in one file, as thread_msvc.c has under conditional compilation) makes
# an anchor ambiguous and is refused.  Two rows citing the same symbol is NOT
# refused: one destruction point legitimately serves several classes.
awk -F '\t' '
    FILENAME == ARGV[1] {
        sub(/\r$/, "")
        gsub(/\\/, "/", $1)
        # The index carries repo-relative paths because basenames are not
        # unique under wirelog/ (session.c exists twice).  Anchors stay
        # basename-scoped to match the existing table convention, so key on the
        # basename and let the ambiguity check below catch a real collision.
        n = split($1, part, "/")
        base = part[n]
        key = base ":" $3
        seen[key]++
        where[key] = where[key] (seen[key] > 1 ? ", " : "") $1 ":" $2
        file[base] = 1
        next
    }
    {
        sub(/\r$/, "")
        split($0, cite, ":")
        if (!(cite[1] in file)) {
            printf "check-ownership-matrix: FAIL: %s cites file %s, which has no indexed definitions\n", $0, cite[1] > "/dev/stderr"
            bad = 1
            next
        }
        if (seen[$0] == 0) {
            printf "check-ownership-matrix: FAIL: anchor %s does not resolve to a definition\n", $0 > "/dev/stderr"
            bad = 1
            next
        }
        if (seen[$0] > 1) {
            printf "check-ownership-matrix: FAIL: anchor %s resolves to %d definitions (%s); it is ambiguous\n", $0, seen[$0], where[$0] > "/dev/stderr"
            bad = 1
        }
    }
    END { exit bad ? 1 : 0 }
' "$tmp_dir/inventory" "$anchors"

# Every cited symbol must have at least one caller inside wirelog/, i.e. be
# reachable from production and not only from tests.  This catches the one
# error class that got past two review rounds: a row anchored on a function
# that reads like the right API but has only test callers, so the row describes
# a path production never takes.  It caught
# col_arrangement_probe_bundle_acquire (tests/test_arrangement_probe.c only)
# and would have caught it without a human reading join.c.  A definition alone
# does not satisfy it; the symbol must appear somewhere other than its own
# definition site.
# Cost note: this is the one per-anchor shell loop left in this file, two
# full-tree greps per anchor, so it is the growth term.  At 37 anchors the gate
# runs ~2 s against a 120 s timeout.  The awk pass above exists because a
# per-row loop timed the sibling threading gate out at 30.09 s on the Windows
# runner (#1462), where process creation is far more expensive; if this table
# roughly triples, collapse these greps into one alternation pass before that
# margin matters.
uncalled=""
while IFS= read -r anchor; do
    symbol=${anchor#*:}
    # `|| true` on both: grep exits 1 when it matches nothing, and under
    # `set -euo pipefail` that would abort the gate with no diagnostic at all.
    uses=$({ grep -rhoE "\b$symbol\b" "$repo_root/wirelog" --include='*.c' --include='*.h' || true; } | wc -l)
    # `.h` occurrences are netted out entirely, so the test is "more than one
    # occurrence in .c files": definition plus at least one other mention.
    # It is a heuristic and fails open, not closed.  A definition paired with a
    # forward declaration in a .c, with a mention in its own doc comment, with
    # a same-named static in another file, or with only an #ifdef-gated test
    # wrapper as its caller all pass: the count is global rather than scoped
    # to the anchor's file, and nothing here evaluates the preprocessor.
    # wl_columnar_eval_retire_prior_deltas has such a wrapper, reachable only
    # under WL_TEST_BDX_SEED.  Its row is sound because a production call
    # exists as well, but the wrapper alone would have satisfied this check.
    # No line number is cited for it on purpose: the bounds check below scans
    # only docs/MEMORY.md, so a line citation in this comment would rot
    # unchecked.
    # There is no false-fail shape here: a genuinely called symbol always has
    # a definition plus a call, and the only construct that could hide a call
    # from grep is token pasting, which `grep -rn '##'` finds nowhere under
    # wirelog/.
    decls=$({ grep -rhoE "\b$symbol\b" "$repo_root/wirelog" --include='*.h' || true; } | wc -l)
    [ "$((uses - decls))" -gt 1 ] || uncalled="$uncalled $anchor"
done <"$anchors"
[ -z "$uncalled" ] || {
    echo "check-ownership-matrix: FAIL: cited symbols have no caller under wirelog/, so the row documents a path production does not take:$uncalled" >&2
    exit 1
}

# Prose citations of the form file.c:NNN or file.c:NNN-MMM are permitted only
# where a contract lives in a comment rather than a callable.  Bounds-check
# them the way check-threading-doc.sh does.
while IFS= read -r citation; do
    citation=${citation//\`/}
    basename=${citation%%:*}
    locations=${citation#*:}
    # No `| head -1`: under pipefail a SIGPIPEd find aborts the gate with no
    # message, and picking one of several same-named files would bounds-check
    # the citation against the wrong one.  session.c exists twice under
    # wirelog/, so an ambiguous basename is refused rather than guessed.
    # `while read` rather than mapfile: this file targets bash 3.2, which macOS
    # still ships and which has no mapfile (the bash-construct ratchet enforces
    # it).
    source_files=()
    while IFS= read -r candidate; do
        source_files+=("$candidate")
    done < <(find "$repo_root/wirelog" -name "$basename" -type f)
    [ "${#source_files[@]}" -gt 0 ] || {
        echo "check-ownership-matrix: FAIL: prose citation '$citation' names no file under wirelog/" >&2
        exit 1
    }
    [ "${#source_files[@]}" -eq 1 ] || {
        echo "check-ownership-matrix: FAIL: prose citation '$citation' is ambiguous; ${#source_files[@]} files share that name" >&2
        exit 1
    }
    source_file="${source_files[0]}"
    total=$(wc -l <"$source_file")
    IFS=',' read -ra location_list <<<"$locations"
    for location in "${location_list[@]}"; do
        if [[ "$location" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            start=${BASH_REMATCH[1]}; end=${BASH_REMATCH[2]}
        elif [[ "$location" =~ ^[0-9]+$ ]]; then
            start=$location; end=$location
        else
            echo "check-ownership-matrix: FAIL: malformed prose citation '$citation'" >&2
            exit 1
        fi
        if [ "$start" -lt 1 ] || [ "$end" -gt "$total" ] || [ "$start" -gt "$end" ]; then
            echo "check-ownership-matrix: FAIL: prose citation '$basename:$location' is out of bounds (1-$total)" >&2
            exit 1
        fi
    done
done < <(grep -oE '`[A-Za-z0-9_.-]+\.(c|h):[0-9]+([,-][0-9]+)*`' "$section" || true)

echo "check-ownership-matrix: OK; $row_count matrix rows, $anchor_count symbol anchors in the section all resolve uniquely"
