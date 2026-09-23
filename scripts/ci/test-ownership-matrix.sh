#!/usr/bin/env bash
# Self-test for check-ownership-matrix.sh (#1384).
#
# A gate is only worth its runtime if it fails when it should.  Every case
# below mutates a throwaway copy of the tree and asserts the gate REJECTS it;
# the final case asserts the unmutated tree passes, so a gate that failed
# everything unconditionally would be caught too.
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
gate="$script_dir/check-ownership-matrix.sh"
failures=0

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# A sandbox needs the doc, the gate, the resolver and enough of wirelog/ for
# the resolver to index; copy the real tree's sources rather than synthesising
# them, so the index the gate resolves against is the real one.
make_sandbox() {
    local dir="$work/$1"
    rm -rf "$dir"
    mkdir -p "$dir/docs" "$dir/scripts/ci"
    cp "$repo_root/docs/MEMORY.md" "$dir/docs/"
    cp "$gate" "$script_dir/ownership_doc_anchors.py" "$dir/scripts/ci/"
    cp -r "$repo_root/wirelog" "$dir/"
    printf '%s' "$dir"
}

run_gate() {
    WIRELOG_OWNERSHIP_DOC_ROOT="$1" \
        WIRELOG_OWNERSHIP_EXPECTED_AUTHOR_READ="${2:-1}" \
        WIRELOG_OWNERSHIP_EXPECTED_REVIEWER_READ="${3:-15}" \
        bash "$1/scripts/ci/check-ownership-matrix.sh" >"$work/out" 2>"$work/err"
}

expect_failure() {
    local name="$1" dir="$2" needle="$3"
    if run_gate "$dir" "${4:-1}" "${5:-15}"; then
        echo "FAIL: $name: gate accepted a tree it should have rejected" >&2
        failures=$((failures + 1))
        return
    fi
    if ! grep -qF "$needle" "$work/err"; then
        echo "FAIL: $name: rejected, but not for the stated reason" >&2
        echo "  wanted: $needle" >&2
        echo "  got:    $(head -1 "$work/err")" >&2
        failures=$((failures + 1))
        return
    fi
    echo "ok: $name"
}

expect_success() {
    local name="$1" dir="$2"
    if run_gate "$dir"; then
        echo "ok: $name"
    else
        echo "FAIL: $name: gate rejected an unmutated tree" >&2
        sed 's/^/  /' "$work/err" >&2
        failures=$((failures + 1))
    fi
}

rewrite_file() {
    local target="$1"
    shift
    local tmp="$target.tmp"
    sed "$@" "$target" >"$tmp"
    mv "$tmp" "$target"
}

# 1. An anchor naming a symbol that does not exist.
dir=$(make_sandbox unresolvable)
rewrite_file "$dir/docs/MEMORY.md" 's/`relation.c:col_rel_destroy`/`relation.c:col_rel_destroy_nonexistent`/'
expect_failure "unresolvable anchor" "$dir" "does not resolve to a definition"

# 2. An anchor whose symbol has two definitions in one file.  thread_msvc.c
#    defines wl_cond_init twice under conditional compilation, so the index
#    genuinely cannot say which one a row means.
dir=$(make_sandbox ambiguous)
rewrite_file "$dir/docs/MEMORY.md" 's/`relation.c:col_rel_destroy`/`thread_msvc.c:wl_cond_init`/'
expect_failure "ambiguous anchor" "$dir" "it is ambiguous"

# 3. An anchor naming a file that contributes no definitions.
dir=$(make_sandbox missing_file)
rewrite_file "$dir/docs/MEMORY.md" 's/`relation.c:col_rel_destroy`/`no_such_file.c:col_rel_destroy`/'
expect_failure "missing cited file" "$dir" "which has no indexed definitions"

# 4. A deleted row.  The check is an exact pin, not a floor: a row silently
#    ADDED is the same governance failure as one removed.  This is also
#    the case that stops the gate from passing on a regex that matches nothing.
dir=$(make_sandbox below_floor)
rewrite_file "$dir/docs/MEMORY.md" '/^| Session relation storage |/d'
expect_failure "row count off its pin" "$dir" "matrix rows, found"

# 5. The heading renamed, which is how the section silently disappears.
dir=$(make_sandbox no_heading)
rewrite_file "$dir/docs/MEMORY.md" 's/^### Pin acquisition and release sites$/### Pin sites/'
expect_failure "missing heading" "$dir" "heading '### Pin acquisition and release sites' missing"

# 6. A column dropped from the table header.
dir=$(make_sandbox no_column)
rewrite_file "$dir/docs/MEMORY.md" 's/| Last reader | Invalidation |/| Invalidation |/'
expect_failure "missing column header" "$dir" "column header 'Last reader' missing"

# 7. A prose citation pointing past the end of its file.
dir=$(make_sandbox bad_prose)
awk '/^### Pin acquisition and release sites$/ { print; print ""; print "Bogus citation `relation.c:99999999`."; next } { print }' \
    "$dir/docs/MEMORY.md" >"$dir/docs/MEMORY.md.tmp"
mv "$dir/docs/MEMORY.md.tmp" "$dir/docs/MEMORY.md"
expect_failure "out-of-bounds prose citation" "$dir" "is out of bounds"

# 8. A symbol that exists and resolves uniquely but has no caller under
#    wirelog/.  col_arrangement_probe_bundle_acquire is real, is defined in
#    arrangement.c and is called only from tests/test_arrangement_probe.c; it
#    reached the matrix twice and was caught both times only by a human reading
#    join.c.  This case is that finding promoted to a check.
dir=$(make_sandbox test_only_symbol)
rewrite_file "$dir/docs/MEMORY.md" 's/`arrangement.c:col_session_pin_diff_arrangement`/`arrangement.c:col_arrangement_probe_bundle_acquire`/'
expect_failure "symbol with no production caller" "$dir" "have no caller under wirelog/"

# 9. A newly author-read row cannot silently become reviewer-read. The
#    reviewer pin is set to the resulting count so this case reaches the
#    independent author-state pin.
dir=$(make_sandbox author_state_pin)
awk '!done && /^\| Timestamp sub-resource / && sub(/\| author-read \|$/, "| reviewer-read |") { done = 1 } { print }' \
    "$dir/docs/MEMORY.md" >"$dir/docs/MEMORY.md.tmp"
mv "$dir/docs/MEMORY.md.tmp" "$dir/docs/MEMORY.md"
expect_failure "author-read count off its pin" "$dir" "rows marked author-read, found 0" 1 16

# 10. A reviewer-read row cannot silently become author-read. The author pin
#    is set to the resulting count so this case reaches the reviewer-state pin.
dir=$(make_sandbox reviewer_state_pin)
awk '!done && sub(/\| reviewer-read \|$/, "| author-read |") { done = 1 } { print }' \
    "$dir/docs/MEMORY.md" >"$dir/docs/MEMORY.md.tmp"
mv "$dir/docs/MEMORY.md.tmp" "$dir/docs/MEMORY.md"
expect_failure "reviewer-read count off its pin" "$dir" "rows marked reviewer-read, found 14" 2 15

# 11. Control: the unmutated tree must pass, or every case above proves nothing.
dir=$(make_sandbox control)
expect_success "control, unmutated tree" "$dir"

if [ "$failures" -ne 0 ]; then
    echo "test-ownership-matrix: $failures case(s) failed" >&2
    exit 1
fi
echo "test-ownership-matrix: OK; 10 rejection cases and 1 control"
