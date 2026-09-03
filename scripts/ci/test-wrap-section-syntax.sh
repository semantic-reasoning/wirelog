#!/usr/bin/env bash
# The two wrap-reading gates must agree on what a section header is (#1343).
#
# check-wrap-revisions.sh asserts the 40-hex shape; check-sbom-wrap-pin.sh
# asserts the snapshot and the wrap name the same commit and deliberately leans
# on the first for the shape. They disagreed:
#
#   [wrap-git]  # the pin
#
# is a legal header -- Python's configparser applies SECTCRE with .match, so
# text after `]` is ignored and meson builds that wrap. check-wrap-revisions.sh
# required `]` at end of line, never recognised the section, and reported
# "OK; release dependency wraps are reproducible" having checked nothing.
# check-sbom-wrap-pin.sh had the mirror-image bug and hard-failed with
# "no hex revision under [wrap-git]" when one was plainly there.
#
# One was a silent pass and one was a false failure, from the same disagreement.
# This file exists so they cannot drift apart again: it drives BOTH gates over
# the same header spellings and requires them to agree about which are legal.
set -euo pipefail

root=${1:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)}
failures=0
cases=0
check() {
    cases=$((cases + 1))
    if [ "$2" = 0 ]; then printf 'test-wrap-section-syntax: ok %s\n' "$1"
    else printf 'test-wrap-section-syntax: FAIL %s\n' "$1" >&2; failures=$((failures + 1)); fi
}
assert() { local n=$1 s=0; shift; "$@" || s=1; check "$n" "$s"; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-wrapsyntax.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# A fixture tree holding only what the two gates read, so neither is influenced
# by the real repository's contents.
# check-wrap-revisions.sh takes no root argument -- it derives repo_root from
# its own location -- so the gate has to be COPIED INTO the fixture tree or it
# silently checks the real repository and every assertion here passes for the
# wrong reason. That is exactly what the first version of this file did.
fixture() {
    local header=$1 revision=$2 dir="$tmp/f"
    rm -rf "$dir"; mkdir -p "$dir/subprojects" "$dir/sbom" "$dir/scripts/ci"
    cp "$root/scripts/ci/check-wrap-revisions.sh" "$dir/scripts/ci/"
    # [provide] carries its OWN revision, different from the real one. A gate
    # that stops anchoring to [wrap-git] -- or treats every section as
    # wrap-git -- then reads this decoy, which is the over-match
    # check-sbom-wrap-pin.sh's own comment says must not be possible. Without
    # it, widening that awk to `in_git = 1` passed every assertion here.
    printf '%s\n' "$header" \
        'url = https://github.com/apache/arrow-nanoarrow.git' \
        "revision = $revision" \
        '' '[provide]' \
        'revision = 00000000000000000000000000000000000000ff' \
        'nanoarrow = nanoarrow_dep' > "$dir/subprojects/nanoarrow.wrap"
    printf 'nanoarrow@0.9.0.9000:Apache\n# nanoarrow-resolved-sha: %s\n' \
        "${3:-$revision}" > "$dir/sbom/snapshot.txt"
    printf '%s' "$dir"
}

# No ${x@Q}: that is bash 4.4+, and the floor here is the macOS runner's 3.2.57,
# where it dies with "bad substitution". An earlier draft used it and ran ZERO
# assertions on 3.2.57 while still exiting 0. The exit status depends on where
# the expansion sits -- at top level or in a function under `set -e` it exits 1
# -- so do not read the clean exit as general; what is general is that the
# construct does not work at the floor and the failure was silent enough to
# reach review.
# Out of scope, stated so nobody adds a row for it: the `[ wrap-git ]` family
# -- inner spaces, `[wrap-gitx]`, `[WRAP-GIT]`, unclosed, `[]` -- still has the
# two gates disagreeing, check-wrap-revisions passing vacuously because its
# `case` compares the untrimmed ${BASH_REMATCH[1]}, and the pin gate hard-
# failing. meson rejects all of them, so no buildable wrap reaches it. A row
# here could only assert "at least one gate objects", which would pin the
# vacuous pass as correct and would have to be rewritten the day someone
# trims the captured name -- a real improvement a test should not obstruct.
GOOD=3f824063f59848e05692ab520de8ab4d9ebb1880

# Both gates must ACCEPT every legal header spelling. A gate that rejects one is
# a false failure; a gate that fails to find one is a vacuous pass. Requiring
# both to accept catches either.
for header in '[wrap-git]' '[wrap-git]  # the pin' '[wrap-git] ; note' '[wrap-git]	' '  [wrap-git]'; do
    d=$(fixture "$header" "$GOOD")
    accepts_shape() { "$1/scripts/ci/check-wrap-revisions.sh" >/dev/null 2>&1; }
    accepts_pin() { "$root/scripts/ci/check-sbom-wrap-pin.sh" "$1" >/dev/null 2>&1; }
    assert "check-wrap-revisions accepts <$header>" accepts_shape "$d"
    assert "check-sbom-wrap-pin accepts <$header>" accepts_pin "$d"
done

# And both must still REJECT a bad revision under every one of those headers --
# otherwise "accepts" could be satisfied by not looking.
for header in '[wrap-git]' '[wrap-git]  # the pin'; do
    d=$(fixture "$header" 'notahex')
    rejects_shape() { ! "$1/scripts/ci/check-wrap-revisions.sh" >/dev/null 2>&1; }
    assert "check-wrap-revisions rejects a non-hex revision under <$header>" \
        rejects_shape "$d"

    # A DIVERGENT BUT HEX sha, deliberately not reusing the notahex fixture:
    # there the snapshot line is also non-hex, so the pin gate fails on "no
    # nanoarrow-resolved-sha" rather than on the disagreement -- passing for the
    # wrong reason. Without this row the pin gate had accept-only coverage here
    # and a stub `exit 0` satisfied every assertion.
    d=$(fixture "$header" "$GOOD" 'ffffffffffffffffffffffffffffffffffffffff')
    rejects_pin() { ! "$root/scripts/ci/check-sbom-wrap-pin.sh" "$1" >/dev/null 2>&1; }
    assert "check-sbom-wrap-pin rejects a divergent sha under <$header>" \
        rejects_pin "$d"
done

# A floor, for the reason scripts/ci/test-gate-skip-exit-codes.sh states at its
# own: "nothing failed" and "nothing ran" are otherwise the same result. A loop
# that stops iterating -- a renamed fixture helper, an emptied header list --
# would print "all cases passed" and exit 0 having asserted nothing, which is
# this issue's defect one level up, inside this issue's own regression test.
if [ "$cases" -lt 14 ]; then
    printf 'test-wrap-section-syntax: FAIL only %d assertions ran; expected at least 14\n' \
        "$cases" >&2
    exit 1
fi

if ((failures)); then
    printf 'test-wrap-section-syntax: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-wrap-section-syntax: all %d cases passed\n' "$cases"
