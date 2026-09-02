#!/usr/bin/env bash
# check-sbom-snapshot-locale.sh - Regression for issue #1291.
#
# The SBOM snapshot baseline is a committed artifact, so its order must not
# depend on the locale of whoever regenerates it. Under glibc's en_US
# collation, leading punctuation is ignored, so entries beginning "./" sort
# among the alphabetic names instead of before them -- regenerating produced
# spurious reordering hunks that buried the real dependency change in a gate
# whose whole purpose is making dependency drift reviewable.
#
# What this asserts: that every `sort` in the two SBOM scripts carries the
# LC_ALL=C pin, plus a sanity check that the fixture's two collations really
# disagree so the pin is not decorative. It is a string check, so it catches
# someone deleting the pin but NOT a pipeline rewritten to reorder after a
# correctly pinned sort.
#
# That second half is now covered behaviourally by scripts/ci/test-generate-sbom.sh,
# which runs the real generator into a temporary directory -- possible since
# #1293 gave it an output-directory parameter. Verified: a mutation inserting an
# unpinned `sort` after the pinned one passes this file and fails that one.
# Both are kept: this one guards check-sbom-snapshot.sh too, which that test
# does not run.
set -euo pipefail

# Meson reads exit 77 as SKIP and exit 0 as a pass, so a branch that cannot
# check anything must exit 77, or this gate is recorded as having asserted
# something it never looked at.  Same defect class as #1301, in a gate added
# while fixing a different one.
SKIP_EXIT=77

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

# Captured, not piped into grep -q: grep -q stops at the first match, locale -a
# takes SIGPIPE, and pipefail reports 141 even though the locale WAS found -- so
# this reads as "not installed" and the gate below exits 0 having asserted
# nothing. Not triggerable at locale -a's size, but it is the same shape that
# has bitten this repository repeatedly, and the here-string costs nothing.
locales_available=$(locale -a 2>/dev/null || true)
locale_name=""
for candidate in en_US.utf8 en_US.UTF-8; do
    if grep -Fxq "$candidate" <<<"$locales_available"; then
        locale_name="$candidate"
        break
    fi
done

if [ -z "$locale_name" ]; then
    echo "check-sbom-snapshot-locale: SKIP: en_US UTF-8 locale not installed"
    exit "$SKIP_EXIT"
fi

command -v jq >/dev/null 2>&1 || {
    echo "check-sbom-snapshot-locale: SKIP: jq not available"
    exit "$SKIP_EXIT"
}

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/wirelog-sbom-locale.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

# Entries chosen so C and en_US collation disagree: "./..." sorts first under
# C and mid-list under en_US, which is exactly the drift observed on #1291.
cat > "$tmpdir/artifacts.json" <<'EOF'
{"artifacts":[
  {"name":"zlib","version":"1.3","licenses":[{"value":"Zlib"}]},
  {"name":"./.github/workflows/lint-pr.yml","version":"UNKNOWN","licenses":[]},
  {"name":"actions/checkout","version":"v5","licenses":[]},
  {"name":"./.github/workflows/lint-main.yml","version":"UNKNOWN","licenses":[]},
  {"name":"r-lib/actions","version":"v2","licenses":[]}
]}
EOF

extract() {
    jq -r '.artifacts[] | "\(.name)@\(.version // "unknown"):\((.licenses // [{}])[0].value // "NOASSERTION")"' \
        "$tmpdir/artifacts.json"
}

# Reproduce the generator's pipeline tail under both collations, purely to
# confirm the fixture discriminates. A fixture whose entries sort identically
# either way would make the check below meaningless.
extract | LC_ALL=C sort > "$tmpdir/under-c.txt"
extract | LC_ALL="$locale_name" sort > "$tmpdir/under-locale-unpinned.txt"

# Sanity: the two collations must actually disagree on this fixture, or the
# test would pass regardless of whether the pin exists.
if cmp -s "$tmpdir/under-c.txt" "$tmpdir/under-locale-unpinned.txt"; then
    echo "check-sbom-snapshot-locale: FAIL: fixture does not distinguish collations under $locale_name" >&2
    exit 1
fi

# The scripts must actually carry the pin. This is the assertion that guards
# the repository; the comparison above only establishes that the fixture can
# tell the two collations apart.
# \bsort\b catches the forms a narrower pattern misses -- `sort -o`,
# `$(sort f)`, `< <(sort f)` -- and whole-line comments are dropped afterwards
# rather than by an `^[^#]*` prefix, which could not cross the `'^#'` literal
# in check-sbom-snapshot.sh's own comment-stripping line and so skipped the
# very sort it was meant to check. Trailing comments after code are still
# scanned; a false positive names its line and is the safe direction.
sort_lines() { grep -n '\bsort\b' "$repo_root/$1" | grep -v '^[0-9]*:[[:space:]]*#' || true; }

for target in scripts/release/generate-sbom.sh scripts/ci/check-sbom-snapshot.sh; do
    total=$(sort_lines "$target" | grep -c . || true)
    # Absence must not pass: a script with no `sort` at all would otherwise
    # satisfy an unpinned-count check by having nothing to count, and the pin
    # this guards would have gone with it.
    if [ "$total" -eq 0 ]; then
        echo "check-sbom-snapshot-locale: FAIL: no sort found in $target; the pipeline this guards has changed shape" >&2
        exit 1
    fi
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        echo "check-sbom-snapshot-locale: FAIL: unpinned sort in $target: $line" >&2
        exit 1
    done < <(sort_lines "$target" | grep -v 'LC_ALL=C sort' || true)
done

echo "check-sbom-snapshot-locale: OK under LC_ALL=$locale_name"
