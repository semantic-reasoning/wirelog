#!/usr/bin/env bash
# check-manifest-collation.sh - Regression for issue #1294.
#
# Two release gates hash a sorted file list and compare the result against a
# value that is pinned or committed:
#
#   scripts/ci/run-doop-perf-gate.sh     vs WIRELOG_DOOP_DATASET_MANIFEST_SHA256
#   scripts/release/run-downstream-matrix.sh  vs downstream-matrix-oracles.tsv
#
# The sorts were unpinned. glibc's en_US collation ignores the hyphen in
# Method-Modifier.facts, moving it after MethodHandleConstant.facts, so a
# runner with a non-C locale produced a different manifest and the gate failed
# with a message accusing the dataset of being substituted. Verified against
# the real zxing dataset: C order reproduces the committed oracle
# (215ddcc5...), en_US does not (472346fb...).
#
# Two halves, asserting different things:
#
#   1. The two manifest functions are lifted from run-downstream-matrix.sh with
#      sed and run over a fixture of the real discriminating filenames under
#      two locales. Because they are the real definitions, unpinning either one
#      in that script makes this half fail -- verified. A retyped copy would
#      instead assert that GNU sort honours an explicit prefix, which nothing
#      here can break.
#   2. check_pinned requires every sort in both scripts to carry the pin. That
#      is what guards run-doop-perf-gate.sh, whose manifest is an inline
#      expression and cannot be lifted.
set -euo pipefail

# Meson reads exit 77 as SKIP and exit 0 as a pass, so a branch that cannot
# check anything must exit 77, or this gate is recorded as having asserted
# something it never looked at.  Same defect class as #1301, in a gate added
# while fixing a different one.
SKIP_EXIT=77

# CDPATH= : a relative invocation makes cd consult CDPATH and print the
# resolved path, so these become two-line values and the script dies with a
# foreign error naming a path that does not exist (#1297).
script_dir="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
repo_root="$(CDPATH= cd -- "$script_dir/../.." && pwd)"

# Captured, not piped into grep -q: grep -q stops at the first match, locale -a
# takes SIGPIPE, and pipefail reports 141 even though the locale WAS found -- so
# this reads as "not installed" and the gate below exits 0 having asserted
# nothing. Reproduced at 80k locale names (#1297).
locales_available=$(locale -a 2>/dev/null || true)
locale_name=""
for candidate in en_US.utf8 en_US.UTF-8; do
    if grep -Fxq "$candidate" <<<"$locales_available"; then
        locale_name="$candidate"
        break
    fi
done

if [ -z "$locale_name" ]; then
    echo "check-manifest-collation: SKIP: en_US UTF-8 locale not installed"
    exit "$SKIP_EXIT"
fi

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/wirelog-manifest-collation.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

# The real names from the zxing DOOP dataset that C and en_US order
# differently. A fixture of alphabetic-only names would make this test pass
# whether or not the pins existed.
data="$tmpdir/data"
mkdir -p "$data"
for f in Method.facts Method-Modifier.facts MethodHandleConstant.facts \
         MethodTypeConstant.facts; do
    printf 'contents of %s\n' "$f" > "$data/$f"
done

# Sanity: the fixture must actually distinguish the two collations, or every
# assertion below holds vacuously.
c_order=$(find "$data" -maxdepth 1 -type f -name '*.facts' -print0 \
    | LC_ALL=C sort -z | tr '\0' '\n')
locale_order=$(find "$data" -maxdepth 1 -type f -name '*.facts' -print0 \
    | LC_ALL="$locale_name" sort -z | tr '\0' '\n')
if [ "$c_order" = "$locale_order" ]; then
    echo "check-manifest-collation: FAIL: fixture does not distinguish C from $locale_name" >&2
    exit 1
fi

# Lift manifest_for_doop out of the real script rather than retyping it: a
# retyped copy keeps asserting the old pipeline's property after the original
# changes. The script cannot be sourced whole -- it runs at top level under
# `set -euo pipefail` -- but the function body extracts cleanly, and
# check_pinned below fails loudly if the shape changes enough to break this.
downstream=$repo_root/scripts/release/run-downstream-matrix.sh
for fn in manifest_for_doop manifest_for_dir; do
    extracted=$(sed -n "/^$fn()/,/^}/p" "$downstream")
    # Syntax-check before eval: a corrupted range would otherwise die with a
    # raw `eval: syntax error` before declare -F could name what went wrong.
    printf '%s\n' "$extracted" | bash -n 2>/dev/null || {
        echo "check-manifest-collation: FAIL: extracted $fn from $downstream does not parse; the function's shape has changed" >&2
        exit 1
    }
    eval "$extracted"
    declare -F "$fn" >/dev/null || {
        echo "check-manifest-collation: FAIL: could not extract $fn from $downstream" >&2
        exit 1
    }
done

# run-doop-perf-gate.sh's dataset manifest was an inline expression when this
# was written, so it could not be lifted; #1297 made it the function
# doop_dataset_manifest, and scripts/ci/test-doop-manifest.sh now lifts and
# exercises it directly. check_pinned below still guards the collation pin at
# that site, which is what this file is for.
for fn in manifest_for_doop manifest_for_dir; do
    # "$BASH", not "$SHELL": $SHELL is the user's login shell, while these
    # functions use `read -r -d ''` and are printed by `declare -f`, both
    # bashisms. On a zsh or fish login this failed with a foreign error on a
    # correct tree -- a gate blaming the wrong thing, which is the defect class
    # this issue exists to remove. $BASH is the running interpreter.
    under_c=$(LC_ALL=C "$BASH" -c "$(declare -f "$fn"); $fn '$data'")
    under_locale=$(LC_ALL="$locale_name" "$BASH" -c "$(declare -f "$fn"); $fn '$data'")
    # The comparison below is satisfied by two empty strings. If the sed range
    # ever truncates at a column-0 `}` inside the body, the extraction still
    # parses and declare -F still finds it, but the function returns nothing --
    # so require a manifest-shaped result before believing the agreement.
    [[ "$under_c" =~ ^[0-9a-f]{64}$ ]] || {
        echo "check-manifest-collation: FAIL: extracted $fn produced no manifest; the extraction is not running the real body" >&2
        exit 1
    }
    if [ "$under_c" != "$under_locale" ]; then
        echo "check-manifest-collation: FAIL: $fn depends on the ambient locale" >&2
        echo "  C:              $under_c" >&2
        echo "  $locale_name: $under_locale" >&2
        exit 1
    fi
done

# The scripts must still carry the pin.
#
# Match `\bsort\b` rather than a specific flag spelling: `sort -z` and
# `sort --zero-terminated` are the same thing, and a plain unpinned `sort`
# added beside a pinned one would otherwise be invisible because the pinned
# one keeps the count non-zero. Whole-line comments are dropped after matching,
# not by an `^[^#]*` prefix, which cannot cross a `#` inside a quoted string.
sort_lines() {
    grep -n '\bsort\b' "$repo_root/$1" | grep -v '^[0-9]*:[[:space:]]*#' || true
}

check_pinned() {
    local target=$1 total
    total=$(sort_lines "$target" | grep -c . || true)
    if [ "$total" -eq 0 ]; then
        echo "check-manifest-collation: FAIL: no sort found in $target; the pipeline this guards has changed shape" >&2
        exit 1
    fi
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        echo "check-manifest-collation: FAIL: unpinned sort in $target: $line" >&2
        exit 1
    done < <(sort_lines "$target" | grep -v 'LC_ALL=C sort' || true)
}

check_pinned scripts/release/run-downstream-matrix.sh
check_pinned scripts/ci/run-doop-perf-gate.sh

echo "check-manifest-collation: OK under LC_ALL=$locale_name"
