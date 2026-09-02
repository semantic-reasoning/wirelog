#!/usr/bin/env bash
# Self-test for the DOOP dataset manifest computations.
#
# Issue #1297. run-doop-perf-gate.sh hashed `sha256sum "$data_dir"/*.facts`
# without stripping the directory, so the pinned manifest depended on the path
# the operator happened to pass. Relative, absolute and trailing-slash spellings
# of one directory produced three different hashes, and the mismatch is reported
# as "DOOP dataset manifest ... != pinned ...", which reads as dataset
# tampering. #1294 removed the locale cause of that same message; this is the
# second, independent cause.
#
# The assertion that matters is not path-invariance on its own -- it is that the
# perf gate and run-downstream-matrix.sh's manifest_for_doop agree. Both compare
# against pinned values, both call the result a "dataset manifest", and they
# have now drifted twice: once on collation (#1294), once on path handling
# (this issue). Asserting they produce the same bytes is what stops a third.
#
# Both functions are lifted from the real scripts with sed rather than retyped:
# a retyped copy keeps asserting the old pipeline's behaviour after the original
# changes.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-doop-manifest: SKIP: needs a POSIX host"; exit 77 ;;
esac
command -v sha256sum >/dev/null 2>&1 || {
    echo "test-doop-manifest: SKIP: sha256sum not available"; exit 77
}

# CDPATH= on every cd here for the same reason the primitive needs it: dirname
# of a relative invocation is relative, so cd consults CDPATH and prints the
# resolved path, giving a two-line $root and a foreign "can't read" error.
root=$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# Absolutised once, rather than hardening every cd below: a relative TMPDIR
# makes $tmp relative, and then each `cd "$tmp/..."` consults CDPATH too. One
# absolute value removes the class instead of the instances.
tmp=$(CDPATH= cd -- "$(mktemp -d "${TMPDIR:-/tmp}/wirelog-doop-manifest.XXXXXX")" && pwd)
trap 'rm -rf "$tmp"' EXIT

failures=0
check() {
    local name=$1 ok=$2
    if [[ "$ok" == 0 ]]; then
        printf 'test-doop-manifest: ok %s\n' "$name"
    else
        printf 'test-doop-manifest: FAIL %s\n' "$name" >&2
        failures=$((failures + 1))
    fi
}
assert_eq() {
    local name=$1 a=$2 b=$3
    if [[ "$a" == "$b" ]]; then
        check "$name" 0
    else
        printf 'test-doop-manifest: FAIL %s\n      %s\n      %s\n' "$name" "$a" "$b" >&2
        failures=$((failures + 1))
    fi
}
assert_ne() {
    local name=$1 a=$2 b=$3
    if [[ "$a" != "$b" ]]; then check "$name" 0; else
        printf 'test-doop-manifest: FAIL %s (both %s)\n' "$name" "$a" >&2
        failures=$((failures + 1))
    fi
}

# Lift both real implementations. A hash-shaped result is required before
# believing any comparison: two empty strings compare equal, so a sed range that
# silently truncated would otherwise report agreement.
lift() {
    local fn=$1 file=$2 extracted
    extracted=$(sed -n "/^$fn()/,/^}/p" "$file")
    printf '%s\n' "$extracted" | bash -n 2>/dev/null || {
        echo "test-doop-manifest: FAIL: extracted $fn from $file does not parse" >&2
        exit 1
    }
    eval "$extracted"
    declare -F "$fn" >/dev/null || {
        echo "test-doop-manifest: FAIL: could not extract $fn from $file" >&2
        exit 1
    }
}
lift manifest_for_doop "$root/scripts/release/run-downstream-matrix.sh"
lift doop_dataset_manifest "$root/scripts/ci/run-doop-perf-gate.sh"

# The real zxing basenames that C and en_US collation order differently, so the
# fixture also exercises the #1294 property rather than passing vacuously.
data="$tmp/doop"
mkdir -p "$data"
# -x.facts is option-shaped, so the `--` in the primitive becomes an assertion
# rather than a comment: without it sha256sum exits "invalid option", and
# `set -e` with pipefail aborts this file at the m_abs assignment below -- the
# shape guard is never reached, so it is the errexit that kills the mutation,
# not the guard. Putting it in a fixture whose assertion compares two spellings
# would NOT work: both sides break together and two empty strings compare equal.
for f in Method.facts Method-Modifier.facts MethodHandleConstant.facts \
         MethodTypeConstant.facts -x.facts; do
    printf 'contents of %s\n' "$f" > "$data/$f"
done

rel_parent=$(CDPATH= cd -- "$tmp" && pwd)
m_abs=$(doop_dataset_manifest "$data")
m_rel=$(CDPATH= cd -- "$rel_parent" && doop_dataset_manifest doop)
m_slash=$(doop_dataset_manifest "$data/")

[[ "$m_abs" =~ ^[0-9a-f]{64}$ ]] || {
    echo "test-doop-manifest: FAIL: lifted doop_dataset_manifest produced no hash" >&2
    exit 1
}

assert_eq 'an absolute and a relative path give the same manifest' "$m_abs" "$m_rel"
assert_eq 'a trailing slash gives the same manifest' "$m_abs" "$m_slash"

# A directory whose name contains a backslash: GNU sha256sum escapes such lines
# by prefixing the whole line, which an after-the-fact prefix strip could not
# remove -- so two spellings of one directory still disagreed. cd'ing in never
# embeds the path at all.
# The backslash goes in the PARENT, with a plainly-named leaf. Putting it in the
# leaf made this assertion vacuous: both spellings then contained the backslash,
# GNU sha256sum escaped both identically, and the two hashes matched under the
# BROKEN implementation as readily as the fixed one -- verified by reverting to
# the sed body, which passed 9/9. The defect needs the backslash in a component
# one spelling includes and the other omits.
bs_parent="$tmp/bs/back\\slash"; bs="$bs_parent/doop"
mkdir -p "$bs"
for f in Method.facts Method-Modifier.facts MethodHandleConstant.facts \
         MethodTypeConstant.facts; do
    printf 'contents of %s\n' "$f" > "$bs/$f"
done
assert_eq 'a backslash in the directory name does not change the manifest' \
    "$(doop_dataset_manifest "$bs")" \
    "$(CDPATH= cd -- "$bs_parent" && doop_dataset_manifest "$(basename "$bs")")"

# The anti-drift assertion, stated at the strength that actually holds: the two
# agree for the shape the pinned value describes -- a canonical path to a
# directory of plain regular .facts files. They do NOT agree in general, and
# pinning the divergences here keeps them documented rather than latent:
#
#   trailing slash   manifest_for_doop is not invariant (`find d/` prints d/x,
#                    but its sed pattern is '  d//', so nothing strips)
#   dotfiles         find -name '*.facts' matches .hidden.facts; the glob does not
#   symlinks         the glob follows a symlinked .facts; find -type f excludes it
#   metacharacters   manifest_for_doop interpolates $dir into a BRE unescaped
#
# All four are properties of manifest_for_doop, which this change does not
# touch; doop_dataset_manifest is invariant in every one. Unifying them is
# worth doing, but it belongs with the shared-helper refactor the issue asks
# for, not inside a fix whose whole point is that this hash must not move.
# Everything that CALLS manifest_for_doop goes inside this guard, not just the
# agreement case: that function interpolates its argument into a BRE unescaped,
# so a bare '[' in the path kills its sed, pipefail makes the assignment
# non-zero and set -e aborts the whole file with a raw sed error naming nothing.
# Testing for the actual BRE metacharacters rather than an allowlist, because
# an allowlist rejected a plain space -- harmless to both functions -- and
# silently dropped the most important assertion here.
# Deliberately NOT including '.': a dot in a BRE still matches its own literal,
# so a path containing one strips correctly, and every mktemp path has one --
# including it here skipped the assertion on essentially every host.
case "$tmp" in
    *[\\[\]*^\$\#]*) downstream_safe=0 ;;
    *)                   downstream_safe=1 ;;
esac

if (( downstream_safe )); then
    m_downstream=$(manifest_for_doop "$data")
    assert_eq 'the perf gate and manifest_for_doop agree on a canonical path' \
        "$m_abs" "$m_downstream"

    # The divergences, asserted so a shared-helper refactor has to address them
    # deliberately instead of silently redefining what "agree" means. These are
    # expected differences, not defects in this change.
    slashed=$(manifest_for_doop "$data/")
    assert_ne 'manifest_for_doop is not trailing-slash invariant, as expected until unification' \
        "$m_abs" "$slashed"
else
    # Not a failure of either script: blaming them for disagreeing on a path
    # manifest_for_doop cannot parse would be the gate-blames-the-wrong-thing
    # defect this issue exists to remove.
    printf 'test-doop-manifest: SKIPPED manifest_for_doop cases (path contains a BRE metacharacter)\n'
fi

# ...and the fixture must be able to tell manifests apart at all, or every
# equality above holds vacuously.
other="$tmp/other"; mkdir -p "$other"
for f in Method.facts Method-Modifier.facts MethodHandleConstant.facts \
         MethodTypeConstant.facts; do
    printf 'DIFFERENT contents of %s\n' "$f" > "$other/$f"
done
assert_ne 'different contents give a different manifest' \
    "$m_abs" "$(doop_dataset_manifest "$other")"

# Identical bytes in every file, so only the NAMES differ from the base
# fixture. Writing per-file contents here would vary both, and then a mutation
# that dropped filenames from the hash entirely would still pass this.
same_bytes="$tmp/same"; mkdir -p "$same_bytes"
for f in Method.facts Method-Modifier.facts MethodHandleConstant.facts \
         MethodTypeConstant.facts; do
    printf 'identical\n' > "$same_bytes/$f"
done
renamed="$tmp/renamed"; mkdir -p "$renamed"
for f in Method.facts Method-Modifier.facts MethodHandleConstant.facts Extra.facts; do
    printf 'identical\n' > "$renamed/$f"
done
assert_ne 'a renamed file gives a different manifest, contents held equal' \
    "$(doop_dataset_manifest "$same_bytes")" "$(doop_dataset_manifest "$renamed")"

# CDPATH is consulted for a RELATIVE argument, and $data_dir is relative by
# default -- so the documented CLI shape is the exposed one. On a hit, cd both
# prints the resolved path (into the command substitution, so the manifest
# becomes a path plus a hash) and reaches a different directory entirely.
cdp_root="$tmp/cdp"; mkdir -p "$cdp_root/real/doop" "$cdp_root/decoy/doop"
for f in A.facts B.facts; do
    printf 'real\n'  > "$cdp_root/real/doop/$f"
    printf 'decoy\n' > "$cdp_root/decoy/doop/$f"
done
truth=$(doop_dataset_manifest "$cdp_root/real/doop")
assert_eq 'CDPATH does not pollute the manifest with a path' "$truth" \
    "$(CDPATH= cd -- "$cdp_root/real" && CDPATH=. doop_dataset_manifest doop)"
assert_eq 'CDPATH cannot silently redirect to another dataset' "$truth" \
    "$(CDPATH= cd -- "$cdp_root/real" && CDPATH="$cdp_root/decoy" doop_dataset_manifest doop)"

# Locale independence (#1294), asserted here too because the same expression
# carries both properties and a future edit could drop either.
locale_name=""
# Captured, not piped into grep -q: grep -q stops at the first match, locale -a
# takes SIGPIPE, and pipefail reports 141 even though the locale WAS found --
# so the `if` reads false and both locale assertions below silently disappear.
# Reproduced at 40k locale names (PIPESTATUS=141 0). The same bug is live at
# check-manifest-collation.sh:35 and should be fixed there too.
locales_available=$(locale -a 2>/dev/null || true)
for cand in en_US.utf8 en_US.UTF-8; do
    if grep -Fxq "$cand" <<<"$locales_available"; then locale_name=$cand; break; fi
done
if [[ -n "$locale_name" && "$(uname -s)" == Linux ]]; then
    c_order=$(printf 'Method-Modifier.facts\nMethodHandleConstant.facts\n' | LC_ALL=C sort)
    l_order=$(printf 'Method-Modifier.facts\nMethodHandleConstant.facts\n' | LC_ALL="$locale_name" sort)
    assert_ne 'the fixture distinguishes C from the locale collation' "$c_order" "$l_order"
    # "$BASH", not bare bash: the lifted function is a bashism and $SHELL or a
    # dash /bin/sh would fail with a foreign error blaming the wrong thing.
    # $data is passed as an argument, not interpolated: single-quoting it broke
    # outright on a TMPDIR containing a quote, and BOTH subshells then produced
    # the empty string, which compared equal and reported ok. That is the exact
    # vacuous pass this file's own comment claims to guard against.
    under_c=$(LC_ALL=C "$BASH" -c "$(declare -f doop_dataset_manifest); doop_dataset_manifest \"\$1\"" _ "$data")
    under_loc=$(LC_ALL="$locale_name" "$BASH" -c "$(declare -f doop_dataset_manifest); doop_dataset_manifest \"\$1\"" _ "$data")
    if [[ "$under_c" =~ ^[0-9a-f]{64}$ ]]; then
        assert_eq 'the manifest does not depend on the ambient locale' \
            "$under_c" "$under_loc"
    else
        check 'the locale subshell produced a manifest' 1
    fi
else
    # Not weakened to a soft check on Linux: there the fixture MUST discriminate,
    # and a skip would hide the fixture going stale. Elsewhere the platform's
    # collation simply cannot, which is not this repository's problem.
    printf 'test-doop-manifest: SKIPPED locale case (needs Linux with en_US installed)\n'
fi

if ((failures)); then
    printf 'test-doop-manifest: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-doop-manifest: all cases passed\n'
