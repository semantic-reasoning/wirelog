#!/usr/bin/env bash
# Self-test for derive-owned-sonames.sh.
#
# Issue #1285. The release closure gate (#1272) can only verify libraries the
# owned-soname set names; one that drops out of the set stops being checked,
# silently, while the gate keeps reporting PASS. The derivation was inline in
# scripts/upgrade/run-upgrade-matrix.sh and therefore only ever executed on a
# release tag. Extracted so it can be exercised here, from fixtures, in
# milliseconds.
set -euo pipefail

# meson hands this script a native path; on Windows that is a drive-letter path
# bash cannot use. See bb4ca712 for the same fix on the downstream self-test.
normalize_root() {
    local supplied=$1
    case "$supplied" in
        [[:alpha:]]:[\\/]* )
            if ! command -v cygpath >/dev/null 2>&1; then
                echo "derive-owned-sonames self-test: cygpath is required to normalize Windows root: $supplied" >&2
                return 2
            fi
            if ! supplied=$(cygpath -u -- "$supplied"); then
                echo "derive-owned-sonames self-test: cygpath could not normalize Windows root: $supplied" >&2
                return 2
            fi
            ;;
    esac
    printf '%s\n' "$supplied"
}

root=${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
if (($#)); then
    root=$(normalize_root "$root")
fi
derive="$root/scripts/release/derive-owned-sonames.sh"
[[ -x "$derive" ]] || {
    echo "derive-owned-sonames self-test: not executable: $derive" >&2
    exit 1
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-owned-selftest.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
check() {
    local name=$1 status=$2
    if [[ "$status" == 0 ]]; then
        printf 'derive-owned-sonames self-test: ok %s\n' "$name"
    else
        printf 'derive-owned-sonames self-test: FAIL %s\n' "$name" >&2
        failures=$((failures + 1))
    fi
}

assert() {
    local name=$1 status=0
    shift
    "$@" || status=1
    check "$name" "$status"
}

# A build tree shaped like meson's: a versioned soname with its symlink chain,
# a subproject library nested deeper, the .p object directories that match
# '*.so.*' but are directories, and a .symbols by-product that matches too.
build="$tmp/build"
mkdir -p "$build/subprojects/nanoarrow" "$build/libwirelog.so.0.60.0.p" \
         "$build/subprojects/nanoarrow/libnanoarrow.so.p"
: >"$build/libwirelog.so.0.60.0"
ln -s libwirelog.so.0.60.0 "$build/libwirelog.so.0"
ln -s libwirelog.so.0 "$build/libwirelog.so"
: >"$build/libwirelog.so.0.60.0.symbols"
: >"$build/subprojects/nanoarrow/libnanoarrow.so"
: >"$build/subprojects/nanoarrow/libnanoarrow.so.symbols"
: >"$build/libwirelog.so.0.60.0.p/some.o"
# A directory named exactly like a library: excluded only by `! -type d`,
# unlike the .p directories, which the grep would reject anyway.
mkdir -p "$build/libdecoy.so.1"
# A duplicate basename across two locations: exercises sort -u.
mkdir -p "$build/subprojects/other"
: >"$build/subprojects/other/libnanoarrow.so"
# macOS forms, on a platform this repository supports.
: >"$build/libwirelog.1.dylib"
# A static library, which must not be collected.
: >"$build/libwirelog.a"

expected=$'libnanoarrow.so\nlibwirelog.1.dylib\nlibwirelog.so\nlibwirelog.so.0\nlibwirelog.so.0.60.0'
actual=$("$derive" "$build")

[[ "$actual" == "$expected" ]]
check "derives exactly the library names, sorted" $?
if [[ "$actual" != "$expected" ]]; then
    printf 'expected:\n%s\ngot:\n%s\n' "$expected" "$actual" >&2
fi

not_listed() { ! printf '%s\n' "$actual" | grep -qx "$1"; }
listed() { printf '%s\n' "$actual" | grep -qx "$1"; }

assert 'a versioned soname is included' listed 'libwirelog.so.0.60.0'
assert 'both symlinks in the chain are included' listed 'libwirelog.so.0'
assert 'a nested subproject library is found without -maxdepth' \
    listed 'libnanoarrow.so'
assert 'a .symbols by-product is excluded' not_listed 'libwirelog.so.0.60.0.symbols'
assert 'a .p object directory is excluded' not_listed 'libwirelog.so.0.60.0.p'
assert 'an object inside a .p directory is excluded' not_listed 'some.o'
assert 'a directory named exactly like a library is excluded' not_listed 'libdecoy.so.1'
assert 'a versioned .dylib is included' listed 'libwirelog.1.dylib'
assert 'a static library is excluded' not_listed 'libwirelog.a'
dedupes() { [[ "$(printf '%s\n' "$actual" | grep -cx 'libnanoarrow.so')" == 1 ]]; }
assert 'a basename appearing twice is emitted once' dedupes

# A tree with no shared libraries must fail rather than emit an empty set: an
# empty set makes the closure check vacuous, because no loaded library can
# match it.
empty="$tmp/empty"
mkdir -p "$empty/subprojects"
: >"$empty/README"
empty_fails() { ! "$derive" "$empty" >/dev/null 2>&1; }
assert 'an empty build tree fails rather than yielding an empty set' empty_fails

# A tree holding only by-products is the same case, and is the one a naive
# `find` would get wrong by reporting success.
byproducts="$tmp/byproducts"
mkdir -p "$byproducts/libwirelog.so.0.p"
: >"$byproducts/libwirelog.so.0.symbols"
byproducts_fail() { ! "$derive" "$byproducts" >/dev/null 2>&1; }
assert 'a tree of only by-products fails' byproducts_fail

# Locale independence: the output feeds a comparison, so its order must not
# depend on the operator (#1291).
locale_name=""
for candidate in en_US.utf8 en_US.UTF-8; do
    if locale -a 2>/dev/null | grep -Fxq "$candidate"; then
        locale_name=$candidate
        break
    fi
done
if [[ -n "$locale_name" ]]; then
    # A colliding pair: en_US ignores the hyphen, so libmethod-modifier sorts
    # as libmethodmodifier and lands after libmethodhandle, while C sorts the
    # hyphen (0x2D) first. Without a pair that collides, the set orders
    # identically either way and this passes whether or not the pin exists.
    : >"$build/libmethod-modifier.so"
    : >"$build/libmethodhandle.so"
    fixture_discriminates() {
        [[ "$(printf 'libmethod-modifier.so\nlibmethodhandle.so\n' | LC_ALL=C sort)" != \
           "$(printf 'libmethod-modifier.so\nlibmethodhandle.so\n' | LC_ALL="$locale_name" sort)" ]]
    }
    assert 'the locale fixture actually distinguishes the two collations' \
        fixture_discriminates
    locale_stable() {
        [[ "$(LC_ALL=C "$derive" "$build")" == \
           "$(LC_ALL="$locale_name" "$derive" "$build")" ]]
    }
    assert 'output does not depend on the ambient locale' locale_stable
    rm -f "$build/libmethod-modifier.so" "$build/libmethodhandle.so"
else
    printf 'derive-owned-sonames self-test: ok locale (en_US absent, skipped)\n'
fi

missing_arg() { ! "$derive" >/dev/null 2>&1; }
assert 'no argument exits non-zero' missing_arg
bad_dir() { ! "$derive" "$tmp/does-not-exist" >/dev/null 2>&1; }
assert 'a missing build directory exits non-zero' bad_dir

if ((failures)); then
    printf 'derive-owned-sonames self-test: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'derive-owned-sonames self-test: all cases passed\n'
