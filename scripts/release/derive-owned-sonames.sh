#!/usr/bin/env bash
# Print the basenames of the shared libraries a build tree produces for itself.
#
# Issue #1272 introduced this set; #1285 extracted it so it can be tested. The
# release upgrade matrix uses it to tell "the consumer loaded a library this
# release installs" apart from "the consumer loaded whatever the host had
# lying around". Everything the closure gate asserts rests on this set being
# right, so it is worth being able to exercise it without a release tag.
#
# usage: derive-owned-sonames.sh BUILD_DIR
#
# Prints one basename per line, LC_ALL=C sorted and de-duplicated. Exits 1 if
# the tree yields none. Not because that would otherwise go unnoticed --
# check-shared-library-closure.sh and run-upgrade-matrix.sh each carry their own
# `-s` guard, both predating this script -- but because failing at the point of
# derivation names the real cause, where the downstream guards can only report
# that the set was empty by the time they saw it.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo 'usage: derive-owned-sonames.sh BUILD_DIR' >&2
    exit 2
fi

build=$1
[[ -d "$build" ]] || {
    echo "derive-owned-sonames: not a directory: $build" >&2
    exit 1
}

# No -maxdepth: a subproject that relocates its build output must not drop
# silently out of the set. `! -type d` keeps meson's libwirelog.so.N.p object
# directories, which match '*.so.*', from being mistaken for libraries. The
# grep keeps the match to real library names -- '*.so.*' also catches build
# by-products such as libnanoarrow.so.symbols. LC_ALL=C so the output does not
# depend on the operator's locale (#1291).
owned=$(find "$build" ! -type d \
    \( -name '*.so' -o -name '*.so.*' -o -name '*.dylib' \) -print |
    sed 's|.*/||' |
    { grep -E '\.(so|dylib)(\.[0-9]+)*$' || true; } |
    LC_ALL=C sort -u)

[[ -n "$owned" ]] || {
    echo "derive-owned-sonames: $build produced no shared libraries" >&2
    exit 1
}

printf '%s\n' "$owned"
