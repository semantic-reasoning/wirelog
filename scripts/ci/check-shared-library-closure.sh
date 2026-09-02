#!/usr/bin/env bash
# Assert that a consumer's runtime shared-library closure resolved entirely
# from the release prefix it was built against.
#
# Issue #1272.  The release upgrade matrix builds a small consumer against an
# installed wirelog prefix and runs it.  The consumer is linked with
# -Wl,-rpath, which emits DT_RUNPATH -- and DT_RUNPATH is *not* transitive: it
# is consulted only for the direct DT_NEEDED entries of the object carrying
# it.  The installed libwirelog.so carries no RUNPATH of its own (meson strips
# the build rpath on install), so its own DT_NEEDED libnanoarrow.so and
# libxxhash.so.0 fall through to LD_LIBRARY_PATH and then to the host's
# system directories.
#
# On a clean CI runner that silently became "cannot open shared object file"
# and the job died with exit 127.  On a developer machine that happens to have
# the libraries installed it is worse: the consumer runs green against the
# *host's* copies, so the gate certifies a release prefix it never tested.
# Both outcomes are invisible to an exit-status check, which is why this runs
# on the loader's own record of what it opened rather than on the exit code.
#
# The trace is captured from the real run (LD_DEBUG=libs on glibc,
# DYLD_PRINT_LIBRARIES=1 on macOS), not from a separate ldd prediction: ldd
# re-resolves in its own environment, so it can disagree with the run it is
# supposed to be describing.
#
# usage: check-shared-library-closure.sh TRACE PREFIX OWNED-SONAMES REPORT
#
#   TRACE          loader trace captured from the consumer's real execution
#   PREFIX         install prefix the consumer was built against
#   OWNED-SONAMES  basenames of the shared libraries this build produces
#   REPORT         written with one line per loaded object (release evidence)
#
# Exits 0 when every owned library resolved from PREFIX, 1 otherwise.
set -euo pipefail

if [[ $# -ne 4 ]]; then
    echo 'usage: check-shared-library-closure.sh TRACE PREFIX OWNED-SONAMES REPORT' >&2
    exit 2
fi

trace=$1
prefix=$2
owned_file=$3
report=$4

fail() {
    echo "shared library closure: $*" >&2
    exit 1
}

[[ -r "$trace" ]] || fail "cannot read loader trace: $trace"
[[ -r "$owned_file" ]] || fail "cannot read owned soname list: $owned_file"

# An empty owned set would make the whole check vacuous: no loaded library
# could ever match it, so the gate would pass without asserting anything.
[[ -s "$owned_file" ]] || fail \
    "owned soname list is empty: the build produced no shared libraries to check ($owned_file)"

# Strip a trailing slash so the "$path" == "$p"/* tests below anchor on a real
# path separator.  Without that anchor /opt/rel would also accept /opt/rel-old.
prefix=${prefix%/}
[[ -n "$prefix" ]] || fail 'prefix must not be empty'

# Compare against both the literal and the physically resolved prefix.  The
# loader prints the search directory it was handed, uncanonicalised, so on a
# host where the prefix sits under a symlinked parent (macOS /var -> /private/var,
# or a symlinked TMPDIR) exactly one of the two forms matches, and which one
# differs by platform.
prefix_real=$prefix
if [[ -d "$prefix" ]]; then
    prefix_real=$(cd "$prefix" && pwd -P) || fail "cannot resolve prefix: $prefix"
fi

inside_prefix() {
    local path=$1
    [[ "$path" == "$prefix"/* || "$path" == "$prefix_real"/* ]]
}

# Extract the absolute path of every object the loader actually mapped.
#
# glibc LD_DEBUG=libs emits one "calling init: <path>" line per loaded object.
# It also emits "trying file=<path>" for every *candidate* it probes, including
# ones that do not exist, so keying on "calling init:" is what distinguishes
# what was loaded from what was merely searched.
loaded=$(sed -n 's/.*calling init: \(\/.*\)$/\1/p' "$trace" || true)
shape=glibc

if [[ -z "$loaded" ]]; then
    # macOS DYLD_PRINT_LIBRARIES.  Classic dyld prints "dyld: loaded: <path>";
    # dyld4 prints "dyld[<pid>]: <uuid> <path>".  Both end in the path, which
    # must be captured to end of line: macOS paths routinely contain spaces
    # ("Application Support"), and a capture that stopped at the first space
    # would drop the library from the report entirely -- passing the very
    # substitution this gate exists to catch.
    loaded=$(sed -n 's/^dyld[^ ]*:.*[ 	]\(\/.*\)$/\1/p' "$trace" || true)
    shape=dyld
fi

[[ -n "$loaded" ]] || fail \
    "no loader trace found in $trace: the run produced no LD_DEBUG=libs or DYLD_PRINT_LIBRARIES records, so the closure was never observed"

: >"$report"
printf '# shared library closure (%s trace: %s)\n' "$shape" "$trace" >>"$report"
printf '# prefix: %s\n' "$prefix" >>"$report"

owned_seen=0
violations=()

while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    soname=${path##*/}
    if grep -qxF "$soname" "$owned_file"; then
        if inside_prefix "$path"; then
            printf 'OWNED\t%s\t%s\n' "$soname" "$path" >>"$report"
            owned_seen=$((owned_seen + 1))
        else
            printf 'FOREIGN\t%s\t%s\n' "$soname" "$path" >>"$report"
            violations+=("$soname -> $path")
        fi
    else
        printf 'EXTERNAL\t%s\t%s\n' "$soname" "$path" >>"$report"
    fi
done <<<"$loaded"

if ((${#violations[@]})); then
    {
        echo 'shared library closure: libraries built by this release resolved outside the release prefix.'
        echo "  prefix: $prefix"
        for violation in "${violations[@]}"; do
            echo "  $violation"
        done
        echo 'The consumer loaded host copies instead of the ones this release installs.'
        echo 'On a machine without those host copies the same run fails to start.'
        echo "  report: $report"
    } >&2
    exit 1
fi

# Coverage guard.  Every owned library resolving correctly is only meaningful
# if at least one of them was actually loaded; an owned list that describes a
# different build entirely would otherwise pass while asserting nothing.
# This is deliberately set-level: it catches the list going wholly stale, not
# a single library dropping out of it while the others still match.
((owned_seen)) || fail \
    "the trace in $trace loaded no owned library, so nothing was verified; the owned soname list in $owned_file does not describe this build"

printf 'shared library closure: %d owned librar%s resolved from %s\n' \
    "$owned_seen" "$( ((owned_seen == 1)) && echo y || echo ies)" "$prefix"
