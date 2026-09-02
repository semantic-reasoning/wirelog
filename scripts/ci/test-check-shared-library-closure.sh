#!/usr/bin/env bash
# Self-test for check-shared-library-closure.sh.
#
# The closure checker only ever runs inside the release upgrade matrix
# (scripts/upgrade/run-upgrade-matrix.sh), which itself only runs on a release
# tag.  Without this self-test the parser would have no pre-release coverage at
# all, so every branch below is exercised from canned loader traces instead of
# from a real build.  Runs in under a second and builds nothing.
set -euo pipefail

# meson hands this script a native path, which on Windows is a drive-letter
# path bash cannot use directly; see bb4ca712 for the same fix on the sibling
# downstream-matrix self-test.
normalize_root() {
    local supplied=$1
    case "$supplied" in
        [[:alpha:]]:[\\/]* )
            if ! command -v cygpath >/dev/null 2>&1; then
                echo "closure self-test: cygpath is required to normalize Windows root: $supplied" >&2
                return 2
            fi
            if ! supplied=$(cygpath -u -- "$supplied"); then
                echo "closure self-test: cygpath could not normalize Windows root: $supplied" >&2
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
checker="$root/scripts/ci/check-shared-library-closure.sh"
[[ -x "$checker" ]] || {
    echo "closure self-test: checker not executable: $checker" >&2
    exit 1
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-closure-selftest.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
case_no=0

# Run the checker and assert its exit status.  On an unexpected status the
# captured output is echoed, because a parser that fails for the wrong reason
# is indistinguishable from one that fails for the right reason.
expect() {
    local name=$1 want=$2 trace=$3 prefix=$4 owned=$5
    local report="$tmp/report.$((++case_no))" out status=0
    out=$("$checker" "$trace" "$prefix" "$owned" "$report" 2>&1) || status=$?
    if [[ "$status" != "$want" ]]; then
        printf 'closure self-test: FAIL %s (want exit %s, got %s)\n' \
            "$name" "$want" "$status" >&2
        printf '%s\n' "$out" | sed 's/^/    /' >&2
        failures=$((failures + 1))
        return
    fi
    printf 'closure self-test: ok %s\n' "$name"
}

# As expect(), but also require a substring in the checker's diagnostics, so a
# failing case cannot pass this test by failing for an unrelated reason.
expect_message() {
    local name=$1 want=$2 needle=$3 trace=$4 prefix=$5 owned=$6
    local report="$tmp/report.$((++case_no))" out status=0
    out=$("$checker" "$trace" "$prefix" "$owned" "$report" 2>&1) || status=$?
    if [[ "$status" != "$want" ]] || [[ "$out" != *"$needle"* ]]; then
        printf 'closure self-test: FAIL %s (want exit %s and %q)\n' \
            "$name" "$want" "$needle" >&2
        printf 'got exit %s:\n' "$status" >&2
        printf '%s\n' "$out" | sed 's/^/    /' >&2
        failures=$((failures + 1))
        return
    fi
    printf 'closure self-test: ok %s\n' "$name"
}

owned_full="$tmp/owned-full.txt"
cat >"$owned_full" <<'EOF'
libnanoarrow.so
libwirelog.so
libwirelog.so.0
libxxhash.so.0
EOF

owned_empty="$tmp/owned-empty.txt"
: >"$owned_empty"

# glibc LD_DEBUG=libs shape, closure fully inside the prefix.  Includes the
# noise lines ("trying file=", search paths) that a real trace carries, so the
# parser is proven to key on "calling init:" and not on incidental matches.
cat >"$tmp/glibc-clean.trace" <<'EOF'
    12345:	find library=libwirelog.so.0 [0]; searching
    12345:	  trying file=/opt/rel/lib/glibc-hwcaps/x86-64-v3/libwirelog.so.0
    12345:	  trying file=/opt/rel/lib/libwirelog.so.0
    12345:	find library=libnanoarrow.so [0]; searching
    12345:	  trying file=/usr/lib/libnanoarrow.so
    12345:	calling init: /lib64/ld-linux-x86-64.so.2
    12345:	calling init: /usr/lib/libc.so.6
    12345:	calling init: /usr/lib/libm.so.6
    12345:	calling init: /opt/rel/lib/libxxhash.so.0
    12345:	calling init: /opt/rel/lib/libnanoarrow.so
    12345:	calling init: /opt/rel/lib/libwirelog.so.0
EOF
expect 'glibc closure inside prefix passes' 0 \
    "$tmp/glibc-clean.trace" /opt/rel "$owned_full"

# The #1272 defect itself: the consumer runs and exits 0, but an owned library
# was served by the host instead of the release prefix.  On a clean runner the
# same condition is a hard "cannot open shared object file".
cat >"$tmp/glibc-host.trace" <<'EOF'
    12345:	calling init: /lib64/ld-linux-x86-64.so.2
    12345:	calling init: /usr/lib/libc.so.6
    12345:	calling init: /opt/rel/lib/libxxhash.so.0
    12345:	calling init: /usr/lib/libnanoarrow.so
    12345:	calling init: /opt/rel/lib/libwirelog.so.0
EOF
expect_message 'owned library served by host fails' 1 'libnanoarrow.so' \
    "$tmp/glibc-host.trace" /opt/rel "$owned_full"

# An empty owned set would make the whole rule vacuous: nothing matches, so
# nothing is ever asserted.  That must be a loud failure, not a silent pass.
expect_message 'empty owned set fails' 1 'no shared libraries' \
    "$tmp/glibc-clean.trace" /opt/rel "$owned_empty"

# Coverage guard.  If the owned set stops naming the libraries that actually
# load -- e.g. a subproject relocates its build output and drops out of the
# set -- the rule silently stops covering anything.  Assert that at least one
# owned library was observed.
cat >"$tmp/glibc-nocoverage.trace" <<'EOF'
    12345:	calling init: /lib64/ld-linux-x86-64.so.2
    12345:	calling init: /usr/lib/libc.so.6
EOF
expect_message 'trace covering no owned library fails' 1 'no owned' \
    "$tmp/glibc-nocoverage.trace" /opt/rel "$owned_full"

# A prefix that is a string prefix of an unrelated directory must not be
# accepted by a naive comparison: /opt/rel-old is outside /opt/rel.
cat >"$tmp/glibc-sibling.trace" <<'EOF'
    12345:	calling init: /usr/lib/libc.so.6
    12345:	calling init: /opt/rel-old/lib/libnanoarrow.so
    12345:	calling init: /opt/rel/lib/libwirelog.so.0
EOF
expect_message 'sibling directory is outside the prefix' 1 'libnanoarrow.so' \
    "$tmp/glibc-sibling.trace" /opt/rel "$owned_full"

# A trailing slash on the prefix is the same prefix.
expect 'trailing slash on prefix is accepted' 0 \
    "$tmp/glibc-clean.trace" /opt/rel/ "$owned_full"

# dyld DYLD_PRINT_LIBRARIES shape.  Both the classic "dyld: loaded:" form and
# the dyld4 "dyld[pid]: <uuid> /path" form appear in the wild; the parser must
# take the trailing absolute path in either.
cat >"$tmp/dyld-clean.trace" <<'EOF'
dyld: loaded: /opt/rel/lib/libwirelog.dylib
dyld[4321]: <A1B2C3D4-0000-0000-0000-000000000000> /opt/rel/lib/libnanoarrow.dylib
dyld: loaded: /usr/lib/libSystem.B.dylib
EOF
owned_dyld="$tmp/owned-dyld.txt"
printf 'libnanoarrow.dylib\nlibwirelog.dylib\n' >"$owned_dyld"
expect 'dyld closure inside prefix passes' 0 \
    "$tmp/dyld-clean.trace" /opt/rel "$owned_dyld"

cat >"$tmp/dyld-host.trace" <<'EOF'
dyld: loaded: /opt/rel/lib/libwirelog.dylib
dyld: loaded: /usr/local/lib/libnanoarrow.dylib
EOF
expect_message 'dyld owned library served by host fails' 1 'libnanoarrow.dylib' \
    "$tmp/dyld-host.trace" /opt/rel "$owned_dyld"

# macOS paths routinely contain spaces.  A parser that captured only up to the
# first space would drop this library from the report entirely and report a
# clean closure -- the exact substitution this gate exists to catch, passing.
cat >"$tmp/dyld-spaces.trace" <<'EOF'
dyld: loaded: /opt/rel/lib/libwirelog.dylib
dyld: loaded: /Users/ci/Application Support/lib/libnanoarrow.dylib
EOF
expect_message 'dyld path containing spaces is not dropped' 1 'Application Support' \
    "$tmp/dyld-spaces.trace" /opt/rel "$owned_dyld"

# A trace the parser does not recognise means the loader never produced one --
# LD_DEBUG unsupported, output redirected elsewhere, a musl host.  That is an
# unusable gate and must fail with a named reason rather than pass vacuously.
printf 'some unrelated program output\n' >"$tmp/unknown.trace"
expect_message 'unrecognised trace shape fails' 1 'no loader trace' \
    "$tmp/unknown.trace" /opt/rel "$owned_full"

: >"$tmp/empty.trace"
expect_message 'empty trace fails' 1 'no loader trace' \
    "$tmp/empty.trace" /opt/rel "$owned_full"

expect_message 'missing trace file fails' 1 'cannot read' \
    "$tmp/does-not-exist.trace" /opt/rel "$owned_full"

# The report is the uploaded release evidence; a passing run must produce one
# that names both the owned and the external resolutions.
report="$tmp/report-content.txt"
"$checker" "$tmp/glibc-clean.trace" /opt/rel "$owned_full" "$report" >/dev/null
report_failures=0
for needle in 'OWNED	libwirelog.so.0	/opt/rel/lib/libwirelog.so.0' \
              'EXTERNAL	libc.so.6	/usr/lib/libc.so.6'; do
    if ! grep -qF "$needle" "$report"; then
        printf 'closure self-test: FAIL report missing %q\n' "$needle" >&2
        report_failures=$((report_failures + 1))
    fi
done
failures=$((failures + report_failures))
((report_failures)) || printf 'closure self-test: ok report records owned and external\n'

if ((failures)); then
    printf 'closure self-test: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'closure self-test: all cases passed\n'
