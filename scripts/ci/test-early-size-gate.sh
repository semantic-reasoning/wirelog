#!/usr/bin/env bash
# Self-test for the early binary-size gate and the scoped CI builds (#1573).
#
# Two halves, asserting different things (pattern: test-required-gates.sh):
#
#   1. Wiring -- ci-pr.yml still compiles the `wirelog` target early in
#      build-primary and runs the size gate on that library BEFORE the full
#      Build and Test steps (exactly once), and the mbedtls / tsan-native
#      legs still compile explicit targets (not the default set) while
#      keeping their documented test steps.  Without this half, reverting
#      the workflow would leave the behavioural assertions green -- the
#      silent-downgrade shape this gate exists to prevent.
#
#   2. Behaviour -- the real gate (scripts/ci/check-text-size.sh) FAILs on
#      an intentionally oversize library and PASSes on a small one, against
#      the committed baseline.  A fixture that only ever supplied a passing
#      library would pin nothing: the negative control must drive the gate's
#      own fail path, which is what #1573 moved earlier in the job so a size
#      regression fails within minutes instead of ~24.
#
# The wiring half runs FIRST and needs only awk + the workflow file; the
# behavioural half needs cc/size and platform-specific size tools, and is the
# only part that may skip (exit 77). This order guarantees the revert guard is
# always enforced even on a host lacking a C toolchain or supported binary
# format. Invoked as `sh test-early-size-gate.sh <repo-root>` by the
# meson test, so stay POSIX-sh clean: no `(( ))`, no arrays, no pipefail.
set -eu

root=${1:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)}
failures=0

check() {
    local name=$1 status=$2
    if [ "$status" = 0 ]; then
        printf 'test-early-size-gate: ok %s\n' "$name"
    else
        printf 'test-early-size-gate: FAIL %s\n' "$name" >&2
        failures=$((failures + 1))
    fi
}
assert() {
    local name=$1 s=0
    shift
    "$@" || s=1
    check "$name" "$s"
}

# --- 1. wiring (always runs; needs only awk + the workflow file) -----------
wf="$root/.github/workflows/ci-pr.yml"
if [ ! -f "$wf" ]; then
    printf 'test-early-size-gate: FAIL: %s missing\n' "$wf" >&2
    exit 1
fi

# One awk pass finds the anchor line inside each named job: the job block is
# a 2-space-indented key, and any column-0 line ends it.  The `run:` anchors
# end at end-of-line where noted, so `meson compile -C builddir` (full build)
# never matches the scoped `... builddir wirelog` line, and the explicit
# target lines are asserted verbatim -- a revert to a bare full compile fails
# the "explicit" assertion AND lights up the "full" one.  The build-primary
# early-build anchor additionally excludes comment lines (`!/#/`): the
# explanatory comment above it contains the same command string, so without
# the exclusion a revert that deletes the run line but leaves the comment
# would still light up the "early" anchor and hide the regression.  A
# duplicate gate (early + late both present) is caught by the count below.
anchors=$(awk '
    /^[^[:space:]]/ { in_job = ""; next }
    /^  build-primary:/  { in_job = "bp"; next }
    /^  build-mbedtls:/  { in_job = "mb"; next }
    /^  tsan-native:/    { in_job = "ts"; next }
    in_job == "bp" && /meson compile -C builddir wirelog/ && !/#/ { print "bp early " NR }
    in_job == "bp" && /run: scripts\/ci\/check-text-size\.sh builddir\/libwirelog\.so/ { print "bp gate " NR }
    in_job == "bp" && /run: meson compile -C builddir$/ { print "bp full " NR }
    in_job == "bp" && /run: meson test -C builddir --print-errorlogs/ { print "bp test " NR }
    in_job == "mb" && /meson compile -C builddir-mbedtls test_cryptographic_hashes test_symbol_digests/ { print "mb explicit " NR }
    in_job == "mb" && /run: meson compile -C builddir-mbedtls$/ { print "mb full " NR }
    in_job == "mb" && /-DWL_MBEDTLS_ENABLED=1/ && /grep -R --/ { print "mb flag " NR }
    in_job == "mb" && /meson test -C builddir-mbedtls cryptographic_hashes symbol_digests/ { print "mb tests " NR }
    in_job == "mb" && /scripts\/ci\/test-mbedtls-prefix\.sh/ { print "mb prefix " NR }
    in_job == "ts" && /meson compile -C builddir-tsan-native wirelog/ { print "ts explicit " NR }
    in_job == "ts" && /run: meson compile -C builddir-tsan-native$/ { print "ts full " NR }
    in_job == "ts" && /meson test -C builddir-tsan-native threading_doc/ { print "ts smoke " NR }
' "$wf")

get() {
    printf '%s\n' "$anchors" | awk -v want="$1 $2" '
        $0 ~ "^" want " " { print $3; exit }
    '
}
count() {
    printf '%s\n' "$anchors" | awk -v a="$1" -v b="$2" '$1==a && $2==b { n++ } END { print n + 0 }'
}

early=$(get bp early)
gate_line=$(get bp gate)
full=$(get bp full)
test_line=$(get bp test)
gate_count=$(count bp gate)

assert 'build-primary compiles the wirelog target early' test -n "$early"
assert 'build-primary runs the size gate on that library' test -n "$gate_line"
assert 'build-primary still has the full Build step' test -n "$full"
assert 'build-primary still runs the test suite' test -n "$test_line"
assert 'build-primary has EXACTLY ONE size-gate instance (no early+late duplicate)' test "$gate_count" = 1

early_gate_order() {
    [ -n "$early" ] && [ -n "$gate_line" ] && [ -n "$full" ] && [ -n "$test_line" ] \
        && [ "$early" -lt "$gate_line" ] && [ "$gate_line" -lt "$full" ] \
        && [ "$full" -lt "$test_line" ]
}
assert 'early gate runs BEFORE the full build and the test suite' early_gate_order

mbe=$(get mb explicit)
mbfull=$(get mb full)
mbflag=$(get mb flag)
mbtests=$(get mb tests)
mbprefix=$(get mb prefix)

assert 'mbedtls leg compiles the two explicit crypto targets' test -n "$mbe"
assert 'mbedtls leg does not full-compile the default set' test -z "$mbfull"
assert 'mbedtls leg still greps the enabled compile flag' test -n "$mbflag"
assert 'mbedtls leg still runs both crypto tests' test -n "$mbtests"
assert 'mbedtls leg still runs the prefix-discovery script' test -n "$mbprefix"

tse=$(get ts explicit)
tsfull=$(get ts full)
tssmoke=$(get ts smoke)

assert 'tsan-native leg compiles the wirelog target' test -n "$tse"
assert 'tsan-native leg does not full-compile the default set' test -z "$tsfull"
assert 'tsan-native leg still runs the threading_doc smoke' test -n "$tssmoke"

# The production size gate has format-aware readers only for ELF and Mach-O.
# Keep enforcing the platform-independent wiring assertions above, but do not
# pass those format-specific fixtures to MinGW or another unsupported toolchain.
if [ "$failures" -gt 0 ]; then
    printf 'test-early-size-gate: %s case(s) failed\n' "$failures" >&2
    exit 1
fi

platform=$(uname -s)
case "$platform" in
    Linux|Darwin)
        ;;
    *)
        printf 'test-early-size-gate: wiring OK; SKIP behavior half: unsupported platform %s\n' \
            "$platform"
        exit 77
        ;;
esac

# --- 2. behaviour (needs cc/size/gate/baseline; the only part that may skip)
gate="$root/scripts/ci/check-text-size.sh"
if ! command -v cc >/dev/null 2>&1 || ! command -v size >/dev/null 2>&1 \
        || [ ! -f "$gate" ] || [ ! -f "$root/tests/baseline_size.txt" ]; then
    # Wiring is already enforced above.  A wiring failure is a real failure;
    # otherwise the behavioural half is a genuine skip (exit 77).
    if [ "$failures" -gt 0 ]; then
        printf 'test-early-size-gate: %s case(s) failed\n' "$failures" >&2
        exit 1
    fi
    printf 'test-early-size-gate: wiring OK; SKIP behaviour half: needs cc, size, the gate script and the committed baseline\n'
    exit 77
fi

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-early-size.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Negative control: an intentionally oversize library must FAIL the gate.
# One function of ~400 KB of nops lands well past baseline + 5120 budget,
# with margin against either direction of future baseline movement.  The
# fixture is assembled rather than compiled so it costs milliseconds instead
# of the ~40 s a 40k-function C file takes at -O0 (meson timeout is 120 s
# and CI runners are slower than a dev box).
#
# The gate itself is platform-aware (ELF `size --format=sysv` on Linux,
# Mach-O `size -m` on Darwin), so this fixture has to assemble on both or
# the Darwin half of the gate goes unexercised.  Mach-O has no `.type`
# directive -- `@function` is ELF-only and the assembler rejects the whole
# file -- and its symbols carry a leading underscore.
case "$(uname -s)" in
    Darwin)
        fixture_sym=_oversize_fixture
        fixture_type=
        ;;
    *)
        fixture_sym=oversize_fixture
        fixture_type='.type oversize_fixture, @function'
        ;;
esac
big_asm="$tmp/oversize.s"
{
    printf '.text\n.globl %s\n' "$fixture_sym"
    if [ -n "$fixture_type" ]; then
        printf '%s\n' "$fixture_type"
    fi
    printf '%s:\n' "$fixture_sym"
    awk 'BEGIN { for (i = 1; i <= 400000; i++) print "\tnop" }'
    printf '\tret\n'
} >"$big_asm"
big_so="$tmp/oversize.so"
# Keep the toolchain's own diagnostics: a fixture that will not build is
# still a hard failure (skipping it would drop the revert guard), but the
# reason has to reach the log or the failure reads as a gate regression.
if ! cc -shared -o "$big_so" "$big_asm" 2>"$tmp/oversize.err"; then
    printf 'test-early-size-gate: could not build the oversize fixture:\n' >&2
    sed 's/^/test-early-size-gate:   /' "$tmp/oversize.err" >&2
    check 'oversize library FAILs the size gate (negative control)' 1
else
    negative_control() {
        local st=0
        "$gate" "$big_so" >/dev/null 2>&1 || st=$?
        [ "$st" = 1 ]
    }
    assert 'oversize library FAILs the size gate (negative control)' negative_control
fi

# Positive control: a small library must PASS against the same baseline.
small_src="$tmp/small.c"
printf 'int wirelog_size_fixture(void) { return 42; }\n' >"$small_src"
small_so="$tmp/small.so"
if ! cc -O0 -fPIC -shared -o "$small_so" "$small_src" 2>"$tmp/small.err"; then
    printf 'test-early-size-gate: could not build the small fixture:\n' >&2
    sed 's/^/test-early-size-gate:   /' "$tmp/small.err" >&2
    check 'small library PASSes the size gate (positive control)' 1
else
    positive_control() {
        "$gate" "$small_so" >/dev/null 2>&1
    }
    assert 'small library PASSes the size gate (positive control)' positive_control
fi

if [ "$failures" -gt 0 ]; then
    printf 'test-early-size-gate: %s case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-early-size-gate: all cases passed\n'
