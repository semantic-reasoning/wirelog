#!/usr/bin/env bash
# Self-test for check-log-erasure.sh (#1799).
#
# `meson` is stubbed on PATH so no compiler runs and every branch is reachable
# in milliseconds; the real gate takes ~7s because it builds libwirelog.
# `strings` is deliberately NOT stubbed -- it finds needles in plain text, so
# the leak fixture is a real file and the sentinel match is the real code path.
#
# The headline case is #1799's acceptance criterion restated: N concurrent
# invocations must record N distinct scratch directories.  Against the
# pre-#1799 gate every record is the same constant string, so the case fails by
# construction rather than by timing.
#
# SAFETY, learned the hard way: this file must never chmod or remove a path it
# did not create.  An earlier revision derived a path from the build directory
# the gate reported and removed it.  Against the FIXED gate that is the mktemp
# scratch root; against the PRE-FIX gate it is the repository root, and running
# the suite against the unfixed gate -- exactly what one does to prove the test
# discriminates -- deleted the worktree.  Every destructive action below is
# therefore guarded by under_work(), and a path outside our own sandbox is
# reported as a failure rather than acted on.  A stub is a separate process
# and cannot call under_work, so the one stub that chmods carries its own
# inline guard and refuses to run with an empty sandbox variable.
set -uo pipefail

# -P/pwd -P for the same reason as $work below, and in the opposite
# direction: the gate derives its own ROOT from `git rev-parse
# --show-toplevel`, which resolves symlinks.  A logical $root here would
# disagree with it when the checkout is reached through a symlink, and the
# "outside the source tree" case would report ok for a scratch directory
# literally inside it -- a vacuous pass on an acceptance criterion.
root=$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
gate="$root/scripts/ci/check-log-erasure.sh"
failures=0
skipped=0

work=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-erasure-selftest.XXXXXX")
case "$work" in
    ""|"/") echo "FATAL: refusing to use sandbox '$work'" >&2; exit 2 ;;
esac
# Resolve the sandbox BEFORE deriving TMPDIR from it.  -P/pwd -P makes the
# root physical, so a symlinked component cannot let an outside path
# string-match the prefix test, and it makes a relative inherited TMPDIR
# absolute -- the gate runs with cd "$root", which would otherwise resolve a
# relative path against the source tree.  Order matters: derive TMPDIR from
# the logical path first and the two disagree, so every scratch directory the
# gate reports fails the prefix test.  That is unconditional on macOS, where
# /var resolves to /private/var and a runner's TMPDIR lives under it.
#
# $1 is deliberately NOT resolved the same way: under_work is called on a
# scratch root the gate has already removed, and `cd` into a gone path would
# reject it.  The residual hole needs a symlink created INSIDE the sandbox;
# nothing here makes one.
work=$(CDPATH= cd -P -- "$work" && pwd -P)
# Re-validate: the guard above ran against the pre-resolution value, and a
# failed `cd` leaves this empty.  Everything downstream would then write to
# absolute paths -- the stub would land at /bin/meson, which succeeds as
# root.  Validating in one place and consuming in another after a rewrite is
# the shape this file has already produced four times.
case "$work" in
    ""|"/") echo "FATAL: sandbox did not resolve to a usable path" >&2; exit 2 ;;
esac
# Point the gate's own scratch at a directory we own, so "outside the source
# tree" is deterministic and TMPDIR handling is exercised at the same time.
export TMPDIR="$work/tmp"
mkdir -p "$TMPDIR" "$work/bin"
# INT/TERM/HUP as well as EXIT, for the same reason the gate under test now
# does it: bash runs no EXIT trap on an uncaught fatal signal, and a killed
# selftest would leave its sandbox behind.
trap 'exit 2' INT TERM HUP
trap 'chmod -R u+w "$work" 2>/dev/null; rm -rf "$work"' EXIT

# The only predicate that may authorise a chmod or an rm in this file.
under_work() {
    # A predicate whose job is authorising rm -rf must not lean on a check in
    # another function: re-assert the root here, reject traversal and relative
    # paths, and only then test containment.  The quoted "$work" makes any glob
    # metacharacter in the sandbox path literal, so a sibling cannot match.
    case "${work:-}" in ""|"/") return 1 ;; esac
    case "$1" in *..*|[!/]*) return 1 ;; esac
    case "$1" in
        "$work"/*) return 0 ;;
        *) return 1 ;;
    esac
}

RECORD="$work/record"
: >"$RECORD"
export RECORD

cat >"$work/bin/meson" <<'STUB'
#!/usr/bin/env bash
# Stub meson.  setup: $2 = build dir, $3 = source root.
# compile: $3 = build dir.
mode="${STUB_MODE:-ok}"
case "$1" in
setup)
    [ "$mode" = setup_fail ] && { echo "stub: setup refused"; exit 1; }
    mkdir -p "$2"
    # One short line, one O_APPEND write: atomic across concurrent stubs.
    printf '%s\n' "$2" >>"$RECORD"
    [ "$mode" = slow ] && sleep 0.3
    exit 0
    ;;
compile)
    [ "$mode" = compile_fail ] && { echo "stub: compile refused"; exit 1; }
    [ "$mode" = noartifact ] && exit 0
    if [ "$mode" = leak ]; then
        # Sentinel first, then bulk, so the needle is found early and the
        # upstream process dies of SIGPIPE under the old pipeline.  ~200 KB is
        # past the 64 KiB pipe buffer, which is what makes this case reproduce
        # the silent-green defect instead of passing over it.
        { echo "wl_log_erasure_sentinel_trace %d"
          head -c 150000 /dev/urandom | base64; } >"$3/libwirelog.so"
    else
        head -c 150000 /dev/urandom | base64 >"$3/libwirelog.so"
    fi
    exit 0
    ;;
esac
exit 0
STUB
chmod +x "$work/bin/meson"
export PATH="$work/bin:$PATH"

run_gate() {
    ( cd "$root" && bash "$gate" ) >"$work/out" 2>"$work/err"
}

expect_status() {
    local name="$1" want="$2" got
    run_gate; got=$?
    if [ "$got" -eq "$want" ]; then
        echo "ok: $name"
    else
        echo "FAIL: $name: expected exit $want, got $got" >&2
        sed 's/^/    /' "$work/err" >&2
        failures=$((failures + 1))
    fi
}

expect_says() {
    local name="$1" needle="$2"
    if grep -Fq -- "$needle" "$work/out" "$work/err"; then
        echo "ok: $name"
    else
        echo "FAIL: $name: output did not mention '$needle'" >&2
        failures=$((failures + 1))
    fi
}

# 1. THE acceptance criterion.  Both assertions are required: "distinct == N"
#    alone passes vacuously at 0 == 0, which is what a gate that aborted before
#    reaching the stub would produce.
: >"$RECORD"
STUB_MODE=slow
export STUB_MODE
pids=""
for i in 1 2 3; do
    ( cd "$root" && bash "$gate" >/dev/null 2>&1 ) &
    pids="$pids $!"
done
concurrent_rc=0
for pid in $pids; do
    wait "$pid" || concurrent_rc=1
done
total=$(wc -l <"$RECORD" | tr -d ' ')
distinct=$(sort -u "$RECORD" | wc -l | tr -d ' ')
if [ "$concurrent_rc" -ne 0 ]; then
    echo "FAIL: concurrent runs: at least one invocation exited non-zero" >&2
    failures=$((failures + 1))
elif [ "$total" -ne 3 ]; then
    echo "FAIL: concurrent runs: expected 3 recorded scratch dirs, got $total (the stub was not reached)" >&2
    failures=$((failures + 1))
elif [ "$distinct" -ne 3 ]; then
    echo "FAIL: concurrent runs: 3 records but only $distinct distinct scratch dirs" >&2
    failures=$((failures + 1))
else
    echo "ok: concurrent runs use distinct scratch directories"
fi
unset STUB_MODE

# 2. A leaked sentinel must fail.
STUB_MODE=leak expect_status "a leaked sentinel exits 1" 1
STUB_MODE=leak run_gate; expect_says "a leaked sentinel says LEAK" "LEAK"

# 3. A clean library passes.
expect_status "a clean library exits 0" 0
run_gate; expect_says "a clean library reports verification" "OK: compile-time erasure verified"

# 4. A setup failure is a setup error, not a leak verdict.
STUB_MODE=setup_fail expect_status "a setup failure exits 2, not 1" 2
STUB_MODE=setup_fail run_gate; expect_says "a setup failure is named" "meson setup failed"

# 5. A compile failure likewise.
STUB_MODE=compile_fail expect_status "a compile failure exits 2" 2

# 6. Compile succeeded but produced nothing.
STUB_MODE=noartifact expect_status "a missing artifact exits 2" 2
STUB_MODE=noartifact run_gate; expect_says "a missing artifact is named as such" "no libwirelog artifact was found"

# 7/8. The scratch directory is removed on success, and was never inside the
#      source tree.  Both read the last recorded path; neither touches it.
: >"$RECORD"
run_gate
last=$(tail -1 "$RECORD")
if [ -n "$last" ] && [ ! -d "$last" ]; then
    echo "ok: the scratch directory is removed after a successful run"
else
    echo "FAIL: scratch directory '$last' survived the run" >&2
    failures=$((failures + 1))
fi
case "$last" in
    "")
        echo "FAIL: no scratch directory was recorded; the location assertion had nothing to test" >&2
        failures=$((failures + 1))
        ;;
    "$root"/*)
        echo "FAIL: scratch directory '$last' is inside the source tree" >&2
        failures=$((failures + 1))
        ;;
    *)
        echo "ok: the scratch directory is outside the source tree"
        ;;
esac

# 9. A cleanup failure is named and exits 2, not 1.  Skipped as root, which
#    ignores the mode bits.  Per-case skip rather than a file-level one, so this
#    file never becomes the always-skip shape.
if [ "$(id -u)" -eq 0 ]; then
    echo "skip: cleanup-failure case (running as root; mode bits are ignored)"
    skipped=1
else
    : >"$RECORD"
    cat >"$work/bin/meson" <<'STUB2'
#!/usr/bin/env bash
case "$1" in
setup)   mkdir -p "$2"; printf '%s\n' "$2" >>"$RECORD"; exit 0 ;;
compile) head -c 1000 /dev/urandom | base64 >"$3/libwirelog.so"; exit 0 ;;
esac
exit 0
STUB2
    chmod +x "$work/bin/meson"
    # Do not chmod anything until the gate has told us where its scratch is and
    # we have confirmed it is inside our sandbox.  A gate that builds elsewhere
    # is a failure to report, not a directory to modify.
    run_gate >/dev/null 2>&1
    probe=$(tail -1 "$RECORD")
    scratch=$(dirname -- "$probe")
    if [ -z "$probe" ] || ! under_work "$scratch"; then
        echo "FAIL: cleanup-failure case: gate scratch '$scratch' is outside the sandbox; refusing to modify it" >&2
        failures=$((failures + 1))
    else
        # Re-run with the scratch root made unwritable while populated; chmod on
        # an EMPTY directory still lets rm -rf succeed, so the build dir must
        # exist first.
        cat >"$work/bin/meson" <<'STUB3'
#!/usr/bin/env bash
case "$1" in
setup)   mkdir -p "$2"; printf '%s\n' "$2" >>"$RECORD"; exit 0 ;;
compile) [ -n "${WORK_SANDBOX:-}" ] || exit 1
         head -c 1000 /dev/urandom | base64 >"$3/libwirelog.so"
         parent="$(dirname -- "$3")"
         case "$parent" in "$WORK_SANDBOX"/*) chmod 500 "$parent" ;; esac
         exit 0 ;;
esac
exit 0
STUB3
        chmod +x "$work/bin/meson"
        WORK_SANDBOX="$work" export WORK_SANDBOX
        : >"$RECORD"
        run_gate; got=$?
        victim=$(dirname -- "$(tail -1 "$RECORD")")
        if under_work "$victim" && [ -d "$victim" ] && [ ! -L "$victim" ]; then
            chmod 700 "$victim" 2>/dev/null || true
            rm -rf "$victim" 2>/dev/null || true
        fi
        # The code alone is not enough: setup, compile and missing-artifact
        # failures all exit 2 as well, so a stub that fails closed would turn
        # this case green without a cleanup failure ever happening.
        if [ "$got" -ne 2 ]; then
            echo "FAIL: a cleanup failure exited $got, expected 2" >&2
            failures=$((failures + 1))
        elif ! grep -Fq -- "could not remove scratch directory" "$work/err"; then
            echo "FAIL: exit 2 but no cleanup failure was reported; the case did not exercise cleanup" >&2
            failures=$((failures + 1))
        else
            echo "ok: a cleanup failure exits 2, not 1"
        fi
    fi
fi

if [ "$failures" -ne 0 ]; then
    if [ "${skipped:-0}" -ne 0 ]; then
        echo "test-check-log-erasure: $failures case(s) failed, ${skipped} skipped" >&2
    else
        echo "test-check-log-erasure: $failures case(s) failed" >&2
    fi
    exit 1
fi
if [ "${skipped:-0}" -ne 0 ]; then
    echo "test-check-log-erasure: OK (${skipped} case(s) skipped)"
else
    echo "test-check-log-erasure: OK"
fi
