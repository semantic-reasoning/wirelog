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
CEILINGS="$work/ceilings"
: >"$RECORD"
: >"$CEILINGS"
export RECORD CEILINGS

cat >"$work/bin/meson" <<'STUB'
#!/usr/bin/env bash
# Stub meson.  setup: $2 = build dir, $3 = source root.
# configure: $2 = build dir.  compile: $3 = build dir.
#
# The ceiling is parsed out of the actual -Dwirelog_log_max_level= argument
# and appended to $CEILINGS, so the selftest can assert the gate asks for
# trace and then error BY NAME.  A setup or configure that carries no such
# argument is a hard error, not a default: without that, a gate edit that
# dropped the flag, or dropped a whole phase, would leave this file green
# while asserting nothing -- which is the #1808 shape one level up.
mode="${STUB_MODE:-ok}"

# ONE pass over argv, for both the ceiling and the refusals.  Two parsers
# disagreed about spelling once already: the refusals learned the two-token
# form and the ceiling lookup did not, so rewriting the gate to `-D <opt>=`
# reported "carried no -Dwirelog_log_max_level" about a command line that
# carried it.  Sharing the walk makes that divergence unrepresentable.
#
# The rule is an ALLOWLIST: the stub accepts the arguments this gate is
# supposed to pass and refuses every other option by name.  It was a
# denylist first, and review widened it four times -- joined spelling, then
# the two-token spelling, then three spellings of `false` -- and it was
# still incomplete, because meson lower-cases a boolean's value, so
# `-Db_lto=fAlSe` disables LTO and got through.  The set a denylist has to
# enumerate is meson's option grammar, which this stub has no business
# modelling.  Under an allowlist an unexpected option is refused whatever it
# means, so -Db_lto= in any spelling, -Dbuildtype=, -Doptimization=,
# --native-file and whatever meson adds next all close under one rule.  The
# matching rule models no part of meson's option grammar; the two facts
# about meson below are about its argument SYNTAX, which the join has to
# mirror, and about why the denylist failed.
#
# Why refuse at all: the gate's whole argument for option (B) is that it
# measures the SHIPPED configuration.  -Db_lto=false would silently turn it
# into the option (A) its own header rejects, and a non-release buildtype
# would measure a build nobody ships.  No stub can observe a build flag's
# EFFECT, so the arguments themselves are what it asserts on.  It runs for
# `configure` as well as `setup`, and that is the point: phase 2's
# configuration is set by `meson configure`, meson applies build options
# there too, and guarding only `setup` left the assertion phase reachable by
# moving a flag one line down.
#
# Tokens are joined before matching because meson accepts `-D <opt>=<val>`
# and `--buildtype <val>` as two tokens and applies them, so the allowlist
# has to see what meson sees.  That is syntax, which is stable, not policy.
#
# What still gets past it: anything that is not an argument.  CFLAGS, CC and
# the rest of the environment are invisible here.
#
# Sets CEILING to the -Dwirelog_log_max_level= value, empty if absent, and
# BUILDTYPES to the number of buildtype specifications seen.
scan_args() {
    verb="$1"; shift
    CEILING=""
    BUILDTYPES=0
    prev=""
    for a in "$@"; do
        if [ -n "$prev" ]; then
            case "$prev" in
                -D)          a="-D$a" ;;
                --buildtype) a="--buildtype=$a" ;;
            esac
            prev=""
        else
            case "$a" in
                -D|--buildtype) prev="$a"; continue ;;
            esac
        fi
        case "$a" in
            -Dwirelog_log_max_level=*)
                CEILING="${a#*=}"
                ;;
            --buildtype=release|-Dbuildtype=release)
                BUILDTYPES=$((BUILDTYPES + 1))
                ;;
            -*)
                echo "stub: meson ${verb} carried an option the gate is not supposed to pass: ${a}" >&2
                echo "      The gate must measure the configuration wirelog ships, so this stub allows only the arguments it needs (#1808)." >&2
                echo "      If the option is deliberate, add it to scan_args' accepted set and record here why it does not move the probe off that configuration." >&2
                return 1
                ;;
        esac
    done
    return 0
}

case "$1" in
setup)
    [ "$mode" = setup_fail ] && { echo "stub: setup refused"; exit 1; }
    scan_args "$@" || exit 1
    lvl="$CEILING"
    if [ -z "$lvl" ]; then
        echo "stub: meson setup carried no -Dwirelog_log_max_level" >&2
        exit 1
    fi
    if [ "$BUILDTYPES" -ne 1 ]; then
        echo "stub: meson setup named ${BUILDTYPES} buildtypes; it must name exactly one, release" >&2
        exit 1
    fi
    mkdir -p "$2"
    # One short line, one O_APPEND write: atomic across concurrent stubs.
    printf '%s\n' "$2" >>"$RECORD"
    printf '%s\n' "$lvl" >>"$CEILINGS"
    printf '%s\n' "$lvl" >"$2/.ceiling"
    [ "$mode" = slow ] && sleep 0.3
    exit 0
    ;;
configure)
    [ "$mode" = configure_fail ] && { echo "stub: configure refused"; exit 1; }
    scan_args "$@" || exit 1
    lvl="$CEILING"
    if [ -z "$lvl" ]; then
        echo "stub: meson configure carried no -Dwirelog_log_max_level" >&2
        exit 1
    fi
    # Phase 2 moves the ceiling and NOTHING else.  A buildtype spec here is
    # refused outright rather than checked for =release: the gate has no
    # reason to change buildtype after setup, so any attempt is an edit that
    # wants reviewing.
    if [ "$BUILDTYPES" -ne 0 ]; then
        echo "stub: meson configure changed buildtype; phase 2 moves the ceiling only" >&2
        exit 1
    fi
    printf '%s\n' "$lvl" >>"$CEILINGS"
    printf '%s\n' "$lvl" >"$2/.ceiling"
    exit 0
    ;;
compile)
    [ "$mode" = compile_fail ] && { echo "stub: compile refused"; exit 1; }
    # Naming the target matters: without it meson builds EVERY target, which
    # is minutes of work against a 180 s timeout rather than one library.
    case " $* " in
        *" wirelog "*) ;;
        *)
            echo "stub: meson compile did not name the wirelog target" >&2
            exit 1
            ;;
    esac
    lvl=$(cat "$3/.ceiling" 2>/dev/null || echo unknown)
    # noartifact removes the artifact only at the error phase, so the control
    # still passes and the case exercises phase 2's discovery path.
    if [ "$mode" = noartifact ] && [ "$lvl" = error ]; then
        rm -f "$3"/libwirelog.so*
        exit 0
    fi
    if [ "$mode" = noartifact_trace ] && [ "$lvl" = trace ]; then
        rm -f "$3"/libwirelog.so*
        exit 0
    fi
    # The sentinel belongs in the library at trace and not at error.  `leak`
    # inverts that for the error build; `no_control` inverts it for trace,
    # which is exactly the state #1808 was about.
    want_sentinel=no
    [ "$lvl" = trace ] && want_sentinel=yes
    [ "$mode" = leak ] && [ "$lvl" = error ] && want_sentinel=yes
    [ "$mode" = no_control ] && [ "$lvl" = trace ] && want_sentinel=no
    if [ "$want_sentinel" = yes ]; then
        # Sentinel first, then bulk, so the needle is found early and the
        # upstream process dies of SIGPIPE under the old pipeline.  ~200 KB is
        # past the 64 KiB pipe buffer, which is what makes the leak case
        # reproduce the silent-green defect instead of passing over it.
        { echo "wl_log_erasure_sentinel_trace %d"
          head -c 150000 /dev/urandom | base64; } >"$3/libwirelog.so"
    else
        head -c 150000 /dev/urandom | base64 >"$3/libwirelog.so"
    fi
    exit 0
    ;;
esac
echo "stub: unrecognised meson verb '$1'" >&2
exit 1
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

# 6b. Phase 1 is a positive control, so a compile that yields no artifact there
#     must also be an error -- and an error, not a leak verdict.  Phase 2's
#     missing artifact (case 6) exits 2 for the same reason: "nothing to
#     search" and "searched and found nothing" must never share an exit code.
STUB_MODE=noartifact_trace \
    expect_status "a missing artifact at ceiling=trace exits 2" 2

# 6c. THE #1808 case.  The sentinel is absent at ceiling=trace, so the probe is
#     dead: its absence at ceiling=error proves nothing.  Before #1808 this was
#     the gate's green path -- LTO dropped the unreferenced sentinel at every
#     ceiling and the gate passed while asserting nothing.  It must now exit 2,
#     NOT 0, and say the positive control failed.
STUB_MODE=no_control expect_status "a dead probe exits 2, not 0" 2
STUB_MODE=no_control run_gate
expect_says "a dead probe names the positive control" "positive control failed"
expect_says "a dead probe says the absence proves nothing" \
    "would prove nothing"

# 6d. meson configure is the step that moves the ceiling from trace to error.
#     If it fails, the second build is still at ceiling=trace and the sentinel
#     is legitimately present; reporting that as a LEAK would be a false alarm.
STUB_MODE=configure_fail expect_status "a configure failure exits 2, not 1" 2

# 6e. The gate must ask for BOTH ceilings BY NAME, in order.  Without this, a
#     future edit that drops -Dwirelog_log_max_level, drops the configure call,
#     or drops a whole phase leaves every case above green while the gate
#     asserts nothing -- the #1808 shape one level up.  The stub exits non-zero
#     on a setup or configure carrying no ceiling, so a dropped flag is caught
#     even if it never reaches this assertion.
: >"$CEILINGS"
run_gate
seq=$(tr '\n' ' ' <"$CEILINGS" | sed 's/ $//')
if [ "$seq" = "trace error" ]; then
    echo "ok: the gate builds at ceiling=trace and then at ceiling=error"
else
    echo "FAIL: ceiling sequence was '$seq', expected 'trace error'" >&2
    failures=$((failures + 1))
fi

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
# Phase-aware like the main stub, for the same reason: an unrecognised verb
# must not fall through to a silent success.
# The same `-D` join as scan_args above -- only that, since these
# fixtures pass no buildtype and read one ceiling.  They drive the real
# gate, so a spelling they did not understand would fail their case for
# a reason that has nothing to do with what the case tests.
lvl_of() {
    prev=""
    for a in "$@"; do
        if [ "$prev" = -D ]; then a="-D$a"; prev=""
        else
            case "$a" in -D) prev=-D; continue ;; esac
        fi
        case "$a" in
            -Dwirelog_log_max_level=*) printf '%s\n' "${a#*=}"; return 0 ;;
        esac
    done
    return 1
}
case "$1" in
setup)     lvl=$(lvl_of "$@") || exit 1
           mkdir -p "$2"; printf '%s\n' "$2" >>"$RECORD"
           printf '%s\n' "$lvl" >"$2/.ceiling"; exit 0 ;;
configure) lvl=$(lvl_of "$@") || exit 1
           printf '%s\n' "$lvl" >"$2/.ceiling"; exit 0 ;;
compile)   if [ "$(cat "$3/.ceiling" 2>/dev/null)" = trace ]; then
               echo "wl_log_erasure_sentinel_trace %d" >"$3/libwirelog.so"
           else
               head -c 1000 /dev/urandom | base64 >"$3/libwirelog.so"
           fi
           exit 0 ;;
esac
exit 1
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
# The same `-D` join as scan_args above -- only that, since these
# fixtures pass no buildtype and read one ceiling.  They drive the real
# gate, so a spelling they did not understand would fail their case for
# a reason that has nothing to do with what the case tests.
lvl_of() {
    prev=""
    for a in "$@"; do
        if [ "$prev" = -D ]; then a="-D$a"; prev=""
        else
            case "$a" in -D) prev=-D; continue ;; esac
        fi
        case "$a" in
            -Dwirelog_log_max_level=*) printf '%s\n' "${a#*=}"; return 0 ;;
        esac
    done
    return 1
}
case "$1" in
setup)     lvl=$(lvl_of "$@") || exit 1
           mkdir -p "$2"; printf '%s\n' "$2" >>"$RECORD"
           printf '%s\n' "$lvl" >"$2/.ceiling"; exit 0 ;;
configure) lvl=$(lvl_of "$@") || exit 1
           printf '%s\n' "$lvl" >"$2/.ceiling"; exit 0 ;;
compile)   [ -n "${WORK_SANDBOX:-}" ] || exit 1
           lvl=$(cat "$3/.ceiling" 2>/dev/null)
           if [ "$lvl" = trace ]; then
               echo "wl_log_erasure_sentinel_trace %d" >"$3/libwirelog.so"
               exit 0
           fi
           head -c 1000 /dev/urandom | base64 >"$3/libwirelog.so"
           # Seal the scratch root only on the LAST build, so the earlier
           # phases run normally and the failure under test is cleanup.
           parent="$(dirname -- "$3")"
           case "$parent" in "$WORK_SANDBOX"/*) chmod 500 "$parent" ;; esac
           exit 0 ;;
esac
exit 1
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
