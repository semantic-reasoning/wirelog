#!/usr/bin/env bash
# scripts/ci/check-log-erasure.sh (Issue #287)
#
# Verify that the compile-time ceiling -Dwirelog_log_max_level=error strips
# TRACE-level WL_LOG call sites from libwirelog. This is the structural
# counterpart to the runtime perf gate: where the gate proves that disabled
# sites do not degrade perf, this script proves the sites were removed from
# .rodata entirely.
#
# Strategy, two phases in one scratch build dir:
#   1. Configure and compile at -Dwirelog_log_max_level=trace and assert the
#      sentinel IS present.  This is the positive control.
#   2. `meson configure` the same dir to =error, recompile, and assert the
#      sentinel is absent.
#
# Phase 1 is not ceremony.  Until #1808 this gate had only phase 2, and
# "absent" and "never built" were the same observation: the sentinel is
# unreferenced and the library is built b_lto=true with hidden visibility, so
# LTO dropped it at BOTH ceilings and the gate was green on a library where
# erasure did nothing.  Measured then: ceiling=trace -> 0 occurrences.
#
# OPTION CHOSEN (#1808): (B) anchor the sentinel with WL_LOG_ATTR_USED in
# wirelog/util/log.c.  Rejected: (A) -Db_lto=false on the setup below.  A is
# one line and it works, but it would make this gate evidence about a
# configuration wirelog does not ship -- meson.build sets b_lto=true in
# default_options and releases are built with LTO.  A gate that verifies a
# configuration nobody ships fails in the same direction as no gate, just
# more quietly.
#
# WHAT THIS GATE IS EVIDENCE ABOUT
#
# That under the toolchain on PATH, and the LTO and visibility settings this
# repository actually ships, a WL_LOG site above the ceiling contributes no
# .rodata to libwirelog.  That claim is load-bearing only because phase 1
# shows the same source DOES put the sentinel in the library one ceiling up.
#
# It is NOT evidence about:
#   - any compiler other than the one on PATH.  WL_LOG_ATTR_USED is a no-op
#     under MSVC, so nothing here speaks to the Windows build; the gate is not
#     registered there.
#   - WL_LOG sites other than the sentinel.  This probes the macro's
#     compile-time guard, not an inventory: a site calling wl_log_emit()
#     directly is invisible to it.  The sentinel is also deliberately
#     unrepresentative of the sites it stands for -- it carries
#     WL_LOG_ATTR_USED and no ordinary call site does, which is the whole
#     point but also the limit.
#   - any ceiling except the two endpoints.  Only error and trace are
#     compared, so a regression that mishandles warn, info or debug passes
#     here in silence.
#   - whether anything wirelog ships is actually BUILT at ceiling=error.
#     The gate configures its own probe, so green means the source erases
#     when asked, not that a released artifact was asked.  (android.yml
#     is the only shipping path that passes the flag -- release-tag.yml's
#     default, ABI, mbedTLS and SBOM jobs do not -- and nothing gates that
#     it keeps doing so.  release-tag.yml and perf-nightly.yml also pass it,
#     but for perf evidence rather than for an artifact.)
#   - link-time behaviour of downstream consumers -- a static-archive link or
#     --gc-sections that this project does not perform.
#   - runtime cost of disabled sites.  That is `meson test --suite perf`.
#
# If phase 1 fails the gate exits 2 and says so.  Do NOT "fix" that by
# deleting the control: a silent absence is the state #1808 existed to end.
# If a future toolchain stops honouring `used` under LTO, phase 1 goes red
# for a reason that is not a wirelog regression.  The fallback then is option
# (A) -- add -Db_lto=false to the phase-1 setup -- and to record in this
# header that erasure under LTO is from that point unverified.  Taking A
# silently, without that note, is the failure mode this block exists to
# prevent.  The selftest's meson stub allows only the arguments the setup
# and the `meson configure` below are supposed to pass, and refuses every
# other option by name -- so -Db_lto= is refused however it is spelled, and
# so is anything else that would move the probe off the shipped
# configuration.  Both verbs, because meson applies build options at either,
# and guarding only the setup would have left phase 2 reachable by moving a
# flag one line down.  It is a tripwire against an accidental edit, not a
# proof: the environment -- CFLAGS, CC -- is not an argument and is
# invisible to it.
#
# Phase 1 configures trace with buildtype=release, which meson.build warns
# about.  The warning is expected and is only shown when a phase fails.
#
# Sentinels:
#   - "wl_log_erasure_sentinel_trace" -- defined in
#     wirelog/util/log.c:wl_log_erasure_sentinel(). DO NOT reuse this string
#     anywhere that is supposed to survive the ceiling.
#
# Exit codes: 0 = OK, 1 = leak detected, 2 = script setup failure.
#
# 1 means a leak and nothing else.  Every other failure -- scratch creation,
# meson, a missing artifact, cleanup -- must exit 2, because a bare `set -e`
# abort exits 1 and would be read as an erasure regression.  A leak verdict is
# never downgraded by a later cleanup problem.

set -euo pipefail

# Issue #1799: the scratch build directory is created per invocation under
# TMPDIR.  It used to be "${ROOT}/build-erasure-check", a fixed path derived
# from the source tree rather than from the build directory the gate was
# invoked for, so two concurrent `meson test` runs in one worktree raced on it
# whatever their -C directories were -- one process's `rm -rf` against
# another's ninja.  mktemp -d makes reentrancy a property of the primitive
# rather than of a convention.
erasure_cleanup() {
    # First statement: anything before it clobbers the pending exit status.
    status=$?
    if [ -n "${SCRATCH_ROOT:-}" ] && [ -d "${SCRATCH_ROOT}" ]; then
        # Every branch below is in a condition context.  A bare command
        # returning non-zero here would abort the trap under `set -e` and skip
        # the explicit exit, so the script would exit with the abort status
        # instead of the verdict it computed.
        if ! rm -rf "${SCRATCH_ROOT}"; then
            echo "ERROR: check-log-erasure: could not remove scratch directory ${SCRATCH_ROOT}; remove it by hand" >&2 || true
            if [ "${status}" -eq 0 ]; then
                status=2
            fi
        fi
    fi
    trap - EXIT
    exit "${status}"
}

ROOT="$(git rev-parse --show-toplevel)"

if ! SCRATCH_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/wirelog-log-erasure.XXXXXX")"; then
    echo "ERROR: check-log-erasure: cannot create a scratch directory under ${TMPDIR:-/tmp}" >&2
    exit 2
fi
# `set -u` does not catch an empty value, and this path is passed to `rm -rf`.
case "${SCRATCH_ROOT}" in
    ""|"/")
        echo "ERROR: check-log-erasure: refusing to use scratch path '${SCRATCH_ROOT}'" >&2
        exit 2
        ;;
esac
# Installed immediately after the assignment: earlier and the body would read
# an unset variable, later and any failure in between leaks the directory.
# INT/TERM/HUP first: bash does not run an EXIT trap on an uncaught fatal
# signal, so a `meson test` timeout or a Ctrl-C would otherwise leave the
# scratch directory behind -- and unlike the old fixed path, nothing reclaims
# it on the next run.  Exiting from these handlers makes the EXIT trap fire.
trap 'exit 2' INT TERM HUP
trap erasure_cleanup EXIT
BUILD_DIR="${SCRATCH_ROOT}/build"

SENTINELS=(
    "wl_log_erasure_sentinel_trace"
)

# Helpers.  `set -e` is suppressed inside a function used as an `if`
# condition, so every command below checks its own status explicitly.
find_lib() {
    LIB=""
    for candidate in "${BUILD_DIR}/libwirelog.so" \
                     "${BUILD_DIR}"/libwirelog.so.* \
                     "${BUILD_DIR}/libwirelog.dylib" \
                     "${BUILD_DIR}"/libwirelog.*.dylib \
                     "${BUILD_DIR}/libwirelog.a"; do
        if [ -f "${candidate}" ]; then LIB="${candidate}"; return 0; fi
    done
    echo "ERROR: check-log-erasure: meson compile succeeded but no libwirelog artifact was found under ${BUILD_DIR} (looked for libwirelog.so, libwirelog.so.*, libwirelog.dylib, libwirelog.*.dylib, libwirelog.a)" >&2
    return 1
}

# Capture once, then match the variable.  `strings "$LIB" | grep -Fq ...` is
# not equivalent: grep -q exits at the first match, strings takes SIGPIPE and
# exits 141, and `set -o pipefail` makes the `if` false -- so a library that
# DOES leak is reported "OK: compile-time erasure verified".  Silent green on
# the one check that exists to keep a TRACE site out of a release build.  The
# sibling gate carries the same fix and the same reasoning; see the comment
# above check_leak() in check-no-testhook-in-libwirelog.sh.
# No `|| true` on strings: the sibling gate needs it because its BSD nm
# fallback legitimately exits non-zero, but a strings that starts, emits part
# of the file and dies would leave a TRUNCATED capture, and a sentinel past
# the cut would read as absent.
capture_strings() {
    if ! LIB_STRINGS="$(strings "${LIB}")"; then
        echo "ERROR: check-log-erasure: strings failed on ${LIB}" >&2
        return 1
    fi
    if [ -z "${LIB_STRINGS}" ]; then
        # Silence must not read as "nothing to find": a missing or unusable
        # `strings` would otherwise make every sentinel look absent.
        echo "ERROR: check-log-erasure: strings produced no output for ${LIB}" >&2
        return 1
    fi
    return 0
}

# A here-string, not a pipe: `printf ... | grep -Fq` reintroduces the very
# SIGPIPE bug described above, because printf is then the upstream process
# that dies when grep -q exits early.  Never `grep -c` either: it returns 1
# when the count is zero, which under `set -e` aborts on the expected result.
sentinel_present() {
    grep -Fq -- "$1" <<<"${LIB_STRINGS}"
}

build_at() {
    local ceiling="$1"
    if ! meson compile -C "${BUILD_DIR}" wirelog \
            >"${SCRATCH_ROOT}/compile-${ceiling}.log" 2>&1; then
        echo "ERROR: check-log-erasure: meson compile failed at ceiling=${ceiling}" >&2
        [ -f "${SCRATCH_ROOT}/compile-${ceiling}.log" ] && sed 's/^/    /' "${SCRATCH_ROOT}/compile-${ceiling}.log" >&2 || true
        return 1
    fi
    find_lib || return 1
    capture_strings || return 1
    return 0
}

# Explicit source-dir arg so the script works when invoked from the meson test
# harness (CWD = build/) as well as from the repo root.  Without the second
# arg, meson infers source-dir from CWD which is wrong under meson test.
# Capture once rather than re-running the failed command to show its output:
# a second `meson setup` against a directory the first one partially populated
# fails differently ("Directory is not empty"), so the diagnostic would
# describe the retry instead of the original failure.  The logs live in
# SCRATCH_ROOT rather than BUILD_DIR because meson owns the latter.
#
# PHASE 1 -- positive control at ceiling=trace.
if ! meson setup "${BUILD_DIR}" "${ROOT}" \
        --buildtype=release \
        -Dwirelog_log_max_level=trace \
        >"${SCRATCH_ROOT}/setup.log" 2>&1; then
    echo "ERROR: check-log-erasure: meson setup failed for ${BUILD_DIR}" >&2
    [ -f "${SCRATCH_ROOT}/setup.log" ] && sed 's/^/    /' "${SCRATCH_ROOT}/setup.log" >&2 || true
    exit 2
fi

build_at trace || exit 2

for s in "${SENTINELS[@]}"; do
    if ! sentinel_present "${s}"; then
        echo "ERROR: check-log-erasure: positive control failed -- sentinel '${s}' is absent from ${LIB} at ceiling=trace, where it must be present." >&2
        echo "       The probe is dead, so an absence at ceiling=error would prove nothing." >&2
        echo "       Check that wl_log_erasure_sentinel still carries WL_LOG_ATTR_USED (#1808)." >&2
        exit 2
    fi
done

# PHASE 2 -- the assertion, at ceiling=error.
# `meson configure` rather than a second `meson setup` into a second directory:
# two build dirs would make the selftest's concurrency case record two scratch
# paths per invocation.  No `rm` of the phase-1 artifact is needed -- the
# ceiling is a project-wide argument, so reconfiguring rebuilds every TU and
# relinks.  Deleting it would be worse than useless: the artifact list
# includes a `libwirelog.so.N.p` DIRECTORY, so an `rm -f ...libwirelog.so*`
# returns 1 and `set -e` turns a clean tree into a false LEAK verdict.
if ! meson configure "${BUILD_DIR}" -Dwirelog_log_max_level=error \
        >"${SCRATCH_ROOT}/configure.log" 2>&1; then
    echo "ERROR: check-log-erasure: meson configure to ceiling=error failed" >&2
    [ -f "${SCRATCH_ROOT}/configure.log" ] && sed 's/^/    /' "${SCRATCH_ROOT}/configure.log" >&2 || true
    exit 2
fi

build_at error || exit 2

FAIL=0
for s in "${SENTINELS[@]}"; do
    if sentinel_present "${s}"; then
        echo "LEAK: sentinel '${s}' present in ${LIB} under -Dwirelog_log_max_level=error" >&2
        FAIL=1
    fi
done

if [ "${FAIL}" -ne 0 ]; then
    echo "Compile-time erasure verification FAILED." >&2
    exit 1
fi
echo "OK: compile-time erasure verified (present at ceiling=trace, sentinels absent from ${LIB##*/} at ceiling=error)"
exit 0
