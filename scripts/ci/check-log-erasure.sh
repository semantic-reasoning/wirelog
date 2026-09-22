#!/usr/bin/env bash
# scripts/ci/check-log-erasure.sh (Issue #287)
#
# Verify that the compile-time ceiling -Dwirelog_log_max_level=error strips
# TRACE-level WL_LOG call sites from libwirelog. This is the structural
# counterpart to the runtime perf gate: where the gate proves that disabled
# sites do not degrade perf, this script proves the sites were removed from
# .rodata entirely.
#
# Strategy:
#   1. Configure an isolated build dir with -Dwirelog_log_max_level=error
#      and --buildtype=release.
#   2. Compile the wirelog library.
#   3. Assert sentinel format strings that appear ONLY in TRACE-level
#      WL_LOG(...) call sites are absent from the binary's .rodata.
#
# LIMITATION (#1808): under the project default b_lto=true the sentinel
# function is unreferenced and hidden, so LTO drops it and its format string at
# BOTH ceilings.  This gate therefore cannot currently fail on a real leak.
# Measured: LTO on + ceiling=trace -> 0 occurrences; LTO off + ceiling=trace
# -> 1.  Do not read a green run as evidence that erasure works until #1808 is
# closed.  The repo's standard remedy is override_options: ['b_lto=false'].
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

SCRIPT_DIR="$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
if ! ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel 2>/dev/null)"; then
    echo "ERROR: check-log-erasure: could not resolve the source root from ${SCRIPT_DIR}" >&2
    exit 2
fi

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

# Explicit source-dir arg so the script works when invoked from meson test
# harness (CWD = build/) as well as from the repo root. Without the second
# arg, meson infers source-dir from CWD which is wrong under meson test.
# Capture once rather than re-running the failed command to show its output:
# a second `meson setup` against a directory the first one partially populated
# fails differently ("Directory is not empty"), so the diagnostic would
# describe the retry instead of the original failure.  The logs live in
# SCRATCH_ROOT rather than BUILD_DIR because meson owns the latter.
if ! meson setup "${BUILD_DIR}" "${ROOT}" \
        --buildtype=release \
        -Dwirelog_log_max_level=error \
        >"${SCRATCH_ROOT}/setup.log" 2>&1; then
    echo "ERROR: check-log-erasure: meson setup failed for ${BUILD_DIR}" >&2
    [ -f "${SCRATCH_ROOT}/setup.log" ] && sed 's/^/    /' "${SCRATCH_ROOT}/setup.log" >&2 || true
    exit 2
fi

if ! meson compile -C "${BUILD_DIR}" wirelog >"${SCRATCH_ROOT}/compile.log" 2>&1; then
    echo "ERROR: check-log-erasure: meson compile failed for ${BUILD_DIR}" >&2
    [ -f "${SCRATCH_ROOT}/compile.log" ] && sed 's/^/    /' "${SCRATCH_ROOT}/compile.log" >&2 || true
    exit 2
fi

LIB=""
for candidate in "${BUILD_DIR}/libwirelog.so" \
                 "${BUILD_DIR}"/libwirelog.so.* \
                 "${BUILD_DIR}/libwirelog.dylib" \
                 "${BUILD_DIR}"/libwirelog.*.dylib \
                 "${BUILD_DIR}/libwirelog.a"; do
    if [[ -f "${candidate}" ]]; then LIB="${candidate}"; break; fi
done
if [[ -z "${LIB}" ]]; then
    echo "ERROR: check-log-erasure: meson compile succeeded but no libwirelog artifact was found under ${BUILD_DIR} (looked for libwirelog.so, libwirelog.so.*, libwirelog.dylib, libwirelog.*.dylib, libwirelog.a)" >&2
    exit 2
fi

# Capture once, then match the variable.  `strings "$LIB" | grep -Fq ...` is
# not equivalent: grep -q exits at the first match, strings takes SIGPIPE and
# exits 141, and `set -o pipefail` makes the `if` false -- so a library that
# DOES leak is reported "OK: compile-time erasure verified".  Silent green on
# the one check that exists to keep a TRACE site out of a release build.  The
# sibling gate carries the same fix and the same reasoning; see the comment
# above check_leak() in check-no-testhook-in-libwirelog.sh.  Reproduced on this
# gate's own artifact, which is already past the 64 KiB pipe buffer.
# No `|| true` here: the sibling gate needs it because its BSD nm fallback
# legitimately exits non-zero, but a strings that starts, emits part of the
# file and dies would leave a TRUNCATED capture, and a sentinel past the cut
# would read as absent.
if ! LIB_STRINGS="$(strings "${LIB}")"; then
    echo "ERROR: check-log-erasure: strings failed on ${LIB}" >&2
    exit 2
fi
if [ -z "${LIB_STRINGS}" ]; then
    # Silence must not read as "nothing to find": a missing or unusable
    # `strings` would otherwise make every sentinel look absent.
    echo "ERROR: check-log-erasure: strings produced no output for ${LIB}" >&2
    exit 2
fi

FAIL=0
for s in "${SENTINELS[@]}"; do
    # A here-string, not a pipe: `printf ... | grep -Fq` reintroduces the
    # very SIGPIPE bug described above, because printf is then the upstream
    # process that dies when grep -q exits early.
    if grep -Fq -- "${s}" <<<"${LIB_STRINGS}"; then
        echo "LEAK: sentinel '${s}' present in ${LIB} under -Dwirelog_log_max_level=error" >&2
        FAIL=1
    fi
done

if [[ ${FAIL} -ne 0 ]]; then
    echo "Compile-time erasure verification FAILED." >&2
    exit 1
fi
echo "OK: compile-time erasure verified (sentinels absent from ${LIB##*/})"
exit 0
