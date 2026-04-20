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
# Sentinels:
#   - "wl_log_erasure_sentinel_trace" -- defined in
#     wirelog/util/log.c:wl_log_erasure_sentinel(). DO NOT reuse this string
#     anywhere that is supposed to survive the ceiling.
#
# Exit codes: 0 = OK, 1 = leak detected, 2 = script setup failure.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
BUILD_DIR="${ROOT}/build-erasure-check"

SENTINELS=(
    "wl_log_erasure_sentinel_trace"
)

rm -rf "${BUILD_DIR}"
# Explicit source-dir arg so the script works when invoked from meson test
# harness (CWD = build/) as well as from the repo root. Without the second
# arg, meson infers source-dir from CWD which is wrong under meson test.
if ! meson setup "${BUILD_DIR}" "${ROOT}" \
        --buildtype=release \
        -Dwirelog_log_max_level=error \
        >/dev/null 2>&1; then
    echo "ERROR: meson setup failed for ${BUILD_DIR}" >&2
    meson setup "${BUILD_DIR}" "${ROOT}" --buildtype=release -Dwirelog_log_max_level=error || true
    exit 2
fi

if ! meson compile -C "${BUILD_DIR}" wirelog >/dev/null 2>&1; then
    echo "ERROR: meson compile -C ${BUILD_DIR} failed" >&2
    meson compile -C "${BUILD_DIR}" wirelog || true
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
    echo "ERROR: libwirelog artifact not found under ${BUILD_DIR}" >&2
    exit 2
fi

FAIL=0
for s in "${SENTINELS[@]}"; do
    if strings "${LIB}" | grep -Fq -- "${s}"; then
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
