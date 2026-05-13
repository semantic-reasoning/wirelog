#!/usr/bin/env bash
# check-abi-manifest.sh - Issue #786 (cross-link #690 libabigail half).
#
# Asserts that the just-built libwirelog shared library is ABI-compatible
# with the committed baseline `abi/libwirelog-1.0.abi.json`.  Where the
# `.symbols` allowlist (#733 K2 / `check-abi-symbols.sh`) only checks
# the dynamic-symbol *set*, this gate uses libabigail's `abidiff` to
# catch the richer surface that `nm -D` alone cannot see:
#
#   - struct member offsets and padding,
#   - function signature changes (parameter or return-type),
#   - visibility / linkage attribute drift,
#   - typedef target changes.
#
# Any incompatible-change finding from `abidiff` fails the gate; the
# operator must either:
#
#   (a) revert the offending change, or
#   (b) deliberately bump SOVERSION (out-of-scope for v1.0.x), or
#   (c) regenerate the baseline via
#       `scripts/release/regenerate-abi-manifest.sh` and commit the diff
#       alongside the API-changing PR.
#
# Usage (typically wired through meson, but also runnable standalone):
#
#   scripts/ci/check-abi-manifest.sh <build_root>
#
# Where <build_root> contains `libwirelog.so` (or a SOVERSION-qualified
# link).
#
# Exit codes:
#
#   0   - manifest matches baseline (or SKIPped gracefully).
#   1   - incompatible-change finding (or missing baseline / inputs).
#
# SKIP conditions (exit 0 with diagnostic):
#
#   - `libwirelog.so` absent (Windows, static-only, cross builds).
#   - `abidiff` not on PATH (libabigail not installed).
#   - Baseline `abi/libwirelog-1.0.abi.json` absent (first-time setup;
#     run `scripts/release/regenerate-abi-manifest.sh` to seed).

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <build_root>" >&2
    exit 1
fi

build_root="$1"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
baseline="$repo_root/abi/libwirelog-1.0.abi.json"
suppr="$repo_root/abi/libwirelog-1.0.suppr"

lib=""
for candidate in \
    "$build_root/libwirelog.so" \
    "$build_root/libwirelog.so.1" ; do
    if [ -e "$candidate" ]; then
        lib="$candidate"
        break
    fi
done

if [ -z "$lib" ]; then
    echo "check-abi-manifest: SKIP: libwirelog.so not found in $build_root" >&2
    echo "  (expected on Windows / static-only / cross builds)" >&2
    exit 0
fi

if ! command -v abidiff >/dev/null 2>&1; then
    echo "check-abi-manifest: SKIP: abidiff not on PATH" >&2
    echo "  install libabigail (Ubuntu: apt-get install -y abigail-tools)" >&2
    exit 0
fi

if [ ! -f "$baseline" ]; then
    echo "check-abi-manifest: SKIP: baseline missing: $baseline" >&2
    echo "  first-time setup: run scripts/release/regenerate-abi-manifest.sh" >&2
    echo "  and commit the resulting abi/libwirelog-1.0.abi.json." >&2
    exit 0
fi

# `--suppr` is optional; pass it only if the suppression file exists so
# the gate stays usable for the v1.0 frozen-ABI case where no
# suppressions are needed.
abidiff_args=()
if [ -f "$suppr" ]; then
    abidiff_args+=(--suppr "$suppr")
fi

# abidiff exit codes (libabigail >= 1.8):
#   0  - no diff or compatible-only diff.
#   1  - tool error (bad inputs, parse failure).
#   2  - leaf-changes only (compatible).
#   3  - ABI changes (mix of compatible + incompatible).
#   4  - ABI incompatible changes (the case we fail on).
#   5  - ABI changes carrying both leaf and ABI bits.
#
# Bit 2 (decimal 4) is the "incompatible" flag.  We fail if it's set.
set +e
abidiff "${abidiff_args[@]}" "$baseline" "$lib" 2>&1
rc=$?
set -e

if [ $rc -eq 0 ] || [ $rc -eq 2 ]; then
    echo "check-abi-manifest: OK; libabigail reports no incompatible changes (rc=$rc)"
    exit 0
fi

# Bit 2 (incompatible) set → fail.  Other bits (3, 5, 7) include it.
if [ $((rc & 4)) -ne 0 ]; then
    echo "check-abi-manifest: FAIL: incompatible ABI changes (abidiff rc=$rc)" >&2
    echo "" >&2
    echo "If the change is deliberate (SOVERSION bump or new v-line)," >&2
    echo "regenerate the baseline:" >&2
    echo "" >&2
    echo "  scripts/release/regenerate-abi-manifest.sh $build_root" >&2
    echo "" >&2
    echo "and commit abi/libwirelog-1.0.abi.json alongside the API change." >&2
    exit 1
fi

# Other non-zero (e.g. tool error rc=1): surface to the operator.
echo "check-abi-manifest: FAIL: abidiff returned rc=$rc" >&2
exit 1
