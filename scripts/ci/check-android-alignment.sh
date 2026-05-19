#!/usr/bin/env bash
# scripts/ci/check-android-alignment.sh - Issue #465 (cross-link #464).
#
# Hard gate: asserts that every PT_LOAD segment in a shared library has
# ELF alignment >= 0x4000 (16 384 bytes, i.e. 16 KB page size).
#
# Background:
#   Android 14+ on arm64 supports both 4 KB and 16 KB page-size kernels.
#   A .so that ships with 4 KB PT_LOAD alignment (0x1000) cannot be
#   loaded on a 16 KB kernel.  The fix is -Wl,-z,max-page-size=16384,
#   wired up in meson.build:91-110 (the wirelog_android_link_args block)
#   and enabled via cross/android-arm64.ini.
#
#   The NDK pin for this project is r27c (reconciled in #464; the issue
#   body's earlier reference to r26d is superseded).
#
#   Canonical recipe: docs/cross-compile-android.md:118-128.
#
# Usage:
#   scripts/ci/check-android-alignment.sh <path/to/libwirelog.so>
#
# Exit codes: 0 on PASS, 1 on FAIL or missing input.
#
# Implementation note - why `readelf -lW` (wide mode):
#   Plain `readelf -l` wraps long lines so each PT_LOAD segment spans two
#   output lines; the Align field lands on the second line and cannot be
#   parsed with a single-line awk rule.  The `-W` flag (wide mode) forces
#   one line per segment, making the Align value reliably the last field.
#
# Why >= 0x4000 (not == 0x4000):
#   A future build with 64 KB page alignment (0x10000) is also correct and
#   must not be rejected.  Using >= future-proofs the gate.
#
# Self-test (negative case -- Linux x86_64 .so uses 0x1000 alignment):
#   meson setup build-host && meson compile -C build-host
#   bash scripts/ci/check-android-alignment.sh build-host/libwirelog.so
#   echo $?   # -> 1, because Linux x86_64 .so uses 0x1000 (4K) PT_LOAD alignment
#
# Self-test (positive case -- Android arm64 .so uses 0x4000 alignment):
#   meson setup build-android-arm64 --cross-file cross/android-arm64.ini -Dandroid=true -Dtests=false -DmbedTLS=disabled
#   meson compile -C build-android-arm64
#   bash scripts/ci/check-android-alignment.sh build-android-arm64/libwirelog.so
#   echo $?   # -> 0

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <path/to/libwirelog.so>" >&2
    exit 1
fi

so_path="$1"

if [ ! -f "$so_path" ]; then
    echo "check-android-alignment: FAIL: file not found: $so_path" >&2
    exit 1
fi

if ! command -v readelf >/dev/null 2>&1; then
    echo "check-android-alignment: FAIL: readelf not found in PATH" >&2
    exit 1
fi

# Minimum required PT_LOAD alignment: 16 KB (16 384 bytes).
# Use >= so that future 64 KB page builds (0x10000) also pass.
min_align=0x4000
min_align_dec=16384

fail=0
min_seen=""

# Parse PT_LOAD lines using wide mode (-lW) so each segment is one line.
# In wide output the Align field is the last column on the LOAD row.
# The awk filter is whitespace-tolerant: some binutils releases pad the
# left margin differently, so `/^[[:space:]]+LOAD[[:space:]]/` is safer
# than a hard-coded two-space prefix.
while IFS= read -r line; do
    # Extract the last whitespace-delimited field (the Align value).
    align_hex=$(printf '%s' "$line" | awk '{print $NF}')

    # Validate the Align format BEFORE handing it to printf.  Bare
    # `printf '%d'` would accept `010` as octal, `4000` as decimal, etc.,
    # which could silently misclassify a future readelf format change.
    if ! printf '%s' "$align_hex" | grep -Eq '^0x[0-9a-fA-F]+$'; then
        echo "check-android-alignment: FAIL: unexpected Align format '$align_hex'" >&2
        echo "  offending line: $line" >&2
        echo "  expected:       0x-prefixed hex (e.g., 0x4000)" >&2
        echo "  (the readelf output format may have changed; investigate)" >&2
        exit 1
    fi

    # Convert hex to decimal for numeric comparison.
    align_dec=$(printf '%d' "$align_hex")

    if [ "$align_dec" -lt "$min_align_dec" ]; then
        echo "check-android-alignment: FAIL: PT_LOAD segment has insufficient alignment" >&2
        echo "  file:         $so_path" >&2
        echo "  offending:    $line" >&2
        echo "  parsed Align: $align_hex ($align_dec bytes)" >&2
        echo "  required:     >= $min_align ($min_align_dec bytes, 16 KB page alignment)" >&2
        echo "" >&2
        echo "Remediation:" >&2
        echo "  Ensure -Wl,-z,max-page-size=16384 is applied at link time." >&2
        echo "  See meson.build:91-110 (wirelog_android_link_args block) and" >&2
        echo "  cross/android-arm64.ini for the supported build path." >&2
        fail=1
    fi

    # Track minimum alignment seen (decimal) for the PASS summary.
    if [ -z "$min_seen" ] || [ "$align_dec" -lt "$min_seen" ]; then
        min_seen="$align_dec"
    fi
done < <(readelf -lW "$so_path" 2>/dev/null | awk '/^[[:space:]]+LOAD[[:space:]]/{print}')

if [ "$fail" -ne 0 ]; then
    exit 1
fi

if [ -z "$min_seen" ]; then
    # An ELF .so with zero PT_LOAD segments is either malformed or
    # produced by a readelf format we can no longer parse.  Treat as
    # FAIL rather than SKIP -- a silent zero-row pass would let a
    # parser regression mask an actually-misaligned build.
    echo "check-android-alignment: FAIL: no PT_LOAD segments parsed from $so_path" >&2
    echo "  (binutils output may have changed format; investigate readelf -lW output)" >&2
    exit 1
fi

# Print min_seen as hex for readability in the PASS line.
min_seen_hex=$(printf '0x%x' "$min_seen")
echo "check-android-alignment: PASS: $so_path (min PT_LOAD Align = $min_seen_hex)"
exit 0
