#!/usr/bin/env bash
# check-abi-symbols.sh - Issue #733 K2 (cross-link #690 B3).
#
# Asserts that the dynamic-symbol table of the just-built libwirelog
# shared library exactly matches the committed allowlist
# `abi/libwirelog-1.0.symbols`.
#
# - The allowlist is the public-API surface frozen for the v1.0
#   ABI: every name on the list is a `WIRELOG_PUBLIC`-annotated
#   symbol exported through one of the 9 installed public headers.
# - Any addition of a new public symbol must update the allowlist
#   in the same PR (the gate fails otherwise).
# - Any accidental loss of an exported symbol (e.g. an annotation
#   reverted in error) trips the gate and points at the missing
#   name.
# - Internal symbols (`wl_*`, `col_*`, `arr_*`, etc.) must NOT
#   appear in the dynamic-symbol table; the v0.40 visibility flip
#   in #733 K1 enforced this via `gnu_symbol_visibility: 'hidden'`.
#
# Usage (typically wired through meson, but also runnable
# standalone):
#
#   scripts/ci/check-abi-symbols.sh <build_root>
#
# Where <build_root> contains `libwirelog.so` (or a SOVERSION-
# qualified link).
#
# Exit codes: 0 on match, 1 on diff or missing inputs.
#
# To regenerate the allowlist after a deliberate API addition:
#
#   meson compile -C build
#   nm -D --defined-only build/libwirelog.so \
#       | awk '$2=="T"{print $3}' | sort -u \
#       > abi/libwirelog-1.0.symbols

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <build_root>" >&2
    exit 1
fi

build_root="$1"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
allowlist="$repo_root/abi/libwirelog-1.0.symbols"

# Skip on platforms where libwirelog.so doesn't exist (Windows,
# static-only builds).  The macOS dylib has different naming; this
# gate is Linux/ELF-focused at v1.0 -- matching #690 B3's stated
# Linux x86_64 + arm64 scope.
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
    echo "check-abi-symbols: SKIP: libwirelog.so not found in $build_root" >&2
    echo "  (expected on Windows / static-only / cross builds)" >&2
    exit 0
fi

if [ ! -f "$allowlist" ]; then
    echo "check-abi-symbols: FAIL: allowlist missing: $allowlist" >&2
    exit 1
fi

actual=$(nm -D --defined-only "$lib" 2>/dev/null \
    | awk '$2=="T"{print $3}' | sort -u)

expected=$(cat "$allowlist")

if [ "$actual" = "$expected" ]; then
    n=$(printf '%s\n' "$actual" | wc -l)
    echo "check-abi-symbols: OK; $n exported symbols match allowlist"
    exit 0
fi

echo "check-abi-symbols: FAIL; exported symbols differ from $allowlist" >&2
echo "" >&2
diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
echo "" >&2
echo "To accept the new symbol set (after a deliberate ABI-impacting" >&2
echo "PR), regenerate the allowlist:" >&2
echo "" >&2
echo "  nm -D --defined-only $lib \\" >&2
echo "      | awk '\$2==\"T\"{print \$3}' | sort -u \\" >&2
echo "      > abi/libwirelog-1.0.symbols" >&2
echo "" >&2
echo "and commit the diff alongside the API change." >&2
exit 1
