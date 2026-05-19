#!/usr/bin/env bash
# Issue #468: verify iOS static archives retain adapter registration symbols.
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <build_root> [archive]" >&2
    exit 2
fi

build_root="$1"
archive="${2:-}"

if [ -z "$archive" ]; then
    for candidate in \
        "$build_root/libwirelog.a" \
        "$build_root/libwirelog_static.a"; do
        if [ -f "$candidate" ]; then
            archive="$candidate"
            break
        fi
    done
fi

if [ -z "$archive" ] || [ ! -f "$archive" ]; then
    echo "check-ios-symbols: FAIL: static archive not found under $build_root" >&2
    echo "  expected libwirelog.a or libwirelog_static.a after meson compile" >&2
    exit 1
fi

symbols="$(nm -gU "$archive" 2>/dev/null || nm -g "$archive")"

for symbol in \
    wirelog_io_register_adapter \
    wirelog_io_unregister_adapter \
    wirelog_io_find_adapter; do
    if ! printf '%s\n' "$symbols" | awk -v sym="$symbol" \
        '$NF == sym || $NF == "_" sym {found = 1} END {exit !found}'; then
        echo "check-ios-symbols: FAIL: missing symbol $symbol in $archive" >&2
        exit 1
    fi
done

echo "check-ios-symbols: OK: adapter symbols retained in $archive"
