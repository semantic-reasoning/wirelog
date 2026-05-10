#!/usr/bin/env bash
# Issue #635: wirelog_easy_open and wirelog_easy_open_opts must be exported by libwirelog.
set -euo pipefail

BUILD_DIR="${1:-build}"
LIB=""
for candidate in "$BUILD_DIR/libwirelog.so" "$BUILD_DIR"/libwirelog.so.* \
                 "$BUILD_DIR/libwirelog.dylib" "$BUILD_DIR"/libwirelog.*.dylib; do
    if [[ -f "$candidate" ]]; then LIB="$candidate"; break; fi
done
if [[ -z "$LIB" ]]; then
    echo "ERROR: libwirelog shared artifact not found under $BUILD_DIR" >&2
    exit 2
fi

nm_dynamic_defined() {
    if nm -D --defined-only "$1" 2>/dev/null; then
        return 0
    fi
    nm "$1" 2>/dev/null | awk '$2 != "U" && $2 != "u"'
}

SYMS="$(nm_dynamic_defined "$LIB")"
# Match either bare or underscore-prefixed symbols so the same probe
# works for ELF (Linux: "wirelog_easy_open") and Mach-O (macOS: "_wl_easy_open").
for sym in wirelog_easy_open wirelog_easy_open_opts; do
    if ! printf '%s\n' "$SYMS" | awk -v sym="$sym" \
        '$NF == sym || $NF == "_" sym {found = 1} END {exit !found}'; then
        echo "ERROR: missing exported symbol $sym in $LIB" >&2
        exit 1
    fi
done

echo "OK: wirelog_easy_open and wirelog_easy_open_opts exported by $LIB"
