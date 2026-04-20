#!/usr/bin/env bash
# Issue #287: libwirelog must NOT contain testhook symbols, and its wl_log_emit
# must originate from log_emit.c (not log_testhook.c). Soft NOTICE on stripped
# builds (no debug info).
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
BUILD_DIR="${1:-$ROOT/build}"
LIB=""
for candidate in "$BUILD_DIR/libwirelog.so" "$BUILD_DIR"/libwirelog.so.* \
                 "$BUILD_DIR/libwirelog.dylib" "$BUILD_DIR"/libwirelog.*.dylib \
                 "$BUILD_DIR/libwirelog.a"; do
    if [[ -f "$candidate" ]]; then LIB="$candidate"; break; fi
done
if [[ -z "$LIB" ]]; then
    echo "ERROR: libwirelog artifact not found under $BUILD_DIR" >&2
    exit 2
fi

# Portable "defined symbols" wrapper. GNU binutils nm supports --defined-only;
# BSD nm (macOS) does not. On BSD, undefined symbols have 'U' in the type
# column and can be filtered via awk. Works on both toolchains.
nm_defined() {
    if nm --defined-only "$1" 2>/dev/null; then
        return 0
    fi
    nm "$1" 2>/dev/null | awk '$2 != "U" && $2 != "u"'
}

# (a) No testhook-branded symbols may be defined in production.
if nm_defined "$LIB" | grep -qE 'wl_log_test_last|wl_log_test_count|wl_log_testhook|__log_testhook'; then
    echo "LEAK: testhook symbols present in $LIB" >&2
    nm_defined "$LIB" | grep -E 'wl_log_test_last|wl_log_test_count|wl_log_testhook|__log_testhook' >&2 || true
    exit 1
fi

# (b) wl_log_emit provenance = log_emit.c. GNU binutils --line-numbers only;
# BSD nm has no equivalent, so this degrades to a soft NOTICE on macOS.
PROV="$(nm --line-numbers --defined-only "$LIB" 2>/dev/null \
        | awk '$NF ~ /:[0-9]+$/ && $0 ~ / wl_log_emit$/ {print $NF; exit}')"
if [[ -z "$PROV" ]]; then
    echo "NOTICE: wl_log_emit symbol not resolvable or stripped build; provenance check skipped"
    exit 0
fi
case "$PROV" in
    *log_emit.c*)
        echo "OK: wl_log_emit provenance = $PROV"
        ;;
    *log_testhook.c*)
        echo "LEAK: wl_log_emit in $LIB originates from log_testhook.c ($PROV)" >&2
        exit 1
        ;;
    *)
        echo "NOTICE: wl_log_emit provenance unverifiable ($PROV)"
        ;;
esac
exit 0
