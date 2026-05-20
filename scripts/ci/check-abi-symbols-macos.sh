#!/usr/bin/env bash
# Issue #788: advisory macOS/Mach-O export-surface check.
#
# This gate is warning-only for v1.0.  It reports drift against the
# platform-specific allowlist but always exits 0 unless the script itself is
# invoked incorrectly.
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <build_root>" >&2
    exit 2
fi

if [ "$(uname -s)" != "Darwin" ]; then
    echo "check-abi-symbols-macos: SKIP: host is not macOS" >&2
    exit 0
fi

build_root="$1"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
allowlist="$repo_root/abi/libwirelog-1.0.macos.symbols"

warn() {
    local msg="$1"
    echo "check-abi-symbols-macos: WARNING: $msg" >&2
    echo "::warning file=abi/libwirelog-1.0.macos.symbols,title=macOS ABI advisory::$msg"
}

lib=""
for candidate in \
    "$build_root/libwirelog.1.dylib" \
    "$build_root/libwirelog.dylib" \
    "$build_root"/libwirelog.*.dylib; do
    if [ -e "$candidate" ]; then
        lib="$candidate"
        break
    fi
done

if [ -z "$lib" ]; then
    echo "check-abi-symbols-macos: SKIP: libwirelog dylib not found in $build_root" >&2
    exit 0
fi

if [ ! -f "$allowlist" ]; then
    warn "allowlist missing: $allowlist"
    exit 0
fi

actual="$(
    nm -gU "$lib" 2>/dev/null \
        | awk '{name = $NF; sub(/^_/, "", name); if (name ~ /^wirelog_/) print name}' \
        | sort -u
)"
expected="$(sort -u "$allowlist")"

if [ "$actual" = "$expected" ]; then
    n="$(printf '%s\n' "$actual" | sed '/^$/d' | wc -l | tr -d ' ')"
    echo "check-abi-symbols-macos: OK; $n exported symbols match allowlist"
    exit 0
fi

warn "exported symbols differ from abi/libwirelog-1.0.macos.symbols"
echo "" >&2
diff <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
echo "" >&2
echo "Regenerate after a deliberate public ABI change:" >&2
echo "  nm -gU $lib | awk '{name = \$NF; sub(/^_/, \"\", name); if (name ~ /^wirelog_/) print name}' | sort -u > abi/libwirelog-1.0.macos.symbols" >&2
exit 0
