#!/usr/bin/env bash
# check-abi-symbols.sh - Issue #733 K2 (cross-link #690 B3).
#
# Asserts that the dynamic-symbol table of the just-built libwirelog
# shared library exactly matches the committed allowlist
# `abi/libwirelog-1.0.symbols`.
#
# - The allowlist is the public-API surface frozen for the v1.0
#   ABI: every name on the list is a `WIRELOG_API`-annotated
#   symbol (alias of `WIRELOG_PUBLIC`; see `wirelog/wirelog-export.h`)
#   exported through one of the 9 installed public headers.
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
# Exit codes: 0 on match, 1 on diff or a missing allowlist, 77 on SKIP
#             (no libwirelog.so, or a Windows POSIX layer) -- reported to
#             meson as a skip rather than a pass (#1301).
#
# To regenerate the allowlist after a deliberate API addition:
#
#   meson compile -C build
#   nm -D --defined-only build/libwirelog.so \
#       | awk '$2=="T"{print $3}' | LC_ALL=C sort -u \
#       > abi/libwirelog-1.0.symbols

set -euo pipefail

# Meson reads exit 77 as SKIP and exit 0 as a pass, so a branch that cannot
# check anything must exit 77 or the gate is recorded as having asserted
# something it never looked at (#1301).  Matches SKIP_EXIT in
# check-clang-tidy-backlog-monotonic.sh and SKIP in run-doop-perf-gate.sh.
#
# 77 is only safe under meson: a bare workflow `run:` step uses `bash -e`,
# where 77 fails the job.  Do not invoke this from one.
SKIP_EXIT=77

# A skip is visible but not fatal: a gate that skips on every run asserts
# nothing, it is merely yellow instead of green. WIRELOG_ABI_REQUIRED=1 turns
# every MISSING-PREREQUISITE skip route into a failure, for the one job where
# this gate must actually enforce. Platform- and policy-scope skips are
# deliberately excluded and annotated at their sites: escalating those would
# assert "this job must not run here", a different claim. Default unset, so
# the gate stays advisory everywhere else. Same shape as WIRELOG_TIDY_REQUIRED
# in check-clang-tidy-backlog-monotonic.sh. (#1303) Routed through one helper
# rather than escalating each site: a later skip added straight to `exit
# "$SKIP_EXIT"` would silently opt out of the escalation, which is the defect
# class this issue exists to close.
#
skip() {
    if [ "${WIRELOG_ABI_REQUIRED:-0}" = "1" ]; then
        echo "check-abi-symbols: FAIL: $*" \
             "(WIRELOG_ABI_REQUIRED=1 forbids skipping)" >&2
        exit 1
    fi
    echo "check-abi-symbols: SKIP: $*"
    exit "$SKIP_EXIT"
}

if [ $# -lt 1 ]; then
    echo "usage: $0 <build_root>" >&2
    exit 1
fi

build_root="$1"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
allowlist="$repo_root/abi/libwirelog-1.0.symbols"

case "$(uname -s)" in
  MSYS*|MINGW*|CYGWIN*)
    # Deliberately NOT routed through skip(): this is a PLATFORM SCOPE
    # decision, not a missing prerequisite. WIRELOG_ABI_REQUIRED exists to say
    # "this job must actually check the ABI", and escalating here would instead
    # say "this job must not run on Windows" -- a different claim, and a false
    # failure if the required job ever moves. The job that sets it is
    # ubuntu-latest, so this branch cannot fire there today either way. (#1303)
    echo "check-abi-symbols: SKIP: Windows POSIX layer ($(uname -s)) detected" >&2
    exit "$SKIP_EXIT"
    ;;
esac

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
    skip "libwirelog.so not found in $build_root" \
         "(expected on Windows / static-only / cross builds)"
fi

if [ ! -f "$allowlist" ]; then
    echo "check-abi-symbols: FAIL: allowlist missing: $allowlist" >&2
    exit 1
fi

actual=$(nm -D --defined-only "$lib" 2>/dev/null \
    | awk '$2=="T"{print $3}' | LC_ALL=C sort -u)

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
echo "      | awk '\$2==\"T\"{print \$3}' | LC_ALL=C sort -u \\" >&2
echo "      > abi/libwirelog-1.0.symbols" >&2
echo "" >&2
echo "and commit the diff alongside the API change." >&2
exit 1
