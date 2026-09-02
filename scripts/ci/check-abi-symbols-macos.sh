#!/usr/bin/env bash
# Issue #788: advisory macOS/Mach-O export-surface check.
#
# This gate is warning-only for v1.0.  Symbol drift against the platform-
# specific allowlist is reported as a warning and exits 0, and so is a missing
# allowlist -- unlike the enforcing gates, which fail on that.
#
# It does exit 77 when it cannot check at all (host is not macOS, or no dylib
# was built), so meson records those runs as skipped rather than passed
# (#1301).  That is a reporting change, not an enforcement one: nothing this
# gate can actually check has become a failure.
set -euo pipefail

# Meson reads exit 77 as SKIP and exit 0 as a pass, so a branch that cannot
# check anything must exit 77 or the gate is recorded as having asserted
# something it never looked at (#1301).  Matches SKIP_EXIT in
# check-clang-tidy-backlog-monotonic.sh and SKIP in run-doop-perf-gate.sh.
#
# 77 is only safe under meson: a bare workflow `run:` step uses `bash -e`,
# where 77 fails the job.  Do not invoke this from one.
SKIP_EXIT=77

if [ $# -lt 1 ]; then
    echo "usage: $0 <build_root>" >&2
    exit 2
fi

if [ "$(uname -s)" != "Darwin" ]; then
    echo "check-abi-symbols-macos: SKIP: host is not macOS" >&2
    exit "$SKIP_EXIT"
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
    exit "$SKIP_EXIT"
fi

# An advisory pass, not a skip, and deliberately still exit 0.
#
# The rule this file follows: 77 means the environment cannot supply the input
# (not macOS, no dylib built); once the environment can, the gate's own
# severity applies -- exit 1 for the enforcing gates, warn-and-0 for this
# advisory one.  That is why the no-dylib branch above became 77 while this one
# did not.
#
# Neither alternative survives contact with the gate's own behaviour.  Real
# symbol drift is reported as a warning and exits 0, so a missing allowlist --
# strictly less informative than drift -- cannot correctly carry a harsher
# status than the drift itself.  And 77 would mean meson shows SKIP for "no
# allowlist" but OK for "the symbols actually diverged": the worse outcome
# getting the better status.
#
# Note that #1301 is self-contradictory here.  It lists this branch as
# out-of-scope by its `if` line (:46) and as needing exit 1 by that same
# branch's `exit` line (:48).  The out-of-scope reading is the coherent one.
if [ ! -f "$allowlist" ]; then
    warn "allowlist missing: $allowlist (advisory gate: not a failure)"
    exit 0
fi

actual="$(
    nm -gU "$lib" 2>/dev/null \
        | awk '{name = $NF; sub(/^_/, "", name); if (name ~ /^wirelog_/) print name}' \
        | LC_ALL=C sort -u
)"
expected="$(LC_ALL=C sort -u "$allowlist")"

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
echo "  nm -gU $lib | awk '{name = \$NF; sub(/^_/, \"\", name); if (name ~ /^wirelog_/) print name}' | LC_ALL=C sort -u > abi/libwirelog-1.0.macos.symbols" >&2
exit 0
