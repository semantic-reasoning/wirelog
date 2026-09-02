#!/usr/bin/env bash
# check-abi-symbols-locale.sh - Regression for issue #886.
#
# Verifies that the Linux ABI symbol gate stays deterministic under a
# locale whose collation reorders underscores after alphabetic continuation
# (for example en_US.utf8).

set -euo pipefail

# Meson reads exit 77 as SKIP and exit 0 as a pass, so a branch that cannot
# check anything must exit 77 or the gate is recorded as having asserted
# something it never looked at (#1301).  Matches SKIP_EXIT in
# check-clang-tidy-backlog-monotonic.sh and SKIP in run-doop-perf-gate.sh.
#
# 77 is only safe under meson: a bare workflow `run:` step uses `bash -e`,
# where 77 fails the job.  Do not invoke this from one.
SKIP_EXIT=77

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

locale_name=""
for candidate in en_US.utf8 en_US.UTF-8; do
    if locale -a 2>/dev/null | grep -Fxq "$candidate"; then
        locale_name="$candidate"
        break
    fi
done

if [ -z "$locale_name" ]; then
    echo "check-abi-symbols-locale: SKIP: en_US UTF-8 locale not installed"
    exit "$SKIP_EXIT"
fi

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/wirelog-abi-locale.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

build_root="$tmpdir/build"
fakebin="$tmpdir/bin"
mkdir -p "$build_root" "$fakebin"
: > "$build_root/libwirelog.so"

cat > "$fakebin/nm" <<'EOF'
#!/usr/bin/env bash
repo_root="${WIRELOG_REPO_ROOT:?}"
while IFS= read -r sym; do
    [ -z "$sym" ] && continue
    printf '0000000000000000 T %s\n' "$sym"
done < "$repo_root/abi/libwirelog-1.0.symbols"
EOF
chmod +x "$fakebin/nm"

WIRELOG_REPO_ROOT="$repo_root" \
PATH="$fakebin:$PATH" \
LC_ALL="$locale_name" \
    "$repo_root/scripts/ci/check-abi-symbols.sh" "$build_root"

echo "check-abi-symbols-locale: OK under LC_ALL=$locale_name"
