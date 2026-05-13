#!/usr/bin/env bash
# regenerate-abi-manifest.sh - Issue #786 (cross-link #690 libabigail half).
#
# Regenerates the committed `abi/libwirelog-1.0.abi.json` baseline used
# by the `meson test --suite abi:abi_manifest` gate.  Run this whenever
# a deliberate ABI-impacting change lands (new public function, changed
# struct layout, etc.) and commit the diff alongside the PR.
#
# Output flags (chosen to keep the manifest source-controllable):
#
#   --no-show-locs        Drop file/line locations (changes per build).
#   --no-elf-needed       Drop the NEEDED entries (libc soname differs
#                         across distros).
#   --type-id-style hash  Stable hash-based type IDs (vs the default
#                         incremental numbering which renumbers on
#                         every regeneration).
#
# Usage:
#
#   meson setup build
#   meson compile -C build
#   scripts/release/regenerate-abi-manifest.sh [build_root]
#
# `build_root` defaults to `build` if not provided.  Exits non-zero if
# `abidw` is missing or the build is incomplete.
#
# Companion: `scripts/ci/check-abi-manifest.sh` consumes the file this
# script writes.  See `docs/RELEASE_PROCESS.md` §1 for the operator
# procedure.

set -euo pipefail

build_root="${1:-build}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
baseline="$repo_root/abi/libwirelog-1.0.abi.json"

if ! command -v abidw >/dev/null 2>&1; then
    echo "regenerate-abi-manifest: FAIL: abidw not on PATH" >&2
    echo "  install libabigail (Ubuntu: apt-get install -y abigail-tools)" >&2
    exit 1
fi

lib=""
for candidate in \
    "$repo_root/$build_root/libwirelog.so" \
    "$repo_root/$build_root/libwirelog.so.1" ; do
    if [ -e "$candidate" ]; then
        lib="$candidate"
        break
    fi
done

if [ -z "$lib" ]; then
    echo "regenerate-abi-manifest: FAIL: libwirelog.so not found under $build_root" >&2
    echo "  expected: $repo_root/$build_root/libwirelog.so[.1]" >&2
    echo "  did you run \`meson compile -C $build_root\`?" >&2
    exit 1
fi

mkdir -p "$(dirname "$baseline")"

abidw \
    --no-show-locs \
    --no-elf-needed \
    --type-id-style hash \
    --out-file "$baseline" \
    "$lib"

n=$(wc -l < "$baseline" | tr -d ' ')
echo "regenerate-abi-manifest: wrote $baseline ($n lines)"
echo "  review with: git diff -- $baseline"
echo "  commit alongside the API-changing PR."
