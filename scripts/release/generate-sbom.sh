#!/usr/bin/env bash
# Issue #744: Generate SBOM artifacts (SPDX 2.3 + CycloneDX 1.5).
# This script generates:
#   - wirelog-<version>.spdx.json (SPDX 2.3 format)
#   - wirelog-<version>.cdx.json (CycloneDX 1.5 format)
#   - sbom/snapshot.txt (committed baseline for CI gate)
#
# usage: generate-sbom.sh <build_root> [out_dir]
#   out_dir defaults to <repo_root>/sbom, which is what every caller wants;
#   it is parameterised so the script can be exercised by a test (#1293).
#
# Requires: syft installed and on PATH.
# See issue #744; the invocation recipe is docs/SECURITY_MODEL.md 3.1.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
# $1 is read for exactly one thing: it names a tree to leave OUT of the scan.
# All three syft calls still scan $repo_root, never the build tree.
#
# Removing the parameter would shift $2 into $1, and the documented
# `generate-sbom.sh build` would then set out_dir=build and write the snapshot
# into ./build, silently leaving the committed baseline stale. That is a
# transition hazard rather than an intrinsic one: make-tarball.sh takes its out
# dir at $1, so the two would be consistent afterwards.
#
# Do not "fix" this by pointing syft AT $build_root -- a meson build tree
# carries no manifests syft catalogs, and scanning one yields zero artifacts.
# What the SBOM actually describes is the real problem; see #1308. That zero is
# also why excluding it changes no output: it only stops syft reading gigabytes
# of instrumented binaries to catalog nothing, which is what timed the CI gate
# out in #1917.
build_root="${1:?Usage: generate-sbom.sh <build_root> [out_dir]}"

# The output directory, defaulting to today's behaviour so no caller changes.
# It exists so the script can be run in a test at all: writing to $repo_root/sbom
# unconditionally meant invoking the real generator would clobber the committed
# drift baseline, so #1291's locale guard could only assert statically that the
# LC_ALL=C pin is present in the source. It could not catch a pipeline rewritten
# to reorder after a correctly pinned sort, because it never ran this script.
# ${2-...} not ${2:-...}: an explicitly EMPTY second argument must not fall back
# to the committed baseline. A caller whose computed out_dir came out empty
# would otherwise write into $repo_root/sbom -- the clobber this parameter
# exists to prevent -- instead of failing at mkdir.
out_dir="${2-$repo_root/sbom}"

# Verify syft is available
if ! command -v syft >/dev/null 2>&1; then
    echo "generate-sbom: FAIL: syft not on PATH" >&2
    echo "  Install: brew install syft  (macOS)" >&2
    echo "  Or: https://github.com/anchore/syft/releases" >&2
    exit 1
fi

# Extract version from meson.build (single source of truth)
# Format: version: 'X.Y.Z-dev' or 'X.Y.Z-rcN' or 'X.Y.Z'
# We strip the -dev/-rcN suffix and use major.minor.patch for filename.
version=$(grep "^  version:" "$repo_root/meson.build" | head -1 \
          | sed -E "s/.*'([^']+)'.*/\1/" | cut -d'-' -f1)

# Ensure sbom/ directory exists
mkdir -p "$out_dir"

output_prefix="$out_dir/wirelog-${version}"

# Release verification checks out the workflow revision under a nested
# workflow-tools/ directory. Exclude that checkout's files from the tagged
# source inventory. The workflow cataloger still reports the local-action
# declaration in release-tag.yml, which remains a legitimate snapshot entry.
# The gate diffs what this writes, so check-sbom-snapshot.sh must use the SAME
# scan boundary; test-generate-sbom.sh asserts both spellings together.
# An array, not a string: a repo path may contain spaces. Plain `+=` and no
# `readarray` -- macOS ships bash 3.2, where readarray/mapfile do not exist.
syft_exclude_args=(--exclude '**/workflow-tools/**')
if [ -d "$build_root" ]; then
    build_abs="$(cd "$build_root" && pwd)"
    case "$build_abs" in
        "$repo_root"/*)
            # MUST be `./`-relative: syft resolves --exclude for a `dir:` scan
            # against the scan root, and an ABSOLUTE path matches everything --
            # which would empty the snapshot instead of trimming it.
            syft_exclude_args+=(--exclude "./${build_abs#"$repo_root"/}/**")
            ;;
    esac
fi

syft_scan() {
    syft dir:"$repo_root" "${syft_exclude_args[@]}" "$@"
}

echo "generate-sbom: Generating SPDX 2.3..."
syft_scan -o spdx-json@2.3="${output_prefix}.spdx.json"

echo "generate-sbom: Generating CycloneDX 1.5..."
syft_scan -o cyclonedx-json@1.5="${output_prefix}.cdx.json"

echo "generate-sbom: Updating snapshot baseline..."
# Extract normalized dependency list: name@version:license.
# LC_ALL=C so the committed baseline has one canonical order regardless of the
# operator's locale: glibc's en_US collation ignores leading punctuation, so
# regenerating under it reorders the "./.github/workflows/..." entries and
# buries the real dependency change in unrelated diff noise.
syft_scan -o syft-json 2>/dev/null \
  | jq -r '.artifacts[] | "\(.name)@\(.version // "unknown"):\((.licenses // [{}])[0].value // "NOASSERTION")"' \
  | LC_ALL=C sort > "$out_dir/snapshot.txt"

# Append the resolved nanoarrow commit SHA as a comment for provenance.
nanoarrow_sha=$(git -C "$repo_root/subprojects/nanoarrow" rev-parse HEAD 2>/dev/null || true)
if [ -n "$nanoarrow_sha" ]; then
    printf '# nanoarrow-resolved-sha: %s\n' "$nanoarrow_sha" >> "$out_dir/snapshot.txt"
fi

echo "generate-sbom: OK; generated:"
echo "  - ${output_prefix}.spdx.json"
echo "  - ${output_prefix}.cdx.json"
echo "  - $out_dir/snapshot.txt (CI baseline)"
