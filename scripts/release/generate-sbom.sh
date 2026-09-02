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
# $1 is never read: all three syft calls scan $repo_root, not the build tree.
# There are no programmatic callers -- only the recipe in
# docs/SECURITY_MODEL.md 3.1 and the hints in check-sbom-snapshot.sh -- so what
# this preserves is compatibility with humans following docs, not with code.
#
# It stays because REMOVING it would shift $2 into $1, and the documented
# `generate-sbom.sh build` would then set out_dir=build and write the snapshot
# into ./build, silently leaving the committed baseline stale. That is a
# transition hazard rather than an intrinsic one: make-tarball.sh takes its out
# dir at $1, so the two would be consistent afterwards.
#
# Do not "fix" this by pointing syft at $build_root -- a meson build tree
# carries no manifests syft catalogs, and scanning one yields zero artifacts.
# What the SBOM actually describes is the real problem; see #1308.
build_root="${1:?Usage: generate-sbom.sh <build_root> [out_dir]}"
: "$build_root"

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

echo "generate-sbom: Generating SPDX 2.3..."
syft dir:"$repo_root" -o spdx-json@2.3="${output_prefix}.spdx.json"

echo "generate-sbom: Generating CycloneDX 1.5..."
syft dir:"$repo_root" -o cyclonedx-json@1.5="${output_prefix}.cdx.json"

echo "generate-sbom: Updating snapshot baseline..."
# Extract normalized dependency list: name@version:license.
# LC_ALL=C so the committed baseline has one canonical order regardless of the
# operator's locale: glibc's en_US collation ignores leading punctuation, so
# regenerating under it reorders the "./.github/workflows/..." entries and
# buries the real dependency change in unrelated diff noise.
syft dir:"$repo_root" -o syft-json 2>/dev/null \
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
