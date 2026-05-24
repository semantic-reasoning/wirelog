#!/usr/bin/env bash
# Issue #744: Generate SBOM artifacts (SPDX 2.3 + CycloneDX 1.5).
# This script generates:
#   - wirelog-<version>.spdx.json (SPDX 2.3 format)
#   - wirelog-<version>.cdx.json (CycloneDX 1.5 format)
#   - sbom/snapshot.txt (committed baseline for CI gate)
#
# Requires: syft installed and on PATH.
# See issue #744 and RELEASE_PROCESS.md §2.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
build_root="${1:?Usage: generate-sbom.sh <build_root>}"

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
          | grep -oP "'[^']+'" | tr -d "'" | cut -d'-' -f1)

# Ensure sbom/ directory exists
mkdir -p "$repo_root/sbom"

output_prefix="$repo_root/sbom/wirelog-${version}"

echo "generate-sbom: Generating SPDX 2.3..."
syft dir:"$repo_root" -o spdx-json@2.3="${output_prefix}.spdx.json"

echo "generate-sbom: Generating CycloneDX 1.5..."
syft dir:"$repo_root" -o cyclonedx-json@1.5="${output_prefix}.cdx.json"

echo "generate-sbom: Updating snapshot baseline..."
# Extract normalized dependency list: name@version:license
syft dir:"$repo_root" -o syft-json 2>/dev/null \
  | jq -r '.artifacts[] | "\(.name)@\(.version // "unknown"):\((.licenses // [{}])[0].value // "NOASSERTION")"' \
  | sort > "$repo_root/sbom/snapshot.txt"

# Append nanoarrow commit SHA as a comment (since revision=main has no hash)
nanoarrow_sha=$(git -C "$repo_root/subprojects/nanoarrow" rev-parse HEAD 2>/dev/null || true)
if [ -n "$nanoarrow_sha" ]; then
    printf '# nanoarrow-resolved-sha: %s\n' "$nanoarrow_sha" >> "$repo_root/sbom/snapshot.txt"
fi

echo "generate-sbom: OK; generated:"
echo "  - ${output_prefix}.spdx.json"
echo "  - ${output_prefix}.cdx.json"
echo "  - sbom/snapshot.txt (CI baseline)"
