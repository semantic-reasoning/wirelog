#!/usr/bin/env bash
# check-release-template.sh - Issue #772 release-template gate.
#
# Asserts the GitHub Releases body for the current tag matches the
# corresponding section of CHANGELOG.md byte-for-byte (modulo
# trailing whitespace), per docs/RELEASE_PROCESS.md section 1.
#
# Intended invocation contexts:
#
#   - PR builds              : SKIP (no tag, no release).
#   - Untagged main builds   : SKIP.
#   - Tag-triggered workflow : enforce.  The release-tag.yml workflow
#                              (#749 B19) calls this script after the
#                              full CI matrix passes.
#
# Usage:
#   scripts/ci/check-release-template.sh
#
# Exit codes:
#   0 - OK or SKIP (with diagnostic to stderr).
#   1 - mismatch between CHANGELOG[X.Y.Z] and the published GitHub
#       Release body, or other hard failure.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

# Detect tag context.  We only enforce when HEAD is exactly a vX.Y.Z
# tag.  Outside that context, SKIP cleanly so PR builds and main-
# branch CI do not flake on this gate.
if ! tag=$(git -C "$repo_root" describe --tags --exact-match 2>/dev/null); then
    echo "check-release-template: SKIP: HEAD is not a release tag" >&2
    exit 0
fi

case "$tag" in
    v[0-9]*)
        version="${tag#v}"
        ;;
    *)
        echo "check-release-template: SKIP: tag '$tag' is not a vX.Y.Z release tag" >&2
        exit 0
        ;;
esac

if ! command -v gh >/dev/null 2>&1; then
    echo "check-release-template: SKIP: gh CLI not available" >&2
    exit 0
fi

# Fetch the published release body, if any.  Absent release => SKIP.
release_body=$(gh release view "$tag" --json body --jq .body 2>/dev/null || true)
if [ -z "$release_body" ]; then
    echo "check-release-template: SKIP: no published GitHub Release for $tag yet" >&2
    exit 0
fi

# Extract the matching CHANGELOG section.
changelog_section=$("$script_dir/../release/extract-changelog-section.sh" \
    "$version" "$repo_root/CHANGELOG.md")
if [ -z "$changelog_section" ]; then
    echo "check-release-template: FAIL: no CHANGELOG section for [$version]" >&2
    echo "  Update CHANGELOG.md with '## [$version] - YYYY-MM-DD' and reset" >&2
    echo "  [Unreleased] per docs/RELEASE_PROCESS.md section 2." >&2
    exit 1
fi

# Normalize trailing whitespace before diffing.
norm() {
    sed -e 's/[[:space:]]*$//' "$1"
}
gh_file=$(mktemp)
cl_file=$(mktemp)
trap 'rm -f "$gh_file" "$cl_file"' EXIT
printf '%s\n' "$release_body"   > "$gh_file"
printf '%s\n' "$changelog_section" > "$cl_file"

if diff -q <(norm "$gh_file") <(norm "$cl_file") >/dev/null 2>&1; then
    echo "check-release-template: OK; GitHub Release body matches CHANGELOG[$version]"
    exit 0
fi

echo "check-release-template: FAIL: GitHub Release body for $tag differs from CHANGELOG[$version]" >&2
echo "" >&2
diff <(norm "$gh_file") <(norm "$cl_file") >&2 || true
echo "" >&2
echo "To fix: regenerate the release body from CHANGELOG via" >&2
echo "" >&2
echo "  gh release edit $tag \\" >&2
echo "      --notes-file <($script_dir/../release/extract-changelog-section.sh $version)" >&2
echo "" >&2
echo "or update CHANGELOG.md to reflect the published release body." >&2
exit 1
