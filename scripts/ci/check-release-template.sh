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
#   - Tag-triggered workflow : intended to enforce; does not.
#                              release-tag.yml (#749 B19) never calls
#                              this script.  It runs `meson test --suite
#                              abi` in the Tag / ABI job, which
#                              `needs: [validate-input]` and so runs in
#                              parallel with the matrix, not after it.
#
#                              TWO separate blockers, both open:
#                              (1) no job supplies a GH_TOKEN, so
#                                  `gh release view` returns nothing;
#                              (2) even with a token it would still skip,
#                                  because the release does not exist
#                                  yet.  Nothing in this repository
#                                  publishes one: today a maintainer runs
#                                  `gh release create` by hand, after the
#                                  tag push that starts this workflow
#                                  (docs/RELEASE_PROCESS.md steps 4-5;
#                                  automation is #1152).  If #1289 lands,
#                                  publication moves into a job that
#                                  needs release-verification, which
#                                  needs abi -- so the gate still runs
#                                  first.  True either way.
#
#                              PR #1289 does NOT fix either; it adds
#                              GH_TOKEN only to the artifacts job and its
#                              own RELEASE_PROCESS.md text tracks this
#                              back to #1288.  Do not assume a token
#                              alone will switch enforcement on.
#
# Callers: meson only (tests/meson.build), which reads 77 as SKIP.  Do
# not invoke this from a bare workflow `run:` step -- GitHub runs those
# under `set -e`, where 77 fails the step rather than skipping it.
#
# Usage:
#   scripts/ci/check-release-template.sh
#
# Exit codes:
#    0 - the release body matches CHANGELOG[X.Y.Z].
#    1 - mismatch between CHANGELOG[X.Y.Z] and the published GitHub
#        Release body, or other hard failure.
#   77 - SKIP; this invocation is not in a context that can enforce.
#        Reported to meson as a skip, not a pass -- see #1288.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

# Meson reads exit 77 as SKIP and exit 0 as a pass, so every branch below that
# cannot enforce must exit 77.  They previously exited 0, which meant this gate
# reported OK on every run while asserting nothing (#1288).
#
# Which branch is taken depends on the context: on PR and main builds HEAD is
# not a tag, so the first one is.  Only release-tag.yml reaches the release
# lookup, and it skips there too -- see the header for the two reasons.  The
# gate therefore enforces nowhere in CI today, and no open PR changes that.
#
# Matches SKIP_EXIT in check-clang-tidy-backlog-monotonic.sh and SKIP in
# run-doop-perf-gate.sh.
SKIP_EXIT=77

# Detect tag context.  We only enforce when HEAD is exactly a vX.Y.Z
# tag.  Outside that context, SKIP cleanly so PR builds and main-
# branch CI do not flake on this gate.
if ! tag=$(git -C "$repo_root" describe --tags --exact-match 2>/dev/null); then
    echo "check-release-template: SKIP: HEAD is not a release tag" >&2
    exit "$SKIP_EXIT"
fi

case "$tag" in
    v[0-9]*)
        version="${tag#v}"
        ;;
    *)
        echo "check-release-template: SKIP: tag '$tag' is not a vX.Y.Z release tag" >&2
        exit "$SKIP_EXIT"
        ;;
esac

if ! command -v gh >/dev/null 2>&1; then
    echo "check-release-template: SKIP: gh CLI not available" >&2
    exit "$SKIP_EXIT"
fi

# Fetch the published release body, if any.  Absent release => SKIP.
release_body=$(gh release view "$tag" --json body --jq .body 2>/dev/null || true)
if [ -z "$release_body" ]; then
    echo "check-release-template: SKIP: no published GitHub Release for $tag yet" >&2
    exit "$SKIP_EXIT"
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
