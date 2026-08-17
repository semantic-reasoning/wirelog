#!/usr/bin/env bash
# scripts/ci/check-clang-tidy-backlog-monotonic.sh - Issue #1100.
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
#
# scripts/ci/clang-tidy-allowlist.txt and clang-tidy-backlog.txt must
# partition the libwirelog.so sources exactly, which is what makes DELETING
# an allowlist line fail check-clang-tidy-ratchet.py.
#
# DEMOTION is the hole that assertion leaves.  Moving a line from the
# allowlist to the backlog keeps the partition exact, so the ratchet still
# passes -- and it is the cheapest possible way to make a regression go
# away.  This check closes it: the backlog is monotonically
# non-increasing, so any line ADDED to it relative to the merge base is a
# failure.
#
# This is a diff over the LIST FILE, not over sources.  A merge-base diff
# of the sources would be wrong (editing a header changes what clang-tidy
# reports in translation units the diff never touches); a merge-base diff
# of a hand-maintained register has no such problem.
#
# The comparison point is the MERGE BASE, not the tip of the base ref.
# Diffing the tip produces a false failure whenever the base advances by
# CLEANING a file: removing a backlog line on main makes that line read as
# an addition on every open branch that has not rebased, in PRs that never
# touched the file.
#
# `git merge-base` is resolved explicitly and the diff runs against the
# WORKING TREE rather than HEAD.  That DIFFERS from the `git diff
# base...HEAD` form used in lint-pr.yml; it is not a superset of it, and
# the two are incomparable:
#
#   - this form additionally catches an UNCOMMITTED demotion, which matters
#     because the ratchet itself analyses the working tree, so the two
#     gates judge the same bytes;
#   - three-dot additionally catches a demotion that was COMMITTED and then
#     reverted in the working tree only, which this form reports as clean.
#     That case does not matter in CI, which always checks out a clean tree
#     -- the pushed branch still carries the commit, so the diff sees it.
#
# Base ref: $WIRELOG_TIDY_BASE_REF, else origin/main.  When the ref or the
# merge base cannot be resolved -- a shallow clone, a tarball export, a
# fresh worktree with no remote -- the check SKIPs rather than inventing a
# verdict, unless WIRELOG_TIDY_REQUIRED=1 (which CI sets): a demotion guard
# that quietly skips in CI is not a guard, so there it is a failure.  CI
# therefore checks out with fetch-depth 0; a shallow clone has no merge
# base and would fail rather than pass.
#
# Exit codes:
#   0  - no entries added to the backlog
#   1  - entries added, or the backlog file is missing
#   77 - SKIP; git history for the base ref is unavailable

set -uo pipefail

SKIP_EXIT=77
required="${WIRELOG_TIDY_REQUIRED:-0}"

# skip <reason...> -- SKIP, or FAIL when the caller forbade skipping.
skip() {
    if [ "$required" = "1" ]; then
        echo "check-clang-tidy-backlog-monotonic: FAIL: $* " \
             "(WIRELOG_TIDY_REQUIRED=1 forbids skipping)" >&2
        exit 1
    fi
    echo "check-clang-tidy-backlog-monotonic: SKIP: $*"
    exit "$SKIP_EXIT"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
backlog_rel="scripts/ci/clang-tidy-backlog.txt"
backlog="$repo_root/$backlog_rel"
base_ref="${WIRELOG_TIDY_BASE_REF:-origin/main}"

if [ ! -f "$backlog" ]; then
    echo "check-clang-tidy-backlog-monotonic: FAIL: missing $backlog" >&2
    exit 1
fi

if ! command -v git > /dev/null 2>&1; then
    skip "git is not on PATH"
fi

if ! git -C "$repo_root" rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    skip "$repo_root is not a git work tree"
fi

if ! git -C "$repo_root" rev-parse --verify --quiet "$base_ref^{commit}" \
        > /dev/null 2>&1; then
    skip "base ref '$base_ref' does not resolve (shallow clone or no" \
         "remote); set WIRELOG_TIDY_BASE_REF to override"
fi

# Resolve the fork point.  Without it there is no honest comparison to make
# -- see the merge-base rationale in the header.
merge_base="$(git -C "$repo_root" merge-base "$base_ref" HEAD 2> /dev/null \
              || true)"
if [ -z "$merge_base" ]; then
    skip "no merge base between '$base_ref' and HEAD (shallow clone?);" \
         "deepen the fetch or set WIRELOG_TIDY_BASE_REF to override"
fi

# The fork point may predate the backlog file itself, in which case git diff
# reports every line as an addition.  Every line being new is exactly right
# for the commit that introduces the register and there is nothing to
# ratchet against, so this is a pass rather than a skip -- it must not turn
# into a failure under WIRELOG_TIDY_REQUIRED=1 on the introducing PR.
#
# This branch is a LOCAL-ONLY weakening: it also returns 0, even under
# WIRELOG_TIDY_REQUIRED=1, for a branch forked before the register existed
# that never merges main, and for a WIRELOG_TIDY_BASE_REF pointed at an old
# tag.  Neither is reachable in CI once #1100 lands -- on a pull_request
# merge ref the merge base is always the base-branch tip, which has the
# file, and WIRELOG_TIDY_BASE_REF is origin/$GITHUB_BASE_REF, never a tag.
if ! git -C "$repo_root" cat-file -e "$merge_base:$backlog_rel" 2> /dev/null
then
    echo "check-clang-tidy-backlog-monotonic: OK ($backlog_rel does not" \
         "exist at the merge base with $base_ref yet; nothing to compare" \
         "against)"
    exit 0
fi

# --unified=0 so context lines cannot be mistaken for additions; '+++' is
# the file header, and comment / blank lines are not entries.
added=$(git -C "$repo_root" diff --unified=0 "$merge_base" -- "$backlog_rel" \
        | sed -n 's/^+\([^+].*\)$/\1/p' \
        | sed 's/[[:space:]]*$//' \
        | grep -v '^#' \
        | grep -v '^$' \
        || true)

if [ -n "$added" ]; then
    echo "check-clang-tidy-backlog-monotonic: FAIL: entries added to" \
         "$backlog_rel relative to the merge base with $base_ref" \
         "($merge_base):" >&2
    echo "" >&2
    printf '  %s\n' $added >&2
    echo "" >&2
    echo "The backlog may only shrink.  If one of these came from the" >&2
    echo "allowlist, the file regressed and the fix is to fix the file, not" >&2
    echo "to demote it.  If it is a genuinely new source that is not clean" >&2
    echo "on arrival, say so explicitly in the PR description; a reviewer" >&2
    echo "has to agree to it." >&2
    exit 1
fi

echo "check-clang-tidy-backlog-monotonic: OK (no entries added vs the merge" \
     "base with $base_ref)"
exit 0
