#!/usr/bin/env bash
# check-wrap-directory-references.sh - Issue #1899.
#
# Assert that every versioned subproject directory a tracked file spells as
# subprojects/NAME-VERSION or subprojects/packagefiles/NAME-VERSION is one a
# wrap declares.
#
# 8ab99d14 bumped subprojects/xxhash.wrap from `directory = xxHash-0.8.3` to
# 0.8.4. #1814 found the .gitignore rule still naming 0.8.3, and #1899 found two
# build-tree library paths doing the same: one in
# scripts/ci/diagnose-arm-sanitizer-timeouts.py and one in
# .github/workflows/diagnose-1575-arm.yml. Each was a hand-kept agreement
# between a version-bearing directory name and a file that spells it, with
# nothing comparing the two. check-subprojects-ignored.sh compares .gitignore;
# this compares the spelled paths in other tracked files.
#
# The declared directories are each tracked subprojects/*.wrap's `directory`
# value under subprojects/, and its `patch_directory` value under
# subprojects/packagefiles/, versioned or not. A token of the form
# subprojects/[packagefiles/]NAME-<digit>... fails unless it is one of them,
# optionally followed by an archive suffix such as .tar.gz; a sentence-ending
# full stop is not part of a token. That catches an old version after a bump,
# a misspelling such as the patch directory's lower-case name used without
# packagefiles/, and a versioned name no wrap declares at all. A bare prefix or
# a glob (subprojects/NAME-*) names no version and is not compared.
#
# What this does NOT see: a directory name assembled from parts
# (subprojects / 'xxHash-0.8.4' in Python) or written without the
# subprojects/ prefix, and anything in a file git grep treats as binary.
# Those still have to be found by hand on a bump.
#
# Also deliberately not checked:
#   - lines whose first non-blank characters are #, //, /* or *: comments here
#     record the history of earlier bumps, this file's included, and naming an
#     old version there is how that history is told. Markdown headings and
#     bullets start the same way and are skipped with them.
#   - CHANGELOG.md, which is history by definition.
#   - test-check-subprojects-ignored.sh and test-check-wrap-directory-
#     references.sh, whose fixtures write wraps of their own; their spellings
#     agree with those fixture wraps, not with the real ones.
#
# Exit codes: 0 = OK, 1 = a stale reference (or a setup error), 77 = SKIP (not
# a git work tree).
#
# 77 is only safe under meson: a bare workflow `run:` step uses `bash -e`,
# where 77 fails the job. Do not invoke this from one.

set -euo pipefail

SKIP_EXIT=77

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"

skip() {
    echo "check-wrap-directory-references: SKIP: $*"
    exit "$SKIP_EXIT"
}

if ! git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    skip "$root is not a git work tree; there are no tracked files to check"
fi

# The declared directories, one path per line, and the wraps declaring them.
declared=""
declared_count=0
while IFS= read -r wrap; do
    [ -n "$wrap" ] || continue
    # `key = value` lines. A trailing `# ...` is dropped from the value: meson
    # itself would keep it, so a wrap carrying one is already broken, and this
    # gate should not fail for a reason the build reports first. `|| [ -n ]`
    # keeps a last line that has no trailing newline, which `read` would
    # otherwise drop -- and with it possibly the only declaration.
    while IFS='=' read -r key value || [ -n "$key" ]; do
        key="$(printf '%s' "$key" | tr -d '[:space:]')"
        value="${value%%#*}"
        value="$(printf '%s' "$value" | tr -d '[:space:]')"
        [ -n "$value" ] || continue
        case "$key" in
            directory) declared="$declared"$'\n'"subprojects/$value" ;;
            patch_directory)
                declared="$declared"$'\n'"subprojects/packagefiles/$value" ;;
            *) continue ;;
        esac
        declared_count=$((declared_count + 1))
    done < "$root/$wrap"
done < <(git -C "$root" ls-files -- 'subprojects/*.wrap')

# A spelled token is accepted if it is a declared directory, or one followed
# by `.` and a non-digit (an archive name such as NAME-1.2.tar.gz).
is_declared() {
    local token="$1" d
    while IFS= read -r d; do
        [ -n "$d" ] || continue
        [ "$token" = "$d" ] && return 0
        case "$token" in
            "$d".[!0-9]*) return 0 ;;
        esac
    done <<<"$declared"
    return 1
}

pattern='subprojects/(packagefiles/)?[A-Za-z][A-Za-z0-9._+-]*-[0-9][A-Za-z0-9._+-]*'
# The caller's git configuration must not change the output shape:
# grep.column inserts a column field and color.grep injects escapes, either of
# which hides comment lines from the check below. `|| rc=$?`: git grep exits 1
# when nothing matches, which is fine, and 128 on an error, which is not.
rc=0
matches="$(git -c grep.column=false -c color.grep=never -C "$root" grep \
    --no-color -nE -e "$pattern" -- . \
    ':(exclude)CHANGELOG.md' \
    ':(exclude)scripts/ci/test-check-subprojects-ignored.sh' \
    ':(exclude)scripts/ci/test-check-wrap-directory-references.sh')" \
    || rc=$?
if [ "$rc" -gt 1 ]; then
    echo "check-wrap-directory-references: ERROR: git grep failed ($rc)" >&2
    exit 1
fi

fail=0
if [ -n "$matches" ]; then
    while IFS= read -r hit; do
        location="${hit%%:*}"
        rest="${hit#*:}"
        location="$location:${rest%%:*}"
        text="${rest#*:}"
        trimmed="${text#"${text%%[![:space:]]*}"}"
        case "$trimmed" in
            '#'*|'//'*|'/*'*|'*'*) continue ;;
        esac
        while IFS= read -r token; do
            # A version does not end in '.', so one ending a sentence is
            # punctuation, not part of the name.
            while [ "${token%.}" != "$token" ]; do token="${token%.}"; done
            if ! is_declared "$token"; then
                echo "check-wrap-directory-references: FAIL: $location names $token, which no subprojects/*.wrap declares" >&2
                fail=1
            fi
        done < <(printf '%s\n' "$text" | grep -oE -e "$pattern")
    done <<<"$matches"
fi

if [ "$fail" -ne 0 ]; then
    exit 1
fi
echo "check-wrap-directory-references: OK; $declared_count declared wrap directories, no undeclared versioned references"
