#!/usr/bin/env bash
# Self-test for check-subprojects-ignored.sh (#1814).
#
# Every case builds a throwaway repository under mktemp -d and passes it to the
# gate as its root. The gate must never be pointed at this repository: one that
# derives its root from its own location checks the real tree, and then every
# case passes for a reason that has nothing to do with the fixture -- the
# failure test-wrap-section-syntax.sh:37-40 records.
#
# The fixture tracks its files BEFORE applying the rule under test, because
# that is the real sequence: a rule changes under files that are already
# tracked. Writing the rule first makes git decline to add them, the tracked
# set is empty, and assertion A has nothing to catch -- measured, and it made
# three negative controls look like gate defects.

set -uo pipefail

# GIT_DIR and GIT_WORK_TREE OVERRIDE `git -C` and a `cd`, they do not
# supplement them. Git exports both inside hooks, and `git bisect run` and
# `git rebase --exec` both set them, so without this every git command below
# would drive the CALLER's repository instead of the fixture -- committing
# their uncommitted work as "fixture". Verified against a victim repo: the
# untracked file was committed to its real HEAD. GIT_INDEX_FILE is worse,
# because it destroys the caller's staging area while this file prints OK.
# The same hazard is written up at test-gate-skip-exit-codes.sh:281 and the
# mitigation below is the one test-check-release-template.sh:94 established.
#
# The identity variables are unset for a different reason than the rest: they
# override `-c user.*`, so an exported empty GIT_AUTHOR_NAME or a malformed
# GIT_COMMITTER_DATE fails the fixture commit outright.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
      GIT_COMMON_DIR GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CEILING_DIRECTORIES \
      GIT_TEMPLATE_DIR GIT_NAMESPACE \
      GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_AUTHOR_DATE \
      GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_COMMITTER_DATE

# The gate runs as a SEPARATE process with plain `git check-ignore`, so the
# `-c` pins on the fixture-building calls below do not reach it: it still reads
# the caller's config. Measured -- a global core.excludesFile listing
# subprojects/ failed 4 of 19 cases on a correct tree.
#
# All FOUR of git's config-injection channels are closed, not the two that are
# obvious. GIT_CONFIG_COUNT with GIT_CONFIG_KEY_n/VALUE_n, and
# GIT_CONFIG_PARAMETERS, each reopened the same hole: 7 of 23 cases failed on a
# correct tree, including the null-control case every other case's meaning
# rests on. And they are the live ones here rather than the theoretical ones --
# on git 2.55 `git bisect run` and `git rebase --exec`, the two scenarios the
# unset list above is written for, propagate GIT_CONFIG_PARAMETERS and do NOT
# propagate GIT_DIR. Same four names as test-release-roundtrip.sh:38 and
# test-version-extraction.sh:28-29.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
unset GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS

root=$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
gate="$root/scripts/ci/check-subprojects-ignored.sh"

work=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-subprojects-selftest.XXXXXX") || exit 1
case "$work" in
    ""|"/")
        echo "test-check-subprojects-ignored: refusing to use work dir '$work'" >&2
        exit 1
        ;;
esac
work=$(CDPATH= cd -P -- "$work" && pwd -P)
case "$work" in
    ""|"/")
        echo "test-check-subprojects-ignored: work dir did not resolve" >&2
        exit 1
        ;;
esac
trap 'rm -rf "$work"' EXIT
trap 'exit 1' INT TERM HUP

mkdir -p "$work/nohooks" "$work/notmpl"

failures=0
cases=0

GOOD='subprojects/*
!subprojects/*.wrap
!subprojects/.wraplock
!subprojects/packagefiles/'

# strip_wraps / strip_tracked: post-build mutators for the floor cases. They
# run before `git add`, so the fixture is committed without those paths.
strip_wraps() {
    rm -f "$1"/subprojects/*.wrap
}
strip_tracked() {
    rm -f "$1"/subprojects/packagefiles/xxhash-0.8.4/LICENSE.build \
          "$1"/subprojects/packagefiles/xxhash-0.8.4/meson_options.txt \
          "$1"/subprojects/packagefiles/xxhash-0.8.4/meson.build \
          "$1"/subprojects/.wraplock
}

# fixture <name> <gitignore-rule> [extra-wrap-body] [mutator]
fixture() {
    local name="$1" rule="$2" extra="${3:-}" mutate="${4:-}"
    local d="$work/$name"
    rm -rf "$d"
    mkdir -p "$d/subprojects/packagefiles/xxhash-0.8.4"
    : >"$d/subprojects/.wraplock"
    printf '[wrap-file]\ndirectory = xxHash-0.8.4\n' >"$d/subprojects/xxhash.wrap"
    printf '[wrap-git]\nrevision = abc\n' >"$d/subprojects/nanoarrow.wrap"
    local f
    for f in meson.build LICENSE.build meson_options.txt; do
        : >"$d/subprojects/packagefiles/xxhash-0.8.4/$f"
    done
    [ -n "$extra" ] && printf '%s' "$extra" >"$d/subprojects/extra.wrap"
    [ -n "$mutate" ] && "$mutate" "$d"
    # These pins close the config that reaches the fixture-BUILDING calls;
    # GIT_CONFIG_GLOBAL/SYSTEM above close what reaches the gate. Each one was
    # measured turning a correct tree into a FAIL naming something other than
    # this gate: a global core.hooksPath (husky, pre-commit) failed 12 of 13
    # cases; an init.templateDir whose info/exclude lists subprojects/ failed
    # 5; a core.excludesFile matching *.wrap failed 1. --no-verify does not
    # cover any of them -- it skips pre-commit and commit-msg, not signing,
    # not reference-transaction, and not the template git init copies in.
    fixture_git() {
        git -C "$d" -c user.email=t@example.invalid -c user.name=t \
            -c commit.gpgsign=false -c core.excludesFile=/dev/null \
            -c core.hooksPath="$work/nohooks" \
            -c core.autocrlf=false -c core.safecrlf=false "$@"
    }
    fixture_git -c init.templateDir="$work/notmpl" init -q >/dev/null 2>&1
    fixture_git add -A >/dev/null 2>&1
    fixture_git commit -qm fixture --no-verify >/dev/null 2>&1
    printf '%s\n' "$rule" >"$d/.gitignore"
    printf '%s' "$d"
}

# expect <name> <want-rc> <description> <rule> [extra-wrap-body] [mutator]
expect() {
    local name="$1" want="$2" desc="$3" rule="$4" extra="${5:-}" mutate="${6:-}"
    local d got
    cases=$((cases + 1))
    d=$(fixture "$name" "$rule" "$extra" "$mutate")
    "$gate" "$d" >"$work/out" 2>"$work/err"
    got=$?
    if [ "$got" -eq "$want" ]; then
        echo "ok: $desc"
    else
        echo "FAIL: $desc: expected exit $want, got $got" >&2
        sed 's/^/    /' "$work/out" "$work/err" >&2
        failures=$((failures + 1))
    fi
}

# says <needle> <description>
says() {
    cases=$((cases + 1))
    if grep -Fq -- "$1" "$work/out" "$work/err"; then
        echo "ok: $2"
    else
        echo "FAIL: $2: output did not mention '$1'" >&2
        failures=$((failures + 1))
    fi
}

# says_not <needle> <description>
#
# For the diagnostic, not the verdict. The culprit list is built by an awk
# filter that drops rows whose pattern is a negation, and that filter has been
# wrong once already: a BRE `grep -v '\t!'` matched the literal characters t!,
# so every innocent row it claimed to drop was printed beside the real culprit
# in identical formatting. Nothing caught it, because no assertion looked at
# what the output does NOT contain.
says_not() {
    cases=$((cases + 1))
    if grep -Fq -- "$1" "$work/out" "$work/err"; then
        echo "FAIL: $2: output listed '$1', which is not a culprit" >&2
        failures=$((failures + 1))
    else
        echo "ok: $2"
    fi
}

# 1. The shipped shape passes. Without this every rejection below could be a
#    gate that fails on everything.
expect good 0 "the deny-by-default rule passes" "$GOOD"

# 2-4. Assertion A: each re-inclusion is load-bearing. A tracked path that the
#      rule swallows must fail, and `git add` would then refuse it by name
#      while `git add -A` would drop it silently.
expect no_wraplock 1 "a rule that swallows the tracked .wraplock fails" \
    'subprojects/*
!subprojects/*.wrap
!subprojects/packagefiles/'
says "tracked paths under subprojects/ are ignored" "the tracked-side failure is named"
# Positive and negative together. says_not alone passes trivially on an empty
# list, so deleting the whole culprit pipeline read exactly like a working
# filter -- the same "absence proves nothing" shape the gate itself guards.
says "subprojects/.wraplock" "the culprit is named in the diagnostic"
says_not "!subprojects/*.wrap" "correctly-visible paths are not listed as culprits"
expect no_wrap_reinclude 1 "a rule that swallows the tracked wrap files fails" \
    'subprojects/*
!subprojects/.wraplock
!subprojects/packagefiles/'
expect no_packagefiles 1 "a rule that swallows tracked packagefiles/ fails" \
    'subprojects/*
!subprojects/*.wrap
!subprojects/.wraplock'

# 5. Assertion C, and THE case for this issue. Every directory that exists
#    today is named, so assertions A and B are satisfied -- this is exactly the
#    pre-#1814 rule, one version bump earlier. Only C rejects it. If this case
#    ever passes, the gate has stopped asserting the thing it was written for.
expect version_pinned 1 "a rule naming today's directories one by one fails" \
    'subprojects/xxHash-0.8.4/
subprojects/packagecache/
subprojects/nanoarrow/
subprojects/xxhash/'
says "rule names particular directories" "the version-pinned rule is named as such"

# 6. Assertion B in isolation: the synthetic probe IS ignored, so C passes, and
#    only the new wrap's unignored directory fails.
expect new_wrap 1 "a wrap whose directory is not ignored fails" \
    'subprojects/xxHash-0.8.4/
subprojects/packagecache/
subprojects/nanoarrow/
subprojects/xxhash/
subprojects/extra/
subprojects/zz-extraction-probe/' '[wrap-file]
directory = notignored
'
says "but .gitignore does not ignore it" "the unignored extraction directory is named"

# 7. packagecache is not a wrap name and not any directory = value, so a probe
#    set built only from the wraps cannot see it (mesonbuild/wrap/wrap.py:377).
expect no_packagecache 1 "an unignored packagecache fails" \
    'subprojects/*
!subprojects/*.wrap
!subprojects/.wraplock
!subprojects/packagefiles/
!subprojects/packagecache/'

# 8. configparser lower-cases keys, so `Directory =` is a real directory key
#    and meson honours it. The gate matches the key case-insensitively; this
#    pins that, because a case-sensitive match would silently stop seeing a
#    legal wrap and the gate would pass while asserting less than it claims.
expect capital_key 1 "a Directory = key is read, not skipped" \
    'subprojects/xxHash-0.8.4/
subprojects/packagecache/
subprojects/nanoarrow/
subprojects/xxhash/
subprojects/extra/
subprojects/zz-extraction-probe/' '[wrap-file]
Directory = NotIgnored
'
says "subprojects/NotIgnored" "the capitalised key's value is the one reported"

# 9. The wrap-NAME probe, which no other case reaches: nanoarrow.wrap sets no
#    `directory`, so meson extracts to the wrap's own name. Every other case
#    puts that name in the rule, so killing this probe survived them all.
expect wrap_name 1 "a wrap name that is not ignored fails" \
    'subprojects/xxHash-0.8.4/
subprojects/packagecache/
subprojects/xxhash/
subprojects/zz-extraction-probe/'
says "subprojects/nanoarrow" "the unignored wrap name is the one reported"

# 10. The two anti-vacuity floors. Both exist so that "nothing ignored" and
#     "nothing scanned" cannot be the same result, and neither was covered:
#     relaxing either one survived every case above.
expect no_wraps 1 "a subprojects/ with no wrap files fails rather than passing" \
    "$GOOD" "" strip_wraps
says "no subprojects/*.wrap to read" "the missing-wraps floor is named"
expect few_tracked 1 "too few tracked paths fails rather than passing" \
    "$GOOD" "" strip_tracked
says "expected at least 3" "the tracked-path floor is named, not the wraps floor"

# 11. The colon delimiter. configparser's delimiters are ('=', ':'), so
#     `directory: x` is a key meson honours. Case 8 exists for the same reason
#     one spelling up -- without a case here, the colon support can be reverted
#     and nothing notices, which is what happened between two review rounds.
expect colon_delim 1 "a directory: key is read, not skipped" \
    'subprojects/xxHash-0.8.4/
subprojects/packagecache/
subprojects/nanoarrow/
subprojects/xxhash/
subprojects/extra/
subprojects/zz-extraction-probe/' '[wrap-file]
directory: ColonValue
'
says "subprojects/ColonValue" "the colon-delimited value is the one reported"

# 12. A path-valued directory is SKIPPED, not failed. meson rejects such a
#     value itself, so probing it would make git exit 128 and this gate would
#     fail a wrap meson never accepts -- the false failure the over-collection
#     claim denies. `.` and `..` are here too because os.path.dirname() returns
#     '' for both, so meson does NOT reject them and only an explicit case
#     catches a skip that stops covering them.
expect path_value 0 "a path-valued directory is skipped, not failed" \
    "$GOOD" '[wrap-file]
directory = ../../escape
'
expect dotdot_value 0 "a .. directory is skipped, not failed" \
    "$GOOD" '[wrap-file]
directory = ..
'

# 13. Not a git work tree: skip with 77, never a pass. Meson reads 0 as a pass,
#    so a gate that cannot look must not exit 0 (#1301).
cases=$((cases + 1))
mkdir -p "$work/norepo"
"$gate" "$work/norepo" >"$work/out" 2>"$work/err"
got=$?
if [ "$got" -eq 77 ]; then
    echo "ok: a non-repository skips with 77 rather than passing"
else
    echo "FAIL: a non-repository exited $got, expected 77" >&2
    failures=$((failures + 1))
fi

# A floor, because "every case passed" and "the case list was emptied" are
# otherwise the same output.
if [ "$cases" -lt 25 ]; then
    echo "test-check-subprojects-ignored: FAIL: ran only $cases assertions (expected at least 25); the case list has changed shape" >&2
    failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
    echo "test-check-subprojects-ignored: $failures case(s) failed" >&2
    exit 1
fi
echo "test-check-subprojects-ignored: OK ($cases assertions)"
