#!/usr/bin/env bash
# Self-test for check-release-template.sh.
#
# Issue #1288. The gate had five exit-0 paths: one real pass and four skips.
# meson reads exit 0 as a pass, so on every run that could not enforce -- which
# is every run, since no job invoking it supplies a GH_TOKEN -- it reported
# `abi / release_template OK` while asserting nothing. A gate that cannot fail
# and reports success is worse than an absent one, because the green line is
# read as evidence.
#
# The four skip paths now exit 77 (meson's SKIP), matching SKIP_EXIT in
# check-clang-tidy-backlog-monotonic.sh and SKIP in run-doop-perf-gate.sh.
#
# This drives the real script -- copied byte-for-byte into a fixture repo, not
# retyped -- through all seven terminal outcomes, asserting the exact status of
# each. The mismatch case is the one the gate exists for and had never been
# exercised.
set -euo pipefail

# The gate shells out to git and to gh, and asserts against a real tag. Windows
# runners have neither the same git behaviour nor a POSIX PATH to strip down
# for the gh-absent case, so restrict to the platform the gate runs on in CI.
case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "check-release-template self-test: SKIP: needs a POSIX host"; exit 77 ;;
esac
command -v git >/dev/null 2>&1 || {
    echo "check-release-template self-test: SKIP: git not available"; exit 77
}

# Derived, not passed in. Taking a root argument meant normalizing meson's
# Windows drive-letter path, and that normalization ran before the POSIX gate
# above and turned a would-be SKIP into a FAIL. Deriving it needs none of that,
# and the POSIX gate above already exited before this line is reached.
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-release-template.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
# Asserts the exact status. `! cmd` would conflate 1 with 77, which is the
# entire distinction this issue is about.
expect_status() {
    local name=$1 want=$2 got=0
    shift 2
    "$@" >"$tmp/out" 2>"$tmp/err" || got=$?
    if [[ "$got" == "$want" ]]; then
        printf 'check-release-template self-test: ok %s\n' "$name"
    else
        printf 'check-release-template self-test: FAIL %s (want exit %s, got %s)\n' \
            "$name" "$want" "$got" >&2
        sed 's/^/    /' "$tmp/err" >&2
        failures=$((failures + 1))
    fi
}
expect_says() {
    local name=$1 needle=$2
    if grep -qF -e "$needle" "$tmp/out" "$tmp/err"; then
        printf 'check-release-template self-test: ok %s\n' "$name"
    else
        printf 'check-release-template self-test: FAIL %s (no %q in output)\n' \
            "$name" "$needle" >&2
        failures=$((failures + 1))
    fi
}

# Build a fixture repo holding copies of the real scripts. Copies, not
# reimplementations: editing the real gate is what this must react to.
repo="$tmp/repo"
mkdir -p "$repo/scripts/ci" "$repo/scripts/release"
for f in scripts/ci/check-release-template.sh scripts/release/extract-changelog-section.sh; do
    cp "$root/$f" "$repo/$f"
    chmod +x "$repo/$f"
done
gate="$repo/scripts/ci/check-release-template.sh"

# extract-changelog-section.sh emits the `## [x] - date` heading inclusively,
# so a real published body starts with it. A fixture body without the heading
# makes the matching case fail for a reason that has nothing to do with #1288.
heading='## [9.9.9] - 2026-01-01'
body=$heading$'\n### Added\n- A thing.\n\n### Fixed\n- Another thing.'
cat > "$repo/CHANGELOG.md" <<EOF
# Changelog

## [Unreleased]

$body

## [9.9.8] - 2025-12-01
### Added
- Older thing.
EOF

# GIT_DIR and GIT_WORK_TREE override `git -C`, they do not merely supplement it.
# Exported -- which git itself does inside hooks, and which `git bisect run` and
# `git rebase --exec` both do -- every command below would drive the REAL
# repository instead of the fixture: committing the developer's uncommitted work
# as "fixture" and planting a v9.9.9 tag on it. Verified: the victim repo's
# modified file was committed and `git status` came back clean. A stray v9.9.9
# is doubly bad here, being a well-formed vX.Y.Z that puts the real gate into
# enforcement against a release that does not exist.
# The identity variables are unset for a different reason than the rest: they
# override `-c user.*`, so an exported empty GIT_AUTHOR_NAME or a malformed
# GIT_COMMITTER_DATE fails the commit outright. Hooks and rebase do export
# these, with valid values -- but not every caller does.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
      GIT_COMMON_DIR GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CEILING_DIRECTORIES \
      GIT_TEMPLATE_DIR GIT_NAMESPACE \
      GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_AUTHOR_DATE \
      GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_COMMITTER_DATE \
      GH_STUB_BODY

# commit.gpgsign is common on developer machines and --no-verify does not
# disable it (that skips hooks, not signing); without this the fixture commit
# dies with "gpg: signing failed: No secret key" and meson reports FAIL with a
# diagnostic naming neither this gate nor #1288. core.excludesFile is unset for
# the same reason: a global ignore matching scripts/ makes `add -A` stage
# nothing and the commit fails on an otherwise correct tree.
# core.hooksPath needs its own empty directory: --no-verify skips pre-commit and
# commit-msg but NOT reference-transaction, so a globally configured hooks path
# (pre-commit, husky) aborts the fixture commit with exit 128 and a diagnostic
# naming hooks rather than this gate.
#
# init.templateDir needs a separate empty directory, and core.hooksPath does not
# stand in for it: `git init` copies the template's info/exclude into
# .git/info/exclude, which core.excludesFile=/dev/null does NOT override.
# A template excluding everything leaves `add -A` with nothing to stage and the
# commit dies "nothing to commit" -- exit 1, meson FAIL, on a correct tree.
# Verified both ways round.
mkdir -p "$tmp/nohooks" "$tmp/notmpl"
fixture_git() {
    git -C "$repo" -c user.email=t@example.com -c user.name=Test \
        -c commit.gpgsign=false -c tag.gpgSign=false \
        -c core.excludesFile=/dev/null -c core.hooksPath="$tmp/nohooks" \
        -c core.autocrlf=false -c core.safecrlf=false "$@"
}

fixture_git -c init.templateDir="$tmp/notmpl" init -q
fixture_git add -A
fixture_git commit -qm 'fixture' --no-verify

# A stub gh whose reply is whatever GH_STUB_BODY holds; empty means "no
# published release", which is how the real gh behaves via the `|| true`.
stub="$tmp/bin"
mkdir -p "$stub"
cat > "$stub/gh" <<'EOS'
#!/usr/bin/env bash
# Assert the gate invokes gh the way the real one is invoked; a stub that
# ignores its arguments would keep passing after the fetch was rewritten.
case "$*" in
    "release view "*" --json body --jq .body") ;;
    *) echo "gh stub: unexpected invocation: $*" >&2; exit 90 ;;
esac
[ -n "${GH_STUB_BODY:-}" ] || exit 1
printf '%s\n' "$GH_STUB_BODY"
EOS
chmod +x "$stub/gh"
run_gate() { PATH="$stub:$PATH" "$gate"; }

# 1. No tag on HEAD.
expect_status 'an untagged HEAD skips, not passes' 77 run_gate
expect_says   'the untagged skip says so' 'HEAD is not a release tag'

# 2. A tag that is not vX.Y.Z.
fixture_git tag nightly-2026
expect_status 'a non-release tag skips, not passes' 77 run_gate
expect_says   'the non-release-tag skip names the tag' "tag 'nightly-2026' is not"
fixture_git tag -d nightly-2026 >/dev/null

# A v-prefixed but non-numeric tag. nightly-2026 above fails both `v[0-9]*` and
# a widened `v*`, so it cannot tell them apart; this can. Widening the pattern
# would make `vNext` a release with version "Next".
fixture_git tag vNext
expect_status 'a v-prefixed non-numeric tag skips' 77 run_gate
expect_says   'the vNext skip names the tag' "tag 'vNext' is not"
fixture_git tag -d vNext >/dev/null

# 3. A release tag, but no published release.
fixture_git tag v9.9.9
expect_status 'an unpublished release skips, not passes' 77 run_gate
expect_says   'the unpublished skip says so' 'no published GitHub Release'

# 4. gh absent. PATH is rebuilt from just the tools the gate needs, so that
#    `command -v gh` fails without also breaking git or sed.
bare="$tmp/bare"
mkdir -p "$bare"
missing=""
for tool in bash git sed mktemp diff rm dirname awk grep cat env; do
    p=$(command -v "$tool" 2>/dev/null) || { missing="$missing $tool"; continue; }
    ln -sf "$p" "$bare/$tool"
done
if [ -n "$missing" ]; then
    # Not "ok": this case did not run. Reporting a pass for an assertion that
    # never executed is exactly the defect #1288 is about.
    printf 'check-release-template self-test: SKIPPED gh-absent case (%s unavailable)\n' "${missing# }"
else
    no_gh() { env -i PATH="$bare" HOME="$tmp" bash "$gate"; }
    expect_status 'an absent gh skips, not passes' 77 no_gh
    expect_says   'the absent-gh skip says so' 'gh CLI not available'
fi

# 5. The published body matches CHANGELOG[9.9.9]. This is the one path that
#    must still exit 0 -- without it, exiting 77 everywhere would pass.
matching() { GH_STUB_BODY="$body" PATH="$stub:$PATH" "$gate"; }
expect_status 'a matching release body passes' 0 matching
expect_says   'the pass names the version' 'matches CHANGELOG[9.9.9]'

# 6. The published body differs. The case the gate exists to catch, and the one
#    that had never been exercised.
differing() {
    GH_STUB_BODY=$'### Added\n- Something else entirely.' PATH="$stub:$PATH" "$gate"
}
expect_status 'a differing release body fails' 1 differing
expect_says   'the failure names the tag' 'body for v9.9.9 differs'

# Trailing whitespace is normalized away, so a body differing only there must
# still pass -- otherwise the gate fails on cosmetics and gets disabled.
#
# Both sides, deliberately. norm() is applied to each file separately, so
# perturbing only the release body leaves an asymmetric `diff -q <(norm x) y`
# green: the CHANGELOG side would be unasserted, and that is the hand-edited
# file where a stray trailing space actually accumulates.
trailing() {
    GH_STUB_BODY="${body//$'\n'/$'   \n'}" PATH="$stub:$PATH" "$gate"
}
expect_status 'trailing whitespace in the release body still passes' 0 trailing

# Non-blank lines only. Padding a blank separator line turns it into "  ",
# which survives the command substitution that strips trailing newlines, so the
# extracted section gains a line the release body never had. That asymmetry is
# a real (if minor) false-failure mode of the gate -- noted on #1288, not fixed
# here, since it predates this change and is not what this case is asserting.
trailing_changelog() {
    local saved rc
    saved=$(cat "$repo/CHANGELOG.md")
    sed 's/[^[:space:]]$/&  /' <<<"$saved" > "$repo/CHANGELOG.md"
    GH_STUB_BODY="$body" PATH="$stub:$PATH" "$gate"
    rc=$?
    printf '%s\n' "$saved" > "$repo/CHANGELOG.md"
    return $rc
}
expect_status 'trailing whitespace in the CHANGELOG still passes' 0 trailing_changelog

# 7. A published body, but no CHANGELOG section for the version. A hard failure,
#    distinct from the mismatch above, and the one terminal outcome the six
#    cases above never reached.
cp "$repo/CHANGELOG.md" "$tmp/CHANGELOG.bak"
cat > "$repo/CHANGELOG.md" <<'EOF'
# Changelog

## [Unreleased]

## [9.9.8] - 2025-12-01
### Added
- Older thing.
EOF
absent_section() { GH_STUB_BODY="$body" PATH="$stub:$PATH" "$gate"; }
expect_status 'a missing CHANGELOG section fails' 1 absent_section
expect_says   'the missing-section failure names the version' 'no CHANGELOG section for [9.9.9]'
cp "$tmp/CHANGELOG.bak" "$repo/CHANGELOG.md"

# The restore must work, or every assertion added after this point silently
# runs against the truncated CHANGELOG.
expect_status 'the fixture CHANGELOG is restored' 0 matching

# The regression itself, read out of the source, to catch a branch added later
# that the cases above do not reach.
#
# Earlier versions of this check keyed off the house "SKIP:" wording and counted
# echo sites against exit sites. Two independent reviews found that shape wrong
# in both directions: it false-failed a refactor to a `skip()` helper -- the
# exact idiom run-doop-perf-gate.sh uses, which this gate's own comment cites as
# precedent -- while a skip branch worded differently escaped it entirely.
#
# Asserting on `exit 0` itself is free of both faults. The gate has exactly one
# honest exit 0, the success path; any second one is a branch reporting a pass
# it did not earn, whatever it prints and however the skips are spelled.
# Matches `exit 0`, `exit  0` and `exit 0;`. It does NOT match a zero written
# indirectly -- `exit $((0))`, `exit "$OK"` -- so a skip branch spelled that way
# escapes every assertion here. That hole is inherent to reading the source with
# grep, and the alternative this replaced was worse: it keyed on the house SKIP
# wording, so a reworded branch escaped it, and it false-failed the legitimate
# `skip()` helper refactor. The behavioural cases above are the real coverage;
# these three exist to catch a branch they cannot reach.
src=$root/scripts/ci/check-release-template.sh
exit_zero_lines() { grep -nE '^[[:space:]]*exit[[:space:]]+0[[:space:]]*;?[[:space:]]*(#.*)?$' "$src"; }

one_exit_zero() { [[ "$(exit_zero_lines | grep -c .)" == 1 ]]; }
expect_status 'the gate has exactly one exit 0' 0 one_exit_zero

# ...and it is the success path, not a skip that happens to be the only one
# left. Without this, deleting the real pass and keeping one skip at exit 0
# satisfies the count above.
#
# Anchored on the echo rather than a fixed -B window: a comment inserted between
# the echo and its exit would false-fail a -B2 grep, which is the same
# brittleness that sank the assertions this replaced.
exit_zero_is_the_pass() {
    local line
    line=$(exit_zero_lines | head -1 | cut -d: -f1)
    [[ -n "$line" ]] || return 1
    # line-1: the exit itself must not be what the reverse search finds.
    # tail -1 rather than `tac | grep -m1`: tac is GNU-only and absent from the
    # macOS/BSD base system, where this suite also runs. Same "nearest
    # preceding echo/exit", no coreutils dependency.
    head -n "$((line - 1))" "$src" | grep -E '^[[:space:]]*(echo|exit)' | tail -1 \
        | grep -q 'OK; GitHub Release body matches'
}
expect_status 'the one exit 0 is the success path' 0 exit_zero_is_the_pass

# The skip code is meson's, not merely something that is not 0. Tolerates
# surrounding whitespace and a trailing comment, unlike grep -qx.
skip_exit_is_77() { grep -qE '^[[:space:]]*SKIP_EXIT=77[[:space:]]*(#.*)?$' "$src"; }
expect_status "SKIP_EXIT is 77, meson's skip code" 0 skip_exit_is_77

if ((failures)); then
    printf 'check-release-template self-test: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'check-release-template self-test: all cases passed\n'
