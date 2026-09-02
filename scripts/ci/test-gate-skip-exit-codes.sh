#!/usr/bin/env bash
# Shared self-test for the meson-registered shell gates' skip contract.
#
# Issue #1301. meson reads exit 0 as a pass and 77 as a skip, so a gate that
# prints "SKIP:" and then exits 0 is recorded as having asserted something it
# never checked. #1288 fixed one such gate; this covers the rest, and is shared
# rather than per-gate so that a gate added later cannot quietly reintroduce the
# defect -- the coverage assertion below fails if a meson-registered check
# script is neither covered here nor explicitly exempted with a reason.
#
# Two kinds of assertion, deliberately:
#
#   - behavioural, where the skip can be provoked (no library, wrong host, a
#     tool absent from PATH). These are the real ones: they run the gate.
#   - source-level, as a backstop for branches that cannot be provoked from
#     here (a missing en_US locale on a host that has it). These only catch a
#     literal `exit 0`, which is stated where it matters.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-gate-skip-exit-codes: SKIP: needs a POSIX host"; exit 77 ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-gate-skip.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

failures=0
last_case=""
expect_status() {
    local name=$1 want=$2 got=0
    shift 2
    last_case=$name
    "$@" >"$tmp/out" 2>"$tmp/err" || got=$?
    if [[ "$got" == "$want" ]]; then
        printf 'test-gate-skip-exit-codes: ok %s\n' "$name"
    else
        printf 'test-gate-skip-exit-codes: FAIL %s (want exit %s, got %s)\n' "$name" "$want" "$got" >&2
        sed 's/^/    /' "$tmp/out" "$tmp/err" >&2
        failures=$((failures + 1))
    fi
}
# Asserts against the output of the most recent expect_status, whose name it
# prints so that a misordered pair is visible rather than silently checking the
# wrong command's output.
expect_says() {
    local name=$1 needle=$2
    if grep -qF -e "$needle" "$tmp/out" "$tmp/err"; then
        printf 'test-gate-skip-exit-codes: ok %s (output of: %s)\n' "$name" "$last_case"
    else
        printf 'test-gate-skip-exit-codes: FAIL %s (no %q in output of: %s)\n' "$name" "$needle" "$last_case" >&2
        failures=$((failures + 1))
    fi
}
check() {
    local name=$1 ok=$2
    if [[ "$ok" == 0 ]]; then
        printf 'test-gate-skip-exit-codes: ok %s\n' "$name"
    else
        printf 'test-gate-skip-exit-codes: FAIL %s\n' "$name" >&2
        failures=$((failures + 1))
    fi
}

# Gates whose skip contract this file owns.
# The five this issue fixes, plus every other meson-registered check script
# that is already clean. Listing the clean ones is the point: the backstop then
# guards them against acquiring the defect, which a list of only the broken ones
# could not do.
COVERED="
check-abi-manifest.sh
check-abi-symbols.sh
check-abi-symbols-locale.sh
check-abi-symbols-macos.sh
check-sbom-snapshot.sh
check-advanced-header.sh
check-clang-tidy-backlog-monotonic.sh
check-doop-catalogue.sh
check-log-erasure.sh
check-perf-gate-execution.sh
check-phase-labels.sh
check-semantics-future.sh
check-threading-doc.sh
check-wl-easy-opts-symbol.sh
check-wrap-revisions.sh
check_log_header_not_public.sh
"

# Gates deliberately outside the contract, each with the reason. A gate belongs
# here only when exit 77 would be wrong for it, not merely inconvenient.
#
#   check-changelog-rc.sh   invoked as a bare `run:` step at ci-pr.yml, where
#                           GitHub uses `bash -e` and 77 fails the job. Its skip
#                           branch is taken on every PR to main, and the job
#                           gates `lint` and therefore every build. Only its
#                           self-test is meson-registered.
#   check-no-testhook-in-libwirelog.sh
#                           its NOTICE follows an assertion that already ran and
#                           passed, so reporting 77 would discard a real result.
#                           This is a genuine exemption: 77 would be wrong here.
#   check-release-template.sh
#                           NOT a genuine exemption -- a merge-order carve-out.
#                           This gate has the defect, and #1288 fixes it on a
#                           separate branch that also adds its own self-test.
#                           Covering it here would make this file's result
#                           depend on which branch merges first.
#                           TODO: move to COVERED once #1288 has landed; until
#                           then this entry is the one thing in this file that
#                           does not meet the rule stated above.
EXEMPT="
check-changelog-rc.sh
check-no-testhook-in-libwirelog.sh
check-release-template.sh
"

# --- coverage: a gate added later cannot slip through -----------------------
# Every check-*.sh that tests/meson.build runs must be listed above. Without
# this the file silently stops covering the tree it claims to.
uncovered=""
found=0
while IFS= read -r script; do
    found=$((found + 1))
    # The one pipeline left in this file, and safe in the direction that
    # matters: the list is ~20 short lines, printf is a builtin, and a spurious
    # non-zero adds to $uncovered and FAILS. Fail-closed, unlike skip_then_zero,
    # which was fail-open and is why that one has no pipeline at all.
    printf '%s\n' "$COVERED$EXEMPT" | grep -qxF "$script" || uncovered="$uncovered $script"
done < <(grep -oE "scripts/[A-Za-z0-9_/-]+\.sh" "$root/tests/meson.build" \
             | sed 's|.*/||' | grep -Ei '^check[_-]' | sort -u)
# A floor, because "nothing uncovered" and "scanned nothing" are otherwise the
# same result: replacing every path in tests/meson.build with a spelling the
# scan cannot match made this report ok with rc 0. A restructure of how those
# paths are written would have retired the guard silently.
if (( found < 15 )); then
    printf 'test-gate-skip-exit-codes: FAIL coverage scan found only %d check scripts in tests/meson.build (expected at least 15); the scan or the file has changed shape\n' "$found" >&2
    failures=$((failures + 1))
elif [[ -n "$uncovered" ]]; then
    printf 'test-gate-skip-exit-codes: FAIL meson runs check scripts absent from COVERED/EXEMPT:%s\n' "$uncovered" >&2
    failures=$((failures + 1))
else
    printf 'test-gate-skip-exit-codes: ok every meson-registered check script is covered or exempted (%d scanned)\n' "$found"
fi

# --- source-level backstop --------------------------------------------------
# A branch that prints SKIP and then exits 0 within the next SIX lines. Matches
# a literal `exit 0` only: an indirect zero (`exit $((0))`) escapes, which is
# inherent to reading source with grep and is why the behavioural cases below
# carry the real weight.
#
# TWO margins, and they are not the same size. Against false positives there
# are 18 lines of slack: the nearest unrelated `exit 0` after any SKIP echo in
# a covered gate is check-abi-symbols.sh:83, 18 lines away. Against false
# NEGATIVES there is exactly ONE line: the largest live SKIP-to-exit gap is 5
# (check-abi-manifest.sh's arm64 branch), and a 7-line gap evades this window
# silently. Adding one diagnostic echo to that branch drops it out of coverage.
# Quote the one-line figure, not the eighteen, when judging whether this is
# still safe -- and prefer adding a behavioural case over widening again.
# -A6, not -A2: check-abi-manifest.sh's arm64 arch-skip prints five diagnostic
# lines between its SKIP echo and its exit, and that branch is the one live on
# the ubuntu-24.04-arm required check. Two other branches sit at exactly the
# old two-line limit, so the window had no margin and any added `echo` would
# have silently dropped a branch out of coverage.
#
# Captured, not piped into `grep -q`: grep -q exits at the first match, the
# upstream grep dies writing to the closed pipe, and `set -o pipefail`
# propagates that -- so the `if` read false and this reported "ok" for a file
# that had the defect. Silently open, which is the failure this file exists to
# catch. Reproduced at 60k lines.
skip_then_zero() {
    local f=$1 window hits
    # echo with either quote style, and printf: a single-quoted `echo 'SKIP: x'`
    # or a `printf 'SKIP: ...'` is ordinary shell and evaded a double-quote-only
    # pattern completely, giving a new gate a vacuously passing line here.
    window=$(grep -A6 -nE "^[^#]*(echo|printf)[[:space:]]+[\"']?[^\"']*SKIP" "$f" 2>/dev/null || true)
    # No pipeline at all. `printf ... | grep -q` and `printf ... | grep -c` are
    # both unsafe here: the consumer stops reading (or the producer outruns the
    # 64 KB buffer), printf takes SIGPIPE, and pipefail turns that into a
    # non-zero status which this function reports as "no defect found" --
    # silently open, the failure this file exists to catch. Measured at 1.5 MB:
    # the piped forms return 1 and 141 respectively, both wrong.
    hits=$(grep -cE '^[0-9]+-[[:space:]]*exit 0[[:space:]]*(#.*)?$' <<<"$window" || true)
    [[ "$hits" != 0 ]]
}
for name in $COVERED; do
    # Not all gates live under scripts/ci: check_log_header_not_public.sh is in
    # scripts/, which is why the scan above matches on basename.
    f="$root/scripts/ci/$name"
    [[ -f "$f" ]] || f="$root/scripts/$name"
    [[ -f "$f" ]] || { check "$name exists" 1; continue; }
    if skip_then_zero "$f"; then
        check "$name has no SKIP branch exiting 0" 1
    else
        check "$name has no SKIP branch exiting 0" 0
    fi
    # If a gate names the constant, it must be meson's skip code.
    # (check-abi-symbols-macos.sh's allowlist branch is an advisory pass, not a
    # skip: it says so and does not print "SKIP", so the scan above ignores it.)
    if grep -qE '^[[:space:]]*SKIP_EXIT=' "$f"; then
        if grep -qE '^[[:space:]]*SKIP_EXIT=77[[:space:]]*(#.*)?$' "$f"; then
            check "$name defines SKIP_EXIT as 77" 0
        else
            check "$name defines SKIP_EXIT as 77" 1
        fi
    fi
done

# --- behavioural: provoke a real skip and read the status -------------------
empty="$tmp/empty-build"; mkdir -p "$empty"

# No libwirelog in the build dir: both ABI gates must skip, not pass.
expect_status 'check-abi-symbols skips on a build tree with no library' 77 \
    "$root/scripts/ci/check-abi-symbols.sh" "$empty"
expect_status 'check-abi-manifest skips on a build tree with no library' 77 \
    "$root/scripts/ci/check-abi-manifest.sh" "$empty"

# syft absent: the SBOM gate must skip. PATH is rebuilt from the tools the gate
# needs so that syft is genuinely absent without breaking anything else.
bare="$tmp/bare"; mkdir -p "$bare"
missing=""
# Only what the gate touches before its syft probe. A wider list made the
# assertion silently opt out on a minimal host for tools it never reaches;
# `git` was needed only by the fixture that has since been deleted.
for tool in bash sh grep sed cat dirname uname; do
    p=$(command -v "$tool" 2>/dev/null) || { missing="$missing $tool"; continue; }
    ln -sf "$p" "$bare/$tool"
done
if [[ -n "$missing" ]]; then
    printf 'test-gate-skip-exit-codes: SKIPPED syft case (%s unavailable)\n' "${missing# }"
else
    no_syft() { PATH="$bare" "$root/scripts/ci/check-sbom-snapshot.sh" "$empty"; }
    expect_status 'check-sbom-snapshot skips when syft is absent' 77 no_syft
fi

# The macOS gate on a non-macOS host, and vice versa.
if [[ "$(uname -s)" != Darwin ]]; then
    expect_status 'check-abi-symbols-macos skips on a non-macOS host' 77 \
        "$root/scripts/ci/check-abi-symbols-macos.sh" "$empty"
else
    expect_status 'check-abi-symbols-macos skips with no dylib present' 77 \
        "$root/scripts/ci/check-abi-symbols-macos.sh" "$empty"
fi

# A missing committed baseline is NOT a skip: those files are in the tree, so
# their absence means one was deleted. The sibling check-abi-symbols.sh has
# always treated the same condition as a hard failure.
fake_build="$tmp/fake-build"; mkdir -p "$fake_build"
: >"$fake_build/libwirelog.so"

# No git fixture here, deliberately. An earlier draft built one with
# `( cd "$d" && git init && git add -A && git commit )`, which is unsafe and was
# also unnecessary: check-abi-manifest.sh invokes git zero times. GIT_DIR and
# GIT_WORK_TREE override `cd` rather than supplementing it, and git exports both
# inside hooks, `git bisect run` and `git rebase --exec` -- so under any of
# those the fixture committed the CALLER's uncommitted work to their real
# repository while this suite printed "all cases passed". Verified against a
# victim repo. The #1288 commit message documents the same hazard; do not
# reintroduce a git fixture here without a reason the gate itself requires.
if [[ "$(uname -s)" == Linux ]] && command -v abidiff >/dev/null 2>&1; then
    no_baseline() {
        local d="$tmp/nobase"; rm -rf "$d"; mkdir -p "$d/abi" "$d/scripts/ci"
        cp "$root/scripts/ci/check-abi-manifest.sh" "$d/scripts/ci/"
        "$d/scripts/ci/check-abi-manifest.sh" "$fake_build"
    }
    expect_status 'a deleted ABI baseline fails rather than skipping' 1 no_baseline
else
    printf 'test-gate-skip-exit-codes: SKIPPED baseline case (needs Linux + abidiff)\n'
fi

# A deleted SBOM baseline must fail, not skip -- the counterpart of the ABI
# baseline case above, and the other half of this change. Without it, mutating
# that exit 1 to exit 0 passes the entire suite: the source backstop cannot see
# it (the branch prints FAIL:, not SKIP:) and nothing else reaches the branch.
sbom_no_baseline() {
    local d="$tmp/sbom-nobase"; rm -rf "$d"; mkdir -p "$d/scripts/ci" "$d/bin"
    cp "$root/scripts/ci/check-sbom-snapshot.sh" "$d/scripts/ci/"
    # A syft that need not work: the gate must reach the baseline check, which
    # sits immediately after the `command -v syft` probe.
    printf '#!/usr/bin/env bash\nexit 0\n' > "$d/bin/syft"
    chmod +x "$d/bin/syft"
    # No sbom/ directory here, so the committed baseline is "deleted".
    PATH="$d/bin:$PATH" "$d/scripts/ci/check-sbom-snapshot.sh"
}
expect_status 'a deleted SBOM baseline fails rather than skipping' 1 sbom_no_baseline
expect_says   'the SBOM baseline failure says it was deleted' 'it was deleted'

if ((failures)); then
    printf 'test-gate-skip-exit-codes: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-gate-skip-exit-codes: all cases passed\n'
