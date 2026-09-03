#!/usr/bin/env bash
# Self-test for the WIRELOG_*_REQUIRED escalation (#1303).
#
# Exit 77 makes a false pass visible but not fatal: a gate that skips on every
# run is still asserting nothing, merely yellow instead of green. The escalation
# turns skips into failures in the one job where each gate must enforce.
#
# Two halves, asserting different things:
#
#   1. Behaviour -- each gate SKIPs with the variable unset and FAILs with it
#      set, on a tree where the prerequisite is genuinely absent. A fixture that
#      supplied the prerequisite would pass either way and pin nothing.
#   2. Wiring -- the workflows still set the variable. Without this the
#      escalation can be deleted from a workflow and every behavioural assertion
#      above keeps passing, which is precisely the silent-downgrade defect this
#      issue exists to close.
set -euo pipefail

# check-abi-symbols.sh's Windows POSIX-layer skip is deliberately NOT escalated
# (it is a platform-scope decision, not a missing prerequisite), so on a Windows
# host `fails_when_required` would see 77 where it asserts 1. This test is
# registered under a bare `if sh.found()`, which DOES resolve on windows-latest,
# so the guard has to be here.
case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-required-gates: SKIP: needs a POSIX host (the Windows skip route is not escalated)"; exit 77 ;;
esac

root=${1:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)}
failures=0

check() {
    local name=$1 status=$2
    if [ "$status" = 0 ]; then
        printf 'test-required-gates: ok %s\n' "$name"
    else
        printf 'test-required-gates: FAIL %s\n' "$name" >&2
        failures=$((failures + 1))
    fi
}
assert() { local n=$1 s=0; shift; "$@" || s=1; check "$n" "$s"; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-required.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
empty="$tmp/empty-build"
mkdir -p "$empty"

# --- 1. behaviour -----------------------------------------------------------
# $empty holds no libwirelog and no sbom baseline, so every gate below reaches a
# genuine skip route rather than a manufactured one.
skips_when_advisory() {
    local gate=$1 st=0
    shift
    ( unset WIRELOG_ABI_REQUIRED WIRELOG_SBOM_REQUIRED
      "$root/scripts/ci/$gate" "$@" ) >/dev/null 2>&1 || st=$?
    [ "$st" = 77 ]
}
fails_when_required() {
    local gate=$1 var=$2 st=0
    shift 2
    ( export "$var=1"; "$root/scripts/ci/$gate" "$@" ) >/dev/null 2>&1 || st=$?
    [ "$st" = 1 ]
}

for gate in check-abi-manifest.sh check-abi-symbols.sh; do
    assert "$gate skips when advisory" skips_when_advisory "$gate" "$empty"
    assert "$gate fails when WIRELOG_ABI_REQUIRED=1" \
        fails_when_required "$gate" WIRELOG_ABI_REQUIRED "$empty"
done

# The SBOM gate needs a PATH without syft to reach its skip route. Without this
# pair, deleting its escalation entirely left the suite green -- half the change
# was behaviourally untested.
nosyft="$tmp/nosyft"
mkdir -p "$nosyft"
for tool in bash sh env python3 jq git sed awk grep sort head printf uname dirname basename mktemp rm cat; do
    p=$(command -v "$tool" 2>/dev/null) && ln -sf "$p" "$nosyft/$tool"
done
without_syft() { ( PATH="$nosyft"; "$@" ); }
sbom_skips_when_advisory() {
    local st=0
    without_syft env -u WIRELOG_SBOM_REQUIRED \
        "$root/scripts/ci/check-sbom-snapshot.sh" "$empty" >/dev/null 2>&1 || st=$?
    [ "$st" = 77 ]
}
sbom_fails_when_required() {
    local st=0
    without_syft env WIRELOG_SBOM_REQUIRED=1 \
        "$root/scripts/ci/check-sbom-snapshot.sh" "$empty" >/dev/null 2>&1 || st=$?
    [ "$st" = 1 ]
}
assert 'check-sbom-snapshot.sh skips when advisory' sbom_skips_when_advisory
assert 'check-sbom-snapshot.sh fails when WIRELOG_SBOM_REQUIRED=1' sbom_fails_when_required

# --- 2. wiring --------------------------------------------------------------
# Assert on the job/step that must carry it, not merely on the file: the
# variable appearing anywhere in a workflow would satisfy a bare grep while
# sitting in the wrong job.
# Assert on the STEP, not the file: the variable appearing anywhere in a
# workflow would satisfy a bare grep while sitting on a different step.
#
# awk over the YAML rather than a parser: no workflow in this repo installs
# PyYAML and nothing else under scripts/ imports it, so a `python3 -c 'import
# yaml'` guard would skip this half in CI -- leaving criterion 5 enforced
# nowhere, which is the failure this test exists to prevent.
#
# The range runs from the named step to the next list item at any indent, so
# neither a neighbouring step nor a job-level `env:` satisfies it.
#
# It asserts step-level PLACEMENT, not merely presence, so three edits fail it
# that a reader might expect to pass: renaming the step, quoting the step name,
# and hoisting the env to job level. The rename is a true failure -- after it,
# nothing guarantees the escalation still sits on the step that runs the gate --
# and the other two are the cost of the placement assertion. Update this test
# alongside any of the three.
sets_in_step() {
    local file=$1 step=$2 var=$3
    awk -v want="- name: $step" -v var="$var" '
        index($0, want)  { inside = 1; next }
        inside && /^[[:space:]]*- / { exit }
        inside && index($0, var ":") && /: *.?1.?$/ { found = 1; exit }
        END { exit found ? 0 : 1 }
    ' "$root/.github/workflows/$file"
}

assert 'release-tag.yml Tag/ABI sets WIRELOG_ABI_REQUIRED' \
    sets_in_step release-tag.yml 'Build and test ABI suite' WIRELOG_ABI_REQUIRED
assert 'ci-pr.yml SBOM step sets WIRELOG_SBOM_REQUIRED' \
    sets_in_step ci-pr.yml 'SBOM snapshot gate' WIRELOG_SBOM_REQUIRED
assert 'release-tag.yml Tag/SBOM sets WIRELOG_SBOM_REQUIRED' \
    sets_in_step release-tag.yml 'Build and test SBOM suite' WIRELOG_SBOM_REQUIRED

# release-verification asserts a hardcoded job count against its own `needs:`
# list. The two must agree: `join(needs.*.result)` silently yields fewer entries
# when a job is dropped, and every remaining one could still be success -- so
# the gate would pass having checked less than it names. That is the same
# defect class the escalation above exists to close, one level up, and adding
# the Tag/SBOM job to `needs:` without touching the count would have caused it.
verification_count_matches_needs() {
    local wf="$root/.github/workflows/release-tag.yml" needs_n asserted_n
    needs_n=$(awk '
        /^  release-verification:/ { inside = 1 }
        inside && /^    needs: \[/ {
            line = $0
            sub(/.*\[/, "", line); sub(/\].*/, "", line)
            n = split(line, parts, /,/)
            print n
            exit
        }' "$wf")
    asserted_n=$(awk '
        /^  release-verification:/ { inside = 1 }
        inside && /\$\{#statuses\[@\]}" -eq / {
            match($0, /-eq [0-9]+/)
            print substr($0, RSTART + 4, RLENGTH - 4)
            exit
        }' "$wf")
    [ -n "$needs_n" ] && [ -n "$asserted_n" ] && [ "$needs_n" = "$asserted_n" ]
}
assert 'release-verification job count matches its needs list' \
    verification_count_matches_needs

# sbom/snapshot.txt is sensitive to syft's cataloger set, so the PR gate and the
# GA gate must scan with the same version or they disagree about the committed
# baseline. Bumping one alone fails closed -- the tag goes red -- but that is
# the drift this file exists to catch, and it is one assertion.
syft_pins_agree() {
    local a b
    a=$(awk -F'"' '/SYFT_VERSION=/ { print $2; exit }' \
        "$root/.github/workflows/ci-pr.yml")
    b=$(awk -F'"' '/SYFT_VERSION=/ { print $2; exit }' \
        "$root/.github/workflows/release-tag.yml")
    # Each file must yield a pin AND they must match. Comparing a deduplicated
    # set instead was satisfied by one file having no pin at all -- the
    # remaining pin still made the set unique. Absence has to be checked
    # separately from disagreement.
    [ -n "$a" ] && [ -n "$b" ] && [ "$a" = "$b" ]
}
assert 'ci-pr.yml and release-tag.yml pin the same syft version' syft_pins_agree

if ((failures)); then
    printf 'test-required-gates: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-required-gates: all cases passed\n'
