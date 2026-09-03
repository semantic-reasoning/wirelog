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

# release-verification must WAIT ON every verification job, not merely agree
# with itself about how many there are. Comparing counts caught drift between
# the `needs:` list and the hardcoded `-eq N`, but not the defect one level up:
# a job never added to `needs:` at all. Reproduced -- a tenth job left out
# entirely kept the whole suite green, and the release gate then passes without
# ever consulting that job's result. Set equality kills that.
#
# It does NOT kill a trailing comma, despite an earlier version of this comment
# claiming so: YAML reads `[a, b,]` as two elements, and both parsers below drop
# the empty before comparing, so the two sets are identical. The trailing comma
# is caught by the COUNT check instead, whose filter counts only non-empty
# elements. Do not collapse the two assertions on the strength of the wrong
# claim.
#
# awk, not a YAML parser -- see the note above `sets_in_step`: installing one
# would skip this whole wiring half in CI, which is this file's own defect class.
#
# The job pattern is anchored at both ends. A substring match is not safe here:
# an early draft of this very check enumerated jobs with `grep -v 'on:'` and
# silently dropped `release-verification`, because its name ends in "on:". The
# optional quotes accept both `"job":` and the single-quoted form (written
# \047 because the awk program is itself single-quoted), which YAML admits on
# the same footing; an unquoted-only
# pattern would silently omit from the expected set -- the same silent-drop
# shape one spelling over.
#
# The `on:` block is excluded by the in_jobs gate, not by appearing before
# `jobs:`: any column-0 line clears it, so the exclusion is order-independent.
release_tag_jobs() {
    awk '
        /^jobs:[[:space:]]*$/ { in_jobs = 1; next }
        in_jobs && /^[^[:space:]]/ { in_jobs = 0 }
        in_jobs && /^  ["\047]?[A-Za-z0-9_-]+["\047]?:[[:space:]]*$/ {
            name = $0
            sub(/^  /, "", name); sub(/:[[:space:]]*$/, "", name)
            gsub(/["\047]/, "", name)
            print name
        }
    ' "$root/.github/workflows/release-tag.yml"
}

# The three jobs that are legitimately absent from `needs:`, each for its own
# reason -- listed by name so a future verification job cannot be exempted by a
# pattern that happens to match it.
#   validate-input       runs BEFORE the others; they all need it.
#   release-verification is the gate itself.
#   release-artifacts    runs AFTER the gate, gated on its result.
needs_covers_every_verification_job() {
    local expected actual
    expected=$(release_tag_jobs \
        | grep -vxE 'validate-input|release-verification|release-artifacts' \
        | sort)
    actual=$(awk '
        /^  ["\047]?[A-Za-z0-9_-]+["\047]?:[[:space:]]*$/ { inside = 0 }
        /^  release-verification:/ { inside = 1; next }
        inside && /^    needs: \[/ {
            line = $0
            sub(/.*\[/, "", line); sub(/\].*/, "", line)
            n = split(line, parts, /,/)
            for (i = 1; i <= n; i++) {
                gsub(/[[:space:]]/, "", parts[i])
                if (parts[i] != "") print parts[i]
            }
            exit
        }' "$root/.github/workflows/release-tag.yml" | sort)
    if [ -z "$expected" ] || [ -z "$actual" ]; then
        echo "  neither side may be empty: expected='$expected' actual='$actual'" >&2
        return 1
    fi
    if [ "$expected" != "$actual" ]; then
        local missing extra
        missing=$(comm -23 <(printf '%s\n' "$expected") <(printf '%s\n' "$actual"))
        extra=$(comm -13 <(printf '%s\n' "$expected") <(printf '%s\n' "$actual"))
        if [ -n "$missing" ]; then
            echo "  jobs not gated by release-verification:" >&2
            printf '%s\n' "$missing" | sed 's/^/    /' >&2
            echo "  add them to its needs:, or to the exempt list in this file" \
                 "if they are genuinely not verification jobs" >&2
        fi
        if [ -n "$extra" ]; then
            echo "  in needs but not a job:" >&2
            printf '%s\n' "$extra" | sed 's/^/    /' >&2
        fi
        return 1
    fi
}
assert 'release-verification needs every verification job' \
    needs_covers_every_verification_job

# The hardcoded count must still track the list it counts.
verification_count_matches_needs() {
    local needs_n asserted_n wf="$root/.github/workflows/release-tag.yml"
    needs_n=$(awk '
        /^  release-verification:/ { inside = 1 }
        inside && /^    needs: \[/ {
            line = $0
            sub(/.*\[/, "", line); sub(/\].*/, "", line)
            n = split(line, parts, /,/)
            c = 0
            for (i = 1; i <= n; i++) {
                gsub(/[[:space:]]/, "", parts[i])
                if (parts[i] != "") c++
            }
            print c
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
