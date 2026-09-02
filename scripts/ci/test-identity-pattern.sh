#!/usr/bin/env bash
# Self-test for the Sigstore certificate-identity pattern.
#
# Issue #1287. cosign matches --certificate-identity-regexp UNANCHORED, and the
# pattern was a bare prefix, so it accepted a SAN from any workflow in this
# repository. The signer and the verifier each carry a copy, and the docs carry
# three more; nothing checked that they agreed or that the pattern rejected
# anything.
#
# Every candidate below is a LITERAL SAN string, written out in full. The issue
# warns why: a test that builds a candidate by concatenating the pattern with a
# workflow name is a tautology -- the constructed string begins with the pattern
# text and matching is unanchored, so it passes for every input, including for a
# pattern that rejects nothing.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-identity-pattern: SKIP: needs a POSIX host"; exit 77 ;;
esac

root=$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
failures=0
check() {
    local name=$1 ok=$2
    if [[ "$ok" == 0 ]]; then printf 'test-identity-pattern: ok %s\n' "$name"
    else printf 'test-identity-pattern: FAIL %s\n' "$name" >&2; failures=$((failures + 1)); fi
}
assert() { local n=$1 ok=0; shift; "$@" || ok=1; check "$n" "$ok"; }
refute() { local n=$1 ok=0; shift; "$@" && ok=1; check "$n" "$ok"; }

# Read the pattern from each script the way a reader would: the DEFAULT
# assignment at the start of the file, not the `identity_regexp=$2` in the
# argument parser. The issue flags that trap explicitly -- a naive match on
# `identity_regexp=` finds the parser line and tests nothing.
# Exactly one top-level assignment, not the first of several: a second one
# would be the effective value while this validated the first. The sibling
# test-release-signing.sh takes the same care for the same reason.
constant_from() {
    local file=$1 name=$2 hits
    hits=$(grep -c "^$name='" "$file")
    [[ "$hits" == 1 ]] || {
        echo "test-identity-pattern: expected exactly one $name= in $file, found $hits" >&2
        return 1
    }
    grep -m1 "^$name='" "$file" | sed "s/^$name='//; s/'\$//"
}
pattern_from() { constant_from "$1" identity_regexp; }

# Reject any escape other than \. : the two engines disagree about what an
# escape MEANS. `\d` is a digit class to Go and a literal `d` to POSIX ERE, and
# some sequences Go rejects outright. Either way, a pattern that passed this
# suite could behave differently under cosign on tag day -- the failure mode
# these tests exist to prevent. A whitelist, so it covers constructs not
# enumerated here.
only_dot_escapes() { ! grep -qE '\\[^.]' <<<"$1"; }
verify_pat=$(pattern_from "$root/scripts/release/verify-release.sh")
sign_pat=$(pattern_from "$root/scripts/release/sign-artifacts.sh")

shaped() { [[ "$1" =~ ^\^https ]]; }
assert 'the verifier pattern was read, and is anchored' shaped "$verify_pat"
assert 'the signer pattern was read, and is anchored' shaped "$sign_pat"
assert 'the verifier pattern uses no escape the two engines read differently' \
    only_dot_escapes "$verify_pat"
# Also the signer: same() covers this transitively, but only while same() holds.
assert 'the signer pattern uses no escape the two engines read differently' \
    only_dot_escapes "$sign_pat"

# The signer and verifier must agree, or an artifact signs under one policy and
# verifies under another.
same() { [[ "$verify_pat" == "$sign_pat" ]]; }
assert 'the signer and verifier use the same pattern' same

# The rationale ABOVE the constant must match too. release_signing_contract
# asserts sign_identity == verify_identity -- the value, not the prose -- so a
# correction applied to one script and not the other survives every other gate,
# and the two files then publish different accounts of the same policy. That is
# not hypothetical: in this change the docs fell two rounds behind the scripts
# for exactly this reason, in a file nothing compared.
rationale_of() {
    sed -n '/^# Anchored at the start/,/^identity_regexp=/p' "$1"
}
rationale_same() {
    local a b
    a=$(rationale_of "$root/scripts/release/verify-release.sh")
    b=$(rationale_of "$root/scripts/release/sign-artifacts.sh")
    # Two empty extractions compare equal, so require the block to be found
    # before believing the agreement -- the same trap check-manifest-collation.sh
    # hit when its sed range stopped matching.
    if [[ "$(printf '%s\n' "$a" | grep -c .)" -lt 10 ]]; then
        printf 'rationale block not found in verify-release.sh\n' >&2
        return 1
    fi
    [[ "$a" == "$b" ]]
}
assert 'the signer and verifier carry the same rationale' rationale_same

# cosign uses Go's regexp, whose syntax grep -E follows closely enough for the
# constructs here (anchors, escaped dots, literals). No early-exit consumer.
matches() { printf '%s\n' "$2" | grep -qE -- "$1"; }

# --- accepted: the real release workflow, on the refs it actually runs from ---
assert 'a tag-push SAN is accepted' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml@refs/tags/v1.0.0'
# release-tag.yml is dispatchable as well as tag-triggered, and a dispatch's
# OIDC ref claim names the dispatched ref rather than the tag the job checks
# out, so rejecting this would break the manual immutable-rerun path from
# #1272. Derived from the workflow's triggers: no certificate has ever been
# minted in this repository, so there is no observed SAN behind this case.
assert 'a workflow_dispatch SAN on main is accepted' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml@refs/heads/main'

# --- rejected -----------------------------------------------------------------
# Most of these the old bare prefix accepted; the two owner cases it already
# rejected, since owner and repo cannot contain a slash. They are kept because
# they are still killable (widening the owner to [^/]+ fails them), not because
# they discriminate old from new.
refute 'a different workflow in this repository is rejected' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/ci-pr.yml@refs/heads/main'
refute 'a lookalike workflow filename is rejected' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml.evil@refs/heads/main'
refute 'a prefixed workflow filename is rejected' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/not-release-tag.yml@refs/heads/main'
refute 'another repository is rejected' matches "$verify_pat" \
  'https://github.com/attacker/wirelog/.github/workflows/release-tag.yml@refs/heads/main'
refute 'another owner with a matching suffix is rejected' matches "$verify_pat" \
  'https://github.com/evil-semantic-reasoning/wirelog/.github/workflows/release-tag.yml@refs/tags/v1.0.0'
# The leading ^ is what stops a SAN that merely CONTAINS the expected identity.
refute 'a SAN with the identity embedded later is rejected' matches "$verify_pat" \
  'https://github.com/attacker/evil/x?u=https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml@refs/tags/v1.0.0'
# The escaped dots matter: an unescaped . would accept any character there.
refute 'a host with a different character where a dot belongs is rejected' matches "$verify_pat" \
  'https://githubXcom/semantic-reasoning/wirelog/.github/workflows/release-tag.yml@refs/tags/v1.0.0'
# The filename dot needs escaping too, and separately: unescaping only that one
# was caught by the docs comparison alone, with no SAN case to name it.
refute 'a workflow filename with a different character where the dot belongs is rejected' \
  matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tagXyml@refs/tags/v1.0.0'
# The trailing @ forces the match to end at the filename.
refute 'the workflow path without an @ref is rejected' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml'
# A workflow file NAMED release-tag.yml@evil.yml is legal in git and satisfies
# the .yml extension, so a bare trailing @ was not a sufficient right-hand bound.
refute 'a filename containing @ does not defeat the bound' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml@evil.yml@refs/heads/main'
# A run mints a certificate for the workflow file AS IT EXISTS AT THE REF BEING
# RUN, so an unconstrained ref lets a branch carrying a modified release-tag.yml
# mint a certificate this accepts. (refs/tags/ still admits the same thing; see
# the RESIDUAL note in verify-release.sh and #1318.)
refute 'a dispatch from a non-main branch is rejected' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml@refs/heads/attacker-branch'
refute 'a branch whose name merely starts with main is rejected' matches "$verify_pat" \
  'https://github.com/semantic-reasoning/wirelog/.github/workflows/release-tag.yml@refs/heads/main-evil'

# --- the docs must publish the same policy they tell consumers to trust -------
# Enumerate the flag USAGES and require each to carry $verify_pat, rather than
# counting occurrences of the correct pattern. Counting the correct pattern is
# satisfied by a fourth site publishing a WEAKER one -- the count stays at 3 and
# the gate passes -- which is the realistic drift here: someone adds a
# verification recipe to SIGNING.md with a hand-typed pattern. Prose mentions of
# the flag name carry no quoted argument, so they are not usages.
# One mechanism for every "the docs must publish what the code implements"
# check, rather than a bespoke extractor per flag. Five rounds of review found
# the same defect one shape over each time -- the count vs the value, one quote
# style vs the other, a space vs `=`, the scripts vs the docs, the identity flag
# vs the issuer -- because every fix was scoped to the instance. Adding a flag
# here is a call, not a new extractor, so the next one inherits the guard.
#
# Quoted values only, for regexp- and URL-valued flags: an unquoted alternative
# would match prose like "the `--cert-identity-regex` flag" and turn a security
# gate into a wording gate. The backtick after a prose mention is neither a
# space nor `=`, which keeps SIGNING.md's flag-naming paragraph out of the
# results. grep is line-oriented, so this also assumes flag and value sit on
# ONE line; a value moved to a continuation line would not be enumerated. Write
# them on one line.
flag_values() {
    grep -oE -- "$1[ =]+('[^']*'|\"[^\"]*\")" "$2" |
        sed "s/^[^\"']*.//; s/.\$//"
}

# --repo and --predicate-type take a bare slug or URL, not a regexp, so the
# recipes leave them unquoted. The charset includes `:` for the URL form -- it
# was omitted at first, and the predicate guard failed on its own first run
# with the value truncated to "https", which is the failure direction this
# whole change is about. Be honest about what excludes prose here: NOT the charset, which
# happily matches "--repo with" in a sentence, but the convention that prose
# backticks the flag -- and a backtick is neither a space nor `=`. The failure
# direction is safe either way, since a prose hit raises the count and fails
# loudly. Its three sites include SIGNING.md's `gh release download` example,
# which is counted deliberately: a download pointed at another fork is as wrong
# as a verification pointed there. A benign fourth gh example will fail this
# with "expected 3, found 4"; update the count.
bare_values() {
    grep -oE -- "$1[ =]+[A-Za-z0-9._:/-]+" "$2" | sed -E "s/^$1[ =]+//"
}

# label, expected value, expected count, then the command that emits the sites.
sites_agree() {
    local label=$1 expected=$2 want=$3 v n=0 bad=0
    shift 3
    while IFS= read -r v; do
        n=$((n + 1))
        if [[ "$v" != "$expected" ]]; then
            printf '%s: a site publishes a different value:\n  %s\n' "$label" "$v" >&2
            bad=1
        fi
    done < <("$@")
    if [[ "$n" -ne "$want" ]]; then
        printf '%s: expected %d sites in SIGNING.md, found %d\n' "$label" "$want" "$n" >&2
        return 1
    fi
    return "$bad"
}

docs=$root/docs/SIGNING.md
verify_issuer=$(constant_from "$root/scripts/release/verify-release.sh" oidc_issuer)
verify_repo=$(constant_from "$root/scripts/release/verify-release.sh" repository)

assert 'every identity flag site in docs/SIGNING.md carries the pattern' \
    sites_agree identity "$verify_pat" 3 \
    flag_values '--cert(ificate)?-identity-regexp?' "$docs"
# The issuer pins WHICH OIDC provider minted the certificate. A recipe naming
# the wrong one verifies against the wrong trust root, and fails as silently as
# a weak identity pattern did -- the same defect, on the same command lines,
# with no guard until now.
assert 'every issuer flag site in docs/SIGNING.md names the GitHub OIDC issuer' \
    sites_agree issuer "$verify_issuer" 3 \
    flag_values '--cert(ificate)?-oidc-issuer' "$docs"
assert 'every --repo site in docs/SIGNING.md names this repository' \
    sites_agree repo "$verify_repo" 3 \
    bare_values '--repo' "$docs"
# The attestation predicate type: same command line as the three above, and the
# only value on it the script did not name until now. A docs/script mismatch
# fails verification loudly rather than accepting something it should not --
# the opposite direction from the identity and issuer holes -- but it is the
# same missing comparison, so it gets the same mechanism.
verify_predicate=$(constant_from "$root/scripts/release/verify-release.sh" predicate_type)
assert 'the attestation predicate type in docs/SIGNING.md matches the script' \
    sites_agree predicate "$verify_predicate" 1 \
    bare_values '--predicate-type' "$docs"

# Every check above compares a NAMED constant's value. None asserts the name is
# USED -- so re-inlining a literal into the gh/cosign invocation, while leaving
# the constant assigned, passes everything: the constant goes dead, the docs
# still agree with it, and the script sends the literal. Nothing breaks while
# the two happen to match, and the next edit to either one is the hazard. That
# is precisely the shape --predicate-type was in before it was named, so guard
# against its return rather than only against the instance.
# [ =] so a benign reformat to --flag="$const" is not reported as a re-inline;
# the assertion name would not explain that failure.
flag_uses_constant() {
    grep -qE -- "$1[ =]\"\\\$$2\"" "$root/scripts/release/$3"
}
v=verify-release.sh
assert 'verifier cosign --certificate-identity-regexp uses the constant' \
    flag_uses_constant --certificate-identity-regexp identity_regexp "$v"
assert 'verifier cosign --certificate-oidc-issuer uses the constant' \
    flag_uses_constant --certificate-oidc-issuer oidc_issuer "$v"
assert 'verifier gh --cert-identity-regex uses the constant' \
    flag_uses_constant --cert-identity-regex identity_regexp "$v"
assert 'verifier gh --cert-oidc-issuer uses the constant' \
    flag_uses_constant --cert-oidc-issuer oidc_issuer "$v"
assert 'verifier gh --repo uses the constant' \
    flag_uses_constant --repo repository "$v"
assert 'verifier gh --predicate-type uses the constant' \
    flag_uses_constant --predicate-type predicate_type "$v"
# The signer's post-signing self-check, for the same reason. Its blast radius is
# smaller -- cosign sign-blob uses the ambient OIDC identity regardless, so what
# gets signed is unchanged, and release-tag.yml round-trips through the verifier
# before publishing -- but a re-inline there turns the self-check into a rubber
# stamp silently, which is the failure mode this whole issue is about, in the
# file documented as needing to match the verifier byte-for-byte.
a=sign-artifacts.sh
assert 'signer cosign --certificate-identity-regexp uses the constant' \
    flag_uses_constant --certificate-identity-regexp identity_regexp "$a"
assert 'signer cosign --certificate-oidc-issuer uses the constant' \
    flag_uses_constant --certificate-oidc-issuer oidc_issuer "$a"

refute 'and no longer publishes the old bare prefix' \
    grep -qF -- "'https://github.com/semantic-reasoning/wirelog/.github/workflows/'" "$root/docs/SIGNING.md"

if ((failures)); then
    printf 'test-identity-pattern: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-identity-pattern: all cases passed\n'
