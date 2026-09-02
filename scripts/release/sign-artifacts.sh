#!/usr/bin/env bash
# Sign a release archive with Sigstore keyless signing.
#
# Issue #750. Produces three files beside the archive:
#
#   <archive>.sig            detached base64 signature
#   <archive>.pem            the ephemeral Fulcio certificate
#   <archive>.cosign.bundle  signature + certificate + Rekor entry, together
#
# The detached pair is what scripts/release/verify-release.sh consumes, and it
# is the published consumer contract. The bundle exists because verification of
# a keyless signature is never offline: the Fulcio certificate is valid for
# about ten minutes, and cosign establishes that the signature was made while
# it was valid from the Rekor entry's inclusion time. Verifying the detached
# pair therefore requires a live Rekor lookup forever, and `--insecure-ignore-tlog`
# on an expired certificate fails outright. The bundle carries the Rekor entry
# inline, so it is the recipe that still works months later.
#
# Only the archive is signed. The .sha256 and .blake3 manifests are already
# bound to it -- verify-release.sh recomputes both over the archive rather than
# trusting the files -- and the provenance attestation is a self-authenticating
# DSSE envelope that `gh attestation verify` checks on its own. Signing those
# would add Fulcio round trips that no verifier in this repository consults.
#
# Requires cosign 2.x. cosign 3 defaults --new-bundle-format to true and then
# rejects the run unless --bundle is the *only* output, having deprecated the
# detached flags; adopting it means changing verify-release.sh's published
# flag contract, which is deliberately out of scope here.
set -euo pipefail

# Must match scripts/release/verify-release.sh byte-for-byte. The
# release_signing_contract test asserts that they do.
# Anchored at the start and naming the release workflow file.  cosign matches
# --certificate-identity-regexp UNANCHORED (pkg/cosign/verify.go compiles the
# pattern and calls MatchString), so a bare prefix accepts a SAN from any
# workflow in this repository.  GitHub's documented job_workflow_ref claim is
# https://github.com/<owner>/<repo>/.github/workflows/<file>@<ref>, and neither
# owner nor repo can contain a slash, so this was never a cross-repository
# weakness -- the exposure is within-repository workflow confusion: any workflow
# granted `id-token: write` could mint a certificate this accepts as a release
# signature.  release-tag.yml is the only such workflow today, but that bounds
# the impact far less than it sounds: job_workflow_ref names the WORKFLOW FILE,
# not the job, and release-tag.yml grants id-token: write at workflow level, so
# every job that does not re-declare permissions: inherits it.  No identity
# pattern can separate them -- the certificate carries no job discriminator at
# all -- so this is bounded by workflow permissions or not at all.  The one
# exception is instructive: the sanitizers job is `uses:` a reusable workflow,
# which declares its own contents: read AND would mint a SAN naming the CALLED
# file, so the anchor already excludes it.  Tracked in #1319, the higher
# priority of the two residuals here because it is a four-line YAML edit.
#
# The ref is constrained, but not to tags alone.  release-tag.yml is reachable
# by workflow_dispatch as well as by a tag push, and a dispatch's OIDC ref
# claim names the dispatched ref rather than the tag the job checks out, so
# requiring @refs/tags/ would reject the manual immutable-rerun path #1272
# exists to provide.  The accepted set is derived from the workflow's triggers,
# not from an observed certificate: signing landed after v0.60.0, so no SAN has
# ever been minted here to read.  Accepting ANY ref is also wrong:
# workflow_dispatch runs the workflow file from the dispatched ref, so a
# write-access actor could push a branch carrying a modified release-tag.yml,
# dispatch it, and mint a certificate this would accept -- bypassing whatever
# review protects main.  refs/heads/main anchored at the end, or any
# refs/tags/, admits both real cases and neither of those.
#
# RESIDUAL 2, and deliberate: refs/tags/ still admits the same forgery, and by
# an even shorter route than dispatch.  `on: push: tags:` runs the workflow file as
# it exists AT THE PUSHED TAG, so pushing v1.99.0 with a modified
# release-tag.yml needs no dispatch at all; dispatching at a tag is the second
# route.  No pattern over the tag name closes either, because the attacker
# chooses the name -- tightening to tags/v[01]\. only invites v1.99.0.  Bounding
# it needs tag protection or a protected Actions environment, which this script
# can neither express nor observe.  Tracked in #1318; do not "fix" it here by
# narrowing the regex.
#
# @refs/ is also the right-hand bound.  A bare trailing @ is not: a workflow
# file named release-tag.yml@evil.yml is legal in git, satisfies the .yml
# extension, and defeats the anchor entirely.  Every ref in that documented
# claim begins refs/, so requiring it costs nothing.  Both facts come from
# GitHub's OIDC documentation, not from a certificate observed here.
identity_regexp='^https://github\.com/semantic-reasoning/wirelog/\.github/workflows/release-tag\.yml@refs/(heads/main$|tags/)'
oidc_issuer='https://token.actions.githubusercontent.com'

usage() {
    cat >&2 <<'EOF'
usage: sign-artifacts.sh ARCHIVE [--identity-regexp RE] [--oidc-issuer URL] [--no-verify]

Signs ARCHIVE with Sigstore keyless signing, writing ARCHIVE.sig,
ARCHIVE.pem and ARCHIVE.cosign.bundle beside it.
EOF
}

verify=1
[[ $# -ge 1 ]] || { usage; exit 2; }
case "$1" in
    -h|--help) usage; exit 0 ;;
    --*) usage; exit 2 ;;
esac
archive=$1
shift
while (($#)); do
    case "$1" in
        --identity-regexp)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            identity_regexp=$2
            shift 2
            ;;
        --oidc-issuer)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            oidc_issuer=$2
            shift 2
            ;;
        --no-verify) verify=0; shift ;;
        *) usage; exit 2 ;;
    esac
done

fail() {
    echo "sign-artifacts: $*" >&2
    exit 1
}

[[ -f "$archive" ]] || fail "not a file: $archive"
command -v cosign >/dev/null || fail 'cosign is required'

# Injected by the runner only when the job holds `id-token: write`. Checking it
# here turns a permissions mistake into an instant, named failure instead of
# three retries against Fulcio with a token that was never going to be issued.
[[ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ]] || fail \
    'no OIDC token endpoint in the environment; keyless signing needs `permissions: id-token: write` on this job'

# cosign prints an ASCII-art banner with `GitVersion: v...` (or JSON with `--json`).
# Extract the semver component directly using pure Bash regex without external tools.
cosign_version() {
    local raw pattern
    raw=$(cosign version 2>&1 || true)
    pattern='[gG]itVersion[":[:space:]]+v?([0-9]+\.[0-9]+\.[0-9]+)'
    if [[ "$raw" =~ $pattern ]]; then
        printf '%s' "${BASH_REMATCH[1]}"
    fi
}

version=$(cosign_version)
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] || fail \
    "could not parse cosign version from: $(cosign version 2>&1 | tr '\n' ' ')"
major=${version%%.*}
if [[ "$major" != 2 ]]; then
    fail "cosign 2.x is required, found $version.
cosign 3 defaults --new-bundle-format to true and then requires --bundle as the
sole output, deprecating --output-signature/--output-certificate. Moving to it
means changing the flags scripts/release/verify-release.sh publishes to
consumers, so the pin is deliberate -- do not widen this check without doing
that migration."
fi

signature="$archive.sig"
certificate="$archive.pem"
bundle="$archive.cosign.bundle"

# Retry only what a retry can fix. A flag error, a missing OIDC token or a
# rejected identity fails the same way three times; sleeping between them just
# makes the release job slower to tell you.
# Includes cosign's classic CI flakes: a TUF mirror fetch failure and a rate
# limit rendered as text rather than a status code. Erring wide is deliberate --
# a false retry costs 20 seconds, a missed transient fails the release.
# The status-code, EOF and TUF alternatives are bounded by hand: `\b` is a GNU
# extension and this runs on macOS too. Without the bounds, matching is
# case-insensitive over cosign's own error text, so a sha256 digest containing
# 500, a certificate serial containing 429, or the word "stuff" would each make
# a permanent failure retry three times.
transient='(^|[[:space:]])(429|5[0-9][0-9])([[:space:]]|[.,:]|$)|timeout|timed out|connection refused|connection reset by peer|context deadline exceeded|TLS handshake|unexpected EOF|: EOF($|[^a-zA-Z])|no such host|temporary failure|name resolution|(^|[^a-z])tuf([^a-z]|$)|remote mirror|too many requests|rate limit|service unavailable|try again'

attempt=1
while :; do
    err=$(mktemp)
    if cosign sign-blob --yes \
        --output-signature "$signature" \
        --output-certificate "$certificate" \
        --bundle "$bundle" \
        "$archive" 2>"$err"; then
        cat "$err" >&2
        rm -f "$err"
        break
    fi
    cat "$err" >&2
    if ((attempt >= 3)) || ! grep -Eqi "$transient" "$err"; then
        rm -f "$err"
        fail "cosign sign-blob failed for $archive (attempt $attempt)"
    fi
    rm -f "$err"
    # 5s then 15s. Overridable so the self-test can exercise the retry path
    # without spending 20 seconds of every PR's abi suite on real sleeps.
    sleep "${WIRELOG_SIGN_RETRY_DELAY_SECONDS:-$((attempt * 10 - 5))}"
    attempt=$((attempt + 1))
done

for produced in "$signature" "$certificate" "$bundle"; do
    [[ -s "$produced" ]] || fail "cosign did not produce $produced"
done

if ((verify)); then
    # Verify from the bundle, not the detached pair: the detached path re-queries
    # Rekor's search index, which lags entry inclusion, and would flake here
    # seconds after signing.
    cosign verify-blob \
        --bundle "$bundle" \
        --certificate-identity-regexp "$identity_regexp" \
        --certificate-oidc-issuer "$oidc_issuer" \
        "$archive"
fi

printf 'signed %s\n' "$archive"
printf '  %s\n' "$signature" "$certificate" "$bundle"
