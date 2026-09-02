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
identity_regexp='https://github.com/semantic-reasoning/wirelog/.github/workflows/'
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
