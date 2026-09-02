#!/usr/bin/env bash
# Verify a release archive and its published checksums.
set -euo pipefail

usage() {
  echo "usage: $0 ARCHIVE [ARCHIVE.sha256] [ARCHIVE.blake3] [--tag TAG] [--signature FILE --certificate FILE] [--attestation FILE --repo OWNER/REPO]" >&2
  exit 2
}
[[ $# -ge 1 ]] || usage
archive=$1
shift
sha_file="$archive.sha256"
b3_file="$archive.blake3"
if [[ $# -gt 0 && "$1" != --* ]]; then sha_file=$1; shift; fi
if [[ $# -gt 0 && "$1" != --* ]]; then b3_file=$1; shift; fi
tag=''
signature=''
certificate=''
attestation=''
repository='semantic-reasoning/wirelog'
identity_regexp='https://github.com/semantic-reasoning/wirelog/.github/workflows/'
oidc_issuer='https://token.actions.githubusercontent.com'
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      [[ $# -ge 2 ]] || usage
      tag=$2
      shift 2
      ;;
    --signature)
      [[ $# -ge 2 ]] || usage
      signature=$2
      shift 2
      ;;
    --certificate)
      [[ $# -ge 2 ]] || usage
      certificate=$2
      shift 2
      ;;
    --attestation)
      [[ $# -ge 2 ]] || usage
      attestation=$2
      shift 2
      ;;
    --repo)
      [[ $# -ge 2 ]] || usage
      repository=$2
      shift 2
      ;;
    --identity-regexp)
      [[ $# -ge 2 ]] || usage
      identity_regexp=$2
      shift 2
      ;;
    --oidc-issuer)
      [[ $# -ge 2 ]] || usage
      oidc_issuer=$2
      shift 2
      ;;
    *) usage ;;
  esac
done
[[ -f "$archive" && -f "$sha_file" && -f "$b3_file" ]] || {
  echo 'archive and both checksum files are required' >&2
  exit 1
}

command -v sha256sum >/dev/null || { echo 'sha256sum is required' >&2; exit 1; }
command -v b3sum >/dev/null || { echo 'b3sum is required' >&2; exit 1; }
archive_dir=$(cd "$(dirname "$archive")" && pwd)
archive_name=$(basename "$archive")
for manifest in "$sha_file" "$b3_file"; do
  manifest_name=$(awk 'NF { print $2; exit }' "$manifest")
  test "$manifest_name" = "$archive_name" || {
    echo "checksum manifest does not name $archive_name: $manifest_name" >&2
    exit 1
  }
done
expected_sha=$(awk 'NF { print $1; exit }' "$sha_file")
expected_b3=$(awk 'NF { print $1; exit }' "$b3_file")
actual_sha=$(sha256sum "$archive" | awk '{print $1}')
actual_b3=$(b3sum "$archive" | awk '{print $1}')
test "$expected_sha" = "$actual_sha" || { echo "SHA256 mismatch for $archive" >&2; exit 1; }
test "$expected_b3" = "$actual_b3" || { echo "BLAKE3 mismatch for $archive" >&2; exit 1; }

echo "verified checksums for $archive"
if [[ -n "$tag" ]]; then
  git verify-tag "$tag"
  tag_commit=$(git rev-parse --verify "$tag^{commit}")
  archive_commit=$(gzip -dc "$archive" | git get-tar-commit-id)
  test "$tag_commit" = "$archive_commit" || {
    echo "tag $tag does not identify the archive source commit" >&2
    exit 1
  }
  echo "verified annotated tag $tag"
fi
if [[ -n "$signature" || -n "$certificate" ]]; then
  [[ -n "$signature" && -n "$certificate" ]] || {
    echo '--signature and --certificate must be supplied together' >&2
    exit 2
  }
  command -v cosign >/dev/null || { echo 'cosign is required for signature verification' >&2; exit 1; }
  cosign verify-blob \
    --certificate "$certificate" \
    --certificate-identity-regexp "$identity_regexp" \
    --certificate-oidc-issuer "$oidc_issuer" \
    --signature "$signature" "$archive"
  echo "verified Sigstore signature for $archive"
fi
if [[ -n "$attestation" ]]; then
  command -v gh >/dev/null || { echo 'gh is required for attestation verification' >&2; exit 1; }
  gh attestation verify "$archive" \
    --repo "$repository" \
    --bundle "$attestation" \
    --predicate-type https://slsa.dev/provenance/v1 \
    --cert-identity-regex "$identity_regexp" \
    --cert-oidc-issuer "$oidc_issuer"
  echo "verified SLSA provenance attestation $attestation"
fi
if [[ -z "$tag" && -z "$signature" && -z "$attestation" ]]; then
  echo 'checksum verification complete; signed inputs were not supplied'
fi
