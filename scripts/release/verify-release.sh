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
# -P on both cd and pwd, and physical resolution throughout: `cd` without -P is
# LOGICAL, so a path like a/link/../f.tar.gz resolves to a/f.tar.gz for the
# shell while the kernel -- and every other stage of this script, including
# cosign, gh attestation and git get-tar-commit-id -- opens the physical
# target. Hashing the logical path made the checksum describe a DIFFERENT FILE
# than the one being verified, and reported "verified checksums for" on a path
# whose real target was tampered. Measured. In the only script third parties
# run to detect tampering, that is the wrong direction to be wrong in.
archive_dir=$(CDPATH= cd -P -- "$(dirname -- "$archive")" && pwd -P)
archive_name=$(basename -- "$archive")
for manifest in "$sha_file" "$b3_file"; do
  manifest_name=$(awk 'NF { print $2; exit }' "$manifest")
  test "$manifest_name" = "$archive_name" || {
    echo "checksum manifest does not name $archive_name: $manifest_name" >&2
    exit 1
  }
done
expected_sha=$(awk 'NF { print $1; exit }' "$sha_file")
expected_b3=$(awk 'NF { print $1; exit }' "$b3_file")
# Hash from inside the archive's directory, against the bare basename, rather
# than passing the full path.
#
# GNU coreutils escapes a filename containing a backslash or a newline: it
# prefixes the WHOLE line with a literal `\` and escapes the character. Passing
# a full path therefore yielded `\<hash>` for any archive under such a path, the
# comparison below could never match, and this reported "SHA256 mismatch" on a
# byte-perfect archive -- to a third party following the published verification
# recipe, which passes an absolute path. Not embedding the path removes the
# class; make-tarball.sh already writes its manifests this way, which is why
# they were unaffected.
#
# The sed is the residue: a basename that itself needs escaping still produces
# the marker, and stripping it is what makes the hash comparable. Neither stage
# stops reading early, so there is no SIGPIPE here.
#
# It is deliberately UNPINNED by any test, and that is worth stating rather than
# hiding. Reaching it needs a backslash in the archive's NAME, and such an
# archive cannot get this far: the manifest-name check above reads `$2` raw, so
# a coreutils-written manifest (which escapes the name) fails there first with
# "checksum manifest does not name". Only a hand-written unescaped manifest
# would exercise this line, and make-tarball.sh never produces one. A fixture
# for that would pin the line via a scenario that cannot occur end to end, which
# is worse than saying so. Closing the class means unescaping on the
# manifest-reading side too; tracked separately.
hash_of() {
    ( CDPATH= cd -P -- "$archive_dir" && "$1" -- "$archive_name" \
        | sed 's/^\\//' | awk '{print $1}' )
}
actual_sha=$(hash_of sha256sum)
actual_b3=$(hash_of b3sum)
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
