#!/usr/bin/env bash
# Verify a release archive and its published checksums.
set -euo pipefail

usage() {
  echo "usage: $0 ARCHIVE [ARCHIVE.sha256] [ARCHIVE.blake3] [--tag TAG] [--signature FILE --certificate FILE] [--attestation FILE --repo OWNER/REPO] [--identity-regexp RE] [--oidc-issuer URL]" >&2
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
# Named rather than inlined in the gh call below, so that the docs-agreement
# check can see it. That mechanism compares SIGNING.md against values this
# script assigns to a NAME; a value hardcoded inside an invocation is not a flag
# it forgot, it is a shape it structurally cannot read. Everything else on that
# command line already had a name, which is exactly why this was the one that
# drifted unguarded.
predicate_type='https://slsa.dev/provenance/v1'
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
    --predicate-type "$predicate_type" \
    --cert-identity-regex "$identity_regexp" \
    --cert-oidc-issuer "$oidc_issuer"
  echo "verified SLSA provenance attestation $attestation"
fi
if [[ -z "$tag" && -z "$signature" && -z "$attestation" ]]; then
  echo 'checksum verification complete; signed inputs were not supplied'
fi
