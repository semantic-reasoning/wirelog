#!/usr/bin/env bash
# Attach release artefacts to the GitHub Release for a tag.
#
# Issue #750. Creates the release as a draft if it does not exist, then uploads
# every file in DIST_DIR that is not already published.
#
# Published assets are never overwritten. Each signing run mints a fresh
# ephemeral key, signature and Rekor entry, so replacing them would silently
# change verification material a consumer may already have fetched and pinned --
# and `workflow_dispatch` on this workflow explicitly targets immutable tags.
#
# Publishing signatures for an already-published archive is safe only because
# that archive is byte-reproducible: make-tarball.sh uses `git archive` plus
# `gzip -n` over a commit the job pins to EXPECTED_SHA, so a re-run's local
# archive is identical to the published one and a signature over the former
# verifies the latter. The guard below counts only the three signing outputs,
# not the archive, so if make-tarball.sh ever became non-deterministic this
# path would publish a signature over a different archive and the guard would
# not notice.
#
# The three signing outputs share one key and one Rekor entry, so they are a
# set. `dist/*` sorts .sha256 between .pem and .sig, so an upload interrupted
# partway can otherwise leave a release carrying a certificate from one signing
# run and a signature from the next: each verifies alone, neither verifies
# against the other, and no-clobber makes it unrepairable by re-running. A
# partial set is refused so a human deletes the assets and re-runs.
#
# usage: publish-release-assets.sh TAG DIST_DIR NOTES_FILE
set -euo pipefail

usage() {
    echo 'usage: publish-release-assets.sh TAG DIST_DIR NOTES_FILE' >&2
}

if [[ ${1:-} == -h || ${1:-} == --help ]]; then
    usage
    exit 0
fi
if [[ $# -ne 3 ]]; then
    usage
    exit 2
fi

tag=$1
dist=$2
notes=$3

fail() {
    printf '%s\n' "$*" | sed 's/^/publish-release-assets: /' >&2
    exit 1
}

[[ -d "$dist" ]] || fail "not a directory: $dist"
# An empty notes file would publish a release with no body.
[[ -s "$notes" ]] || fail "release notes are empty or missing: $notes"

archive=$(find "$dist" -maxdepth 1 -type f -name 'wirelog-*.tar.gz' -print -quit)
[[ -n "$archive" ]] || fail "no source archive in $dist"
archive_name=${archive##*/}

if ! gh release view "$tag" >/dev/null 2>&1; then
    gh release create "$tag" --draft --verify-tag \
        --title "wirelog ${tag#v}" --notes-file "$notes"
fi

existing=$(gh release view "$tag" --json assets --jq '.assets[].name')

published() { printf '%s\n' "$existing" | grep -qxF "$1"; }

present=0
for suffix in .sig .pem .cosign.bundle; do
    if published "$archive_name$suffix"; then
        present=$((present + 1))
    fi
done
if ((present != 0 && present != 3)); then
    fail "$tag carries $present of 3 signing assets ($archive_name.sig, .pem, .cosign.bundle);
delete them from the release and re-run, so the published set comes from one
signing run. Mixing them leaves a certificate and a signature from different
ephemeral keys, which verify individually but not against each other."
fi

uploaded=0
for asset in "$dist"/*; do
    [[ -f "$asset" ]] || continue
    name=${asset##*/}
    if published "$name"; then
        printf 'already published, leaving untouched: %s\n' "$name"
        continue
    fi
    gh release upload "$tag" "$asset"
    uploaded=$((uploaded + 1))
done
printf 'published %d new asset(s) to %s\n' "$uploaded" "$tag"
