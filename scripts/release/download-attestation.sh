#!/usr/bin/env bash
# Download the SLSA provenance attestation bundle for a release archive.
#
# Issue #1290. The workflow attested the archive and then downloaded the bundle
# in the next step, back to back, with no delay and no retry. Those are two
# separate API operations: if the attestations API is not read-your-writes
# consistent, the download returns nothing, and the job died on a bare
# `test -n` with no indication of why -- after every verification job had
# already passed, on a tag that cannot be rebuilt.
#
# This lives in a script rather than inline in the workflow so it can be
# exercised without cutting a release. That is the point: the path it guards
# runs for the first time on a real one.
#
# usage: download-attestation.sh DIST_DIR VERSION [REPO]
#
# Exit codes:
#    0 - the bundle was downloaded and renamed.
#    1 - it could not be, with a diagnosis.
#    2 - usage error.
set -euo pipefail

usage() {
    echo "usage: download-attestation.sh DIST_DIR VERSION [REPO]" >&2
    exit 2
}
[[ $# -ge 2 && $# -le 3 ]] || usage
dist=$1
version=$2
repo=${3:-${GITHUB_REPOSITORY:-}}
[[ -n "$repo" ]] || { echo 'download-attestation: repository is required (argument or GITHUB_REPOSITORY)' >&2; exit 2; }
[[ -d "$dist" ]] || { echo "download-attestation: not a directory: $dist" >&2; exit 2; }

command -v gh >/dev/null 2>&1 || {
    echo 'download-attestation: gh is required' >&2
    exit 1
}

# Bounded, and short. Replication lag on a read-after-write is the failure this
# exists for, so seconds are the right scale; a long backoff would only delay a
# genuine outage. Overridable so the self-test does not sleep.
attempts=${WIRELOG_ATTESTATION_ATTEMPTS:-5}
delay=${WIRELOG_ATTESTATION_DELAY:-3}

archive=$(find "$dist" -maxdepth 1 -type f -name 'wirelog-*.tar.gz' -print -quit)
[[ -n "$archive" ]] || {
    echo "download-attestation: no wirelog-*.tar.gz in $dist" >&2
    echo '  the attestation is for the archive; build it first.' >&2
    exit 1
}
archive_name=$(basename -- "$archive")

# Clear any bundle left by an earlier run BEFORE the loop. Success below is
# decided by "a sha256*.jsonl exists", and with `|| true` swallowing gh's
# status a stale file would satisfy that on the first attempt -- one gh call,
# its failure discarded, and last release's provenance renamed and published as
# this one's. The inline version this replaces could not do that, because gh's
# exit aborted the step; the retry loop is what makes the guard necessary.
rm -f -- "$dist"/sha256*.jsonl

# A non-numeric or zero attempt count would run the loop zero times, never call
# gh, and still print the full replication-lag diagnosis -- a confidently wrong
# answer.
[[ "$attempts" =~ ^[1-9][0-9]*$ ]] || {
    echo "download-attestation: attempts must be a positive integer, got: $attempts" >&2
    exit 2
}

found=""
for attempt in $(seq 1 "$attempts"); do
    # `|| true`: a failed download is the expected transient case, not a reason
    # to abort before the retry loop has had its say. The output is kept so the
    # final diagnosis can quote gh rather than guess.
    ( CDPATH= cd -P -- "$dist" \
      && gh attestation download "$archive_name" --repo "$repo" --limit 1 ) \
      >"$dist/.attestation-download.log" 2>&1 || true
    found=$(find "$dist" -maxdepth 1 -type f -name 'sha256*.jsonl' -print -quit)
    [[ -n "$found" ]] && break
    if [[ "$attempt" -lt "$attempts" ]]; then
        echo "download-attestation: attempt $attempt/$attempts found no bundle; retrying in ${delay}s" >&2
        sleep "$delay"
    fi
done

if [[ -z "$found" ]]; then
    echo "download-attestation: no attestation bundle for $archive_name after $attempts attempts" >&2
    echo '  The attest step and this download are separate API operations, so the' >&2
    echo '  usual cause is replication lag rather than a missing attestation.' >&2
    echo "  Check https://github.com/$repo/attestations before re-running; if the" >&2
    echo '  attestation is listed there, re-running this job alone is enough.' >&2
    echo '  gh said:' >&2
    # Order matters: >&2 first duplicates the real stderr into stdout, THEN
    # 2>/dev/null silences sed's own errors. Reversed, stdout follows fd2 to
    # /dev/null and the gh output this line exists to show is swallowed.
    sed 's/^/    /' "$dist/.attestation-download.log" >&2 2>/dev/null || true
    rm -f "$dist/.attestation-download.log"
    exit 1
fi

rm -f "$dist/.attestation-download.log"
mv -- "$found" "$dist/wirelog-$version.intoto.jsonl"
echo "download-attestation: OK; wirelog-$version.intoto.jsonl"
