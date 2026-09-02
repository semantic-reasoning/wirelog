#!/usr/bin/env bash
# Self-test for download-attestation.sh.
#
# Issue #1290. The attestation download had no retry, and the path it guards
# runs for the first time on a real release tag -- which cannot be rebuilt if
# it fails. gh is stubbed here so the retry behaviour is exercised without
# cutting one.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-attestation-download: SKIP: needs a POSIX host"; exit 77 ;;
esac

root=$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
tmp=$(CDPATH= cd -P -- "$(mktemp -d "${TMPDIR:-/tmp}/wirelog-attest.XXXXXX")" && pwd -P)
trap 'rm -rf "$tmp"' EXIT
dl="$root/scripts/release/download-attestation.sh"

failures=0
last_case=""
expect_status() {
    local name=$1 want=$2 got=0
    shift 2
    last_case=$name
    "$@" >"$tmp/out" 2>"$tmp/err" || got=$?
    if [[ "$got" == "$want" ]]; then printf 'test-attestation-download: ok %s\n' "$name"
    else
        printf 'test-attestation-download: FAIL %s (want exit %s, got %s)\n' "$name" "$want" "$got" >&2
        sed 's/^/    /' "$tmp/out" "$tmp/err" >&2; failures=$((failures + 1))
    fi
}
expect_says() {
    local name=$1 needle=$2
    if grep -qF -e "$needle" "$tmp/out" "$tmp/err"; then
        printf 'test-attestation-download: ok %s (output of: %s)\n' "$name" "$last_case"
    else
        printf 'test-attestation-download: FAIL %s (no %q in output of: %s)\n' "$name" "$needle" "$last_case" >&2
        failures=$((failures + 1))
    fi
}
check() { local n=$1 ok=$2; if [[ "$ok" == 0 ]]; then printf 'test-attestation-download: ok %s\n' "$n"
    else printf 'test-attestation-download: FAIL %s\n' "$n" >&2; failures=$((failures + 1)); fi; }
# The condition runs inside the helper: `cond; check $?` aborts under set -e
# before check runs, truncating the suite at the first failure.
assert() { local n=$1 ok=0; shift; "$@" || ok=1; check "$n" "$ok"; }
refute() { local n=$1 ok=0; shift; "$@" && ok=1; check "$n" "$ok"; }

bin="$tmp/bin"; mkdir -p "$bin"
# A gh that succeeds only on the Nth call, writing the bundle the real one
# would. GH_STUB_SUCCEED_ON=0 never succeeds. The counter is a file because
# each invocation is a fresh process.
cat > "$bin/gh" <<'EOS'
#!/usr/bin/env bash
n=$(( $(cat "$GH_STUB_COUNT" 2>/dev/null || echo 0) + 1 ))
printf '%s' "$n" > "$GH_STUB_COUNT"
if [ "${GH_STUB_SUCCEED_ON:-1}" != 0 ] && [ "$n" -ge "${GH_STUB_SUCCEED_ON:-1}" ]; then
    printf '{"bundle":"stub"}\n' > "sha256:deadbeef.jsonl"
    exit 0
fi
echo "no attestations found for subject" >&2
exit 1
EOS
chmod +x "$bin/gh"

newdist() {
    local d="$tmp/$1"; rm -rf "$d"; mkdir -p "$d"
    printf 'archive\n' > "$d/wirelog-9.9.9.tar.gz"
    printf '%s\n' "$d"
}
run() {
    local d=$1 on=$2
    GH_STUB_COUNT="$d/.count" GH_STUB_SUCCEED_ON="$on" \
    WIRELOG_ATTESTATION_ATTEMPTS=3 WIRELOG_ATTESTATION_DELAY=0 \
    PATH="$bin:$PATH" "$dl" "$d" 9.9.9 owner/repo
}

# First attempt succeeds: the ordinary path.
d=$(newdist first)
expect_status 'a first-attempt download succeeds' 0 run "$d" 1
assert 'the bundle is renamed to the release name' test -f "$d/wirelog-9.9.9.intoto.jsonl"
refute 'the raw sha256 bundle name is gone' test -f "$d/sha256:deadbeef.jsonl"
refute 'no download log is left behind' test -f "$d/.attestation-download.log"

# The case this issue exists for: the first call finds nothing, a later one
# succeeds. Without a retry this was a hard failure on an unrebuildable tag.
d=$(newdist lag)
expect_status 'a download that only succeeds on the third attempt still succeeds' 0 run "$d" 3
assert 'and produces the bundle' test -f "$d/wirelog-9.9.9.intoto.jsonl"
attempts_made() { [[ "$(cat "$d/.count")" == 3 ]]; }
assert 'it actually retried rather than succeeding by luck' attempts_made

# Never succeeds: must fail with a diagnosis, not a bare non-zero.
d=$(newdist never)
expect_status 'a download that never succeeds fails' 1 run "$d" 0
expect_says 'the failure names replication lag as the usual cause' 'replication lag'
# Needle chosen against the stub's own stderr: the stub says "no attestations
# found for subject", so a needle of 'attestations' was satisfied by the quoted
# gh output and gave false credit for the diagnosis. Deleting both URL lines
# left the suite green. This matches text only the script can produce.
expect_says 'it points at the attestations page' 're-running this job alone'
expect_says "it quotes gh rather than guessing" 'no attestations found'
exhausted() { [[ "$(cat "$d/.count")" == 3 ]]; }
assert 'it used every attempt before giving up' exhausted
refute 'and cleans up its log' test -f "$d/.attestation-download.log"

# A bundle left by an earlier run must not be mistaken for this run's download.
# With gh failing every time, the stale file would otherwise satisfy the
# existence check on the first attempt and be published as this release's
# provenance.
d=$(newdist stale)
printf 'STALE-LEFTOVER\n' > "$d/sha256:cafebabe.jsonl"
expect_status 'a stale bundle does not mask a failing download' 1 run "$d" 0
refute 'and the stale bundle is not renamed into place' test -f "$d/wirelog-9.9.9.intoto.jsonl"

# A zero or non-numeric attempt count must be rejected, not silently run the
# loop zero times and then report the replication-lag diagnosis.
d=$(newdist badattempts)
bad_attempts() {
    GH_STUB_COUNT="$d/.count" GH_STUB_SUCCEED_ON=1 \
    WIRELOG_ATTESTATION_ATTEMPTS=0 WIRELOG_ATTESTATION_DELAY=0 \
    PATH="$bin:$PATH" "$dl" "$d" 9.9.9 owner/repo
}
expect_status 'a zero attempt count is a usage error' 2 bad_attempts
expect_says   'and says what was wrong' 'positive integer'

# Argument handling.
expect_status 'no arguments is a usage error' 2 "$dl"
expect_status 'a missing dist directory is a usage error' 2 "$dl" "$tmp/nope" 9.9.9 o/r
d=$(newdist norepo)
no_repo() { GH_STUB_COUNT="$d/.count" PATH="$bin:$PATH" GITHUB_REPOSITORY= "$dl" "$d" 9.9.9; }
expect_status 'a missing repository is a usage error' 2 no_repo
expect_says 'and says so' 'repository is required'

# No archive to attest: distinct from "no attestation for the archive".
d="$tmp/empty"; rm -rf "$d"; mkdir -p "$d"
no_archive() { GH_STUB_COUNT="$d/.count" PATH="$bin:$PATH" "$dl" "$d" 9.9.9 o/r; }
expect_status 'a dist with no archive fails' 1 no_archive
expect_says 'and names the missing archive, not a missing attestation' 'no wirelog-*.tar.gz'

if ((failures)); then
    printf 'test-attestation-download: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-attestation-download: all cases passed\n'
