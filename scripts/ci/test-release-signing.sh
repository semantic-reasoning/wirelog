#!/usr/bin/env bash
# Contract test for release artifact signing and publication.
#
# Issue #750. The release-tag workflow only runs on a tag, so a mistake in its
# signing or publication steps is discovered on tag day -- which is what this
# issue was filed to prevent. Everything below is a static or shim-driven
# assertion: no cosign, no network, no build tree, runs in about a second.
set -euo pipefail

# meson hands this script a native path; on Windows that is a drive-letter path
# bash cannot use. See bb4ca712 for the same fix on the downstream self-test.
normalize_root() {
    local supplied=$1
    case "$supplied" in
        [[:alpha:]]:[\\/]* )
            if ! command -v cygpath >/dev/null 2>&1; then
                echo "release signing: cygpath is required to normalize Windows root: $supplied" >&2
                return 2
            fi
            if ! supplied=$(cygpath -u -- "$supplied"); then
                echo "release signing: cygpath could not normalize Windows root: $supplied" >&2
                return 2
            fi
            ;;
    esac
    printf '%s\n' "$supplied"
}

root=${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
if (($#)); then
    root=$(normalize_root "$root")
fi

workflow="$root/.github/workflows/release-tag.yml"
signer="$root/scripts/release/sign-artifacts.sh"
verifier="$root/scripts/release/verify-release.sh"
for required in "$workflow" "$signer" "$verifier"; do
    [[ -r "$required" ]] || { echo "release signing: missing $required" >&2; exit 1; }
done

failures=0
# Assertions must not trip `set -e`: a failing check has to be recorded and the
# run continue, so that one break does not hide every check after it.
check() {
    local name=$1 status=$2
    if [[ "$status" == 0 ]]; then
        printf 'release signing: ok %s\n' "$name"
    else
        printf 'release signing: FAIL %s\n' "$name" >&2
        failures=$((failures + 1))
    fi
}

# assert NAME CONDITION... -- CONDITION is a command, run for its exit status.
assert() {
    local name=$1 status=0
    shift
    "$@" || status=1
    check "$name" "$status"
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-signing-selftest.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

assert 'signer parses' bash -n "$signer"
assert 'verifier parses' bash -n "$verifier"

# --- constants agree between signer and verifier -----------------------------
#
# Both files also assign these names in their argument parsers (`foo=$2`), so
# anchor on a column-0 assignment opening a single quote and require exactly one
# match -- otherwise the extractor silently reads the parser line instead.
extract_one() {
    local file=$1 name=$2 matches
    matches=$(grep -c "^$name='" "$file" || true)
    [[ "$matches" == 1 ]] || {
        printf 'expected exactly one top-level %s= in %s, found %s\n' \
            "$name" "$file" "$matches" >&2
        return 1
    }
    sed -n "s/^$name='\\(.*\\)'\$/\\1/p" "$file"
}

sign_identity=$(extract_one "$signer" identity_regexp) || sign_identity='<unextractable>'
verify_identity=$(extract_one "$verifier" identity_regexp) || verify_identity='<unextractable>'
sign_issuer=$(extract_one "$signer" oidc_issuer) || sign_issuer='<unextractable>'
verify_issuer=$(extract_one "$verifier" oidc_issuer) || verify_issuer='<unextractable>'

identity_agrees() {
    [[ "$sign_identity" == "$verify_identity" && "$sign_identity" != '<unextractable>' ]]
}
issuer_agrees() {
    [[ "$sign_issuer" == "$verify_issuer" && "$sign_issuer" != '<unextractable>' ]]
}
assert "signer and verifier agree on the certificate identity ($sign_identity vs $verify_identity)" identity_agrees
assert 'signer and verifier agree on the OIDC issuer' issuer_agrees
assert 'OIDC issuer is the GitHub Actions issuer' \
    test "$sign_issuer" = 'https://token.actions.githubusercontent.com'

# The verifier hard-codes owner/repo separately from the identity pattern, in
# the same file, with nothing linking them. A partial rename breaks every
# signature; this catches it without asserting anything about anchoring, which
# is #1287's business.
repository=$(extract_one "$verifier" repository) || repository='<unextractable>'
identity_names_repository() {
    # Compare against the pattern with regex escapes removed: since #1287 the
    # identity is an anchored regexp with escaped dots (github\.com), so a
    # literal substring test for github.com no longer matches even though the
    # repository is named correctly. This assertion is about the repository
    # name, not the regexp syntax -- anchoring is #1287's business and is
    # asserted in test-identity-pattern.sh.
    # Unescape only `\.` -- a blanket strip of every backslash also erases an
    # INVALID escape, so a pattern like `g\ithub\.com` (which GNU grep accepts
    # silently and Go refuses to compile) would pass this assertion. Verified.
    local plain=${sign_identity//\\./.}
    [[ "$plain" == *"https://github.com/$repository/.github/workflows/"* ]]
}
assert "certificate identity names the repository the verifier expects ($repository)" \
    identity_names_repository

# --- the workflow wires it up correctly --------------------------------------
#
# Extract the release-artifacts job by indentation. Assert the block is real
# before asserting anything about its contents, so a mis-extraction fails loudly
# rather than passing vacuously.
block="$tmp/release-artifacts.yml"
awk '/^  release-artifacts:$/ {inside=1} inside && /^  [a-z]/ && !/^  release-artifacts:$/ {exit} inside {print}' \
    "$workflow" >"$block"
# Match against the block with comments stripped. Several assertions below
# name strings this workflow also discusses in prose -- `id-token: write` and
# `contents: write` both appear in comments -- so matching the raw text would
# be satisfied by the commentary rather than by the configuration.
nocomment="$tmp/release-artifacts.nocomment.yml"
sed 's/^[[:space:]]*#.*$//' "$block" >"$nocomment"

# `--` matters: several of the patterns below begin with a dash.
in_block() { grep -Fq -e "$1" "$nocomment"; }
not_in_block() { ! grep -Fq -e "$1" "$nocomment"; }
line_of() { grep -nF -e "$1" "$nocomment" | head -1 | cut -d: -f1; }

# Extract a named step's body, so an assertion about one step cannot be
# satisfied by a different step that happens to contain the same string.
step_block() {
    awk -v want="      - name: $1" '
        $0 == want { inside = 1; print; next }
        inside && /^      - / { exit }
        inside { print }
    ' "$nocomment"
}

# The job-level permissions mapping replaces the workflow-level one wholesale,
# so assert against the mapping itself rather than anywhere in the job.
permissions_block() {
    awk '/^    permissions:$/ { inside = 1; next }
         inside && /^    [a-z]/ { exit }
         inside { print }' "$nocomment"
}

block_extracted() { [[ -s "$block" ]] && grep -q 'make-tarball.sh' "$block"; }
assert 'release-artifacts block extracted (sentinel present)' block_extracted

gated() {
    in_block "needs.release-verification.result == 'success'" &&
        in_block 'needs: [release-verification]'
}
assert 'signing job is gated on the verification gate' gated
assert 'release-artifacts has a timeout' in_block 'timeout-minutes:'
publish_has_token() { step_block 'Publish artifacts to the GitHub Release' | grep -Fq 'GH_TOKEN:'; }
assert 'the publish step has its own GH_TOKEN' publish_has_token
download_has_token() { step_block 'Download provenance attestation bundle' | grep -Fq 'GH_TOKEN:'; }
assert 'the attestation download step has its own GH_TOKEN' download_has_token

# A job-level permissions block replaces the workflow-level one wholesale, so
# every scope this job needs must be listed or it silently becomes `none`.
grants() { permissions_block | grep -Fq -e "$1"; }
for scope in 'contents: write' 'id-token: write' 'attestations: write'; do
    assert "release-artifacts permissions block grants $scope" grants "$scope"
done

assert 'release-artifacts signs the archive' in_block 'sign-artifacts.sh'

ordered() {
    local sign_line publish_line upload_line
    sign_line=$(line_of 'sign-artifacts.sh')
    publish_line=$(line_of 'publish-release-assets.sh')
    upload_line=$(line_of 'uses: actions/upload-artifact')
    [[ -n "$sign_line" && -n "$publish_line" && -n "$upload_line" ]] &&
        ((sign_line < publish_line)) && ((publish_line < upload_line))
}
assert 'sign, then publish to the release, then upload the evidence artifact' ordered

round_trips() { in_block '--signature' && in_block '--certificate'; }
assert 'the pipeline verifies its own output with verify-release.sh' round_trips

# --tag makes verify-release.sh run `git verify-tag`, which needs the GPG key
# that #1154 has not established yet.
no_tag_flag() {
    grep -Fq 'verify-release.sh' "$nocomment" &&
        ! grep -A3 -F 'verify-release.sh' "$nocomment" | grep -Fq -e '--tag'
}
assert 'round-trip does not use --tag (needs the GPG key from #1154)' no_tag_flag

assert 'cosign installer is pinned to a commit SHA' \
    grep -Eq 'sigstore/cosign-installer@[0-9a-f]{40}' "$block"
assert 'cosign binary is pinned to an exact 2.x release' \
    grep -Eq "cosign-release: *'?v2\.[0-9]+\.[0-9]+'?" "$block"

# An empty release body would ship a version with no notes. Process
# substitution hides both the exit status and the empty-output case from
# `set -e`, so the notes must go through a checked file.
assert 'release notes are not passed through an unchecked process substitution' \
    not_in_block '--notes-file <('
assert 'a failed run uploads evidence under a distinct name' \
    in_block 'release-artifacts-partial-'
# A timeout-minutes expiry does not reliably satisfy failure().
assert 'a cancelled run still preserves partial evidence' in_block 'cancelled()'

# Published verification material must never mutate: a re-run mints a fresh
# certificate, signature and Rekor entry.
assert 'published assets are never overwritten' not_in_block '--clobber'
no_clobber_in_publisher() { ! grep -Fq -e '--clobber' "$root/scripts/release/publish-release-assets.sh"; }
assert 'the publisher never overwrites either' no_clobber_in_publisher

# The three signing outputs share one ephemeral key and one Rekor entry.
partial_guard() { in_block 'publish-release-assets.sh'; }
assert 'publication goes through publish-release-assets.sh' partial_guard

# Swapping the arguments keeps the script present but fails at run time, on tag
# day, with `not a directory`.
publish_args() { in_block 'publish-release-assets.sh "$TAG_REF" dist "$notes"'; }
assert 'the publisher is called with tag, dist, notes in that order' publish_args

signer_verifies_from_bundle() {
    grep -A4 -F 'cosign verify-blob' "$signer" | grep -Fq -e '--bundle' &&
        ! grep -A4 -F 'cosign verify-blob' "$signer" | grep -Fq -e '--signature'
}
assert 'the signer round-trips from the bundle, not the detached pair' \
    signer_verifies_from_bundle

# A reusable-workflow call would change the OIDC subject to the caller's path
# and ref, silently invalidating every signature against the identity pattern.
# Assert the precondition directly -- release-tag.yml must not be callable --
# rather than scanning for a `uses:` that GitHub would already reject.
not_reusable() { ! grep -Eq '^ *workflow_call:' "$workflow"; }
assert 'release-tag.yml is not callable as a reusable workflow' not_reusable

invoked_once() {
    local count
    count=$(grep -rlF "$1" "$root/.github/workflows/" 2>/dev/null | wc -l)
    [[ "$count" == 1 ]]
}
assert 'sign-artifacts.sh is invoked from exactly one workflow' \
    invoked_once 'sign-artifacts.sh'
assert 'publish-release-assets.sh is invoked from exactly one workflow' \
    invoked_once 'publish-release-assets.sh'

# --- signer behaviour, driven by shims, no network ---------------------------
shim_dir="$tmp/bin"
mkdir -p "$shim_dir"
calls="$tmp/calls"

make_shim() {
    local version=$1 mode=$2
    cat >"$shim_dir/cosign" <<EOF
#!/usr/bin/env bash
if [[ "\$1" == version ]]; then
    if [[ "\$2" == --json ]]; then printf '{"gitVersion":"v%s"}\n' '$version'
    else printf 'GitVersion:    v%s\nGitCommit:     abc\n' '$version'; fi
    exit 0
fi
if [[ "\$1" == sign-blob ]]; then
    echo sign-blob >>'$calls'
    case '$mode' in
        ok)
            while (( \$# )); do
                case "\$1" in
                    --output-signature|--output-certificate|--bundle) : >"\$2"; echo x >"\$2"; shift 2 ;;
                    *) shift ;;
                esac
            done
            exit 0 ;;
        transient) echo 'Error: Post "https://fulcio.sigstore.dev/api/v2/signingCert": dial tcp: connection refused' >&2; exit 1 ;;
        ratelimit) echo 'Error: Post "https://rekor.sigstore.dev/api/v1/log/entries": 503' >&2; exit 1 ;;
        tuf) echo 'Error: updating to TUF trusted root' >&2; exit 1 ;;
        permanent) echo 'Error: unknown flag: --nope' >&2; exit 1 ;;
        digest) echo 'Error: signing dist/stuff.tar.gz: sha256:a1b500c2 for cert 4290ab rejected' >&2; exit 1 ;;
    esac
fi
exit 0
EOF
    chmod +x "$shim_dir/cosign"
}

run_signer() {
    : >"$calls"
    PATH="$shim_dir:$PATH" ACTIONS_ID_TOKEN_REQUEST_URL='https://example.invalid/token' \
        WIRELOG_SIGN_RETRY_DELAY_SECONDS=0 \
        "$signer" "$@" --no-verify
}

archive="$tmp/wirelog-9.9.9.tar.gz"
echo payload >"$archive"

make_shim 3.1.3 ok
rejects_v3() {
    local out status=0
    out=$(run_signer "$archive" 2>&1) || status=$?
    [[ "$status" != 0 && "$out" == *'--bundle'* ]]
}
assert 'cosign 3 is rejected, naming the --bundle migration' rejects_v3

make_shim 2.6.5 ok
# Pre-seed a signature so the run has to be correct about what it signs rather
# than incidentally right because nothing existed.
echo stale >"$archive.sig"
produces_three() {
    run_signer "$archive" >/dev/null 2>&1 || return 1
    [[ -s "$archive.sig" && -s "$archive.pem" && -s "$archive.cosign.bundle" &&
        ! -e "$archive.sig.sig" && ! -e "$archive.pem.sig" ]]
}
assert 'cosign 2 produces .sig, .pem and .cosign.bundle and signs only the archive' \
    produces_three

attempts_were() {
    run_signer "$archive" >/dev/null 2>&1 || true
    [[ "$(wc -l <"$calls" | tr -d ' ')" == "$1" ]]
}

make_shim 2.6.5 permanent
assert 'a permanent cosign failure is not retried' attempts_were 1

make_shim 2.6.5 transient
assert 'a transient cosign failure is retried three times' attempts_were 3

# The two flakes most likely to hit a release: a Fulcio/Rekor rate limit, and
# cosign's TUF trusted-root fetch.
# Payloads chosen so each is matched by exactly one alternative of the
# classifier: a bare status code with no prose keyword, and a TUF failure with
# no "remote mirror".
make_shim 2.6.5 ratelimit
assert 'a bare 5xx status is retried' attempts_were 3

make_shim 2.6.5 tuf
assert 'a TUF trusted-root failure is retried' attempts_were 3

# Bounding those alternatives must not make them fire on ordinary error text:
# a digest containing 500, a serial containing 429, or the word "stuff".
make_shim 2.6.5 digest
assert 'a digest containing a status-code-shaped number is not retried' attempts_were 1

make_shim 2.6.5 ok
needs_oidc() {
    local out status=0
    out=$(PATH="$shim_dir:$PATH" "$signer" "$archive" --no-verify 2>&1) || status=$?
    [[ "$status" != 0 && "$out" == *'id-token'* ]]
}
assert 'a missing OIDC token endpoint fails immediately with a named reason' needs_oidc

# Verify that version parsing works for text banner format as well as JSON.
make_shim_banner_only() {
    local version=$1
    cat >"$shim_dir/cosign" <<EOF
#!/usr/bin/env bash
if [[ "\$1" == version ]]; then
    printf 'GitVersion:    v%s\nGitCommit:     abc\n' '$version'
    exit 0
fi
if [[ "\$1" == sign-blob ]]; then
    echo sign-blob >>'$calls'
    while (( \$# )); do
        case "\$1" in
            --output-signature|--output-certificate|--bundle) : >"\$2"; echo x >"\$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    exit 0
fi
exit 0
EOF
    chmod +x "$shim_dir/cosign"
}

make_shim_banner_only 2.6.5
banner_version_works() {
    : >"$calls"
    run_signer "$archive" >/dev/null 2>&1 || return 1
    [[ "$(wc -l <"$calls" | tr -d ' ')" == 1 ]]
}
assert 'version parsing succeeds with text banner format' banner_version_works
make_shim 2.6.5 ok

# --- publication: drive the real script with a fake gh ------------------------
#
# The partial-set guard is the fix for a defect whose regression would be
# silent, unrepairable by re-running, and visible only to a consumer. Asserting
# its error message is not enough: gutting the suffix list or neutering the
# threshold keeps the message and breaks the guard. Exercise the behaviour.
publisher="$root/scripts/release/publish-release-assets.sh"
assert 'publisher parses' bash -n "$publisher"

pub_dir="$tmp/pub"
gh_calls="$tmp/gh-calls"

# $1 is the newline-separated list of asset names the fake release already has.
# $2 is 'absent' to make the release not exist yet, so the `gh release create`
# branch runs -- the only branch the first real release takes.
setup_publish() {
    rm -rf "$pub_dir"
    mkdir -p "$pub_dir/dist" "$pub_dir/bin"
    for suffix in .tar.gz .tar.gz.sha256 .tar.gz.blake3 .tar.gz.sig .tar.gz.pem \
                  .tar.gz.cosign.bundle; do
        echo x >"$pub_dir/dist/wirelog-9.9.9$suffix"
    done
    echo notes >"$pub_dir/notes.md"
    : >"$gh_calls"
    cat >"$pub_dir/bin/gh" <<EOF
#!/usr/bin/env bash
echo "\$*" >>'$gh_calls'
case "\$2" in
    view)
        if [[ "\$*" == *assets* ]]; then
            printf '%s\n' $(printf '%q' "$1")
            exit 0
        fi
        # The bare existence probe.
        [[ '${2:-present}' == absent ]] && exit 1
        exit 0 ;;
esac
exit 0
EOF
    chmod +x "$pub_dir/bin/gh"
}

run_publish() {
    PATH="$pub_dir/bin:$PATH" "$publisher" v9.9.9 "$pub_dir/dist" "$pub_dir/notes.md"
}

uploads() { grep -c '^release upload' "$gh_calls" || true; }

creates() { grep -c '^release create' "$gh_calls" || true; }

setup_publish '' absent
publish_creates() {
    run_publish >/dev/null 2>&1 &&
        [[ "$(creates)" == 1 ]] && [[ "$(uploads)" == 6 ]]
}
assert 'an absent release is created and receives all six assets' publish_creates

# The release is created as a draft so the publish decision stays with the
# maintainer; dropping --draft would turn a tag push into an immediate public
# release. This is the branch the first real release takes.
created_as_draft() { grep '^release create' "$gh_calls" | grep -Fq -e '--draft'; }
assert 'a created release is a draft' created_as_draft
created_with_notes() { grep '^release create' "$gh_calls" | grep -Fq -e '--notes-file'; }
assert 'a created release gets its body from the notes file' created_with_notes
created_verifying_tag() { grep '^release create' "$gh_calls" | grep -Fq -e '--verify-tag'; }
assert 'a created release verifies the tag exists' created_verifying_tag

setup_publish '' present
publish_fresh() {
    run_publish >/dev/null 2>&1 && [[ "$(creates)" == 0 ]] && [[ "$(uploads)" == 6 ]]
}
assert 'an existing release with no assets is not re-created' publish_fresh

setup_publish 'wirelog-9.9.9.tar.gz.sig
wirelog-9.9.9.tar.gz.pem
wirelog-9.9.9.tar.gz.cosign.bundle' present
publish_complete() { run_publish >/dev/null 2>&1 && [[ "$(uploads)" == 3 ]]; }
assert 'a complete signing set is skipped, other assets still upload' publish_complete

# The exact defect: .sha256 sorts between .pem and .sig, so an interrupted
# upload leaves two of three and the re-run would mix ephemeral keys.
for partial in 'wirelog-9.9.9.tar.gz.pem' \
               'wirelog-9.9.9.tar.gz.pem
wirelog-9.9.9.tar.gz.cosign.bundle' \
               'wirelog-9.9.9.tar.gz.sig
wirelog-9.9.9.tar.gz.pem'; do
    setup_publish "$partial" present
    refuses() { ! run_publish >/dev/null 2>&1 && [[ "$(uploads)" == 0 ]]; }
    assert "a partial signing set is refused ($(printf '%s\n' "$partial" | wc -l | tr -d ' ') of 3 published)" refuses
done

# Emptiness must be caught before the release is created, not after.
setup_publish '' absent
: >"$pub_dir/notes.md"
empty_notes() {
    ! run_publish >/dev/null 2>&1 &&
        [[ "$(uploads)" == 0 ]] && [[ "$(creates)" == 0 ]]
}
assert 'empty release notes are refused before anything is created' empty_notes

setup_publish '' present
rm -f "$pub_dir"/dist/wirelog-9.9.9.tar.gz
no_archive() { ! run_publish >/dev/null 2>&1; }
assert 'a dist without a source archive is refused' no_archive

assert 'publisher --help exits 0' "$publisher" --help
publisher_no_args() { ! "$publisher" >/dev/null 2>&1; }
assert 'publisher with no arguments exits non-zero' publisher_no_args

assert 'signer --help exits 0' "$signer" --help
no_args_fails() { ! "$signer" >/dev/null 2>&1; }
assert 'signer with no arguments exits non-zero' no_args_fails

if ((failures)); then
    printf 'release signing: %d check(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'release signing: all checks passed\n'
