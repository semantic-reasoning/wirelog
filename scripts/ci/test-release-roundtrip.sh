#!/usr/bin/env bash
# Round-trip coverage for make-tarball.sh and verify-release.sh.
#
# Issue #1295. Neither script had any automated coverage. verify-release.sh is
# the only script in this repository that third parties run -- docs publish it
# as the recipe for verifying a release -- so a break in it is found by
# consumers rather than by CI. make-tarball.sh is run by release-tag.yml and
# nothing asserted anything about its output.
#
# The asymmetry that motivated this: #1291 landed with a regression test,
# #1292's one-line `sha256sum is required` guard landed with none, because
# there was no harness to add one to. Case 5 below is that harness.
#
# Both scripts are run for real against a throwaway repository; nothing here
# reads or writes the working tree.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-release-roundtrip: SKIP: needs a POSIX host"; exit 77 ;;
esac
for tool in git sha256sum b3sum gzip awk tar od cmp; do
    command -v "$tool" >/dev/null 2>&1 || {
        # b3sum is installed only by release-tag.yml, not the PR workflow.
        echo "test-release-roundtrip: SKIP: $tool not available"; exit 77
    }
done

# GIT_DIR and GIT_WORK_TREE override `git -C` and a plain `cd`, and git exports
# both inside hooks, `git bisect run` and `git rebase --exec`. make-tarball.sh
# resolves its repo with `git rev-parse --show-toplevel`, so under any of those
# it would archive the CALLER's repository instead of the fixture.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
      GIT_COMMON_DIR GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CEILING_DIRECTORIES \
      GIT_TEMPLATE_DIR GIT_NAMESPACE \
      GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_AUTHOR_DATE \
      GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_COMMITTER_DATE \
      GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS

# BASH_ENV is sourced by every non-interactive bash, so a value that needs PATH
# breaks precisely the stripped-PATH cases below -- the ones pinning #1292.
unset BASH_ENV

root=$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(CDPATH= cd -- "$(mktemp -d "${TMPDIR:-/tmp}/wirelog-roundtrip.XXXXXX")" && pwd)
trap 'rm -rf "$tmp"' EXIT

failures=0
last_case=""
expect_status() {
    local name=$1 want=$2 got=0
    shift 2
    last_case=$name
    "$@" >"$tmp/out" 2>"$tmp/err" || got=$?
    if [[ "$got" == "$want" ]]; then
        printf 'test-release-roundtrip: ok %s\n' "$name"
    else
        printf 'test-release-roundtrip: FAIL %s (want exit %s, got %s)\n' "$name" "$want" "$got" >&2
        sed 's/^/    /' "$tmp/out" "$tmp/err" >&2
        failures=$((failures + 1))
    fi
}
# Asserts against the output of the most recent expect_status, whose name it
# prints so a misordered pair is visible rather than silently checking the
# wrong command's output.
expect_says() {
    local name=$1 needle=$2
    if grep -qF -e "$needle" "$tmp/out" "$tmp/err"; then
        printf 'test-release-roundtrip: ok %s (output of: %s)\n' "$name" "$last_case"
    else
        printf 'test-release-roundtrip: FAIL %s (no %q in output of: %s)\n' "$name" "$needle" "$last_case" >&2
        failures=$((failures + 1))
    fi
}
check() {
    local name=$1 ok=$2
    if [[ "$ok" == 0 ]]; then printf 'test-release-roundtrip: ok %s\n' "$name"
    else printf 'test-release-roundtrip: FAIL %s\n' "$name" >&2; failures=$((failures + 1)); fi
}
# The condition runs inside the helper: `cond; check $?` aborts under set -e
# before check runs, so the first failure would truncate the suite instead of
# reporting it.
assert() { local n=$1 ok=0; shift; "$@" || ok=1; check "$n" "$ok"; }
refute() { local n=$1 ok=0; shift; "$@" && ok=1; check "$n" "$ok"; }

# A throwaway repository for make-tarball.sh to archive. Config is pinned for
# the reasons each line names: all of these otherwise fail a correct tree with
# a diagnostic naming neither script.
repo="$tmp/repo"
mkdir -p "$repo" "$tmp/nohooks" "$tmp/notmpl"
fixture_git() {
    git -C "$repo" -c user.email=t@example.com -c user.name=Test \
        -c commit.gpgsign=false -c tag.gpgSign=false \
        -c core.excludesFile=/dev/null -c core.attributesFile=/dev/null \
        -c core.hooksPath="$tmp/nohooks" \
        -c core.autocrlf=false -c core.safecrlf=false "$@"
}
printf "project('wirelog', 'c',\n  version: '9.9.9',\n)\n" > "$repo/meson.build"
printf 'int main(void) { return 0; }\n' > "$repo/main.c"
fixture_git -c init.templateDir="$tmp/notmpl" init -q
fixture_git add -A
fixture_git commit -qm 'fixture' --no-verify

mt="$root/scripts/release/make-tarball.sh"
vr="$root/scripts/release/verify-release.sh"
dist="$tmp/dist"

# --- make-tarball.sh --------------------------------------------------------
build() { ( CDPATH= cd -- "$repo" && "$mt" "$dist" ); }
expect_status 'make-tarball builds an archive' 0 build
archive="$dist/wirelog-9.9.9.tar.gz"
assert 'the archive is created'          test -f "$archive"
assert 'the sha256 manifest is created'  test -f "$archive.sha256"
assert 'the blake3 manifest is created'  test -f "$archive.blake3"

# Reproducibility, which is what release verification rests on. Note the
# script's comment credits `gzip -n` for this, and that is not where it comes
# from: gzip embeds an mtime only when compressing a NAMED file, and here it
# reads a pipe, so `gzip -9` and `gzip -n -9` produce byte-identical output.
# Measured -- both write MTIME 00000000. `-n` is harmless defensive coding that
# no mutation of this file can kill; the determinism is git-archive's.
dist2="$tmp/dist2"
# `|| true`: unguarded, a failing second build aborts the file under set -e
# after four ok lines and prints no FAIL at all -- an empty CI log for a real
# failure. The assertion below reports it instead.
( CDPATH= cd -- "$repo" && "$mt" "$dist2" ) >"$tmp/build2.log" 2>&1 || {
    echo "test-release-roundtrip: note: the second build failed:" >&2
    sed 's/^/    /' "$tmp/build2.log" >&2
}
assert 'repeated builds are byte-identical' \
    cmp -s "$archive" "$dist2/wirelog-9.9.9.tar.gz"

# Pin the header field directly rather than only comparing two runs: two builds
# in the same second would agree even if a wall-clock mtime were embedded, so
# the comparison above cannot distinguish "deterministic" from "fast".
gzip_mtime_is_zero() {
    local mt
    mt=$(od -An -tx1 -j4 -N4 "$archive" | tr -d ' \n')
    [[ "$mt" == "00000000" ]]
}
assert 'the gzip header carries no timestamp' gzip_mtime_is_zero

# An unparseable version must fail rather than produce wirelog-.tar.gz.
badrepo="$tmp/badrepo"; mkdir -p "$badrepo"
printf "project('wirelog', 'c',\n  version: 'not-a-version',\n)\n" > "$badrepo/meson.build"
# Same pins as fixture_git, for the same reasons -- autocrlf and safecrlf
# together abort with "LF would be replaced by CRLF" on a correct tree, and
# this helper had drifted from the one above.
bad_git() { git -C "$badrepo" -c user.email=t@e -c user.name=T \
    -c commit.gpgsign=false -c tag.gpgSign=false \
    -c core.hooksPath="$tmp/nohooks" -c core.excludesFile=/dev/null \
    -c core.attributesFile=/dev/null \
    -c core.autocrlf=false -c core.safecrlf=false "$@"; }
bad_git -c init.templateDir="$tmp/notmpl" init -q
bad_git add -A; bad_git commit -qm bad --no-verify
build_bad() { ( CDPATH= cd -- "$badrepo" && "$mt" "$tmp/baddist" ); }
expect_status 'an unparseable project version fails' 1 build_bad
expect_says   'the version failure names the version' 'invalid project version'

# The archive's CONTENTS, not just its checksums. Without this, dropping a
# source file from the archive, or changing the extraction prefix, passed every
# assertion -- and those are precisely what a consumer discovers.
listing="$tmp/listing.txt"
tar -tzf "$archive" > "$listing" 2>/dev/null || true
assert 'the archive contains the project sources' \
    grep -qxF 'wirelog-9.9.9/main.c' "$listing"
assert 'the archive contains meson.build' \
    grep -qxF 'wirelog-9.9.9/meson.build' "$listing"
# The non-empty check is not redundant: `grep -qv` over zero lines returns 1,
# which negates to success, so this would pass vacuously on an empty listing.
prefix_is_versioned() {
    [[ -s "$listing" ]] || return 1
    ! grep -qvE '^wirelog-9\.9\.9/' "$listing"
}
assert 'every entry sits under the versioned prefix' prefix_is_versioned

# --- verify-release.sh, the happy path --------------------------------------
expect_status 'verify accepts a freshly built archive' 0 "$vr" "$archive"
expect_says   'the pass reports checksum verification' 'verified checksums for'
expect_says   'the pass says signed inputs were absent' 'signed inputs were not supplied'

# --- verify-release.sh, the cases it exists for -----------------------------
corrupt="$tmp/corrupt"; mkdir -p "$corrupt"
cp "$archive" "$archive.sha256" "$archive.blake3" "$corrupt/"
ca="$corrupt/wirelog-9.9.9.tar.gz"
# Flip the recorded hash while leaving the name intact: a corrupted manifest
# has to be distinguishable from a manifest naming the wrong file.
#
# The replacement is chosen against the existing digit rather than hardcoded.
# An earlier version substituted a literal "0", which is a no-op whenever the
# hash already begins with 0 -- and the fixture's commit differs per run, so
# the hash does too. That flaked 2 runs in 15: the manifest was unchanged, the
# archive verified correctly, and the assertion failed. A corruption fixture
# has to be built from what actually changes the value, not from the shape of
# "corrupt a digit".
flip_first_hex() {
    awk 'NR==1 { first = substr($0, 1, 1)
                 sub(/^./, (first == "0" ? "1" : "0"))
                 print; next } { print }' "$1" > "$1.tmp"
    mv "$1.tmp" "$1"
}
flip_first_hex "$ca.sha256"
expect_status 'a corrupted sha256 manifest fails' 1 "$vr" "$ca"
expect_says   'the failure names SHA256, not the archive name' 'SHA256 mismatch'
cp "$archive.sha256" "$ca.sha256"

flip_first_hex "$ca.blake3"
expect_status 'a corrupted blake3 manifest fails' 1 "$vr" "$ca"
expect_says   'the failure names BLAKE3' 'BLAKE3 mismatch'
cp "$archive.blake3" "$ca.blake3"

# A manifest naming a different archive is a distinct failure from a bad hash.
sed 's/wirelog-9.9.9.tar.gz/wirelog-0.0.0.tar.gz/' "$archive.sha256" > "$ca.sha256"
expect_status 'a manifest naming another archive fails' 1 "$vr" "$ca"
expect_says   'the failure says the manifest names the wrong file' 'does not name'
cp "$archive.sha256" "$ca.sha256"

expect_status 'a missing archive is an error' 1 "$vr" "$tmp/nonexistent.tar.gz"
expect_says   'the missing archive is reported as such, not as a missing tool' \
    'archive and both checksum files are required'

# #1292's guard -- the assertion that issue could not have, because there was
# no harness for it.
#
# Note which half does the pinning: the STATUS is 1 either way, because with
# the guard deleted verify-release.sh:70 degenerates to `cd ""` and set -e
# exits 1 anyway. Only the message assertion tells "sha256sum is missing" from
# "something else went wrong", so expect_says is what pins #1292 here and
# expect_status is scaffolding for it.
# "$BASH", not "$vr" and not bare `bash`. Executing the script directly runs
# its `#!/usr/bin/env bash` shebang, and env cannot find bash under a stripped
# PATH -- 127, and the guard under test never runs. A bare `bash` fails the
# same way, because the PATH assignment applies to that lookup too. $BASH is
# the running interpreter's absolute path, so no lookup happens.
# Each stripped PATH omits exactly ONE tool and keeps the rest. An earlier
# version held only the other checksum tool, so deleting the guard under test
# still exited 1 -- via `dirname: command not found` -- and the status
# assertion passed against code with the guard removed.
mkdir -p "$tmp/nosha" "$tmp/nob3"
for t in dirname basename awk gzip cat sed head tr od cmp; do
    p=$(command -v "$t" 2>/dev/null) || continue
    ln -sf "$p" "$tmp/nosha/$t"; ln -sf "$p" "$tmp/nob3/$t"
done
ln -sf "$(command -v b3sum)"     "$tmp/nosha/b3sum"
ln -sf "$(command -v sha256sum)" "$tmp/nob3/sha256sum"
no_sha() { PATH="$tmp/nosha" "$BASH" "$vr" "$archive"; }
expect_status 'a missing sha256sum is an error' 1 no_sha
expect_says   'the missing sha256sum is named' 'sha256sum is required'

no_b3() { PATH="$tmp/nob3" "$BASH" "$vr" "$archive"; }
expect_status 'a missing b3sum is an error' 1 no_b3
expect_says   'the missing b3sum is named' 'b3sum is required'

# --- argument handling ------------------------------------------------------
expect_status 'no arguments is a usage error' 2 "$vr"
expect_status '--signature without --certificate is a usage error' 2 \
    "$vr" "$archive" --signature "$tmp/sig"
expect_says   'the pairing requirement is explained' 'must be supplied together'
expect_status '--tag without a value is a usage error' 2 "$vr" "$archive" --tag
expect_status 'an unknown option is a usage error' 2 "$vr" "$archive" --nope

if ((failures)); then
    printf 'test-release-roundtrip: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-release-roundtrip: all cases passed\n'
