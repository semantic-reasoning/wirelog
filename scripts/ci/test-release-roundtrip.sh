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

# --- paths that make coreutils escape its output (#1311) --------------------
# GNU sha256sum prefixes the whole line with a literal backslash when the
# filename contains one, so passing a full path yielded "\<hash>" and the
# comparison reported a mismatch on a byte-perfect archive. The fixture puts
# the backslash in a PARENT directory with a plain archive name: that is the
# shape the published recipe produces, and it is what distinguishes "the path
# was escaped" from "the name was escaped".
bs_dir="$tmp/bs/back\\slash"
mkdir -p "$bs_dir"
cp "$archive" "$archive.sha256" "$archive.blake3" "$bs_dir/"
bs_archive="$bs_dir/wirelog-9.9.9.tar.gz"
expect_status 'an archive under a backslash path verifies' 0 "$vr" "$bs_archive"
expect_says   'and reports verification, not a mismatch' 'verified checksums for'

# The same path must still reject a genuinely corrupt archive -- otherwise the
# fix would have turned a false failure into a false pass.
bs_bad="$tmp/bsbad/back\\slash"
mkdir -p "$bs_bad"
cp "$archive" "$archive.sha256" "$archive.blake3" "$bs_bad/"
printf 'corruption' >> "$bs_bad/wirelog-9.9.9.tar.gz"
expect_status 'a corrupt archive under a backslash path still fails' 1 \
    "$vr" "$bs_bad/wirelog-9.9.9.tar.gz"
expect_says   'and names the mismatch' 'SHA256 mismatch'

# A symlinked parent followed by `..`: bash's `cd` resolves that LOGICALLY,
# while the kernel -- and every other stage of verify-release.sh -- opens the
# physical target. Hashing the logical path made the checksum describe a
# different file than the one being verified, and reported success on a path
# whose real target was tampered. This is the fail-OPEN direction, so it is
# pinned in both directions.
sym="$tmp/sym"; mkdir -p "$sym/a" "$sym/b"
cp "$archive" "$sym/good.tar.gz"
sed 's/wirelog-9\.9\.9\.tar\.gz/good.tar.gz/' "$archive.sha256" > "$sym/good.tar.gz.sha256"
sed 's/wirelog-9\.9\.9\.tar\.gz/good.tar.gz/' "$archive.blake3" > "$sym/good.tar.gz.blake3"
ln -s "$sym/b" "$sym/a/link"
cp "$sym/good.tar.gz" "$sym/good.tar.gz.sha256" "$sym/good.tar.gz.blake3" "$sym/a/"
logical_path="$sym/a/link/../good.tar.gz"

expect_status 'a good archive reached through a symlinked parent verifies' 0 \
    "$vr" "$logical_path"

# Now tamper ONLY the physical target. The logical sibling stays good, so a
# logical resolution would hash the good copy and report success.
printf 'TAMPERED' >> "$sym/good.tar.gz"
expect_status 'a tampered physical target is rejected, not masked by the logical path' 1 \
    "$vr" "$logical_path"
expect_says   'and the rejection names the mismatch' 'SHA256 mismatch'

# --- argument handling ------------------------------------------------------
expect_status 'no arguments is a usage error' 2 "$vr"
expect_status '--signature without --certificate is a usage error' 2 \
    "$vr" "$archive" --signature "$tmp/sig"
expect_says   'the pairing requirement is explained' 'must be supplied together'
expect_status '--tag without a value is a usage error' 2 "$vr" "$archive" --tag
expect_status 'an unknown option is a usage error' 2 "$vr" "$archive" --nope

# --- #1315: names coreutils escapes, and one it does not ----------------------
#
# Built from REAL sha256sum/b3sum output, not hand-written manifests: a
# hand-written unescaped manifest would pin the parsing code without exercising
# the escaping condition at all, which is the whole point of these cases.
#
# `a\nb.tar.gz` is the discriminator. Unescaping the manifest name -- the
# obvious fix, and the one #1315 suggested -- is ambiguous here: coreutils
# writes it `a\\nb.tar.gz`, and replacing `\\` then `\n` in sequence yields a
# newline. Verifying by ESCAPING the expected name instead has no such case.
# `has space.tar.gz` needs no escaping at all and was broken separately, by
# reading the name as awk's `$2`.
# Under $tmp, which line 46's trap already removes. A second mktemp with its
# own trap would REPLACE that trap and orphan $tmp every run.
escaping_dir="$tmp/escaping"

verifies_awkward_name() {
    local name=$1 dir="$escaping_dir/$2"
    mkdir -p "$dir"
    printf 'payload' >"$dir/$name"
    ( cd "$dir" && sha256sum -- "$name" >"$name.sha256" && b3sum -- "$name" >"$name.blake3" )
    "$vr" "$dir/$name" "$dir/$name.sha256" "$dir/$name.blake3" >/dev/null 2>&1
}

# No `command -v b3sum` guard here: the tool check near the top already exits 77
# for the whole suite when b3sum is absent, so a guard would be dead code whose
# else-branch printed a fake `ok` outside this file's SKIP protocol.
assert 'an archive whose name contains a backslash verifies' \
    verifies_awkward_name 'back\slash.tar.gz' backslash
assert 'an archive whose name contains a newline verifies' \
    verifies_awkward_name "$(printf 'new\nline.tar.gz')" newline
assert 'an archive whose name contains a carriage return verifies' \
    verifies_awkward_name "$(printf 'car\rriage.tar.gz')" carriage
assert 'an archive whose name contains a space verifies' \
    verifies_awkward_name 'has space.tar.gz' space
# The one that would pass under a naive unescape while being wrong.
assert 'a name containing a literal backslash-n verifies' \
    verifies_awkward_name 'a\nb.tar.gz' backslash_n

# A terminal newline is the one basename shape command substitution cannot
# preserve. Generate both manifests with the real coreutils so this exercises
# the complete reader path rather than a hand-written name.
terminal_newline_name() {
    local name=$'trailing\n' dir="$escaping_dir/terminal-newline"
    mkdir -p "$dir"
    printf 'payload' >"$dir/$name"
    ( cd "$dir" && sha256sum -- "$name" >"$name.sha256" &&
      b3sum -- "$name" >"$name.blake3" )
    "$vr" "$dir/$name" "$dir/$name.sha256" "$dir/$name.blake3" \
        >/dev/null 2>&1
}
assert 'an archive whose name ends in a newline verifies' terminal_newline_name

# --- #1316: make-tarball.sh's manifest-writing hardening ----------------------
#
# Three hardenings, each with its own fixture, because a fixture that passes
# with any one of them removed is not covering it. Verified: each mutant below
# is killed by exactly one of these three and by no other.
#
#   CDPATH=  a RELATIVE out_dir found through CDPATH sends `cd` to whichever
#            directory CDPATH matched, so the manifest is written in the wrong
#            place -- or, in the current form, the resolved path `cd` echoes is
#            captured into archive_dir and the next cd fails.
#   -P       an out_dir of the form `x/dir/link/..` resolves to the link
#            target's PARENT for the kernel that writes the archive, but to
#            `x/dir` for a logical `cd`.
#   --       an out_dir beginning with `-` is parsed as options. Note this is
#            caught at `mkdir -p --`, which runs BEFORE the manifest subshells;
#            hardening only those two would leave this case unreachable and the
#            `--` untestable.
harden_dir="$tmp/harden"

# Everything runs in ONE subshell at $outer_cd, because make-tarball.sh prints
# paths as given -- a relative out_dir yields relative paths, which only resolve
# from the directory it was invoked in.
builds_manifest_naming_archive() {
    local out=$1 outer_cd=$2 cdpath=$3
    (
        cd "$outer_cd" || exit 1
        local lines arch man
        lines=$( CDPATH="$cdpath" "$mt" "$out" ) || exit 1
        arch=$(printf '%s\n' "$lines" | sed -n 1p)
        man=$(printf '%s\n' "$lines" | sed -n 2p)
        [ -f "$man" ] || exit 1
        [ "$(sed -n 's/^[0-9a-f]*  //p' -- "$man")" = "$(basename -- "$arch")" ]
    )
}

# All three run from inside $repo: make-tarball.sh derives its repo root from
# the working directory, so a relative out_dir must be relative to that.
mkdir -p "$harden_dir/decoy/dist-rel"
assert 'a relative out_dir is unaffected by CDPATH' \
    builds_manifest_naming_archive dist-rel "$repo" "$harden_dir/decoy"

mkdir -p "$harden_dir/x/dir" "$harden_dir/y/target"
ln -sfn "$harden_dir/y/target" "$harden_dir/x/dir/link"
assert 'an out_dir reached through a symlink and .. writes beside its archive' \
    builds_manifest_naming_archive "$harden_dir/x/dir/link/.." "$repo" ''

assert 'an out_dir beginning with a dash is not parsed as options' \
    builds_manifest_naming_archive -outdir "$repo" ''

# --- #1331: make-tarball.sh's ref resolution must stop Git option parsing ---
# Use a git shim so this assertion checks the exact argument vector rather than
# relying on a version-specific diagnostic for an invalid ref. The shim still
# delegates every other Git operation to the real executable, and rejects the
# candidate if the protected rev-parse call lacks --end-of-options.
real_git=$(command -v git)
git_shim_dir="$tmp/git-shim"
mkdir -p "$git_shim_dir"
cat >"$git_shim_dir/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
real_git=${WIRELOG_REAL_GIT:?}
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ "${args[$i]}" == rev-parse && $((i + 1)) -lt ${#args[@]} &&
          "${args[$((i + 1))]}" == --verify ]]; then
        # The script probes support with `HEAD` before resolving the caller's
        # ref. Only enforce the exact argument vector for the dash-leading ref
        # call; the probe itself is delegated to the real Git.
        if [[ "${args[$((i + 2))]-}" == '-not-a-ref^{commit}' ||
              "${args[$((i + 3))]-}" == '-not-a-ref^{commit}' ]]; then
            if [[ $((i + 4)) -ne ${#args[@]} ||
                  "${args[$((i + 2))]}" != --end-of-options ]]; then
                echo 'test-release-roundtrip: rev-parse arguments are not safely ordered' >&2
                exit 97
            fi
        fi
    fi
done
exec "$real_git" "$@"
EOF
chmod +x "$git_shim_dir/git"
dash_ref_fails_closed() {
    local out="$tmp/dash-ref-dist"
    (
        PATH="$git_shim_dir:$PATH"
        export PATH WIRELOG_REAL_GIT="$real_git"
        CDPATH= cd -- "$repo"
        "$mt" "$out" '-not-a-ref'
    )
}
expect_status 'a dash-leading ref is passed after Git end-of-options' 128 \
    dash_ref_fails_closed
refute 'the dash-leading ref failure is not the shim regression' \
    grep -qF 'omitted --end-of-options' "$tmp/out" "$tmp/err"

if ((failures)); then
    printf 'test-release-roundtrip: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-release-roundtrip: all cases passed\n'
