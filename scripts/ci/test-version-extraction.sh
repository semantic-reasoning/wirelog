#!/usr/bin/env bash
# Regression test for make-tarball.sh's project-version extraction.
#
# Issue #1312. The version was read with `... | sed -n '...p' | head -1` under
# `set -euo pipefail`. head exits after the first line; once sed crosses its
# first 4 KiB stdout flush it writes into a closed pipe, takes SIGPIPE, and
# pipefail turns the release build into a bare exit 141 with no message.
#
# The threshold is far lower than it looks: about 700 matching `version:`
# lines, roughly 14 KB. This meson.build is already 25 KB, so the fixture below
# is not an exotic size -- it is smaller than the real file.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-version-extraction: SKIP: needs a POSIX host"; exit 77 ;;
esac
for t in git sha256sum b3sum gzip sed; do
    command -v "$t" >/dev/null 2>&1 || {
        echo "test-version-extraction: SKIP: $t not available"; exit 77
    }
done

# make-tarball.sh resolves its repo with `git rev-parse --show-toplevel`, so
# these would otherwise point it at the caller's repository.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY \
      GIT_COMMON_DIR GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CEILING_DIRECTORIES \
      GIT_TEMPLATE_DIR GIT_NAMESPACE GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM \
      GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS \
      GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_AUTHOR_DATE \
      GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_COMMITTER_DATE

root=$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(CDPATH= cd -- "$(mktemp -d "${TMPDIR:-/tmp}/wirelog-version-extract.XXXXXX")" && pwd)
trap 'rm -rf "$tmp"' EXIT

failures=0
check() {
    local name=$1 ok=$2
    if [[ "$ok" == 0 ]]; then printf 'test-version-extraction: ok %s\n' "$name"
    else printf 'test-version-extraction: FAIL %s\n' "$name" >&2; failures=$((failures + 1)); fi
}
# The condition runs inside the helper. `cond; check $?` aborts under set -e
# before check runs, which silently truncates the suite at the first failure.
assert() { local n=$1 ok=0; shift; "$@" || ok=1; check "$n" "$ok"; }

mkdir -p "$tmp/nohooks" "$tmp/notmpl"
mkrepo() {
    local dir=$1
    mkdir -p "$dir"
    printf 'int main(void) { return 0; }\n' > "$dir/main.c"
    git -C "$dir" -c init.templateDir="$tmp/notmpl" init -q
    git -C "$dir" -c user.email=t@example.com -c user.name=Test \
        -c commit.gpgsign=false -c core.hooksPath="$tmp/nohooks" \
        -c core.excludesFile=/dev/null -c core.attributesFile=/dev/null \
        -c core.autocrlf=false -c core.safecrlf=false add -A
    git -C "$dir" -c user.email=t@example.com -c user.name=Test \
        -c commit.gpgsign=false -c core.hooksPath="$tmp/nohooks" \
        commit -qm fixture --no-verify
}
# GIT_CEILING_DIRECTORIES is SET, not merely unset: unsetting removes a guard,
# whereas setting it makes `git rev-parse --show-toplevel` fail loudly rather
# than walk up into the caller's repository if the fixture's `git init` ever
# silently no-ops.
build() {
    ( CDPATH= cd -- "$1" \
      && GIT_CEILING_DIRECTORIES="$tmp" "$root/scripts/release/make-tarball.sh" "$2" )
}

# Sized to cross BOTH thresholds, because the two rejected forms fail at
# different ones:
#
#   sed | head -1   dies once sed crosses its first 4 KiB stdout flush after
#                   head exits -- about 700 matching lines, ~14 KB.
#   git show | sed ...q   dies once the whole blob exceeds ~70 KB -- the
#                   64 KiB pipe capacity plus sed's 4 KiB read block, which it
#                   drains before quitting -- independent of how many lines
#                   match. Measured: 0/10 failures at 68,046 B, 10/10 at
#                   72,046 B. Do not shrink this fixture toward 64 KiB; it
#                   would pass vacuously.
#
# 5000 lines is ~100 KB, past both. A 40 KB fixture caught the first form and
# silently missed the second, which is the trap this file exists to close.
big="$tmp/big"
{ printf "project('wirelog', 'c',\n  version: '1.2.3',\n"
  for i in $(seq 1 5000); do printf "  version: '9.9.9',\n"; done
  printf ')\n'; } > "$tmp/big-meson.build"
mkdir -p "$big"; cp "$tmp/big-meson.build" "$big/meson.build"; mkrepo "$big"
assert 'a meson.build past the SIGPIPE threshold still builds' build "$big" "$tmp/out-big"
assert 'and produces the FIRST version, not a later one' \
    test -f "$tmp/out-big/wirelog-1.2.3.tar.gz"

# The ordinary case must keep working: the version is not on line 1, so a form
# that quits after the first line processed returns empty.
small="$tmp/small"
mkdir -p "$small"
printf "project('wirelog', 'c',\n  default_options: [],\n  version: '4.5.6',\n)\n" > "$small/meson.build"
mkrepo "$small"
assert 'a version below line 1 is found' build "$small" "$tmp/out-small"
assert 'the archive carries that version' test -f "$tmp/out-small/wirelog-4.5.6.tar.gz"

# A tree with no version at all must fail with the named error, not silently.
none="$tmp/none"
mkdir -p "$none"
printf "project('wirelog', 'c')\n" > "$none/meson.build"
mkrepo "$none"
no_version() { ! build "$none" "$tmp/out-none" >/dev/null 2>&1; }
assert 'a meson.build with no version fails' no_version

if ((failures)); then
    printf 'test-version-extraction: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-version-extraction: all cases passed\n'
