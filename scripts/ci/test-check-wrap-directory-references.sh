#!/usr/bin/env bash
# Self-test for check-wrap-directory-references.sh (Issue #1899).
#
# Each case builds a throwaway git repository with its own subprojects/*.wrap
# and runs the gate against it, so the cases do not depend on the real wraps
# or on which xxHash version is current.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-check-wrap-directory-references: SKIP: needs a POSIX host"; exit 77 ;;
esac
command -v git >/dev/null 2>&1 || {
    echo "test-check-wrap-directory-references: SKIP: git not available"; exit 77; }

root=$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
gate="$root/scripts/ci/check-wrap-directory-references.sh"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-wrap-refs.XXXXXX")
case "$tmp" in ""|"/") echo "FATAL: refusing sandbox '$tmp'" >&2; exit 2 ;; esac
trap 'rm -rf "$tmp"' EXIT

failures=0
cases=0

# A repository whose foo.wrap declares directory = foo-1.2 and
# patch_directory = foo-1.2, plus an unversioned bar.wrap.
fixture() {
    local d="$tmp/$1"
    mkdir -p "$d/subprojects/packagefiles/foo-1.2" "$d/scripts"
    printf '[wrap-file]\ndirectory = foo-1.2  # pinned\npatch_directory = foo-1.2\n' \
        >"$d/subprojects/foo.wrap"
    printf '[wrap-git]\nurl = https://example.invalid/bar.git\n' >"$d/subprojects/bar.wrap"
    printf 'project_files\n' >"$d/subprojects/packagefiles/foo-1.2/meson.build"
    printf 'ldpath = "build/subprojects/foo-1.2:build/subprojects/bar"\n' \
        >"$d/scripts/run.py"
    printf '%s\n' "$d"
}

commit() {
    env -i PATH="$PATH" HOME="$tmp" GIT_CONFIG_NOSYSTEM=1 \
        git -C "$1" -c init.defaultBranch=main -c user.name=t -c user.email=t@example.invalid \
        -c commit.gpgSign=false -c core.hooksPath=/dev/null "${@:2}" >/dev/null
}

track() {
    commit "$1" init -q 2>/dev/null || true
    commit "$1" add -A
    commit "$1" commit -qm fixture
}

# expect NAME STATUS DIR [NEEDLE]: run the gate on DIR and require STATUS and,
# when given, NEEDLE in its output.
expect() {
    local name=$1 want=$2 dir=$3 needle=${4:-} got=0
    cases=$((cases + 1))
    "$gate" "$dir" >"$tmp/out" 2>&1 || got=$?
    if [ "$got" -eq "$want" ] \
        && { [ -z "$needle" ] || grep -Fq -- "$needle" "$tmp/out"; }; then
        echo "ok: $name"
    else
        echo "FAIL: $name (exit $got, wanted $want${needle:+, needle '$needle'})" >&2
        sed 's/^/    /' "$tmp/out" >&2
        failures=$((failures + 1))
    fi
}

d=$(fixture clean); track "$d"
expect 'current spellings pass' 0 "$d" '2 declared wrap directories'

d=$(fixture stale-source); printf 'see subprojects/foo-1.1/src\n' >"$d/notes.txt"; track "$d"
expect 'a stale source path fails and names the file and line' 1 "$d" \
    'notes.txt:1 names subprojects/foo-1.1'

d=$(fixture stale-build)
printf 'path=$PWD/builddir/subprojects/foo-1.1:$PWD/builddir/subprojects/bar\n' >"$d/ci.yml"
track "$d"
expect 'a stale build-tree path fails' 1 "$d" 'ci.yml:1 names subprojects/foo-1.1'

d=$(fixture stale-patch); printf 'cp subprojects/packagefiles/foo-1.0/x .\n' >"$d/do.txt"; track "$d"
expect 'a stale patch_directory path fails' 1 "$d" \
    'names subprojects/packagefiles/foo-1.0, which no subprojects/*.wrap declares'

d=$(fixture comment)
printf '# was subprojects/foo-1.1 before the bump\n  // subprojects/foo-1.1\n * subprojects/foo-1.1\n' \
    >"$d/history.c"
track "$d"
expect 'comment lines are not checked' 0 "$d"

d=$(fixture changelog); printf -- '- bumped from subprojects/foo-1.1\n' >"$d/CHANGELOG.md"; track "$d"
expect 'CHANGELOG.md is exempt' 0 "$d"

d=$(fixture untracked); track "$d"; printf 'subprojects/foo-1.1\n' >"$d/scratch.txt"
expect 'an untracked file is not checked' 0 "$d"

d=$(fixture second-on-line)
printf 'a=subprojects/foo-1.2:subprojects/foo-0.9\n' >"$d/two.txt"; track "$d"
expect 'every reference on a line is compared, not just the first' 1 "$d" \
    'two.txt:1 names subprojects/foo-0.9'

d=$(fixture lookalike); printf 'subprojects/foobar-1.1 subprojects/food-1.1\n' >"$d/other.txt"
track "$d"
expect 'a versioned name no wrap declares fails, however close to one' 1 "$d" \
    'other.txt:1 names subprojects/foobar-1.1'

d=$(fixture prefix); printf 'for d in subprojects/foo-*; do :; done\nx="subprojects/foo-"\n' \
    >"$d/glob.txt"; track "$d"
expect 'a version-less prefix or glob is not a stale reference' 0 "$d"

# The fixture's own files spell subprojects/foo-1.2, which only the last,
# unterminated line declares: dropping that line would fail them.
d=$(fixture no-final-newline); printf '[wrap-file]\nsource_url = x\ndirectory = foo-1.2' \
    >"$d/subprojects/foo.wrap"; track "$d"
expect 'a declaration on a last line without a newline is still read' 0 "$d" \
    '1 declared wrap directories'

d=$(fixture sentence); printf 'Meson extracts it into subprojects/foo-1.2.\n' >"$d/README.txt"
track "$d"
expect 'a full stop after the current spelling is punctuation' 0 "$d"

d=$(fixture archive); printf 'tar xf subprojects/foo-1.2.tar.gz\n' >"$d/fetch.txt"
track "$d"
expect 'the declared name with an archive suffix is the declared name' 0 "$d"

d=$(fixture unversioned); printf 'lib=build/subprojects/bar-1.0\n' >"$d/lib.txt"
track "$d"
expect 'a version on a wrap whose directory has none fails' 1 "$d" \
    'lib.txt:1 names subprojects/bar-1.0'

d=$(fixture user-config); printf '# subprojects/foo-1.1 before the bump\n' >"$d/old.txt"
track "$d"
export GIT_CONFIG_PARAMETERS="'grep.column=true' 'color.grep=always'"
expect "the caller's grep.column and color.grep do not change the verdict" 0 "$d"
unset GIT_CONFIG_PARAMETERS

mkdir -p "$tmp/not-a-repo/subprojects"
printf '[wrap-file]\ndirectory = foo-1.2\n' >"$tmp/not-a-repo/subprojects/foo.wrap"
export GIT_CEILING_DIRECTORIES="$tmp"
expect 'outside a git work tree the gate skips' 77 "$tmp/not-a-repo" 'SKIP'
unset GIT_CEILING_DIRECTORIES

if [ "$cases" -lt 16 ]; then
    echo "FAIL: only $cases cases ran" >&2
    failures=$((failures + 1))
fi
if [ "$failures" -ne 0 ]; then
    echo "test-check-wrap-directory-references: $failures case(s) failed" >&2
    exit 1
fi
echo "test-check-wrap-directory-references: all $cases cases passed"
