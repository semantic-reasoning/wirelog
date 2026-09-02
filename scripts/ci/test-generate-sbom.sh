#!/usr/bin/env bash
# Self-test for generate-sbom.sh.
#
# Issue #1293. The generator hardcoded its outputs to $repo_root/sbom/, so
# running the real script from a test would clobber the committed drift
# baseline. #1291's locale guard therefore had to assert statically that every
# `sort` carries the LC_ALL=C pin -- it catches someone deleting the pin, but
# not a pipeline rewritten to reorder after a correctly pinned sort, because it
# never runs the generator. check-abi-symbols-locale.sh (#886) can run the real
# gate for the same bug class purely because that script takes its build root as
# an argument. The difference is the parameterisation, and this closes it.
#
# The script derives repo_root from its own location, so the fixture is a
# throwaway tree with a copy of the script in it: every path it computes then
# lands under $tmp, and the real sbom/ is untouchable by construction rather
# than by care.
set -euo pipefail

case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux|Darwin) ;;
    *) echo "test-generate-sbom: SKIP: needs a POSIX host"; exit 77 ;;
esac
command -v jq >/dev/null 2>&1 || { echo "test-generate-sbom: SKIP: jq not available"; exit 77; }

root=$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
tmp=$(CDPATH= cd -- "$(mktemp -d "${TMPDIR:-/tmp}/wirelog-gen-sbom.XXXXXX")" && pwd)
trap 'rm -rf "$tmp"' EXIT

# Captured HERE, before the fixture is built and before any generator runs.
# An earlier version captured it just above the assertion, i.e. after the whole
# test body -- so it compared the tree against itself across a window in which
# nothing happened, and reported ok while the test clobbered sbom/snapshot.txt.
# The comparison is only meaningful if one side predates the code under test.
#
# Compared against a prior state rather than against "clean": the documented
# release recipe legitimately modifies sbom/snapshot.txt, and demanding
# cleanliness made an operator's next `meson test` fail for a reason unrelated
# to this test.
#
# --ignored because .gitignore hides sbom/*.spdx.json and sbom/*.cdx.json, so
# without it the tripwire covers only one of the generator's three outputs.
#
# Known and by design: if TMPDIR is inside $root/sbom, this fails. git does not
# report EMPTY untracked directories, so the capture below sees nothing where
# the harness's scratch dir will be, and by assertion time the fixture has
# populated it. Put TMPDIR elsewhere. Excluding $tmp here was considered and
# rejected: this is the one assertion that must have no exceptions, and it has
# already failed open twice in this file's history.
#
# git's own status is checked: `[[ -z "$(git ...)" ]]` reported success when git
# ERRORED (absent, not a repo, dubious ownership), so the tripwire proving this
# test is safe passed precisely when it could not check. GIT_DIR is unset
# because `git -C` does not override it.
sbom_state() {
    env -u GIT_DIR -u GIT_WORK_TREE -u GIT_INDEX_FILE \
        git -C "$root" status --porcelain --ignored -- sbom/
}
if ! baseline_before=$(sbom_state); then baseline_before=$'\x01GIT-FAILED'; fi

failures=0
expect_status() {
    local name=$1 want=$2 got=0
    shift 2
    "$@" >"$tmp/out" 2>"$tmp/err" || got=$?
    if [[ "$got" == "$want" ]]; then printf 'test-generate-sbom: ok %s\n' "$name"
    else
        printf 'test-generate-sbom: FAIL %s (want exit %s, got %s)\n' "$name" "$want" "$got" >&2
        sed 's/^/    /' "$tmp/out" "$tmp/err" >&2; failures=$((failures + 1))
    fi
}
check() {
    local name=$1 ok=$2
    if [[ "$ok" == 0 ]]; then printf 'test-generate-sbom: ok %s\n' "$name"
    else printf 'test-generate-sbom: FAIL %s\n' "$name" >&2; failures=$((failures + 1)); fi
}
# `cond; check "$name" $?` is unsafe under set -e: a failing condition is a
# standalone command and aborts the file before check runs, so the first real
# failure silently truncates the suite instead of reporting. Run the condition
# inside the helper, where its status is consumed by an `if`.
assert() {
    local name=$1 ok=0
    shift
    "$@" || ok=1
    check "$name" "$ok"
}
refute() {
    local name=$1 ok=0
    shift
    "$@" && ok=1
    check "$name" "$ok"
}

# A fixture repo_root holding a copy of the real script. Nothing here can reach
# the working tree: repo_root is derived from the script's own directory.
repo="$tmp/repo"
mkdir -p "$repo/scripts/release" "$repo/subprojects/nanoarrow" "$repo/bin"
cp "$root/scripts/release/generate-sbom.sh" "$repo/scripts/release/"
printf "project('wirelog', 'c',\n  version: '9.9.9-dev',\n)\n" > "$repo/meson.build"

# Entries chosen so C and en_US collation disagree: glibc's en_US ignores the
# leading punctuation, so "./..." sorts among the alphabetic names instead of
# before them. A fixture that sorted identically either way could not tell a
# pinned sort from an unpinned one.
cat > "$repo/bin/syft" <<'EOS'
#!/usr/bin/env bash
# Honour `-o <format>=<path>` by writing there, the way real syft does; a stub
# that only ever printed to stdout would silently pass the document-location
# assertions no matter where the script pointed them.
#
# Also record the dir: argument. Without this nothing asserts WHAT the generator
# scans, and changing dir:"$repo_root" to dir:"$build_root" passed every
# assertion -- the likeliest future edit, since the script documents build_root
# as unused.
outfile=""
prev=""
for a in "$@"; do
    case "$a" in dir:*) printf '%s\n' "${a#dir:}" >> "$SYFT_SCANNED" ;; esac
    case "$prev" in -o) case "$a" in *=*) outfile=${a#*=} ;; esac ;; esac
    prev=$a
done
emit() {
cat <<'JSON'
{"artifacts":[
  {"name":"zlib","version":"1.3","licenses":[{"value":"Zlib"}]},
  {"name":"./.github/workflows/lint-pr.yml","version":"UNKNOWN","licenses":[]},
  {"name":"actions/checkout","version":"v5","licenses":[]},
  {"name":"./.github/workflows/lint-main.yml","version":"UNKNOWN","licenses":[]},
  {"name":"r-lib/actions","version":"v2","licenses":[]}
]}
JSON
}
if [ -n "$outfile" ]; then emit > "$outfile"; else emit; fi
EOS
chmod +x "$repo/bin/syft"

scanned="$tmp/scanned.txt"
gen() { SYFT_SCANNED="$scanned" PATH="$repo/bin:$PATH" \
        "$repo/scripts/release/generate-sbom.sh" "$@"; }

# --- the parameter, which is the point of the issue ------------------------
out="$tmp/out-dir"
expect_status 'an explicit output directory is accepted' 0 gen "$tmp/fake-build" "$out"
assert 'the snapshot lands in the given directory' test -f "$out/snapshot.txt"
assert 'the SPDX document lands there too' test -f "$out/wirelog-9.9.9.spdx.json"
assert 'the CycloneDX document lands there too' test -f "$out/wirelog-9.9.9.cdx.json"
refute 'the default directory is not touched when one is given' test -e "$repo/sbom"

# What the generator SCANS, not just where it writes. The script takes a
# build_root it does not read; if that ever changes the SBOM would describe a
# different tree and every other assertion here would still pass.
#
# These two PIN TODAY'S BEHAVIOUR rather than assert a desired property. The
# follow-up (#1308) will make the build root load-bearing, and when it does
# this assertion must be INVERTED as part of that work -- its failure there is
# the change landing, not a regression.
# Strict: EVERY recorded scan must be repo_root, not merely one of them. The
# earlier pair asserted "at least one is repo_root" and "none is the build
# root", which together let a partial divergence through -- two of the three
# calls retargeted to some third path passed silently, so the SPDX document
# could describe a different tree than the snapshot. One assertion covering all
# three also avoids keeping a second one that can no longer fail on its own.
all_scans_are_repo_root() {
    [[ -s "$scanned" ]] || return 1
    ! grep -qvxF "$repo" "$scanned"
}
assert 'every syft scan targets repo_root, never the build root' all_scans_are_repo_root

# --- the default, which every existing caller relies on --------------------
expect_status 'the output directory is optional' 0 gen "$tmp/fake-build"
assert 'omitting it still writes to repo_root/sbom' test -f "$repo/sbom/snapshot.txt"

# An explicitly empty out_dir must fail rather than silently fall back to the
# committed baseline. `${2:-...}` treats empty as absent and would write there,
# which is the clobber this parameter exists to prevent -- so the distinction
# between ${2-} and ${2:-} is behaviour, not style.
empty_out_fails() { ! gen "$tmp/fake-build" "" >/dev/null 2>&1; }
assert 'an empty output directory fails rather than using the default' empty_out_fails

# --- what could not be asserted before: the real pipeline's ordering --------
# #1291's guard can only check that the pin is present in the source. This runs
# the generator under a foreign locale and compares bytes, so a pipeline that
# reordered AFTER a correctly pinned sort would be caught.
locale_name=""
locales_available=$(locale -a 2>/dev/null || true)
for cand in en_US.utf8 en_US.UTF-8; do
    if grep -Fxq "$cand" <<<"$locales_available"; then locale_name=$cand; break; fi
done
if [[ -n "$locale_name" && "$(uname -s)" == Linux ]]; then
    c_out="$tmp/under-c"; l_out="$tmp/under-locale"
    # `|| true`: an unprotected command here is the same set -e truncation the
    # helpers above exist to avoid, and it skipped the baseline tripwire below
    # exactly when the generator was broken.
    LC_ALL=C              gen "$tmp/fake-build" "$c_out" >/dev/null 2>&1 || true
    LC_ALL="$locale_name" gen "$tmp/fake-build" "$l_out" >/dev/null 2>&1 || true
    assert 'the snapshot is byte-identical under C and en_US' \
        cmp -s "$c_out/snapshot.txt" "$l_out/snapshot.txt"

    # The fixture must actually discriminate, or the comparison above holds
    # whether or not the pin exists.
    unpinned_c=$(LC_ALL=C sort < "$c_out/snapshot.txt" 2>/dev/null || true)
    unpinned_l=$(LC_ALL="$locale_name" sort < "$c_out/snapshot.txt" 2>/dev/null || true)
    discriminates() { [[ "$unpinned_c" != "$unpinned_l" ]]; }
    assert 'the fixture distinguishes C from en_US collation' discriminates
else
    printf 'test-generate-sbom: SKIPPED locale case (needs Linux with en_US installed)\n'
fi

# --- the committed baseline is unreachable from here -----------------------
# Asserted rather than assumed: the whole reason this script could not be
# tested was that running it wrote into the working tree.
baseline_unchanged() {
    local now
    [[ "$baseline_before" != $'\x01GIT-FAILED' ]] || return 1
    now=$(sbom_state) || return 1
    [[ "$now" == "$baseline_before" ]]
}
assert 'this test does not change the state of the real sbom/' baseline_unchanged

if ((failures)); then
    printf 'test-generate-sbom: %d case(s) failed\n' "$failures" >&2
    exit 1
fi
printf 'test-generate-sbom: all cases passed\n'
