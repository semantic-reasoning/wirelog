#!/usr/bin/env bash
# check-subprojects-ignored.sh - Issue #1814.
#
# Assert that .gitignore and the wrap files agree about subprojects/.
#
# .gitignore:68 named subprojects/xxHash-0.8.3/ while subprojects/xxhash.wrap
# pinned `directory = xxHash-0.8.4`. The rule named a directory meson no longer
# creates and did not name the one it does, so every configure left 104
# untracked files in the worktree, on every machine, for several releases.
# Nothing objected because nothing compared the two. This is that comparison.
#
# Three assertions, because two of them can be satisfied by the very rule this
# issue removed and only the third rejects it:
#
#   A  No TRACKED path under subprojects/ is ignored. This is what pays for
#      the deny-by-default rule: `subprojects/*` would otherwise swallow the
#      wrap files themselves.
#   B  Every directory meson creates under subprojects/ IS ignored.
#   C  A name meson has never created is ignored too -- i.e. the rule is
#      version-independent rather than a list of today's names.
#
# B alone cannot fail under a correct rule, and it also passes under
# `subprojects/xxHash-0.8.4/`, the version-pinned form whose next bump
# reintroduces the defect. C is what distinguishes them. C is a behavioural
# probe rather than a grep for the literal rule text because `/subprojects/*`
# and `subprojects/* ` (trailing space, which git strips) both have the
# property and both fail a literal match -- measured.
#
# Exit codes: 0 = OK, 1 = a rule is wrong, 77 = SKIP (not a git work tree).
#
# 77 is only safe under meson: a bare workflow `run:` step uses `bash -e`,
# where 77 fails the job. Do not invoke this from one.

set -euo pipefail

SKIP_EXIT=77

root="${1:-$(cd "$(dirname "$0")/../.." && pwd)}"

skip() {
    echo "check-subprojects-ignored: SKIP: $*"
    exit "$SKIP_EXIT"
}

if ! scratch="$(mktemp -d "${TMPDIR:-/tmp}/wirelog-subprojects-ignored.XXXXXX")"; then
    echo "check-subprojects-ignored: ERROR: cannot create a scratch directory under ${TMPDIR:-/tmp}" >&2
    exit 1
fi
case "$scratch" in
    ""|"/")
        echo "check-subprojects-ignored: ERROR: refusing to use scratch path '$scratch'" >&2
        exit 1
        ;;
esac
trap 'rm -rf "$scratch"' EXIT
trap 'exit 1' INT TERM HUP

fail=0
note_fail() {
    echo "check-subprojects-ignored: FAIL: $*" >&2
    fail=1
}

if ! git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    skip "$root is not a git work tree; there are no ignore rules to check"
fi

# check-ignore's exit codes are 0 = something matched, 1 = nothing matched,
# 128 = error. `if ! git check-ignore ...` reads 128 as "nothing matched", so
# every status is tested by value.
#
# The status is returned, not printed, because this is called from `if` and an
# `exit` inside a command substitution leaves only the subshell: an earlier
# draft printed the status and exited on 128, and the caller then saw an empty
# string, concluded "not ignored", and emitted a second FAIL line misreporting
# a git error as a missing ignore rule.
ignore_error=""
is_ignored() {
    local rc=0
    ignore_error=""
    git -C "$root" check-ignore --no-index -q -- "$1" || rc=$?
    case "$rc" in
        0) return 0 ;;
        1) return 1 ;;
        *)
            ignore_error="git check-ignore exited $rc"
            return 1
            ;;
    esac
}

# The probe carries a trailing slash for two reasons, and a reviewer who
# removes it breaks both. A directory-only pattern such as subprojects/foo-*/
# does not match a path given without one. And check-ignore stats the path even
# under --no-index, so without the slash the answer depends on whether meson
# has ever run on this machine -- the assertion would be host-state-dependent
# rather than a property of the rules.
require_ignored() {
    if is_ignored "subprojects/$1/"; then
        return 0
    fi
    if [ -n "$ignore_error" ]; then
        note_fail "could not test subprojects/$1: ${ignore_error} ($2)"
    else
        note_fail "meson creates subprojects/$1 but .gitignore does not ignore it ($2)"
    fi
}

# --- A. No tracked path under subprojects/ is ignored. -----------------------
#
# --no-index is mandatory: without it check-ignore declines to report a path
# that is tracked, so this returns "nothing ignored" for every possible input
# and asserts nothing.
#
# The verdict is taken from the NON-VERBOSE form. `check-ignore -v` prints a
# row for every path matching any pattern INCLUDING the negations, and exits 0
# either way -- measured: a correct tree and a tree missing
# `!subprojects/.wraplock` both give rc 0 and three rows, so a -v verdict
# cannot tell them apart. -v is re-run only to name the culprits on failure.
# The list is held in a file, not a variable: command substitution strips NUL
# bytes, so a -z list read into a shell variable silently becomes empty and the
# floor below would be the only thing standing between that and a vacuous pass.
tracked_list="${scratch}/tracked"
git -C "$root" ls-files -z subprojects/ >"$tracked_list"
tracked_count=$(tr -cd '\000' <"$tracked_list" | wc -c | tr -d ' ')
# "nothing ignored" and "nothing scanned" are otherwise the same result.
if [ "$tracked_count" -lt 3 ]; then
    note_fail "found only $tracked_count tracked paths under subprojects/ (expected at least 3); the scan or the tree has changed shape"
else
    a_rc=0
    git -C "$root" check-ignore --no-index --stdin -z <"$tracked_list" >/dev/null || a_rc=$?
    case "$a_rc" in
        1) : ;;
        0)
            note_fail "these tracked paths under subprojects/ are ignored; a re-inclusion is missing:"
            # -v emits four NUL-separated fields per row: source, line,
            # pattern, path. Put each field on its own line and reassemble in
            # awk, which is portable. An earlier draft used `sed 's/../\n/'`
            # to split rows and `grep -v '\t!'` to drop the negation rows;
            # neither works. `\n` on the right-hand side of a sed s/// is a
            # GNU extension, so the macOS leg got one run-on line, and `\t` in
            # a BRE is the literal character t, so grep kept every row it
            # claimed to drop and printed "stray \ before t" into CI output.
            # Dropping negation rows matters: they name correctly-visible
            # paths, and listing them hands the contributor a rule that is not
            # the culprit.
            #
            # Known limit: converting NUL to newline discards the framing -z
            # exists for, so a tracked path containing a literal newline slips
            # the NR%4 record boundary and scrambles the listing. The VERDICT
            # is unaffected -- it comes from the non-verbose run above -- and
            # no such path exists here; stated so the next reader does not
            # infer newline-safety from the -z.
            git -C "$root" check-ignore --no-index --stdin -z -v <"$tracked_list" \
                | tr '\000' '\n' \
                | awk 'NR%4==1{s=$0} NR%4==2{n=$0} NR%4==3{p=$0}
                       NR%4==0 && p !~ /^!/ {print "    " s ":" n ":" p "\t" $0}' >&2 || true
            ;;
        *)
            echo "check-subprojects-ignored: ERROR: git check-ignore exited $a_rc" >&2
            exit 1
            ;;
    esac
fi

# --- B. Every directory meson creates under subprojects/ is ignored. ---------
#
# The probe set mirrors meson's own enumeration rather than inventing one.
# mesonbuild/wrap/wrap.py:409-411 builds
#     ignore_dirs = {'packagecache', 'packagefiles'}
#     ignore_dirs |= {wrap.directory, wrap.name}   # per wrap
# as the children it expects; anything else it treats as a hand-placed
# subproject. packagefiles is excluded from the probes below because it is a
# TRACKED input that must stay visible -- assertion A covers it.
require_ignored packagecache "mesonbuild/wrap/wrap.py:377 creates it"

# `directory =` is read WITHOUT section anchoring, deliberately. Meson takes it
# from the first section only (wrap.py:298,304 and `values.get('directory',
# self.name)` at :211), but #1343 showed that two gates anchoring on a section
# header disagreed about what one is -- `[wrap-git]  # pin` is legal and meson
# builds it, while one gate silently found no section and reported OK. A reader
# with no anchoring cannot under-collect from a misread header. It can
# over-collect, which can only demand that an extra name be ignored; under a
# deny-by-default rule that demand is always already satisfied. Do not "fix"
# this by adding section anchoring, and do not add a row to
# test-wrap-section-syntax.sh: that file keeps the two ANCHORED readers in
# agreement and this reader has nothing to agree about.
#
# The value is used raw apart from surrounding whitespace.
# check-wrap-revisions.sh strips an inline `#`, but meson does not -- it uses
# ConfigParser with
# inline_comment_prefixes unset (wrap.py:292), so `directory = x # y` is the
# literal directory name `x # y`. Copying that strip would make this gate
# disagree with the thing it is checking.
wraps=0
for wrap in "$root"/subprojects/*.wrap; do
    [ -e "$wrap" ] || continue
    wraps=$((wraps + 1))
    base=${wrap##*/}
    require_ignored "${base%.wrap}" "wrap name, used when the wrap sets no directory"
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        # configparser's default delimiters are ('=', ':'), so `directory: x`
        # is a key meson honours. Strip whichever one comes first.
        value=${line#*[=:]}
        value=${value#"${value%%[![:space:]]*}"}
        value=${value%"${value##*[![:space:]]}"}
        # An empty, path-valued or dot `directory` is not this gate's
        # business to reject, and skipping all three is what makes the
        # over-collection claim above true. A value containing `/` would be
        # probed as subprojects/../../escape/, which git answers with exit 128
        # ("outside repository"); `.` and `..` need naming separately because
        # os.path.dirname() returns '' for both, so meson's own path check
        # (wrap.py:212-213) accepts them and the `*/*` case below does not see
        # them. An unanchored read would otherwise fail a wrap meson accepts,
        # which is exactly the false failure the claim denies. An empty value
        # is an over-collected continuation line from another key.
        [ -n "$value" ] || continue
        case "$value" in */*|.|..) continue ;; esac
        require_ignored "$value" "directory key in ${base}"
    done <<EOF
$(grep -iE '^[[:space:]]*directory[[:space:]]*[=:]' "$wrap" || true)
EOF
done
if [ "$wraps" -lt 1 ]; then
    note_fail "found no subprojects/*.wrap to read; the scan or the tree has changed shape"
fi

# --- C. The rule is version-independent. -------------------------------------
#
# A fixed synthetic name meson has never created. Under a correct rule it is
# ignored because everything under subprojects/ is; under the version-pinned
# rule this issue removes, it is not. Fixed rather than random so a failure
# reproduces. No dot, so !subprojects/*.wrap cannot re-include it.
if ! is_ignored 'subprojects/zz-extraction-probe/'; then
    note_fail "subprojects/zz-extraction-probe/ is not ignored, so the rule names particular directories rather than covering any extraction directory; the next wrap bump will leave the worktree dirty again (#1814)"
fi

if [ "$fail" -ne 0 ]; then
    echo "check-subprojects-ignored: FAILED" >&2
    exit 1
fi
echo "check-subprojects-ignored: OK; ${tracked_count} tracked path(s) visible, ${wraps} wrap(s) and packagecache ignored, rule is version-independent"
