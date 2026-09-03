#!/usr/bin/env bash
# check-wrap-revisions.sh - Issue #715 release dependency pin guard.
#
# Reject Meson wrap files that are not reproducible. Release dependency
# wraps must pin git dependencies to immutable commits and checksum archive
# downloads.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"

fail=0

trim() {
    local s=$1
    s=${s#"${s%%[![:space:]]*}"}
    s=${s%"${s##*[![:space:]]}"}
    printf '%s' "$s"
}

strip_inline_comment() {
    local s=$1
    s=${s%%#*}
    trim "$s"
}

report_fail() {
    if [ "$fail" -eq 0 ]; then
        echo "check-wrap-revisions: FAIL: unreproducible wrap dependency found" >&2
    fi
    echo "  $1" >&2
    fail=1
}

check_wrap() {
    local wrap=$1
    local rel=${wrap#$repo_root/}
    local section=""
    local section_line=0
    local revision=""
    local revision_line=0
    local source_url_line=0
    local source_hash=""
    local patch_url_line=0
    local patch_hash=""

    finish_section() {
        case "$section" in
            wrap-git)
                if [ -z "$revision" ]; then
                    report_fail "$rel:$section_line: [wrap-git] missing revision"
                elif ! [[ "$revision" =~ ^[0-9a-fA-F]{40}$ ]]; then
                    report_fail "$rel:$revision_line: revision must be a 40-hex commit, got '$revision'"
                fi
                ;;
            wrap-file)
                if [ "$source_url_line" -ne 0 ] && [ -z "$source_hash" ]; then
                    report_fail "$rel:$source_url_line: source_url requires non-empty source_hash"
                fi
                if [ "$patch_url_line" -ne 0 ] && [ -z "$patch_hash" ]; then
                    report_fail "$rel:$patch_url_line: patch_url requires non-empty patch_hash"
                fi
                ;;
        esac
    }

    reset_section_state() {
        revision=""
        revision_line=0
        source_url_line=0
        source_hash=""
        patch_url_line=0
        patch_hash=""
    }

    local line_no=0
    local line key value
    while IFS= read -r line || [ -n "$line" ]; do
        line_no=$((line_no + 1))
        line=$(trim "$line")
        [ -z "$line" ] && continue
        [[ "$line" == \#* ]] && continue

        # No `$` anchor: Python's configparser applies SECTCRE with .match, so
        # text after `]` is ignored and `[wrap-git]  # the pin` resolves
        # normally -- meson builds that wrap. Requiring `]` at end of line made
        # this loop not recognise the header at all, so `section` stayed empty,
        # the `wrap-git:revision` case never fired, and the gate reported
        # "OK; release dependency wraps are reproducible" having checked
        # nothing. Measured: a wrap with that header and `revision = notahex`
        # passed. The literal `]` is still required, so this cannot over-match.
        # Deliberately `[^]]+` and not a literal port of configparser's
        # SECTCRE, which is greedy: do not "correct" this to `.+`. The
        # literal `]` is still required, which is the direction that
        # matters. (#1343)
        if [[ "$line" =~ ^\[([^]]+)\] ]]; then
            local new_section=${BASH_REMATCH[1]}
            finish_section
            section=$new_section
            section_line=$line_no
            reset_section_state
            continue
        fi

        if [[ "$line" =~ ^([^=]+)=(.*)$ ]]; then
            key=$(trim "${BASH_REMATCH[1]}")
            value=$(strip_inline_comment "${BASH_REMATCH[2]}")
            case "$section:$key" in
                wrap-git:revision)
                    revision=$value
                    revision_line=$line_no
                    ;;
                wrap-file:source_url)
                    if [ -n "$value" ]; then
                        source_url_line=$line_no
                    fi
                    ;;
                wrap-file:source_hash)
                    source_hash=$value
                    ;;
                wrap-file:patch_url)
                    if [ -n "$value" ]; then
                        patch_url_line=$line_no
                    fi
                    ;;
                wrap-file:patch_hash)
                    patch_hash=$value
                    ;;
            esac
        fi
    done <"$wrap"

    finish_section
}

for wrap in "$repo_root"/subprojects/*.wrap; do
    [ -e "$wrap" ] || continue
    check_wrap "$wrap"
done

if [ "$fail" -ne 0 ]; then
    echo "" >&2
    echo "Pin [wrap-git] dependencies to 40-hex commits and provide" >&2
    echo "source_hash/patch_hash for [wrap-file] archive URLs." >&2
    exit 1
fi

echo "check-wrap-revisions: OK; release dependency wraps are reproducible"
