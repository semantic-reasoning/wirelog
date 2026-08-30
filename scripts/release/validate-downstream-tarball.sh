#!/usr/bin/env bash
# Validate candidate archive members before extraction.
set -euo pipefail

[[ $# -eq 2 ]] || { echo 'usage: validate-downstream-tarball.sh ARCHIVE MEMBERS_OUTPUT' >&2; exit 2; }
tarball=$1
members_output=$2
command -v tar >/dev/null || { echo 'tar is required' >&2; exit 2; }

if command -v cygpath >/dev/null 2>&1; then
    case "$tarball" in
        [[:alpha:]]:[\\/]* ) tarball=$(cygpath -u -- "$tarball") ;;
    esac
    case "$members_output" in
        [[:alpha:]]:[\\/]* ) members_output=$(cygpath -u -- "$members_output") ;;
    esac
fi

tar --quoting-style=escape -tzf "$tarball" > "$members_output"
while IFS= read -r member; do
    case "$member" in
        ''|/*|.|..|./*|../*|*/../*|*/..)
            echo "unsafe archive member path: $member" >&2
            exit 1
            ;;
    esac
done < "$members_output"

tar -tvzf "$tarball" | awk '$1 !~ /^[-d]/ { bad = 1 } END { exit bad + 0 }' || {
    echo 'archive contains symlink, hardlink, or special-file entries' >&2
    exit 1
}
