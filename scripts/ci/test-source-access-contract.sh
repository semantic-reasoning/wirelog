#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
project_root=$(cd "$script_dir/../.." && pwd)
checker="$script_dir/check-source-access-contract.sh"
tmp_root=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-source-access.XXXXXX")
trap 'rm -rf "$tmp_root"' EXIT

# Resolve grep before restricting PATH; keep MSYS executables beside their
# runtime DLLs instead of copying or symlinking them into the fixture.
grep_abs=$(type -P grep)
grep_abs=$(cd "$(dirname "$grep_abs")" && pwd)/$(basename "$grep_abs")
tool_path="$tmp_root/bin"
mkdir "$tool_path"
# Any accidental PATH lookup of bash or grep must fail the valid fixture.
for utility in bash grep; do
    printf '#!%s\necho "source-access self-test: poisoned %s invoked" >&2\nexit 127\n' \
        "$BASH" "$utility" > "$tool_path/$utility"
    chmod +x "$tool_path/$utility"
done

make_fixture() {
    fixture_root=$1
    mkdir -p "$fixture_root/wirelog/columnar"
    cp "$project_root/wirelog/columnar/source_access.h" \
       "$fixture_root/wirelog/columnar/source_access.h"
    : > "$fixture_root/meson.build"
    : > "$fixture_root/wirelog/meson.build"
}

run_checker() (
    # Both the function and its executable path must reach the child Bash.
    export grep_abs
    grep() { "$grep_abs" "$@"; }
    export -f grep
    PATH="$tool_path" WIRELOG_SOURCE_ACCESS_ROOT=$1 "$BASH" "$checker"
)

valid_root="$tmp_root/valid"
make_fixture "$valid_root"
run_checker "$valid_root"

missing_header_root="$tmp_root/missing-header"
mkdir -p "$missing_header_root/wirelog/columnar"
: > "$missing_header_root/meson.build"
: > "$missing_header_root/wirelog/meson.build"
if missing_header_output=$(run_checker "$missing_header_root" 2>&1); then
    echo "source-access self-test: missing-header fixture unexpectedly passed" >&2
    exit 1
fi
case "$missing_header_output" in
    *'source-access: missing header'*) ;;
    *)
        echo "source-access self-test: missing-header diagnostic not found" >&2
        exit 1
        ;;
esac

allocation_root="$tmp_root/allocation"
make_fixture "$allocation_root"
printf '%s\n' 'void free (void *pointer);' >> \
    "$allocation_root/wirelog/columnar/source_access.h"
if allocation_output=$(run_checker "$allocation_root" 2>&1); then
    echo "source-access self-test: allocation fixture unexpectedly passed" >&2
    exit 1
fi
case "$allocation_output" in
    *'source-access: allocation call in gate'*) ;;
    *)
        echo "source-access self-test: allocation diagnostic not found" >&2
        exit 1
        ;;
esac

comment_root="$tmp_root/comment-only"
make_fixture "$comment_root"
sed -i.bak \
    '/^[[:space:]]*wl_columnar_source_access_writer_release[[:space:]]*([[:space:]]*$/d' \
    "$comment_root/wirelog/columnar/source_access.h"
rm "$comment_root/wirelog/columnar/source_access.h.bak"
printf '%s\n' \
    '/* wl_columnar_source_access_writer_release */' >> \
    "$comment_root/wirelog/columnar/source_access.h"
if comment_output=$(run_checker "$comment_root" 2>&1); then
    echo "source-access self-test: comment-only fixture unexpectedly passed" >&2
    exit 1
fi
case "$comment_output" in
    *'source-access: missing wl_columnar_source_access_writer_release'*) ;;
    *)
        echo "source-access self-test: comment-only diagnostic not found" >&2
        exit 1
        ;;
esac

production_root="$tmp_root/production"
make_fixture "$production_root"
printf '%s\n' 'wirelog/columnar/source_access.h' > \
    "$production_root/wirelog/meson.build"
if production_output=$(run_checker "$production_root" 2>&1); then
    echo "source-access self-test: production-list fixture unexpectedly passed" >&2
    exit 1
fi
case "$production_output" in
    *'source-access: gate must not enter production source lists'*) ;;
    *)
        echo "source-access self-test: production-list diagnostic not found" >&2
        exit 1
        ;;
esac

echo "source-access self-test: OK"
