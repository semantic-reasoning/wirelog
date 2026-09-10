#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
project_root=$(cd "$script_dir/../.." && pwd)
checker="$script_dir/check-source-access-contract.sh"
tmp_root=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-source-access.XXXXXX")
trap 'rm -rf "$tmp_root"' EXIT

tool_path="$tmp_root/bin"
mkdir "$tool_path"
for utility in bash grep mkdir cp rm; do
    ln -s "$(command -v "$utility")" "$tool_path/$utility"
done

make_fixture() {
    fixture_root=$1
    mkdir -p "$fixture_root/wirelog/columnar"
    cp "$project_root/wirelog/columnar/source_access.h" \
       "$fixture_root/wirelog/columnar/source_access.h"
    : > "$fixture_root/meson.build"
    : > "$fixture_root/wirelog/meson.build"
}

run_checker() {
    PATH="$tool_path" WIRELOG_SOURCE_ACCESS_ROOT=$1 "$checker"
}

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
if run_checker "$allocation_root"; then
    echo "source-access self-test: allocation fixture unexpectedly passed" >&2
    exit 1
fi

comment_root="$tmp_root/comment-only"
make_fixture "$comment_root"
sed -i.bak \
    '/^[[:space:]]*wl_columnar_source_access_writer_release[[:space:]]*([[:space:]]*$/d' \
    "$comment_root/wirelog/columnar/source_access.h"
rm "$comment_root/wirelog/columnar/source_access.h.bak"
printf '%s\n' \
    '/* wl_columnar_source_access_writer_release */' >> \
    "$comment_root/wirelog/columnar/source_access.h"
if run_checker "$comment_root"; then
    echo "source-access self-test: comment-only fixture unexpectedly passed" >&2
    exit 1
fi

production_root="$tmp_root/production"
make_fixture "$production_root"
printf '%s\n' 'wirelog/columnar/source_access.h' > \
    "$production_root/wirelog/meson.build"
if run_checker "$production_root"; then
    echo "source-access self-test: production-list fixture unexpectedly passed" >&2
    exit 1
fi

echo "source-access self-test: OK"
