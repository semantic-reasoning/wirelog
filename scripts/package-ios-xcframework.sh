#!/usr/bin/env bash
# Build wirelog.xcframework from Meson iOS cross builds.
#
# This is an out-of-Meson packaging helper. It is intentionally not wired into
# `meson test`, `ninja install`, or any default build target.
set -euo pipefail

usage() {
    cat <<'USAGE'
usage: scripts/package-ios-xcframework.sh [options]

Options:
  --output DIR       Output directory for wirelog.xcframework (default: dist/ios)
  --build-root DIR   Directory for temporary Meson builds (default: builddir-ios-xcframework)
  --clean            Remove the build root and output xcframework before building
  -h, --help         Show this help

Builds:
  - ios-arm64                 cross/ios-arm64.ini
  - ios-arm64-simulator       cross/ios-simulator-arm64.ini
  - ios-x86_64-simulator      cross/ios-simulator-x86_64.ini

The simulator slices are fat-packed with lipo before xcodebuild assembles
wirelog.xcframework. Each slice receives a Headers/ directory containing the
public headers, generated wirelog-version.h, and module.modulemap.
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="$repo_root/dist/ios"
build_root="$repo_root/builddir-ios-xcframework"
clean=false

while [ $# -gt 0 ]; do
    case "$1" in
        --output)
            output_dir="${2:?--output requires a directory}"
            shift 2
            ;;
        --build-root)
            build_root="${2:?--build-root requires a directory}"
            shift 2
            ;;
        --clean)
            clean=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

output_dir="$(mkdir -p "$output_dir" && cd "$output_dir" && pwd)"
build_root="$(mkdir -p "$build_root" && cd "$build_root" && pwd)"
xcframework="$output_dir/wirelog.xcframework"

require_tool() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: required tool not found: $1" >&2
        exit 1
    fi
}

require_tool meson
require_tool lipo
require_tool xcodebuild
require_tool xcrun

if [ "$clean" = true ]; then
    rm -rf "$build_root" "$xcframework"
    mkdir -p "$build_root" "$output_dir"
fi

build_slice() {
    local name="$1"
    local cross_file="$2"
    local build_dir="$build_root/$name"

    if [ -d "$build_dir/meson-info" ]; then
        meson setup --reconfigure "$build_dir" \
            -Dios=true \
            -Dtests=false \
            -DmbedTLS=disabled \
            -Db_lto=false
    else
        meson setup "$build_dir" "$repo_root" \
            --cross-file "$repo_root/$cross_file" \
            -Dios=true \
            -Dtests=false \
            -DmbedTLS=disabled \
            -Db_lto=false
    fi
    meson compile -C "$build_dir"
}

archive_for() {
    local build_dir="$1"
    for candidate in \
        "$build_dir/libwirelog.a" \
        "$build_dir/libwirelog_static.a"; do
        if [ -f "$candidate" ]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    echo "error: static wirelog archive not found in $build_dir" >&2
    exit 1
}

copy_headers() {
    local build_dir="$1"
    local headers_dir="$2"
    local header
    local basename

    rm -rf "$headers_dir"
    mkdir -p "$headers_dir/io" "$headers_dir/wirelog/io"

    for header in \
        wirelog/wirelog.h \
        wirelog/wirelog-types.h \
        wirelog/wirelog-ir.h \
        wirelog/wirelog-parser.h \
        wirelog/wirelog-optimizer.h \
        wirelog/wirelog-export.h \
        wirelog/wirelog-easy.h \
        wirelog/wirelog-advanced.h \
        wirelog/wirelog-extension.h; do
        basename="${header##*/}"
        cp "$repo_root/$header" "$headers_dir/$basename"
        cp "$repo_root/$header" "$headers_dir/wirelog/$basename"
    done
    cp "$repo_root/wirelog/io/io_adapter.h" "$headers_dir/io/"
    cp "$repo_root/wirelog/io/io_adapter.h" "$headers_dir/wirelog/io/"
    cp "$build_dir/wirelog/wirelog-version.h" "$headers_dir/wirelog-version.h"
    cp "$build_dir/wirelog/wirelog-version.h" "$headers_dir/wirelog/wirelog-version.h"
    cp "$repo_root/wirelog/module.modulemap" "$headers_dir/"
}

validate_header_layout() {
    local headers_dir="$1"
    local header
    local basename
    local public_headers=(
        wirelog/wirelog.h
        wirelog/wirelog-types.h
        wirelog/wirelog-ir.h
        wirelog/wirelog-parser.h
        wirelog/wirelog-optimizer.h
        wirelog/wirelog-export.h
        wirelog/wirelog-easy.h
        wirelog/wirelog-advanced.h
        wirelog/wirelog-extension.h
    )

    for header in "${public_headers[@]}"; do
        basename="${header##*/}"
        test -f "$headers_dir/$basename"
        test -f "$headers_dir/wirelog/$basename"
    done
    test -f "$headers_dir/io/io_adapter.h"
    test -f "$headers_dir/wirelog/io/io_adapter.h"
    test -f "$headers_dir/wirelog-version.h"
    test -f "$headers_dir/wirelog/wirelog-version.h"
    test -f "$headers_dir/module.modulemap"
}

validate_xcframework() {
    local plist="$xcframework/Info.plist"

    if [ ! -f "$plist" ]; then
        echo "error: missing xcframework Info.plist: $plist" >&2
        exit 1
    fi

    for identifier in ios-arm64 ios-arm64_x86_64-simulator; do
        if ! /usr/libexec/PlistBuddy -c 'Print :AvailableLibraries' "$plist" \
            | grep -q "LibraryIdentifier = $identifier"; then
            echo "error: Info.plist missing LibraryIdentifier $identifier" >&2
            exit 1
        fi
    done
}

device_build="$build_root/ios-arm64"
sim_arm64_build="$build_root/ios-simulator-arm64"
sim_x86_64_build="$build_root/ios-simulator-x86_64"

build_slice ios-arm64 cross/ios-arm64.ini
build_slice ios-simulator-arm64 cross/ios-simulator-arm64.ini
build_slice ios-simulator-x86_64 cross/ios-simulator-x86_64.ini

package_dir="$build_root/package"
device_headers="$package_dir/ios-arm64/Headers"
sim_headers="$package_dir/ios-arm64_x86_64-simulator/Headers"
device_lib="$package_dir/ios-arm64/libwirelog.a"
sim_lib="$package_dir/ios-arm64_x86_64-simulator/libwirelog.a"

rm -rf "$package_dir" "$xcframework"
mkdir -p "$(dirname "$device_lib")" "$(dirname "$sim_lib")"

cp "$(archive_for "$device_build")" "$device_lib"
lipo -create \
    "$(archive_for "$sim_arm64_build")" \
    "$(archive_for "$sim_x86_64_build")" \
    -output "$sim_lib"

copy_headers "$device_build" "$device_headers"
validate_header_layout "$device_headers"
copy_headers "$sim_arm64_build" "$sim_headers"
validate_header_layout "$sim_headers"

xcodebuild -create-xcframework \
    -library "$device_lib" \
    -headers "$device_headers" \
    -library "$sim_lib" \
    -headers "$sim_headers" \
    -output "$xcframework"

validate_xcframework

echo "OK: created $xcframework"
