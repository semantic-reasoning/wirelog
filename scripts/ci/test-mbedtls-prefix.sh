#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
prefix=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-mbedtls-prefix.XXXXXX")
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-mbedtls-build.XXXXXX")
trap 'rm -rf "$prefix" "$build_dir"' EXIT

mkdir -p "$prefix/include" "$prefix/lib" "$prefix/empty-pkgconfig"
cp -a /usr/include/psa "$prefix/include/"
if [[ -d /usr/include/mbedtls ]]; then
    cp -a /usr/include/mbedtls "$prefix/include/"
fi

for component in tfpsacrypto mbedcrypto mbedtls mbedx509; do
    library=$(find /usr/lib /lib -type f -o -type l 2>/dev/null \
        | grep -E "/lib${component}\\.(so|a)(\\.|$)" \
        | head -n 1 || true)
    if [[ -n "$library" ]]; then
        library_dir=$(dirname "$library")
        cp -a "$library_dir/lib${component}.so"* "$prefix/lib/" 2>/dev/null || true
        cp -a "$library_dir/lib${component}.a"* "$prefix/lib/" 2>/dev/null || true
    fi
done

if [[ ! -e "$prefix/include/psa/crypto.h" ]]; then
    echo "metadata-free mbedTLS fixture: PSA headers are unavailable" >&2
    exit 1
fi

configure_log="$build_dir/configure.log"
if ! PKG_CONFIG_LIBDIR="$prefix/empty-pkgconfig" PKG_CONFIG_PATH= \
    CMAKE_PREFIX_PATH= CMAKE_FIND_ROOT_PATH= \
    meson setup "$build_dir" "$repo_root" -Dtests=true \
    -DmbedTLS=enabled -DmbedTLS_prefix="$prefix" >"$configure_log" 2>&1; then
    cat "$configure_log"
    exit 1
fi
if ! grep -q 'mbedTLS discovery: metadata-free prefix' "$configure_log"; then
    cat "$configure_log"
    echo "metadata-free mbedTLS fixture was not selected" >&2
    exit 1
fi
meson compile -C "$build_dir"
meson test -C "$build_dir" cryptographic_hashes symbol_digests --print-errorlogs

incomplete_prefix=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-mbedtls-incomplete.XXXXXX")
trap 'rm -rf "$prefix" "$build_dir" "$incomplete_prefix"' EXIT
mkdir -p "$incomplete_prefix/include"
cp -a "$prefix/include/psa" "$incomplete_prefix/include/"
if PKG_CONFIG_LIBDIR="$incomplete_prefix/empty-pkgconfig" PKG_CONFIG_PATH= \
    CMAKE_PREFIX_PATH= CMAKE_FIND_ROOT_PATH= \
    meson setup "$build_dir/incomplete" "$repo_root" -Dtests=false \
    -DmbedTLS=enabled -DmbedTLS_prefix="$incomplete_prefix" \
    >"$build_dir/incomplete.log" 2>&1; then
    cat "$build_dir/incomplete.log"
    echo "incomplete mbedTLS prefix unexpectedly configured" >&2
    exit 1
fi
grep -q 'missing libraries' "$build_dir/incomplete.log"
