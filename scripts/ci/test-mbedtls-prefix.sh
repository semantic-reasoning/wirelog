#!/usr/bin/env bash
set -euo pipefail

fail() { echo "metadata-free mbedTLS fixture: $*" >&2; exit 1; }
[[ $# == 0 ]] || fail 'arguments are unsupported'
neutral_inputs=(CFLAGS CPPFLAGS CXXFLAGS LDFLAGS CC_LD CXX_LD CPATH
    C_INCLUDE_PATH CPLUS_INCLUDE_PATH OBJC_INCLUDE_PATH COMPILER_PATH GCC_EXEC_PREFIX)
for input in "${neutral_inputs[@]}"; do
    [[ -z ${!input:-} ]] || fail "nonempty $input is unsupported"
done
compiler=${CC:-gcc}
[[ $compiler =~ ^[a-zA-Z0-9_/.+-]+$ ]] || fail 'CC must be one compiler executable'
[[ $compiler != */* || $compiler == /* ]] || fail 'CC must not be a relative path'
compiler=$(command -v "$compiler") || fail 'CC executable is unavailable'
compiler=$(realpath -e "$compiler")
[[ -x $compiler && -r $compiler ]] || fail 'CC executable is unreadable'
compiler_args=("$compiler")
compiler_probe=$("${compiler_args[@]}" -dM -E -x c - </dev/null) || fail 'compiler macro probe failed'
[[ -n $compiler_probe ]] || fail 'empty compiler macro probe'
declare -A compiler_macros=()
while IFS= read -r definition; do
    if [[ $definition =~ ^#define[[:space:]]+([a-zA-Z_][a-zA-Z_0-9]*)([[:space:]]+(.*))?$ ]]; then
        macro=${BASH_REMATCH[1]}
        value=${BASH_REMATCH[3]:-}
        case $macro in
            __INTEL_COMPILER|__INTEL_LLVM_COMPILER|__NVCOMPILER|__PGI|__TINYC__|__ibmxl__|__IBMCPP__|__ARMCOMPILER_VERSION|__CC_ARM|__SUNPRO_C|__BORLANDC__|__COMPCERT__)
                fail "unsupported compatibility compiler: $macro" ;;
            __clang__|__clang_major__|__clang_minor__|__clang_patchlevel__|__GNUC__|__GNUC_MINOR__|__GNUC_PATCHLEVEL__)
                [[ $value =~ ^[[:space:]]*([0-9]+)[[:space:]]*$ ]] || fail "malformed compiler macro: $macro"
                value=${BASH_REMATCH[1]}
                [[ ! -v compiler_macros[$macro] || ${compiler_macros[$macro]} == "$value" ]] || fail "inconsistent compiler macro: $macro"
                compiler_macros[$macro]=$value ;;
        esac
    fi
done <<< "$compiler_probe"
if [[ -v compiler_macros[__clang__] ]]; then
    [[ ${compiler_macros[__clang__]} == 1 && ${compiler_macros[__clang_major__]:-} =~ ^[0-9]+$ &&
        ${compiler_macros[__clang_minor__]:-} =~ ^[0-9]+$ && ${compiler_macros[__clang_patchlevel__]:-} =~ ^[0-9]+$ ]] || fail 'unsupported compiler family: incomplete Clang macros'
    compiler_family="Clang ${compiler_macros[__clang_major__]}.${compiler_macros[__clang_minor__]}.${compiler_macros[__clang_patchlevel__]}"
elif [[ ${compiler_macros[__GNUC__]:-} =~ ^0*[1-9][0-9]*$ &&
    ${compiler_macros[__GNUC_MINOR__]:-} =~ ^[0-9]+$ && ${compiler_macros[__GNUC_PATCHLEVEL__]:-} =~ ^[0-9]+$ ]]; then
    compiler_family="GNU ${compiler_macros[__GNUC__]}.${compiler_macros[__GNUC_MINOR__]}.${compiler_macros[__GNUC_PATCHLEVEL__]}"
else
    fail 'unsupported compiler family: missing GNU/Clang macros'
fi
compiler_version=$("$compiler" --version) || fail 'compiler version diagnostic failed'
export CC="$compiler"
echo "compiler: $CC; family: $compiler_family; ${compiler_version%%$'\n'*}"
echo "neutral inputs (empty): ${neutral_inputs[*]}; no native/cross files"

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
home_root=$(realpath -e "$HOME")
temp_root=$(realpath -m "${TMPDIR:-$HOME/.tmp}")
[[ $temp_root == "$home_root/"* ]] || fail 'temporary root must be beneath HOME'
mkdir -p "$temp_root"
temp_root=$(realpath -e "$temp_root")
[[ $temp_root == "$home_root/"* ]] || fail 'temporary root escapes HOME'
owned=()
cleanup() { if ((${#owned[@]})); then rm -rf -- "${owned[@]}"; fi; }
trap cleanup EXIT
acquire() {
    local path
    path=$(mktemp -d "$temp_root/wirelog-mbedtls-$1.XXXXXX")
    owned+=("$path")
    printf -v "$2" '%s' "$path"
    echo "owned fixture: $path"
}
acquire prefix prefix
acquire build build_dir
mkdir -p "$prefix/include" "$prefix/lib" "$prefix/empty-pkgconfig"
for family in psa mbedtls tf-psa-crypto; do
    if [[ -d /usr/include/$family ]]; then
        cp -a "/usr/include/$family" "$prefix/include/"
    fi
done
[[ -e $prefix/include/psa/crypto.h ]] || fail 'PSA headers are unavailable'

selectors=(MBEDTLS_CONFIG_FILE MBEDTLS_USER_CONFIG_FILE MBEDTLS_INCLUDE_AFTER_RAW_CONFIG
    MBEDTLS_PSA_CRYPTO_CONFIG_FILE MBEDTLS_PSA_CRYPTO_USER_CONFIG_FILE
    TF_PSA_CRYPTO_CONFIG_FILE TF_PSA_CRYPTO_USER_CONFIG_FILE TF_PSA_CRYPTO_INCLUDE_AFTER_RAW_CONFIG
    MBEDTLS_PSA_CRYPTO_PLATFORM_FILE MBEDTLS_PSA_CRYPTO_STRUCT_FILE MBEDTLS_PLATFORM_STD_MEM_HDR)
selector_pattern=$(IFS='|'; echo "${selectors[*]}")
# Audit actual installed headers, including inactive custom include branches.
while IFS= read -r hook; do
    [[ $hook =~ ^($selector_pattern)$ ]] || fail "unaudited configuration include selector: $hook"
done < <(find "$prefix/include" -type f -name '*.h' -exec \
    awk '/^[[:space:]]*#[[:space:]]*include[[:space:]]+/ {
        sub(/^[[:space:]]*#[[:space:]]*include[[:space:]]+/, "")
        if ($0 !~ /^[<"]/) { sub(/[[:space:]].*$/, ""); print }
    }' {} + | sort -u)

verify_provider_header_origins() {
    local fixture=$1 log_base=$2 root unit marker resolved family
    local -a expected
    root=$(realpath -e "$fixture/include") || return 1
    unit='#include <psa/crypto.h>'
    expected=(psa/crypto.h)
    if [[ -f $root/mbedtls/build_info.h ]]; then
        unit+=$'\n#include <mbedtls/build_info.h>'; expected+=(mbedtls/build_info.h)
    elif [[ -f $root/mbedtls/version.h ]]; then
        unit+=$'\n#include <mbedtls/version.h>'; expected+=(mbedtls/version.h)
    fi
    if [[ -f $root/tf-psa-crypto/build_info.h ]]; then
        unit+=$'\n#include <tf-psa-crypto/build_info.h>'; expected+=(tf-psa-crypto/build_info.h)
    fi
    if ! "${compiler_args[@]}" -I "$root" -E -dD -x c - <<< "$unit" \
        >"$log_base.origins" 2>"$log_base.errors"; then
        cat "$log_base.errors" >&2; return 1
    fi
    if ! "${compiler_args[@]}" -I "$root" -E -dM -x c - <<< "$unit" \
        >"$log_base.macros" 2>>"$log_base.errors"; then return 1; fi
    # dD preserves definitions subsequently undefined; dM records final macros.
    if grep -E "^[[:space:]]*#[[:space:]]*define[[:space:]]+($selector_pattern)([[:space:](]|$)" \
        "$log_base.origins" "$log_base.macros"; then
        echo 'custom provider configuration selector refused' >&2; return 1
    fi
    : >"$log_base.providers"
    while IFS= read -r marker; do
        [[ $marker =~ ^#[[:space:]]+[0-9]+[[:space:]]+\"([^\"]*)\"([[:space:]][0-9]+)*[[:space:]]*$ ]] || {
            echo "unsupported source marker: $marker" >&2; return 1;
        }
        marker=${BASH_REMATCH[1]}
        [[ $marker != *\\* ]] || { echo 'escaped source marker refused' >&2; return 1; }
        case $marker in '<built-in>'|'<command-line>'|'<command line>'|'<stdin>') continue ;; esac
        [[ $marker != '<'* ]] || { echo "unsupported synthetic marker: $marker" >&2; return 1; }
        case /$marker in
            */psa/*) family=psa ;;
            */mbedtls/*) family=mbedtls ;;
            */tf-psa-crypto/*) family=tf-psa-crypto ;;
            *) continue ;;
        esac
        resolved=$(realpath -e "$marker") || return 1
        [[ $resolved == "$root/$family/"* ]] || {
            echo "escaped provider header: $marker -> $resolved" >&2; return 1;
        }
        echo "$resolved" >>"$log_base.providers"
    done < <(grep -E '^#[[:space:]]+[0-9]+' "$log_base.origins")
    for marker in "${expected[@]}"; do
        grep -Fxq "$root/$marker" "$log_base.providers" || {
            echo "missing provider origin: $marker" >&2; return 1;
        }
    done
    sort -u "$log_base.providers"
    echo "provider origins verified: $fixture; required ${expected[*]}"
}

for component in tfpsacrypto mbedcrypto mbedtls mbedx509; do
    library=$(find /usr/lib /lib \( -type f -o -type l \) 2>/dev/null \
        | grep -E "/lib${component}\\.(so|a)(\\.|$)" | head -n 1 || true)
    if [[ -n $library ]]; then
        library_dir=$(dirname "$library")
        cp -a "$library_dir/lib${component}.so"* "$prefix/lib/" 2>/dev/null || true
        cp -a "$library_dir/lib${component}.a"* "$prefix/lib/" 2>/dev/null || true
    fi
done
verify_provider_header_origins "$prefix" "$build_dir/complete"
echo 'complete verified fixture public versions:'
awk '$1 == "#define" && $2 ~ /^(MBEDTLS|TF_PSA_CRYPTO)_VERSION_(MAJOR|MINOR|PATCH|NUMBER|STRING|STRING_FULL)$/ {
    print
    if ($2 ~ /^MBEDTLS_/) mbedtls++
    else tf++
}
END {
    if (!mbedtls) print "mbedTLS public version fields unavailable"
    if (!tf) print "TF PSA Crypto public version fields unavailable"
}' "$build_dir/complete.macros"
echo 'end complete verified fixture public versions'

acquire escape escape_prefix
cp -a "$prefix/." "$escape_prefix/"
rm "$escape_prefix/include/psa/crypto.h"
ln -s /usr/include/psa/crypto.h "$escape_prefix/include/psa/crypto.h"
if verify_provider_header_origins "$escape_prefix" "$build_dir/escape"; then
    fail 'escaped provider header unexpectedly verified'
fi
echo 'escaped provider symlink refused'
if [[ -d $prefix/include/tf-psa-crypto ]]; then
    acquire missing-tf missing_tf
    cp -a "$prefix/." "$missing_tf/"
    rm -rf "$missing_tf/include/tf-psa-crypto"
    if verify_provider_header_origins "$missing_tf" "$build_dir/missing-tf"; then
        fail 'missing TF public tree unexpectedly verified'
    fi
    echo 'missing TF public tree refused with host installation present'
fi

configure_log="$build_dir/configure.log"
if ! PKG_CONFIG_LIBDIR="$prefix/empty-pkgconfig" PKG_CONFIG_PATH= \
    CMAKE_PREFIX_PATH= CMAKE_FIND_ROOT_PATH= \
    meson setup "$build_dir" "$repo_root" -Dtests=true \
    -DmbedTLS=enabled -DmbedTLS_prefix="$prefix" >"$configure_log" 2>&1; then
    cat "$configure_log"; exit 1
fi
grep -q 'mbedTLS discovery: metadata-free prefix' "$configure_log" || {
    cat "$configure_log"; fail 'metadata-free fixture was not selected';
}
cat "$configure_log"
meson compile -C "$build_dir" -j8 test_cryptographic_hashes test_symbol_digests
LD_LIBRARY_PATH="$prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    meson test -C "$build_dir" --no-rebuild cryptographic_hashes symbol_digests --print-errorlogs
LD_LIBRARY_PATH="$prefix/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    ldd "$build_dir/tests/test_cryptographic_hashes" >"$build_dir/runtime-libraries.log"
cat "$build_dir/runtime-libraries.log"
while IFS= read -r library; do
    resolved=$(realpath -e "$library")
    [[ $resolved == "$prefix/lib/"* ]] || fail "runtime provider library escapes prefix: $resolved"
    printf 'runtime provider library: %s -> %s\n' "$library" "$resolved"
done < <(sed -nE 's/^[[:space:]]*lib(tfpsacrypto|mbedcrypto|mbedtls|mbedx509)[^[:space:]]*[[:space:]]+=>[[:space:]]+([^[:space:]]+).*/\2/p' \
    "$build_dir/runtime-libraries.log")

acquire incomplete incomplete_prefix
mkdir -p "$incomplete_prefix/include" "$incomplete_prefix/empty-pkgconfig"
cp -a "$prefix/include/." "$incomplete_prefix/include/"
if PKG_CONFIG_LIBDIR="$incomplete_prefix/empty-pkgconfig" PKG_CONFIG_PATH= \
    CMAKE_PREFIX_PATH= CMAKE_FIND_ROOT_PATH= \
    meson setup "$build_dir/incomplete" "$repo_root" -Dtests=false \
    -DmbedTLS=enabled -DmbedTLS_prefix="$incomplete_prefix" \
    >"$build_dir/incomplete.log" 2>&1; then
    cat "$build_dir/incomplete.log"; fail 'missing-library prefix unexpectedly configured'
fi
grep -q 'missing libraries' "$build_dir/incomplete.log"
echo 'independent missing-library prefix refused'
