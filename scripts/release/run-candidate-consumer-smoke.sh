#!/usr/bin/env bash
# Build and run supported public-API consumers against this exact checkout.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: run-candidate-consumer-smoke.sh --output-dir DIR [--source-root DIR]
    [--candidate-sha SHA] [--expected-output FILE] [--prefix DIR]
    [--keep-prefix DIR] [--expected-library FILE]
EOF
}

output_dir=""
source_root=""
candidate_sha=""
expected_output=""
prefix_override=""
keep_prefix=""
expected_library=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir) output_dir=${2:-}; shift 2 ;;
        --source-root) source_root=${2:-}; shift 2 ;;
        --candidate-sha) candidate_sha=${2:-}; shift 2 ;;
        --expected-output) expected_output=${2:-}; shift 2 ;;
        --prefix) prefix_override=${2:-}; shift 2 ;;
        --keep-prefix) keep_prefix=${2:-}; shift 2 ;;
        --expected-library) expected_library=${2:-}; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage; exit 2 ;;
    esac
done
if [[ -z "$source_root" ]]; then
    source_root=$(git rev-parse --show-toplevel)
fi
[[ -n "$output_dir" && -d "$source_root" ]] || { usage; exit 2; }
case "$(uname -s)" in
    Linux) ;;
    *) echo 'candidate consumer smoke requires Linux' >&2; exit 77 ;;
esac
cc_bin=${CC:-cc}
command -v "$cc_bin" >/dev/null || { echo "compiler is required: $cc_bin" >&2; exit 2; }

mkdir -p "$output_dir"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-candidate-consumer.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
prefix="$tmp/prefix"
build="$tmp/build"
if [[ -z "$candidate_sha" ]]; then
    candidate_sha=$(git -C "$source_root" rev-parse HEAD)
fi
if [[ -n "$prefix_override" ]]; then
    prefix=$prefix_override
else
    command -v meson >/dev/null || { echo 'meson is required' >&2; exit 2; }
    command -v ninja >/dev/null || { echo 'ninja is required' >&2; exit 2; }
    meson setup "$build" "$source_root" --buildtype=release -Dtests=false \
        -Dprefix="$prefix" >"$output_dir/configure.log" 2>&1
    meson compile -C "$build" >"$output_dir/build.log" 2>&1
    meson install -C "$build" >"$output_dir/install.log" 2>&1
    if [[ -n "$keep_prefix" ]]; then
        rm -rf "$keep_prefix"
        cp -a "$prefix" "$keep_prefix"
    fi
fi

candidate_lib=$(find -L "$prefix" -type f -name 'libwirelog.so.*' -print -quit)
[[ -n "$candidate_lib" ]] || {
    echo 'installed candidate libwirelog.so was not found' >&2
    exit 1
}
libdir=$(dirname "$candidate_lib")
if [[ -n "$expected_library" ]]; then
    candidate_lib=$expected_library
fi

candidate_library_loaded() {
    local trace=$1
    local candidate_real alias alias_real

    if [[ -n "$expected_library" ]]; then
        grep -Fq "calling init: $candidate_lib" "$trace"
        return
    fi

    candidate_real=$(readlink -f -- "$candidate_lib")
    while IFS= read -r -d '' alias; do
        alias_real=$(readlink -f -- "$alias")
        if [[ "$alias_real" == "$candidate_real" ]] \
            && grep -Fq "calling init: $alias" "$trace"; then
            return 0
        fi
    done < <(find "$libdir" -maxdepth 1 \( -type f -o -type l \) \
        -name 'libwirelog.so.*' -print0)
    return 1
}

if [[ -n "$expected_output" ]]; then
    [[ -f "$expected_output" ]] || { echo "expected output not found: $expected_output" >&2; exit 2; }
    expected=$(cat "$expected_output")
else
    expected=$'reach 1 2\nreach 1 3\nreach 2 3'
fi

run_consumer() {
    local name=$1 source=$2
    local exe="$tmp/$name"
    local trace_prefix="$tmp/$name-loader"
    local trace="$output_dir/$name-loader.log"
    local actual="$output_dir/$name.out"
    "$cc_bin" -std=c11 -Wall -Wextra -Werror -I"$prefix/include" \
        "$source" -L"$libdir" -lwirelog \
        -Wl,-rpath,"$libdir" -o "$exe" \
        >"$output_dir/$name-compile.log" 2>&1
    rm -f "$trace_prefix".[0-9]*
    LD_LIBRARY_PATH="$libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        LD_DEBUG=libs LD_DEBUG_OUTPUT="$trace_prefix" \
        "$exe" "$source_root/tests/release/candidate_program.dl" \
        >"$actual" 2>"$output_dir/$name-runtime.log"
    cat "$trace_prefix".[0-9]* >"$trace" 2>/dev/null || {
        echo "missing loader trace for $name" >&2
        exit 1
    }
    candidate_library_loaded "$trace" || {
        echo "candidate library provenance check failed for $name" >&2
        exit 1
    }
    diff -u <(printf '%s\n' "$expected") "$actual" \
        >"$output_dir/$name-output.diff" || {
        echo "candidate consumer output mismatch: $name" >&2
        cat "$output_dir/$name-output.diff" >&2
        exit 1
    }
}

run_consumer easy "$source_root/tests/release/candidate_easy.c"
run_consumer session "$source_root/tests/release/candidate_session.c"
printf 'candidate_sha=%s\nlib=%s\nstatus=passed\n' \
    "$candidate_sha" "$candidate_lib" >"$output_dir/status.txt"
