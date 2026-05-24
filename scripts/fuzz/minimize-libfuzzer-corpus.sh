#!/usr/bin/env bash
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
# For commercial licenses, contact: inquiry@cleverplant.com
#
# Minimize libFuzzer corpora produced by the wirelog soak runner.

set -euo pipefail

TARGETS=(parser csv_reader intern compound_arena)

usage() {
    cat <<EOF
Usage: $0 --build-dir DIR --input-dir DIR --out-dir DIR (--all | --target NAME...)

Options:
  --build-dir DIR  Meson build directory containing tests/*_fuzz binaries.
  --input-dir DIR  Soak artifact root containing <target>/corpus directories,
                  or a direct corpus directory when exactly one --target is used.
  --out-dir DIR    Output root for minimized corpora and evidence files.
  --target NAME    Target to minimize. Repeatable. Names: parser, csv_reader, intern, compound_arena.
  --all            Minimize all targets from a soak artifact root.
  --help           Show this help.

The minimizer runs libFuzzer merge mode as:
  <binary> -merge=1 <out-dir>/<target>/corpus <input-corpus> -artifact_prefix=<out-dir>/<target>/artifacts/

It never mutates tests/fuzz/corpus/* or the input corpus.
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

repo_root() {
    git -C "$(cd "$(dirname "$0")/../.." && pwd)" rev-parse --show-toplevel
}

is_valid_target() {
    local target=$1
    local known
    for known in "${TARGETS[@]}"; do
        [ "$target" = "$known" ] && return 0
    done
    return 1
}

binary_name() {
    case "$1" in
        parser) printf 'parser_fuzz' ;;
        csv_reader) printf 'csv_reader_fuzz' ;;
        intern) printf 'intern_fuzz' ;;
        compound_arena) printf 'compound_arena_fuzz' ;;
        *) return 1 ;;
    esac
}

write_tool_versions() {
    local out=$1
    {
        if command -v clang >/dev/null 2>&1; then
            printf 'clang=%s\n' "$(clang --version | head -n 1)"
        else
            printf 'clang=unavailable\n'
        fi
        if command -v llvm-symbolizer >/dev/null 2>&1; then
            printf 'llvm_symbolizer=%s\n' "$(llvm-symbolizer --version | head -n 1)"
        else
            printf 'llvm_symbolizer=unavailable\n'
        fi
    } >>"$out"
}

absolute_path() {
    if command -v realpath >/dev/null 2>&1; then
        realpath -m "$1"
        return
    fi
    case "$1" in
        /*) printf '%s' "$1" ;;
        *) printf '%s/%s' "$(pwd)" "$1" ;;
    esac
}

ensure_non_overlapping_corpora() {
    local target=$1
    local input_corpus=$2
    local output_corpus=$3
    local input_abs output_abs

    input_abs=$(absolute_path "$input_corpus")
    output_abs=$(absolute_path "$output_corpus")

    if [ "$input_abs" = "$output_abs" ]; then
        die "input and output corpora overlap for $target: both resolve to $input_abs"
    fi

    case "$output_abs" in
        "$input_abs"/*)
            die "output corpus is inside input corpus for $target: $output_abs inside $input_abs"
            ;;
    esac

    case "$input_abs" in
        "$output_abs"/*)
            die "input corpus is inside output corpus for $target: $input_abs inside $output_abs"
            ;;
    esac
}

input_corpus_for_target() {
    local target=$1
    if [ "${#selected_targets[@]}" -eq 1 ] && [ -d "$input_dir" ] \
            && [ ! -d "$input_dir/$target/corpus" ]; then
        printf '%s' "$input_dir"
    else
        printf '%s/%s/corpus' "$input_dir" "$target"
    fi
}

preflight_target() {
    local target=$1
    local bin_name binary input_corpus output_corpus

    bin_name=$(binary_name "$target")
    binary="$build_dir/tests/$bin_name"
    input_corpus=$(input_corpus_for_target "$target")
    output_corpus="$out_dir/$target/corpus"

    [ -x "$binary" ] || die "target binary not found or not executable: $binary"
    [ -d "$input_corpus" ] || die "input corpus directory not found for $target: $input_corpus"
    ensure_non_overlapping_corpora "$target" "$input_corpus" "$output_corpus"
}

run_target() {
    local target=$1
    local bin_name binary input_corpus target_dir output_corpus artifact_dir log_dir log_file metadata
    local start_ts end_ts status

    bin_name=$(binary_name "$target")
    binary="$build_dir/tests/$bin_name"
    input_corpus=$(input_corpus_for_target "$target")
    target_dir="$out_dir/$target"
    output_corpus="$target_dir/corpus"
    artifact_dir="$target_dir/artifacts"
    log_dir="$target_dir/logs"
    log_file="$log_dir/minimize.log"
    metadata="$target_dir/metadata.txt"

    [ -x "$binary" ] || die "target binary not found or not executable: $binary"
    [ -d "$input_corpus" ] || die "input corpus directory not found for $target: $input_corpus"
    ensure_non_overlapping_corpora "$target" "$input_corpus" "$output_corpus"

    mkdir -p "$output_corpus" "$artifact_dir" "$log_dir"
    start_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    local -a cmd=("$binary" "-merge=1" "$output_corpus" "$input_corpus" "-artifact_prefix=$artifact_dir/")

    {
        printf 'git_commit=%s\n' "$git_commit"
        printf 'target=%s\n' "$target"
        printf 'binary=%s\n' "$binary"
        printf 'input_corpus=%s\n' "$input_corpus"
        printf 'output_corpus=%s\n' "$output_corpus"
        printf 'artifact_dir=%s\n' "$artifact_dir"
        printf 'log_file=%s\n' "$log_file"
        printf 'start_timestamp=%s\n' "$start_ts"
        printf 'command='
        printf '%q ' "${cmd[@]}"
        printf '\n'
    } >"$metadata"
    write_tool_versions "$metadata"

    printf '[%s] minimizing %s\n' "$target" "${cmd[*]}"
    set +e
    "${cmd[@]}" >"$log_file" 2>&1
    status=$?
    set -e
    end_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    {
        printf 'end_timestamp=%s\n' "$end_ts"
        printf 'exit_status=%s\n' "$status"
    } >>"$metadata"

    {
        printf 'target=%s\n' "$target"
        printf 'metadata=%s\n' "$metadata"
        printf 'log=%s\n' "$log_file"
        printf 'artifacts=%s\n' "$artifact_dir"
        printf 'input_corpus=%s\n' "$input_corpus"
        printf 'output_corpus=%s\n' "$output_corpus"
        printf 'exit_status=%s\n' "$status"
        printf '\n'
    } >>"$manifest"

    if [ "$status" -ne 0 ]; then
        echo "ERROR: target $target minimization failed with status $status; see $log_file" >&2
        return "$status"
    fi
    return 0
}

build_dir=""
input_dir=""
out_dir=""
run_all=0
selected_targets=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --build-dir)
            [ "$#" -ge 2 ] || die "--build-dir requires a directory"
            build_dir=$2
            shift 2
            ;;
        --input-dir)
            [ "$#" -ge 2 ] || die "--input-dir requires a directory"
            input_dir=$2
            shift 2
            ;;
        --out-dir)
            [ "$#" -ge 2 ] || die "--out-dir requires a directory"
            out_dir=$2
            shift 2
            ;;
        --target)
            [ "$#" -ge 2 ] || die "--target requires a name"
            is_valid_target "$2" || die "unknown target '$2' (expected: ${TARGETS[*]})"
            selected_targets+=("$2")
            shift 2
            ;;
        --all)
            run_all=1
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

[ -n "$build_dir" ] || die "--build-dir is required"
[ -d "$build_dir" ] || die "build directory not found: $build_dir"
[ -n "$input_dir" ] || die "--input-dir is required"
[ -d "$input_dir" ] || die "input directory not found: $input_dir"
[ -n "$out_dir" ] || die "--out-dir is required"

if [ "$run_all" -eq 1 ]; then
    selected_targets=("${TARGETS[@]}")
elif [ "${#selected_targets[@]}" -eq 0 ]; then
    die "select at least one --target or use --all"
fi

if [ "$run_all" -eq 1 ] && [ -d "$input_dir/corpus" ]; then
    die "--all requires a soak artifact root with per-target corpus directories, not a direct corpus"
fi

root=$(repo_root)
build_dir=$(cd "$build_dir" && pwd)
input_dir=$(cd "$input_dir" && pwd)
out_dir=$(absolute_path "$out_dir")

case "$out_dir" in
    "$root/tests/fuzz/corpus"|"$root/tests/fuzz/corpus"/*)
        die "--out-dir must not be inside tracked tests/fuzz/corpus"
        ;;
    "$input_dir"|"$input_dir"/*)
        die "--out-dir must not be inside the input corpus/root"
        ;;
esac

for target in "${selected_targets[@]}"; do
    preflight_target "$target"
done

mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)

manifest="$out_dir/manifest.txt"
git_commit=$(git -C "$root" rev-parse HEAD)

{
    printf 'git_commit=%s\n' "$git_commit"
    printf 'start_timestamp=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'build_dir=%s\n' "$build_dir"
    printf 'input_dir=%s\n' "$input_dir"
    printf 'out_dir=%s\n' "$out_dir"
    printf 'targets=%s\n' "${selected_targets[*]}"
    printf '\n'
} >"$manifest"
write_tool_versions "$manifest"
printf '\n' >>"$manifest"

overall=0
for target in "${selected_targets[@]}"; do
    if run_target "$target"; then
        :
    else
        status=$?
        if [ "$overall" -eq 0 ]; then
            overall=$status
        fi
    fi
done

{
    printf 'end_timestamp=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'overall_exit_status=%s\n' "$overall"
} >>"$manifest"

if [ "$overall" -ne 0 ]; then
    echo "libFuzzer corpus minimization failed; see $manifest" >&2
    exit "$overall"
fi

echo "libFuzzer corpus minimization completed; manifest: $manifest"
