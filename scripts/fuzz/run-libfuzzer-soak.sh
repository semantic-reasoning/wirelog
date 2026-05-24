#!/usr/bin/env bash
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
# For commercial licenses, contact: inquiry@cleverplant.com
#
# Run bounded libFuzzer soak campaigns over committed wirelog fuzz seeds.

set -euo pipefail

TARGETS=(parser csv_reader intern compound_arena)

usage() {
    cat <<EOF
Usage: $0 --build-dir DIR [--work-dir DIR] --duration VALUE (--all | --target NAME...)

Options:
  --build-dir DIR   Meson build directory containing tests/*_fuzz binaries.
  --work-dir DIR    Artifact/work root. Default: \${TMPDIR:-/tmp}/wirelog-libfuzzer-soak-<timestamp>
  --duration VALUE  Run duration per target. Supports integer seconds or s/m/h suffixes.
                   Examples: 60, 30s, 10m, 24h.
  --target NAME     Target to run. Repeatable. Names: parser, csv_reader, intern, compound_arena.
  --all             Run all targets.
  --help            Show this help.

The runner copies tests/fuzz/corpus/<target>/ into writable per-target corpus
directories before invoking libFuzzer. Source corpora are never mutated.
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

parse_duration() {
    local value=$1
    case "$value" in
        ''|*[!0-9smh]*)
            return 1
            ;;
        *s)
            local n=${value%s}
            [[ "$n" =~ ^[0-9]+$ ]] || return 1
            printf '%s' "$n"
            ;;
        *m)
            local n=${value%m}
            [[ "$n" =~ ^[0-9]+$ ]] || return 1
            printf '%s' "$((n * 60))"
            ;;
        *h)
            local n=${value%h}
            [[ "$n" =~ ^[0-9]+$ ]] || return 1
            printf '%s' "$((n * 3600))"
            ;;
        *)
            [[ "$value" =~ ^[0-9]+$ ]] || return 1
            printf '%s' "$value"
            ;;
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

copy_corpus() {
    local source_dir=$1
    local work_corpus=$2

    rm -rf "$work_corpus"
    mkdir -p "$work_corpus"
    cp -a "$source_dir/." "$work_corpus"
}

run_target() {
    local target=$1
    local bin_name binary source_corpus target_dir work_corpus artifact_dir log_dir log_file metadata
    local start_ts end_ts status

    bin_name=$(binary_name "$target")
    binary="$build_dir/tests/$bin_name"
    source_corpus="$root/tests/fuzz/corpus/$target"
    target_dir="$work_dir/$target"
    work_corpus="$target_dir/corpus"
    artifact_dir="$target_dir/artifacts"
    log_dir="$target_dir/logs"
    log_file="$log_dir/libfuzzer.log"
    metadata="$target_dir/metadata.txt"

    [ -x "$binary" ] || die "target binary not found or not executable: $binary"
    [ -d "$source_corpus" ] || die "source corpus directory not found: $source_corpus"

    mkdir -p "$artifact_dir" "$log_dir"
    copy_corpus "$source_corpus" "$work_corpus"

    start_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    local -a cmd=("$binary" "$work_corpus" "-max_total_time=$duration_seconds" "-artifact_prefix=$artifact_dir/")

    {
        printf 'git_commit=%s\n' "$git_commit"
        printf 'target=%s\n' "$target"
        printf 'duration_input=%s\n' "$duration_input"
        printf 'duration_seconds=%s\n' "$duration_seconds"
        printf 'binary=%s\n' "$binary"
        printf 'source_corpus=%s\n' "$source_corpus"
        printf 'working_corpus=%s\n' "$work_corpus"
        printf 'artifact_dir=%s\n' "$artifact_dir"
        printf 'log_file=%s\n' "$log_file"
        printf 'start_timestamp=%s\n' "$start_ts"
        printf 'command='
        printf '%q ' "${cmd[@]}"
        printf '\n'
    } >"$metadata"
    write_tool_versions "$metadata"

    printf '[%s] running %s\n' "$target" "${cmd[*]}"
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
        printf 'exit_status=%s\n' "$status"
        printf '\n'
    } >>"$manifest"

    if [ "$status" -ne 0 ]; then
        echo "ERROR: target $target failed with status $status; see $log_file" >&2
        return "$status"
    fi
    return 0
}

build_dir=""
work_dir=""
duration_input=""
run_all=0
selected_targets=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --build-dir)
            [ "$#" -ge 2 ] || die "--build-dir requires a directory"
            build_dir=$2
            shift 2
            ;;
        --work-dir)
            [ "$#" -ge 2 ] || die "--work-dir requires a directory"
            work_dir=$2
            shift 2
            ;;
        --duration)
            [ "$#" -ge 2 ] || die "--duration requires a value"
            duration_input=$2
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
[ -n "$duration_input" ] || die "--duration is required"
duration_seconds=$(parse_duration "$duration_input") \
    || die "invalid duration '$duration_input' (use seconds or s/m/h suffix)"
[ "$duration_seconds" -gt 0 ] || die "duration must be greater than zero"

if [ "$run_all" -eq 1 ]; then
    selected_targets=("${TARGETS[@]}")
elif [ "${#selected_targets[@]}" -eq 0 ]; then
    die "select at least one --target or use --all"
fi

root=$(repo_root)
build_dir=$(cd "$build_dir" && pwd)
if [ -z "$work_dir" ]; then
    work_dir="${TMPDIR:-/tmp}/wirelog-libfuzzer-soak-$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "$work_dir"
work_dir=$(cd "$work_dir" && pwd)
manifest="$work_dir/manifest.txt"
git_commit=$(git -C "$root" rev-parse HEAD)

{
    printf 'git_commit=%s\n' "$git_commit"
    printf 'start_timestamp=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'build_dir=%s\n' "$build_dir"
    printf 'work_dir=%s\n' "$work_dir"
    printf 'duration_input=%s\n' "$duration_input"
    printf 'duration_seconds=%s\n' "$duration_seconds"
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
    echo "libFuzzer soak failed; see $manifest" >&2
    exit "$overall"
fi

echo "libFuzzer soak completed; manifest: $manifest"
