#!/usr/bin/env bash
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
# For commercial licenses, contact: inquiry@cleverplant.com
#
# Shared AFL++ entrypoint helper for Issue #743 fuzz targets.

set -euo pipefail

wl_afl_usage() {
    cat <<EOF
Usage: $0 [options] [-- afl-fuzz-args...]

Options:
  -b, --binary PATH   Fuzz target binary to execute.
                     Default: build-fuzz/tests/$WL_AFL_BINARY_NAME
  -w, --work-dir DIR  Writable seed-copy directory.
                     Default: \${TMPDIR:-/tmp}/wirelog-afl/$WL_AFL_TARGET
  -o, --out-dir DIR   AFL++ output directory.
                     Default: \${TMPDIR:-/tmp}/wirelog-afl/$WL_AFL_TARGET-out
  -h, --help          Show this help.

The script copies tests/fuzz/corpus/$WL_AFL_CORPUS_NAME into the work
directory before invoking afl-fuzz. It never mutates tracked source corpora.
The target binary must already exist; this script does not rebuild wirelog.
EOF
}

wl_afl_die() {
    echo "ERROR: $*" >&2
    exit 1
}

wl_afl_main() {
    local repo_root
    repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

    local binary="$repo_root/build-fuzz/tests/$WL_AFL_BINARY_NAME"
    local work_root="${TMPDIR:-/tmp}/wirelog-afl/$WL_AFL_TARGET"
    local out_dir="${TMPDIR:-/tmp}/wirelog-afl/$WL_AFL_TARGET-out"
    local -a afl_args=()

    while [ "$#" -gt 0 ]; do
        case "$1" in
            -b|--binary)
                [ "$#" -ge 2 ] || wl_afl_die "$1 requires a path"
                binary=$2
                shift 2
                ;;
            -w|--work-dir)
                [ "$#" -ge 2 ] || wl_afl_die "$1 requires a directory"
                work_root=$2
                shift 2
                ;;
            -o|--out-dir)
                [ "$#" -ge 2 ] || wl_afl_die "$1 requires a directory"
                out_dir=$2
                shift 2
                ;;
            -h|--help)
                wl_afl_usage
                return 0
                ;;
            --)
                shift
                afl_args+=("$@")
                break
                ;;
            *)
                afl_args+=("$1")
                shift
                ;;
        esac
    done

    command -v afl-fuzz >/dev/null 2>&1 \
        || wl_afl_die "afl-fuzz not found in PATH"

    [ -x "$binary" ] \
        || wl_afl_die "target binary not found or not executable: $binary"

    local source_corpus="$repo_root/tests/fuzz/corpus/$WL_AFL_CORPUS_NAME"
    [ -d "$source_corpus" ] \
        || wl_afl_die "source corpus directory not found: $source_corpus"

    local seed_dir="$work_root/seeds"
    rm -rf "$seed_dir"
    mkdir -p "$seed_dir" "$out_dir"
    cp -a "$source_corpus/." "$seed_dir"

    exec afl-fuzz -i "$seed_dir" -o "$out_dir" "${afl_args[@]}" -- "$binary" @@
}
