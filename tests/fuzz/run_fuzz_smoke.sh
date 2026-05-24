#!/usr/bin/env bash
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
# For commercial licenses, contact: inquiry@cleverplant.com
#
# Helper to run libFuzzer smoke binaries against a writable corpus copy.

set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <fuzzer-binary> <source-corpus-dir> <work-dir> [fuzzer-args...]" >&2
    exit 64
fi

fuzzer_binary=$1
source_corpus=$2
work_dir=$3
shift 3

if [ ! -x "$fuzzer_binary" ]; then
    echo "ERROR: fuzzer binary not found or not executable: $fuzzer_binary" >&2
    exit 1
fi

if [ ! -d "$source_corpus" ]; then
    echo "ERROR: source corpus directory not found: $source_corpus" >&2
    exit 1
fi

readonly corpus_dir="$work_dir/fuzz_corpus"
readonly artifact_dir="$work_dir/artifacts"

mkdir -p "$work_dir"
rm -rf "$corpus_dir"
mkdir -p "$corpus_dir"
cp -a "$source_corpus/." "$corpus_dir"
mkdir -p "$artifact_dir"

has_artifact_prefix=0
for arg in "$@"; do
    case "$arg" in
        -artifact_prefix=* )
            has_artifact_prefix=1
            ;;
    esac
done

if [ "$has_artifact_prefix" -eq 0 ]; then
    set -- "-artifact_prefix=$artifact_dir/" "$@"
fi

"$fuzzer_binary" "$corpus_dir" "$@"
