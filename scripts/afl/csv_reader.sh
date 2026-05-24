#!/usr/bin/env bash
set -euo pipefail

WL_AFL_TARGET=csv_reader
WL_AFL_BINARY_NAME=csv_reader_fuzz
WL_AFL_CORPUS_NAME=csv_reader

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
wl_afl_main "$@"
