#!/usr/bin/env bash
set -euo pipefail

WL_AFL_TARGET=compound_arena
WL_AFL_BINARY_NAME=compound_arena_fuzz
WL_AFL_CORPUS_NAME=compound_arena

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
wl_afl_main "$@"
