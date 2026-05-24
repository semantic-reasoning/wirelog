#!/usr/bin/env bash
set -euo pipefail

WL_AFL_TARGET=intern
WL_AFL_BINARY_NAME=intern_fuzz
WL_AFL_CORPUS_NAME=intern

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
wl_afl_main "$@"
