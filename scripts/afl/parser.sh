#!/usr/bin/env bash
set -euo pipefail

WL_AFL_TARGET=parser
WL_AFL_BINARY_NAME=parser_fuzz
WL_AFL_CORPUS_NAME=parser

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
wl_afl_main "$@"
