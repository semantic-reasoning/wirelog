#!/usr/bin/env bash
# Issue #287: wirelog/util/log.h must never be transitively included by any
# installed/public header. List matches install_headers() entries in
# meson.build (wirelog_public_headers + wirelog/io/io_adapter.h). Text grep is
# sufficient for v1 (transitive preprocessor walk is a follow-up issue).
# Keep in sync with meson.build when new public headers are installed.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

PUBLIC_HEADERS=(
    wirelog/wirelog.h
    wirelog/wirelog-types.h
    wirelog/wirelog-parser.h
    wirelog/wirelog-ir.h
    wirelog/wirelog-optimizer.h
    wirelog/wirelog-export.h
    wirelog/wl_easy.h
    wirelog/io/io_adapter.h
)

FAIL=0
for h in "${PUBLIC_HEADERS[@]}"; do
    if [[ ! -f "$h" ]]; then continue; fi
    if grep -qE '#[[:space:]]*include[[:space:]]+[<"]wirelog/util/log\.h[>"]' "$h"; then
        echo "LEAK: public header $h includes wirelog/util/log.h" >&2
        FAIL=1
    fi
done
exit $FAIL
