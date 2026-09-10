#!/usr/bin/env bash
set -euo pipefail

root="${WIRELOG_SOURCE_ACCESS_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
header="$root/wirelog/columnar/source_access.h"
[ -f "$header" ] || { echo "source-access: missing header" >&2; exit 1; }

if rg -n 'malloc|calloc|realloc|free\s*\(' "$header"; then
    echo "source-access: allocation call in gate" >&2
    exit 1
fi

for symbol in \
    wl_columnar_source_access_reader_acquire \
    wl_columnar_source_access_reader_release \
    wl_columnar_source_access_writer_acquire \
    wl_columnar_source_access_writer_release; do
    rg -q "$symbol" "$header" || {
        echo "source-access: missing $symbol" >&2
        exit 1
    }
done

if rg -n 'wirelog/columnar/source_access\.h|source_access\.h' \
    "$root/meson.build" "$root/wirelog/meson.build"; then
    echo "source-access: gate must not enter production source lists" >&2
    exit 1
fi

echo "source-access: allocation-free, test-only contract OK"
