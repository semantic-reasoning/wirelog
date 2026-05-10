#!/bin/sh
# check-advanced-header.sh - Issue #717 / #703 ABI guard.
#
# wirelog/wirelog-advanced.h is a public header.  Internal headers
# anywhere in the wirelog/ tree must NOT leak through it.  The check
# is allowlist-based: any quoted include not on the allowlist fails.
# System includes (<stdint.h>, <stddef.h>, ...) are unconstrained.

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
HEADER="$REPO_ROOT/wirelog/wirelog-advanced.h"

die() {
    printf 'check-advanced-header: %s\n' "$1" >&2
    exit 1
}

[ -f "$HEADER" ] || die "header not found: $HEADER"

ALLOWED='#include[[:space:]]*"wirelog/(wirelog\.h|wirelog-types\.h|wirelog-parser\.h|wirelog-ir\.h|wirelog-optimizer\.h|wirelog-export\.h|wirelog_easy\.h|wirelog-advanced\.h|io/io_adapter\.h)"[[:space:]]*$'

quoted_includes=$(grep -nE '^[[:space:]]*#include[[:space:]]*"' "$HEADER" || true)
suspicious=$(printf '%s\n' "$quoted_includes" | grep -vE "$ALLOWED" || true)

if [ -n "$suspicious" ]; then
    printf 'check-advanced-header: internal or unlisted header in public surface:\n%s\n' "$suspicious" >&2
    printf '\nallowed quoted includes (allowlist):\n  wirelog/wirelog.h\n  wirelog/wirelog-types.h\n  wirelog/wirelog-parser.h\n  wirelog/wirelog-ir.h\n  wirelog/wirelog-optimizer.h\n  wirelog/wirelog-export.h\n  wirelog/wirelog-easy.h\n  wirelog/wirelog-advanced.h\n  wirelog/io/io_adapter.h\n' >&2
    exit 1
fi

printf 'check-advanced-header: OK\n'
exit 0
