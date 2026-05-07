#!/bin/sh
# check-advanced-header.sh - Issue #717 / #703 ABI guard.
#
# wirelog/wirelog-advanced.h is a public header.  It must not include
# any internal header that the project keeps unshipped (session.h,
# backend.h, exec_plan.h, exec_plan_gen.h, intern.h, session_facts.h,
# wirelog-internal.h).  External consumers see only what this script
# allows; any leak here is a 1.0 ABI risk.

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
HEADER="$REPO_ROOT/wirelog/wirelog-advanced.h"

die() {
    printf 'check-advanced-header: %s\n' "$1" >&2
    exit 1
}

[ -f "$HEADER" ] || die "header not found: $HEADER"

FORBIDDEN='session\.h|backend\.h|exec_plan\.h|exec_plan_gen\.h|intern\.h|session_facts\.h|wirelog-internal\.h'

leak=$(grep -nE "^[[:space:]]*#include.*($FORBIDDEN)" "$HEADER" || true)
if [ -n "$leak" ]; then
    printf 'check-advanced-header: internal header leaked into public surface:\n%s\n' "$leak" >&2
    exit 1
fi

printf 'check-advanced-header: OK\n'
exit 0
