#!/usr/bin/env bash
# check-threading-doc.sh - Issue #734 backstop.
#
# Asserts that the atomics audit table in `docs/THREADING.md` has one
# row for every `atomic_*` call site in `wirelog/` production sources
# (`.c` and `.h` under the project root, excluding MSVC shim
# definitions in `mem_ledger.h` and the macro-definition block at the
# top of `lockfree_queue.c`).
#
# A row is a line in `docs/THREADING.md` whose first cell starts with
# a backtick-quoted file:line reference under `wirelog/`.  Rows are
# pattern-matched as: `| \`file:line\` | ...`.
#
# Exit codes:
#   0 - row count in doc matches site count in code.
#   1 - mismatch (atomic_* added or removed; doc needs an audit
#       refresh) or other hard failure.
#
# Intended to register under `meson test --suite abi:threading_doc`.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
doc="$repo_root/docs/THREADING.md"

if [ ! -f "$doc" ]; then
    echo "check-threading-doc: FAIL: docs/THREADING.md missing" >&2
    exit 1
fi

# Count atomic_* call sites in wirelog/ production sources.
# Restricted to .c and .h.  The MSVC shim block in mem_ledger.h
# (lines containing `#define atomic_`) is excluded; ditto the
# WL_ATOMIC_* macro definitions inside lockfree_queue.c (the
# definitions are at lines 22-37 and 47-57; the actual atomic_*
# tokens are inside the macro bodies and are the contract sites we
# do want to count as "atomic operations on the SPSC ring").
sites=$(grep -rEn '\batomic_(load|store|fetch_add|fetch_sub|fetch_or|fetch_and|fetch_xor|compare_exchange_weak|compare_exchange_strong|exchange|init|thread_fence)(_explicit)?\s*\(' \
        "$repo_root/wirelog" \
        --include='*.c' --include='*.h' \
    | grep -v '#define atomic_' \
    | grep -v '#define WL_ATOMIC' \
    | wc -l)

# Count audit-table rows in docs/THREADING.md.  Rows begin with
# `| \`wirelog/...` or `| \`mem_ledger.c:...` (file:line ref in the
# first table cell).  Pattern: a line starting with `| \`` followed
# by an identifier and a colon and a digit, ending the first cell
# with a backtick.
rows=$(grep -cE '^\| `[A-Za-z_./-]+:[0-9]+`' "$doc" || true)

if [ "$sites" -ne "$rows" ]; then
    echo "check-threading-doc: FAIL:" >&2
    echo "  atomic_* call sites in wirelog/: $sites" >&2
    echo "  audit table rows in $doc: $rows" >&2
    echo "" >&2
    echo "Drift detected.  Either:" >&2
    echo "  (a) the audit table is stale -- update docs/THREADING.md §5" >&2
    echo "      with a new row for the added atomic_* call site, or" >&2
    echo "  (b) an atomic_* call site was removed -- delete the matching" >&2
    echo "      row from §5." >&2
    echo "" >&2
    echo "Re-run after fixing.  See docs/THREADING.md §5 for the row" >&2
    echo "format and the per-site justification convention." >&2
    exit 1
fi

echo "check-threading-doc: OK; $sites atomic_* call sites match $rows audit rows in docs/THREADING.md"
exit 0
