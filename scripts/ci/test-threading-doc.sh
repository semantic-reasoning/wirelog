#!/usr/bin/env bash
# Focused regression tests for the symbol-anchored threading-doc checker.
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
checker="$script_dir/check-threading-doc.sh"
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

make_fixture() {
    rm -rf "$fixture"
    mkdir -p "$fixture/wirelog/columnar" "$fixture/docs"
}

run_checker() {
    WIRELOG_THREADING_DOC_ROOT="$fixture" \
        WIRELOG_THREADING_EXPECTED_ROWS=3 "$checker"
}

expect_failure() {
    local label=$1
    shift
    if "$@" >"$fixture/stdout" 2>"$fixture/stderr"; then
        echo "test-threading-doc: FAIL: $label unexpectedly passed" >&2
        exit 1
    fi
    grep -F "$label" "$fixture/stderr" >/dev/null || {
        echo "test-threading-doc: FAIL: diagnostic for $label missing" >&2
        cat "$fixture/stderr" >&2
        exit 1
    }
}

make_fixture
printf '%s\n' \
    'int foo(void) {' \
    '    atomic_load_explicit(&value, memory_order_relaxed);' \
    '}' >"$fixture/wirelog/foo.c"
printf '%s\n' \
    '#define LOAD(p) \' \
    '    atomic_load_explicit((p), memory_order_acquire)' >"$fixture/wirelog/intern.c"
printf '%s\n' \
    'int session(void) {' \
    '    atomic_store_explicit(&value, 1, memory_order_relaxed);' \
    '}' >"$fixture/wirelog/columnar/session.c"
cat >"$fixture/docs/THREADING.md" <<'EOF'
| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `foo.c:foo` | value | `atomic_load_explicit` | relaxed | test |
| `intern.c:LOAD` | value | `atomic_load_explicit` | acquire | test |
| `session.c:session` | value | `atomic_store_explicit` | relaxed | test |
EOF
run_checker >/dev/null

# A header-like cell is not an audit row, while the macro continuation is.
grep -q '3 audit rows' <(WIRELOG_THREADING_DOC_ROOT="$fixture" \
    WIRELOG_THREADING_EXPECTED_ROWS=3 "$checker" 2>/dev/null) || exit 1

# A duplicated anchor cannot hide an omitted source site.
sed 's/intern.c:LOAD/foo.c:foo/' "$fixture/docs/THREADING.md" \
    >"$fixture/docs/THREADING.md.tmp" && mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
expect_failure 'duplicate audit anchor' run_checker
sed 's/foo.c:foo` | value | `atomic_load_explicit` | acquire/intern.c:LOAD` | value | `atomic_load_explicit` | acquire/' \
    "$fixture/docs/THREADING.md" >"$fixture/docs/THREADING.md.tmp" && mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"

# A renamed function and an invalid operation are hard failures.
sed 's/foo.c:foo/foo.c:renamed/' "$fixture/docs/THREADING.md" \
    >"$fixture/docs/THREADING.md.tmp" && mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
expect_failure 'does not resolve uniquely' run_checker
sed 's/foo.c:renamed/foo.c:foo/' "$fixture/docs/THREADING.md" \
    >"$fixture/docs/THREADING.md.tmp" && mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
sed 's/atomic_store_explicit`/not_atomic`/' "$fixture/docs/THREADING.md" \
    >"$fixture/docs/THREADING.md.tmp" && mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
expect_failure 'has invalid operation' run_checker

# A valid but wrong operation must not pass anchor-only resolution.
sed 's/atomic_load_explicit`/atomic_store_explicit`/' "$fixture/docs/THREADING.md" \
    >"$fixture/docs/THREADING.md.tmp" && mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
expect_failure 'resolves to atomic_load_explicit, documented atomic_store_explicit' run_checker

# A function pointer declaration/call and comment/string text do not create sites.
make_fixture
printf '%s\n' \
    '/* atomic_load_explicit(&fake, x); */' \
    'const char *s = "atomic_store_explicit(&fake, x)";' \
    'void (*callback)(void);' \
    'int real(void) {' \
    '    atomic_load_explicit(&value, memory_order_relaxed);' \
    '}' >"$fixture/wirelog/foo.c"
cat >"$fixture/docs/THREADING.md" <<'EOF'
| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `foo.c:real` | value | `atomic_load_explicit` | relaxed | test |
EOF
WIRELOG_THREADING_EXPECTED_ROWS=1 WIRELOG_THREADING_DOC_ROOT="$fixture" \
    "$checker" >/dev/null

echo 'test-threading-doc: OK'
