#!/usr/bin/env bash
# Focused regression tests for the symbol-anchored threading-doc checker.
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
checker="$script_dir/check-threading-doc.sh"
# Invoke the checker with "$BASH", not via its shebang. The shebang resolves to
# the first bash on PATH, which need not be the interpreter running this suite
# -- so a portability defect that only appears under the runner's bash (macOS
# ships 3.2.57) would be masked here by a newer bash from the environment. This
# is what makes the #1320 case below able to fail.
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT

make_fixture() {
    rm -rf "$fixture"
    mkdir -p "$fixture/wirelog/columnar" "$fixture/docs"
}

run_checker() {
    WIRELOG_THREADING_DOC_ROOT="$fixture" \
        WIRELOG_THREADING_EXPECTED_ROWS=3 "$BASH" "$checker"
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
    WIRELOG_THREADING_EXPECTED_ROWS=3 "$BASH" "$checker" 2>/dev/null) || exit 1

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
    "$BASH" "$checker" >/dev/null

# #1320: a prose citation naming a file that does not exist must be REPORTED,
# not abort the checker. On bash 3.2 -- which the macOS CI runners ship, and
# this suite has no platform gate -- iterating an empty array under `set -u` is
# an unbound-variable error, so resolve_reference_source died before its caller
# could name the citation. bash 4.4 changed that, so on any modern bash this
# case passes with or without the guard: what it pins is the DIAGNOSTIC, and
# the portability guard is verified by the assertion below it.
#
# Verified by hand against a real GNU bash 3.2.57 (the macOS version, built
# from source with all 57 official patches): unfixed, this fixture dies with
# `candidates[@]: unbound variable`; fixed, it prints the message asserted here.
# Note for anyone repeating that: unpatched 3.2.0 additionally rejects this
# repository's inline `=~ ^([0-9]+)-(...)$` as a syntax error, which 3.2.57
# accepts -- 3.2.0 is not a faithful proxy for the runner.
make_fixture
printf '%s\n' '#include <stdatomic.h>' 'static _Atomic int value;' \
    'int real(void) {' \
    '    atomic_load_explicit(&value, memory_order_relaxed);' \
    '}' >"$fixture/wirelog/foo.c"
cat >"$fixture/docs/THREADING.md" <<'EOF'
| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `foo.c:real` | value | `atomic_load_explicit` | relaxed | test |

Prose citing `absent.c:1-5`, which does not exist under wirelog/.
EOF
missing_out=$(WIRELOG_THREADING_EXPECTED_ROWS=1 WIRELOG_THREADING_DOC_ROOT="$fixture" \
    "$BASH" "$checker" 2>&1 || true)
case "$missing_out" in
    *"prose citation 'absent.c:1-5' does not resolve"*) ;;
    *)
        echo "test-threading-doc: FAIL: expected the unresolvable-citation diagnostic" >&2
        printf 'got: %s\n' "$missing_out" >&2
        exit 1
        ;;
esac
# The abort this replaced named the array, not the citation. Assert its absence
# directly, so a future edit that reintroduces the bare iteration is caught here
# on bash 3.2 rather than only on a macOS runner.
case "$missing_out" in
    *'unbound variable'*)
        echo "test-threading-doc: FAIL: checker aborted on an empty candidate set" >&2
        printf 'got: %s\n' "$missing_out" >&2
        exit 1
        ;;
esac

echo 'test-threading-doc: OK'
