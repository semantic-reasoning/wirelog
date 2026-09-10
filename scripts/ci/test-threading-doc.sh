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

run_checker_rows() {
    WIRELOG_THREADING_DOC_ROOT="$fixture" \
        WIRELOG_THREADING_EXPECTED_ROWS="$1" "$BASH" "$checker"
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

# Issue #1464: a CRLF helper inventory (Windows text-mode stdout) must pass.
# The checker resolves the helper beside itself, so a copy of the checker in
# a scratch directory next to a stub helper that runs the real one and
# rewrites LF to CRLF exercises the inventory path exactly.  The rows path
# already discards a CR in its extraction, so only the inventory can fail.
crlf_dir=$(mktemp -d)
cp "$checker" "$crlf_dir/check-threading-doc.sh"
# The stub hands the real helper's path to a native Python, which cannot
# open an MSYS POSIX path on the Windows runner; cygpath gives the mixed
# form (D:/...) that both bash and Python accept.  The helper's text-mode
# stdout already ends lines with CRLF on Windows, so the stub normalises
# to LF before rewriting: every host then emits exactly one CR per line.
real_helper=$script_dir/threading_doc_anchors.py
if command -v cygpath >/dev/null 2>&1; then
    real_helper=$(cygpath -m "$real_helper")
fi
printf '%s\n' \
    '#!/usr/bin/env python3' \
    'import subprocess, sys' \
    "real = '$real_helper'" \
    'out = subprocess.run([sys.executable, real] + sys.argv[1:], check=True,' \
    '                     capture_output=True).stdout' \
    "sys.stdout.buffer.write(out.replace(b'\\r\\n', b'\\n').replace(b'\\n', b'\\r\\n'))" \
    >"$crlf_dir/threading_doc_anchors.py"
WIRELOG_THREADING_DOC_ROOT="$fixture" WIRELOG_THREADING_EXPECTED_ROWS=3 \
    "$BASH" "$crlf_dir/check-threading-doc.sh" \
    || { echo 'test-threading-doc: FAIL CRLF helper inventory must pass' >&2; rm -rf "$crlf_dir"; exit 1; }
rm -rf "$crlf_dir"

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

# The order of the four per-row checks is load-bearing, and until #1462 nothing
# pinned it: the duplicate fixture above passes the uniqueness and operation
# checks wherever the duplicate check sits, so a reordered implementation still
# satisfied it.  Each row below is a duplicate that ALSO fails an earlier check,
# so the earlier diagnostic is reported only if the order is right.
make_fixture
printf '%s\n' \
    'int one(void) {' \
    '    atomic_load_explicit(&value, memory_order_relaxed);' \
    '}' >"$fixture/wirelog/foo.c"
cat >"$fixture/docs/THREADING.md" <<'EOF'
| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `foo.c:one` | value | `atomic_load_explicit` | relaxed | test |
| `foo.c:one` | value | `not_atomic` | relaxed | test |
EOF
expect_failure 'has invalid operation' run_checker_rows 2

sed 's/`not_atomic`/`atomic_store_explicit`/' "$fixture/docs/THREADING.md" \
    >"$fixture/docs/THREADING.md.tmp" && mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
expect_failure 'resolves to atomic_load_explicit, documented atomic_store_explicit' \
    run_checker_rows 2

# A documented subset must not pass merely because every documented row
# resolves.  This is the failure mode the suite never exercised, and it is the
# one that depends on how the audit set is accumulated.
make_fixture
printf '%s\n' \
    'int one(void) {' \
    '    atomic_load_explicit(&value, memory_order_relaxed);' \
    '}' \
    'int two(void) {' \
    '    atomic_load_explicit(&value, memory_order_relaxed);' \
    '}' >"$fixture/wirelog/foo.c"
cat >"$fixture/docs/THREADING.md" <<'EOF'
| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `foo.c:one` | value | `atomic_load_explicit` | relaxed | test |
EOF
expect_failure 'audit citations do not cover the exact atomic-site inventory' \
    run_checker_rows 1

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
