#!/usr/bin/env bash
# Focused regression tests for check-threading-doc.sh.

set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
checker="$script_dir/check-threading-doc.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

make_fixture() {
    rm -rf "$fixture"
    mkdir -p "$fixture/wirelog/columnar" "$fixture/docs"
}

run_checker() {
    WIRELOG_THREADING_DOC_ROOT="$fixture" "$checker"
}

replace_doc() {
    local old=$1
    local new=$2
    sed "s#$old#$new#" "$fixture/docs/THREADING.md" > "$fixture/docs/THREADING.md.tmp"
    mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
}

delete_doc_lines() {
    local pattern=$1
    sed "/$pattern/d" "$fixture/docs/THREADING.md" > "$fixture/docs/THREADING.md.tmp"
    mv "$fixture/docs/THREADING.md.tmp" "$fixture/docs/THREADING.md"
}

expect_failure() {
    local label=$1
    shift
    if "$@" >"$fixture/stdout" 2>"$fixture/stderr"; then
        echo "test-threading-doc: FAIL: $label unexpectedly passed" >&2
        cat "$fixture/stderr" >&2
        exit 1
    fi
    grep -F "$label" "$fixture/stderr" >/dev/null || {
        echo "test-threading-doc: FAIL: diagnostic for $label missing" >&2
        cat "$fixture/stderr" >&2
        exit 1
    }
}

# A unique basename resolves and a backslash-continued macro is accepted.
make_fixture
printf '%s\n' 'atomic_load_explicit(&value, memory_order_relaxed);' 'not an atomic site' > "$fixture/wirelog/foo.c"
printf '%s\n' '#define LOAD(p) \' '    atomic_load_explicit((p), memory_order_acquire)' > "$fixture/wirelog/intern.c"
printf '%s\n' 'int session(void) {' '    atomic_store_explicit(&value, 1, memory_order_relaxed);' '}' > "$fixture/wirelog/columnar/session.c"
printf '%s\n' '/* duplicate basename must not be selected */' > "$fixture/wirelog/session.c"
cat > "$fixture/docs/THREADING.md" <<'EOF'
| `foo.c:1` | value | `atomic_load_explicit` | relaxed | test |
| `intern.c:1` | value | `atomic_load_explicit` | acquire | test |
| `session.c:2` | value | `atomic_store_explicit` | relaxed | test |
EOF
run_checker >/dev/null

# A duplicate valid row cannot hide an omitted source site: exact inventory
# comparison must catch it even when counts still match.
replace_doc 'intern.c:1' 'foo.c:1'
expect_failure 'audit citations do not cover the exact atomic-site inventory' run_checker
replace_doc 'foo.c:1` | value | `atomic_load_explicit` | acquire' 'intern.c:1` | value | `atomic_load_explicit` | acquire'

# The documented operation must match the cited source operation.
replace_doc 'atomic_load_explicit` | relaxed' 'atomic_store_explicit` | relaxed'
expect_failure 'foo.c:1 does not contain atomic_store_explicit' run_checker
replace_doc 'atomic_store_explicit` | relaxed' 'atomic_load_explicit` | relaxed'
replace_doc 'atomic_load_explicit` | relaxed' 'not_atomic` | relaxed'
expect_failure 'foo.c:1 has invalid operation' run_checker
replace_doc 'not_atomic` | relaxed' 'atomic_load_explicit` | relaxed'
replace_doc 'session.c:2.*atomic_load_explicit' 'session.c:2` | value | `atomic_store_explicit'

# An existing file with a non-atomic cited line is rejected.
replace_doc 'foo.c:1' 'foo.c:2'
expect_failure 'foo.c:2 does not contain atomic_load_explicit' run_checker

# The ambiguity rule reports both candidates instead of picking one.
replace_doc 'foo.c:2' 'foo.c:1'
mkdir -p "$fixture/wirelog/extra"
printf '%s\n' 'atomic_load_explicit(&value, memory_order_relaxed);' > "$fixture/wirelog/extra/foo.c"
expect_failure 'foo.c:1 is ambiguous' run_checker

# Out-of-range citations name the offending row and source.
rm "$fixture/wirelog/foo.c"
printf '%s\n' 'atomic_load_explicit(&value, memory_order_relaxed);' > "$fixture/wirelog/foo.c"
rm "$fixture/wirelog/extra/foo.c"
replace_doc 'foo.c:1' 'foo.c:99'
expect_failure 'foo.c:99 does not contain atomic_load_explicit' run_checker

# A basename with no source candidate reports the row and remediation.
replace_doc 'foo.c:99' 'missing.c:1'
expect_failure 'missing.c:1 has no matching source' run_checker

# The original count backstop remains active.
delete_doc_lines '`missing.c:1`'
expect_failure 'atomic_* call sites in wirelog/' run_checker

echo 'test-threading-doc: OK'
