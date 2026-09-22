#!/usr/bin/env bash
# Source-root discovery regression for the logger ABI gates (#1663).
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(CDPATH= cd -P -- "${SCRIPT_DIR}/../.." && pwd -P)"
SANDBOX=""
failures=0

cleanup() {
    status=$?
    trap - EXIT INT TERM HUP
    if [ -n "${SANDBOX}" ] && [ "${SANDBOX}" != "/" ]; then
        if [ -d "${SANDBOX}/ordinary checkout with spaces/.git" ]; then
            fixture_git -C "${SANDBOX}/ordinary checkout with spaces" worktree remove \
                --force "${SANDBOX}/linked worktree with spaces" >/dev/null 2>&1 || true
        fi
        if ! rm -rf -- "${SANDBOX}"; then
            echo "ERROR: source-root selftest: could not remove sandbox ${SANDBOX}" >&2 || true
            if [ "${status}" -eq 0 ]; then status=2; fi
        fi
    fi
    exit "${status}"
}
trap 'exit 2' INT TERM HUP
trap cleanup EXIT

fixture_git() {
    env -i PATH="${PATH}" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null \
        git -c core.hooksPath=/dev/null -c commit.gpgSign=false \
        -c user.name="Wirelog test" -c user.email=wirelog-test@example.invalid "$@"
}

if ! SANDBOX="$(mktemp -d "${TMPDIR:-/tmp}/wirelog log abi root.XXXXXX")"; then
    echo "ERROR: source-root selftest: cannot create sandbox" >&2
    exit 2
fi
SANDBOX="$(CDPATH= cd -P -- "${SANDBOX}" && pwd -P)"
case "${SANDBOX}" in ""|"/") echo "ERROR: unsafe sandbox path" >&2; exit 2 ;; esac

ORDINARY="${SANDBOX}/ordinary checkout with spaces"
LINKED="${SANDBOX}/linked worktree with spaces"
OUTSIDE="${SANDBOX}/unrelated caller directory"
STRAY="${SANDBOX}/scripts without repository"
mkdir -p "${ORDINARY}/scripts/ci" "${ORDINARY}/wirelog/util" \
    "${ORDINARY}/wirelog/io" "${ORDINARY}/build dir" "${OUTSIDE}" \
    "${SANDBOX}/bin" "${SANDBOX}/tmp" "${STRAY}/scripts/ci"

cp "${REPO_ROOT}/scripts/ci/check-log-erasure.sh" "${ORDINARY}/scripts/ci/"
cp "${REPO_ROOT}/scripts/check_log_header_not_public.sh" "${ORDINARY}/scripts/"
cp "${REPO_ROOT}/scripts/ci/check-log-erasure.sh" "${STRAY}/scripts/ci/"
cp "${REPO_ROOT}/scripts/check_log_header_not_public.sh" "${STRAY}/scripts/"

PUBLIC_HEADERS=(
    wirelog/wirelog.h
    wirelog/wirelog-types.h
    wirelog/wirelog-parser.h
    wirelog/wirelog-ir.h
    wirelog/wirelog-optimizer.h
    wirelog/wirelog-export.h
    wirelog/wirelog-easy.h
    wirelog/wirelog-advanced.h
    wirelog/wirelog-extension.h
    wirelog/io/io_adapter.h
)
for header in "${PUBLIC_HEADERS[@]}"; do
    mkdir -p "${ORDINARY}/$(dirname -- "${header}")"
    : >"${ORDINARY}/${header}"
done

fixture_git -C "${ORDINARY}" init -q
fixture_git -C "${ORDINARY}" add scripts wirelog
fixture_git -C "${ORDINARY}" commit -qm "source-root fixture"
fixture_git -C "${ORDINARY}" worktree add --detach "${LINKED}" HEAD >/dev/null
mkdir -p "${LINKED}/build dir"

cat >"${SANDBOX}/bin/meson" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
setup)
    mkdir -p "$2"
    printf '%s\n' "$3" >>"$RECORD_SOURCE"
    printf '%s\n' "$PWD" >>"$RECORD_CWD"
    ;;
compile)
    if [ "${STUB_MODE:-ok}" = leak ]; then
        printf '%s\n' 'wl_log_erasure_sentinel_trace injected fixture' >"$3/libwirelog.so"
    else
        printf '%s\n' 'ordinary nonempty object evidence' >"$3/libwirelog.so"
    fi
    ;;
esac
STUB
chmod +x "${SANDBOX}/bin/meson"
export PATH="${SANDBOX}/bin:${PATH}"
export TMPDIR="${SANDBOX}/tmp"
export RECORD_SOURCE="${SANDBOX}/setup source roots"
export RECORD_CWD="${SANDBOX}/setup working directories"
: >"${RECORD_SOURCE}"
: >"${RECORD_CWD}"

fail() {
    echo "FAIL: $*" >&2
    failures=$((failures + 1))
}

run_from() {
    local cwd="$1" script="$2"
    (
        cd -- "${cwd}"
        env -i PATH="${PATH}" TMPDIR="${TMPDIR}" \
            RECORD_SOURCE="${RECORD_SOURCE}" RECORD_CWD="${RECORD_CWD}" \
            STUB_MODE="${STUB_MODE:-ok}" "${script}"
    )
}

check_erasure() {
    local layout="$1" expected_root="$2" cwd="$3" mode="$4"
    local script="${expected_root}/scripts/ci/check-log-erasure.sh"
    local rc source cwd_record
    : >"${RECORD_SOURCE}"
    : >"${RECORD_CWD}"
    if [ "${mode}" = leak ]; then
        if run_from "${cwd}" "${script}" >"${SANDBOX}/out" 2>"${SANDBOX}/err"; then
            rc=0
        else rc=$?; fi
        if [ "${rc}" -ne 1 ] || ! grep -Fq -- "LEAK: sentinel" "${SANDBOX}/err"; then
            fail "${layout} erasure sentinel did not produce exit 1 with LEAK"
        fi
    else
        if run_from "${cwd}" "${script}" >"${SANDBOX}/out" 2>"${SANDBOX}/err"; then
            rc=0
        else rc=$?; fi
        if [ "${rc}" -ne 0 ]; then
            fail "${layout} erasure control exited ${rc}"
            sed 's/^/    /' "${SANDBOX}/err" >&2
        fi
    fi
    source="$(tail -n 1 "${RECORD_SOURCE}")"
    cwd_record="$(tail -n 1 "${RECORD_CWD}")"
    if [ "${source}" != "${expected_root}" ]; then
        fail "${layout} erasure setup used source '${source}', expected '${expected_root}'"
    fi
    if [ "${cwd_record}" != "${cwd}" ]; then
        fail "${layout} erasure stub ran from '${cwd_record}', expected '${cwd}'"
    fi
}

check_header() {
    local layout="$1" expected_root="$2" cwd="$3" mode="$4"
    local script="${expected_root}/scripts/check_log_header_not_public.sh"
    local rc header="${expected_root}/wirelog/wirelog.h"
    if [ "${mode}" = leak ]; then
        printf '%s\n' '#include "wirelog/util/log.h"' >>"${header}"
        if run_from "${cwd}" "${script}" >"${SANDBOX}/out" 2>"${SANDBOX}/err"; then
            rc=0
        else rc=$?; fi
        if [ "${rc}" -ne 1 ] \
            || ! grep -Fq -- "LEAK: public header wirelog/wirelog.h" "${SANDBOX}/err"; then
            fail "${layout} header negative fixture did not produce a LEAK and exit 1"
        fi
        : >"${header}"
    else
        if run_from "${cwd}" "${script}" >"${SANDBOX}/out" 2>"${SANDBOX}/err"; then
            rc=0
        else rc=$?; fi
        if [ "${rc}" -ne 0 ]; then
            fail "${layout} clean header fixture exited ${rc}"
        fi
    fi
}

for layout in ordinary linked; do
    if [ "${layout}" = ordinary ]; then checkout="${ORDINARY}"; else checkout="${LINKED}"; fi
    in_tree_build="${checkout}/build dir"
    check_erasure "${layout}/external CWD" "${checkout}" "${OUTSIDE}" ok
    check_erasure "${layout}/in-tree build CWD" "${checkout}" "${in_tree_build}" ok
    check_header "${layout}/external CWD" "${checkout}" "${OUTSIDE}" clean
    check_header "${layout}/in-tree build CWD" "${checkout}" "${in_tree_build}" clean
    check_header "${layout}/external CWD" "${checkout}" "${OUTSIDE}" leak
    check_header "${layout}/in-tree build CWD" "${checkout}" "${in_tree_build}" leak
    export STUB_MODE=leak
    check_erasure "${layout}/external CWD" "${checkout}" "${OUTSIDE}" leak
    export STUB_MODE=ok
    echo "ok: ${layout} checkout source-root and rejection fixtures"
done

if run_from "${OUTSIDE}" "${STRAY}/scripts/ci/check-log-erasure.sh" \
    >"${SANDBOX}/out" 2>"${SANDBOX}/err"; then
    rc=0
else rc=$?; fi
if [ "${rc}" -ne 2 ] \
    || ! grep -Fq -- "could not resolve the source root" "${SANDBOX}/err"; then
    fail "erasure discovery outside a repository did not fail as setup error 2"
fi
if run_from "${OUTSIDE}" "${STRAY}/scripts/check_log_header_not_public.sh" \
    >"${SANDBOX}/out" 2>"${SANDBOX}/err"; then
    rc=0
else rc=$?; fi
if [ "${rc}" -eq 0 ] \
    || ! grep -Fq -- "could not resolve the source root" "${SANDBOX}/err"; then
    fail "header discovery outside a repository did not fail closed"
fi

if [ "${failures}" -ne 0 ]; then
    echo "test-log-abi-source-root: ${failures} case(s) failed" >&2
    exit 1
fi
echo "test-log-abi-source-root: OK"
