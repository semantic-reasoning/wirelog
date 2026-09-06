#!/usr/bin/env bash
# Static and negative fixtures for the GA downstream matrix contract.
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

normalize_root() {
    local supplied=$1
    case "$supplied" in
        [[:alpha:]]:[\\/]* )
            if ! command -v cygpath >/dev/null 2>&1; then
                echo "ERROR: cygpath is required to normalize Windows root: $supplied" >&2
                return 2
            fi
            if ! supplied=$(cygpath -u -- "$supplied"); then
                echo "ERROR: cygpath could not normalize Windows root: $supplied" >&2
                return 2
            fi
            ;;
    esac
    printf '%s\n' "$supplied"
}

root=${1:-$(cd "$script_dir/../.." && pwd)}
if [ "$#" -gt 0 ]; then
    root=$(normalize_root "$root")
fi
oracle="$root/scripts/release/downstream-matrix-oracles.tsv"
runner="$root/scripts/release/run-downstream-matrix.sh"
phase_runner="$root/scripts/release/run-downstream-phase.sh"
windows_phase_runner="$root/scripts/release/run-downstream-phase-windows.ps1"
archive_validator="$root/scripts/release/validate-downstream-tarball.sh"
isolation_writer="$root/scripts/release/write-isolation-evidence.sh"
download="$root/bench/data/doop/download.sh"

bash -n "$runner" "$phase_runner" "$archive_validator" "$isolation_writer" "$download"
"$runner" --help >/dev/null 2>&1
set +e
"$runner" >/dev/null 2>&1
missing_args_rc=$?
set -e
test "$missing_args_rc" = 2

count=$(awk -F '\t' '!/^#/ && NF { count++ } END { print count + 0 }' "$oracle")
test "$count" = 6
awk -F '\t' '!/^#/ && NF != 7 { bad++ } END { exit bad + 0 }' "$oracle"
expected='cspa-fast galen polonius ddisasm crdt doop'
actual=$(awk -F '\t' '!/^#/ && NF { print $1 }' "$oracle" | paste -sd ' ' -)
test "$actual" = "$expected"
grep -Fq '154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7' "$oracle"
grep -Fq 'MATRIX_REPEAT=1' "$runner"
grep -Fq -- '--repeat "$MATRIX_REPEAT"' "$runner"
grep -Fq -- '--skip-workload NAME' "$runner"
grep -Fq 'deferred-to-perf-nightly' "$runner"
grep -Fq 'skip_workload' "$runner"
grep -Fq 'cspa-fast) row_name=cspa' "$runner"
grep -Fq 'wanted="$row_name"' "$runner"
grep -Fq 'candidate-commit:' "$oracle"
grep -Fq 'tracked-in-tarball' "$oracle"
grep -Fq 'validate_provenance' "$runner"
grep -Fq 'validate_provenance_path' "$runner"
grep -Fq 'realpath -e' "$runner"
grep -Fq 'DOOP_ZXING_URL override is not allowed' "$runner"
grep -Fq 'validate-downstream-tarball.sh' "$runner"
grep -Fq -- '--quoting-style=escape' "$archive_validator"
grep -Fq 'archive contains symlink' "$archive_validator"
grep -Fq -- '--no-same-owner' "$runner"
grep -Fq 'run_phase' "$runner"
grep -Fq 'MATRIX_DOWNLOAD_TIMEOUT_SECONDS' "$runner"
grep -Fq 'MATRIX_EXTRACTION_TIMEOUT_SECONDS' "$runner"
grep -Fq 'MATRIX_CONFIGURE_TIMEOUT_SECONDS' "$runner"
grep -Fq 'MATRIX_BUILD_TIMEOUT_SECONDS' "$runner"
grep -Fq 'phases.tsv' "$runner"
grep -Fq 'archive-members.tsv' "$runner"
grep -Fq 'run-downstream-phase.sh' "$runner"
grep -Fq 'function Resolve-GitBash' "$windows_phase_runner"
grep -Fq 'GIT_INSTALL_ROOT' "$windows_phase_runner"
grep -Fq '[\\/]Git[\\/]bin[\\/]bash\.exe$' "$windows_phase_runner"
grep -Fq -- "-in @('bash', 'bash.exe')" "$windows_phase_runner"
test "$(grep -Fc '$nativeCommand = Resolve-GitBash' "$windows_phase_runner")" = 2
grep -Fq 'Get-Command $Command -CommandType Application' "$windows_phase_runner"
grep -Fq 'archive-validate' "$runner"
grep -Fq 'isolation-evidence.tsv' "$runner"
grep -Fq 'WIRELOG_GA_ISOLATION_CONTROL' "$runner"
grep -Fq 'WIRELOG_GA_ISOLATION_ASSERTION' "$runner"
grep -Fq 'WIRELOG_GA_ISOLATION_CONTROL' "$root/.github/workflows/release-tag.yml"
grep -Fq 'WIRELOG_GA_ISOLATION_CONTROL: github-hosted-runner' "$root/.github/workflows/release-tag.yml"
grep -Fq 'WIRELOG_GA_ISOLATION_ASSERTION: hosted-runner' "$root/.github/workflows/release-tag.yml"
grep -Fq 'declared_labels' "$isolation_writer"
grep -Fq 'assertion_result' "$isolation_writer"
grep -Fq 'tsv_escape' "$isolation_writer"

isolation_fixture=$(mktemp)
"$isolation_writer" "$isolation_fixture" \
    0123456789012345678901234567890123456789 'workflow name' 'run id' \
    '2026-08-30T00:00:00Z' 'runner name' 'wirelog-ga' $'control\tvalue\nnext' \
    dedicated-runner-group 'repository-variable:test'
awk -F '\t' 'NR == 1 { if (NF != 14) bad = 1; next } { if (NF != 14) bad = 1; rows++ } END { if (rows != 1) bad = 1; exit bad + 0 }' "$isolation_fixture" >/dev/null
grep -Fq 'control\tvalue\nnext' "$isolation_fixture"
grep -Fq $'\tPASS\t' "$isolation_fixture"
printf 'bad\n' > "$isolation_fixture"
set +e
awk -F '\t' 'NR == 1 { if (NF != 14) bad = 1; next } { if (NF != 14) bad = 1; rows++ } END { if (rows != 1) bad = 1; exit bad + 0 }' "$isolation_fixture" >/dev/null
malformed_isolation_rc=$?
set -e
test "$malformed_isolation_rc" -ne 0
rm -f "$isolation_fixture"
grep -Fq 'flowlog_benchmark/resolve/da9e91b3ff75d94604f57ba2b21ef3aa97e241ec/' "$download"

negative_fixture=$(mktemp)
negative_log=$(mktemp)
malicious_root=$(mktemp -d)
malicious_archive=$(mktemp "${TMPDIR:-/tmp}/wirelog-malicious.XXXXXX")
malicious_output=$(mktemp -d)
malicious_log=$(mktemp)
traversal_archive=$(mktemp "${TMPDIR:-/tmp}/wirelog-traversal.XXXXXX")
traversal_output=$(mktemp -d)
traversal_log=$(mktemp)
trap 'rm -f "$negative_fixture" "$negative_log" "$malicious_archive" "$malicious_archive.sha256" "$malicious_log" "$traversal_archive" "$traversal_log"; rm -rf "$malicious_root" "$malicious_output" "$traversal_output"' EXIT
printf 'deliberately invalid checksum fixture\n' > "$negative_fixture"
negative_url="file://${negative_fixture}"
if command -v cygpath >/dev/null 2>&1; then
    negative_path=$(cygpath -m -- "$negative_fixture")
    negative_url="file:///${negative_path#/}"
fi
set +e
DOOP_ZXING_URL="$negative_url" \
DOOP_ZXING_SHA256=0000000000000000000000000000000000000000000000000000000000000000 \
    "$download" >"$negative_log" 2>&1
mismatch_rc=$?
set -e
test "$mismatch_rc" -ne 0
grep -Fq 'checksum mismatch' "$negative_log"

printf 'candidate\n' > "$malicious_root/meson.build"
python3 -c '
import sys, tarfile, io
archive = sys.argv[1]
with tarfile.open(archive, "w:gz") as t:
    ti = tarfile.TarInfo(name="meson.build")
    ti.size = 10
    t.addfile(ti, io.BytesIO(b"candidate\n"))
    ti2 = tarfile.TarInfo(name="escape-link")
    ti2.type = tarfile.SYMTYPE
    ti2.linkname = "meson.build"
    t.addfile(ti2)
' "$malicious_archive" 2>/dev/null || python -c '
import sys, tarfile, io
archive = sys.argv[1]
with tarfile.open(archive, "w:gz") as t:
    ti = tarfile.TarInfo(name="meson.build")
    ti.size = 10
    t.addfile(ti, io.BytesIO(b"candidate\n"))
    ti2 = tarfile.TarInfo(name="escape-link")
    ti2.type = tarfile.SYMTYPE
    ti2.linkname = "meson.build"
    t.addfile(ti2)
' "$malicious_archive" 2>/dev/null || {
    ln -s meson.build "$malicious_root/escape-link"
    tar -czf "$malicious_archive" -C "$malicious_root" meson.build escape-link
}
sha256sum "$malicious_archive" > "$malicious_archive.sha256"
set +e
"$runner" --tarball "$malicious_archive" \
    --candidate-sha 0000000000000000000000000000000000000000 \
    --oracle-sha256 0000000000000000000000000000000000000000000000000000000000000000 \
    --output-dir "$malicious_output" >"$malicious_log" 2>&1
malicious_rc=$?
set -e
test "$malicious_rc" -ne 0
grep -Fq 'archive contains symlink' "$malicious_output/archive-validate.log"

tar --transform='s#meson.build#../escape#' -czf "$traversal_archive" -C "$malicious_root" meson.build
set +e
"$runner" --tarball "$traversal_archive" \
    --candidate-sha 0000000000000000000000000000000000000000 \
    --oracle-sha256 0000000000000000000000000000000000000000000000000000000000000000 \
    --output-dir "$traversal_output" >"$traversal_log" 2>&1
traversal_rc=$?
set -e
test "$traversal_rc" -ne 0
grep -Fq 'unsafe archive member path' "$traversal_output/archive-validate.log"

phase_root=$(mktemp -d)
phase_tsv="$phase_root/phases.tsv"
printf 'phase\tstatus\ttimeout_seconds\treturn_code\tlog\n' > "$phase_tsv"
set +e
"$phase_runner" fail 10 "$phase_root/fail.log" "$phase_tsv" -- bash -c 'printf failure >&2; exit 7'
phase_fail_rc=$?
"$phase_runner" timeout 1 "$phase_root/timeout.log" "$phase_tsv" -- bash -c 'sleep 2'
phase_timeout_rc=$?
"$phase_runner" args 10 "$phase_root/args.log" "$phase_tsv" -- bash -c 'test "$1" = "a b"' _ 'a b'
phase_args_rc=$?
descendant_pid_file="$phase_root/descendant.pid"
"$phase_runner" descendant 1 "$phase_root/descendant.log" "$phase_tsv" -- bash -c 'sleep 30 & echo $! > "$1"; wait' _ "$descendant_pid_file"
phase_descendant_rc=$?
detached_pid_file="$phase_root/detached.pid"
"$phase_runner" detached 10 "$phase_root/detached.log" "$phase_tsv" -- bash -c 'sleep 30 & echo $! > "$1"; exit 0' _ "$detached_pid_file"
phase_detached_rc=$?
"$phase_runner" invalid 0 "$phase_root/invalid.log" "$phase_tsv" -- true
phase_invalid_rc=$?
set -e
test "$phase_fail_rc" = 7
test "$phase_timeout_rc" = 124
test "$phase_invalid_rc" = 2
grep -Fq $'fail\tFAIL\t10\t7\tfail.log' "$phase_tsv"
grep -Fq $'timeout\tTIMEOUT\t1\t124\ttimeout.log' "$phase_tsv"
grep -Fq $'args\tPASS\t10\t0\targs.log' "$phase_tsv"
test "$phase_args_rc" = 0
test "$phase_descendant_rc" = 124
descendant_pid=$(cat "$descendant_pid_file")
! kill -0 "$descendant_pid" 2>/dev/null
test "$phase_detached_rc" = 0
detached_pid=$(cat "$detached_pid_file")
! kill -0 "$detached_pid" 2>/dev/null
grep -Fq 'failure' "$phase_root/fail.log"
rm -rf "$phase_root"

echo 'downstream matrix contract fixtures: OK'
