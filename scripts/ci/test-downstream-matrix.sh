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
download="$root/bench/data/doop/download.sh"

bash -n "$runner" "$download"
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
grep -Fq 'candidate-commit:' "$oracle"
grep -Fq 'tracked-in-tarball' "$oracle"
grep -Fq 'validate_provenance' "$runner"
grep -Fq 'validate_provenance_path' "$runner"
grep -Fq 'realpath -e' "$runner"
grep -Fq 'DOOP_ZXING_URL override is not allowed' "$runner"
grep -Fq 'validate_tarball' "$runner"
grep -Fq -- '--null -tzf' "$runner"
grep -Fq 'archive contains symlink' "$runner"
grep -Fq -- '--no-same-owner' "$runner"
grep -Fq 'flowlog_benchmark/resolve/da9e91b3ff75d94604f57ba2b21ef3aa97e241ec/' "$download"

negative_fixture=$(mktemp)
negative_log=$(mktemp)
malicious_root=$(mktemp -d)
malicious_archive=$(mktemp --suffix=.tar.gz)
malicious_output=$(mktemp -d)
malicious_log=$(mktemp)
trap 'rm -f "$negative_fixture" "$negative_log" "$malicious_archive" "$malicious_log"; rm -rf "$malicious_root" "$malicious_output"' EXIT
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
ln -s /etc/passwd "$malicious_root/escape-link"
tar -czf "$malicious_archive" -C "$malicious_root" meson.build escape-link
sha256sum "$malicious_archive" > "$malicious_archive.sha256"
set +e
"$runner" --tarball "$malicious_archive" \
    --candidate-sha 0000000000000000000000000000000000000000 \
    --oracle-sha256 0000000000000000000000000000000000000000000000000000000000000000 \
    --output-dir "$malicious_output" >"$malicious_log" 2>&1
malicious_rc=$?
set -e
test "$malicious_rc" -ne 0
grep -Fq 'archive contains symlink' "$malicious_log"

echo 'downstream matrix contract fixtures: OK'
