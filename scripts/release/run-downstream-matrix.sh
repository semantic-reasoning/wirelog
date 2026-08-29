#!/usr/bin/env bash
# Build and execute the six-workload GA matrix from an exact candidate tarball.
# Issue #1163 / #1156.
set -euo pipefail

usage() {
    cat >&2 <<'EOF'
usage: run-downstream-matrix.sh --tarball ARCHIVE --candidate-sha SHA \
    --oracle-sha256 SHA --output-dir DIR

The archive must be a deterministic wirelog source tarball.  The script
extracts it into an isolated directory, verifies the candidate SHA and every
tracked dataset manifest, downloads the pinned DOOP archive, builds the
candidate, and fails on any missing workload, non-zero exit, or oracle drift.
EOF
}

tarball=""
candidate_sha=""
oracle_sha256=""
output_dir=""
MATRIX_WORKERS=1
MATRIX_REPEAT=1
MATRIX_DOWNLOAD_TIMEOUT_SECONDS=${MATRIX_DOWNLOAD_TIMEOUT_SECONDS:-1800}
MATRIX_EXTRACTION_TIMEOUT_SECONDS=${MATRIX_EXTRACTION_TIMEOUT_SECONDS:-1800}
MATRIX_CONFIGURE_TIMEOUT_SECONDS=${MATRIX_CONFIGURE_TIMEOUT_SECONDS:-120}
MATRIX_BUILD_TIMEOUT_SECONDS=${MATRIX_BUILD_TIMEOUT_SECONDS:-600}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
run_started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --tarball) tarball=${2:-}; shift 2 ;;
        --candidate-sha) candidate_sha=${2:-}; shift 2 ;;
        --oracle-sha256) oracle_sha256=${2:-}; shift 2 ;;
        --output-dir) output_dir=${2:-}; shift 2 ;;
        *) usage; exit 2 ;;
    esac
done
[[ -n "$tarball" && -f "$tarball" && "$candidate_sha" =~ ^[0-9a-f]{40}$ \
    && "$oracle_sha256" =~ ^[0-9a-f]{64}$ && -n "$output_dir" ]] || { usage; exit 2; }
for phase_timeout in "$MATRIX_DOWNLOAD_TIMEOUT_SECONDS" "$MATRIX_EXTRACTION_TIMEOUT_SECONDS" \
    "$MATRIX_CONFIGURE_TIMEOUT_SECONDS" "$MATRIX_BUILD_TIMEOUT_SECONDS"; do
    [[ "$phase_timeout" =~ ^[1-9][0-9]*$ ]] || {
        echo "phase timeout must be a positive integer: $phase_timeout" >&2
        exit 2
    }
done
command -v sha256sum >/dev/null || { echo 'sha256sum is required' >&2; exit 2; }
command -v meson >/dev/null || { echo 'meson is required' >&2; exit 2; }
command -v ninja >/dev/null || { echo 'ninja is required' >&2; exit 2; }
command -v tar >/dev/null || { echo 'tar is required' >&2; exit 2; }
command -v timeout >/dev/null || { echo 'timeout is required' >&2; exit 2; }
command -v gzip >/dev/null || { echo 'gzip is required' >&2; exit 2; }
command -v git >/dev/null || { echo 'git is required' >&2; exit 2; }
command -v gcc >/dev/null || { echo 'gcc is required' >&2; exit 2; }
command -v curl >/dev/null || { echo 'curl is required' >&2; exit 2; }
command -v unzip >/dev/null || { echo 'unzip is required' >&2; exit 2; }
command -v realpath >/dev/null || { echo 'realpath is required' >&2; exit 2; }

mkdir -p "$output_dir"
printf 'phase\tstatus\ttimeout_seconds\treturn_code\tlog\n' > "$output_dir/phases.tsv"
root=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-downstream.XXXXXX")
run_status=FAILED
finish() {
    printf 'status=%s\ncompleted_at=%s\n' "$run_status" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        > "$output_dir/run-status.txt"
    rm -rf "$root"
}
trap finish EXIT

run_phase() {
    local phase=$1 timeout_seconds=$2 log=$3
    shift 3
    "$script_dir/run-downstream-phase.sh" "$phase" "$timeout_seconds" "$log" \
        "$output_dir/phases.tsv" -- "$@"
}

run_phase archive-validate "$MATRIX_EXTRACTION_TIMEOUT_SECONDS" "$output_dir/archive-validate.log" \
    "$script_dir/validate-downstream-tarball.sh" "$tarball" "$output_dir/archive-members.tsv"
run_phase archive-extract "$MATRIX_EXTRACTION_TIMEOUT_SECONDS" "$output_dir/archive-extract.log" \
    tar --no-same-owner --no-same-permissions -xzf "$tarball" -C "$root"
candidate_root=$(find "$root" -mindepth 1 -maxdepth 1 -type d -print -quit)
[[ -n "$candidate_root" && -f "$candidate_root/meson.build" ]] || {
    echo 'candidate archive does not contain one top-level wirelog tree' >&2
    exit 1
}
archive_sha256=$(sha256sum "$tarball" | awk '{print $1}')
checksum_file="${tarball}.sha256"
[[ -s "$checksum_file" ]] || { echo "missing archive checksum: $checksum_file" >&2; exit 1; }
expected_archive_sha256=$(awk 'NF { print $1; exit }' "$checksum_file")
[[ "$expected_archive_sha256" == "$archive_sha256" ]] || {
    echo "archive checksum $archive_sha256 != sidecar $expected_archive_sha256" >&2
    exit 1
}
archive_commit=$(gzip -dc "$tarball" | git get-tar-commit-id)
[[ "$archive_commit" == "$candidate_sha" ]] || {
    echo "archive commit $archive_commit != candidate $candidate_sha" >&2
    exit 1
}
metadata="$output_dir/host-metadata.txt"
{
    echo "candidate_sha=$candidate_sha"
    echo "tarball_sha256=$archive_sha256"
    echo "hostname=$(hostname -f 2>/dev/null || hostname)"
    echo "kernel=$(uname -srv)"
    echo "machine=$(uname -m)"
    echo "distro=$(awk -F= '$1 == \"PRETTY_NAME\" {gsub(/^\"|\"$/, \"\", $2); print $2}' /etc/os-release 2>/dev/null || echo unknown)"
    echo "cpu_count=$(getconf _NPROCESSORS_ONLN)"
    echo "disk_available_bytes=$(df -PB1 "$output_dir" | awk 'NR == 2 {print $4}')"
    governor=/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
    echo "cpu_governor=$(cat "$governor" 2>/dev/null || echo unavailable)"
    echo "runner_name=${RUNNER_NAME:-unknown}"
    echo "runner_group=${RUNNER_GROUP:-unknown}"
    echo "run_id=${GITHUB_RUN_ID:-local}"
    echo "memory_bytes=$(awk '/MemTotal:/ {print $2 * 1024}' /proc/meminfo)"
    echo "meson_version=$(meson --version)"
    echo "ninja_version=$(ninja --version)"
    echo "gcc_version=$(gcc --version | head -1)"
    echo "started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$metadata"
printf 'workload\texpected_manifest\tactual_manifest\n' > "$output_dir/data-manifests.tsv"
printf 'workload\tprovenance_id\tacquisition_command\tresolved_provenance\n' > "$output_dir/data-provenance.tsv"
"$script_dir/write-isolation-evidence.sh" "$output_dir/isolation-evidence.tsv" \
    "$candidate_sha" "${GITHUB_WORKFLOW:-local}" "${GITHUB_RUN_ID:-local}" \
    "$run_started_at" "${RUNNER_NAME:-unknown}" "${RUNNER_GROUP:-unknown}" \
    "${WIRELOG_GA_ISOLATION_CONTROL:-unverified}" \
    "${WIRELOG_GA_ISOLATION_ASSERTION:-unverified}" \
    "repository-variable:WIRELOG_GA_ISOLATION_ASSERTION"

oracle_file="$candidate_root/scripts/release/downstream-matrix-oracles.tsv"
[[ -s "$oracle_file" ]] || { echo "missing oracle file: $oracle_file" >&2; exit 1; }
actual_oracle_sha256=$(sha256sum "$oracle_file" | awk '{print $1}')
[[ "$actual_oracle_sha256" == "$oracle_sha256" ]] || {
    echo "oracle checksum $actual_oracle_sha256 != expected $oracle_sha256" >&2
    exit 1
}

manifest_for_dir() {
    local dir=$1
    find "$dir" -type f -print0 | sort -z \
        | while IFS= read -r -d '' path; do
            sha256sum "$path" | sed "s#  $dir/#  #"
        done | sha256sum | awk '{print $1}'
}

manifest_for_doop() {
    local dir=$1
    find "$dir" -maxdepth 1 -type f -name '*.facts' -print0 | sort -z \
        | while IFS= read -r -d '' path; do
            sha256sum "$path" | sed "s#  $dir/#  #"
        done | sha256sum | awk '{print $1}'
}

validate_data_path() {
    local path=$1
    case "$path" in
        ''|/*|.|..|../*|*/../*|*/..)
            echo "invalid data path: $path" >&2
            return 1
            ;;
    esac
    [[ "$path" != *$'\n'* && "$path" != *$'\r'* ]] || {
        echo 'data path contains a newline' >&2
        return 1
    }
}

validate_provenance() {
    local provenance=$1
    [[ "$provenance" =~ ^(candidate-commit:[A-Za-z0-9._/-]+|huggingface:[A-Za-z0-9._/-]+@[0-9a-f]{40}#[A-Za-z0-9._/-]+)$ ]] || {
        echo "invalid immutable provenance id: $provenance" >&2
        return 1
    }
}

validate_provenance_path() {
    local provenance=$1 data_path=$2
    case "$provenance" in
        candidate-commit:*)
            [[ "${provenance#candidate-commit:}" == "$data_path" ]] || {
                echo "candidate provenance path does not match data path: $provenance" >&2
                return 1
            }
            ;;
        huggingface:*)
            [[ "$data_path" == bench/data/doop && "$1" == huggingface:* ]] || {
                echo "remote provenance is only valid for DOOP" >&2
                return 1
            }
            ;;
    esac
}

validate_acquisition() {
    case "$1" in
        tracked-in-tarball|download-doop-zxing) ;;
        *) echo "unknown acquisition command id: $1" >&2; return 1 ;;
    esac
}

declare -A seen_workloads=()
workload_count=0
while IFS=$'\t' read -r workload expected_tuples expected_iterations data_path expected_manifest provenance acquisition extra; do
    [[ -z "$workload" || "$workload" == \#* ]] && continue
    [[ -z "$extra" && -n "$provenance" && -n "$acquisition" ]] || {
        echo "oracle row must contain exactly seven fields: $workload" >&2
        exit 1
    }
    [[ "$expected_tuples" =~ ^[0-9]+$ && "$expected_iterations" =~ ^[0-9]+$ ]] || {
        echo "invalid numeric oracle for $workload" >&2
        exit 1
    }
    [[ "$expected_manifest" =~ ^[0-9a-f]{64}$ || "$expected_manifest" =~ ^archive:[0-9a-f]{64}\;files:[0-9a-f]{64}$ ]] || {
        echo "invalid manifest oracle for $workload" >&2
        exit 1
    }
    validate_data_path "$data_path"
    validate_provenance "$provenance"
    validate_provenance_path "$provenance" "$data_path"
    validate_acquisition "$acquisition"
    [[ -z "${seen_workloads[$workload]+x}" ]] || { echo "duplicate workload: $workload" >&2; exit 1; }
    seen_workloads[$workload]=1
    workload_count=$((workload_count + 1))
    data_dir=$(realpath -e -- "$candidate_root/$data_path") || {
        echo "missing data path: $data_path" >&2
        exit 1
    }
    candidate_root_real=$(realpath -e -- "$candidate_root")
    [[ "$data_dir" == "$candidate_root_real"/* ]] || {
        echo "data path escapes candidate tree: $data_path" >&2
        exit 1
    }
    if [[ "$workload" == doop ]]; then
        [[ "$acquisition" == download-doop-zxing ]] || { echo 'DOOP must use download-doop-zxing' >&2; exit 1; }
        [[ -z "${DOOP_ZXING_URL:-}" ]] || { echo 'DOOP_ZXING_URL override is not allowed in GA matrix' >&2; exit 1; }
    else
        [[ "$acquisition" == tracked-in-tarball ]] || { echo "$workload must use tracked-in-tarball" >&2; exit 1; }
    fi
    resolved_provenance=${provenance/candidate-commit:/candidate-commit:$candidate_sha:}
    printf '%s\t%s\t%s\t%s\n' "$workload" "$provenance" "$acquisition" "$resolved_provenance" >> "$output_dir/data-provenance.tsv"
    if [[ "$expected_manifest" == archive:* ]]; then
        archive_sha=${expected_manifest#archive:}
        archive_sha=${archive_sha%%;files:*}
        expected_files=${expected_manifest#*;files:}
        run_phase doop-acquisition "$MATRIX_DOWNLOAD_TIMEOUT_SECONDS" "$output_dir/doop-download.log" \
            env DOOP_ZXING_SHA256="$archive_sha" "$candidate_root/bench/data/doop/download.sh"
        actual_manifest=$(manifest_for_doop "$data_dir")
        [[ "$actual_manifest" == "$expected_files" ]] || {
            echo "doop data manifest $actual_manifest != $expected_files" >&2
            exit 1
        }
        printf '%s\t%s\t%s\n' "$workload" "$expected_manifest" "archive:$archive_sha;files:$actual_manifest" >> "$output_dir/data-manifests.tsv"
    else
        [[ -d "$data_dir" ]] || { echo "missing data: $data_dir" >&2; exit 1; }
        actual_manifest=$(manifest_for_dir "$data_dir")
        printf '%s\t%s\t%s\n' "$workload" "$expected_manifest" "$actual_manifest" >> "$output_dir/data-manifests.tsv"
        [[ "$actual_manifest" == "$expected_manifest" ]] || {
            echo "$workload data manifest $actual_manifest != $expected_manifest" >&2
            exit 1
        }
    fi
done < "$oracle_file"
[[ "$workload_count" -eq 6 ]] || {
    echo "oracle file contains $workload_count workloads; expected 6" >&2
    exit 1
}

build_dir="$root/build"
run_phase configure "$MATRIX_CONFIGURE_TIMEOUT_SECONDS" "$output_dir/configure.log" \
    meson setup "$build_dir" "$candidate_root" --buildtype=release -Dtests=false \
    -DmbedTLS=disabled
run_phase build "$MATRIX_BUILD_TIMEOUT_SECONDS" "$output_dir/build.log" \
    meson compile -C "$build_dir" bench_flowlog
bench="$build_dir/bench/bench_flowlog"
[[ -x "$bench" ]] || { echo "missing benchmark binary: $bench" >&2; exit 1; }

printf 'workload\tstatus\tworkers\trepeat\ttimeout_seconds\treturn_code\ttuples\titerations\texpected_tuples\texpected_iterations\tmedian_ms\tlog\n' \
    > "$output_dir/results.tsv"

run_workload() {
    local workload=$1 data_option=$2 data_dir=$3 expected_tuples=$4 expected_iterations=$5
    local log="$output_dir/$workload.log" row tuples iterations status median timeout_seconds
    if [[ "$workload" == doop ]]; then timeout_seconds=2400; else timeout_seconds=600; fi
    if timeout --signal=TERM --kill-after=30s "${timeout_seconds}s" \
        "$bench" --workload "$workload" "$data_option" "$data_dir" \
        --workers "$MATRIX_WORKERS" --repeat "$MATRIX_REPEAT" >"$log" 2>&1; then
        :
    else
        return_code=$?
        status=FAIL
        if [[ "$return_code" == 124 || "$return_code" == 137 ]]; then status=TIMEOUT; fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t\t\t%s\t%s\t\t%s\n' "$workload" "$status" "$MATRIX_WORKERS" "$MATRIX_REPEAT" "$timeout_seconds" "$return_code" "$expected_tuples" "$expected_iterations" "$(basename "$log")" >> "$output_dir/results.tsv"
        echo "$workload failed; see $log" >&2
        return 1
    fi
    row_count=$(awk -F '\t' -v wanted="$workload" '$1 == wanted { count++ } END { print count + 0 }' "$log")
    [[ "$row_count" == 1 ]] || {
        echo "$workload produced $row_count result rows; expected exactly one" >&2
        return 1
    }
    row=$(awk -F '\t' -v wanted="$workload" '$1 == wanted { line=$0 } END { print line }' "$log")
    median=$(awk -F '\t' '{print $7}' <<<"$row")
    tuples=$(awk -F '\t' '{print $10}' <<<"$row")
    iterations=$(awk -F '\t' '{print $11}' <<<"$row")
    status=$(awk -F '\t' '{print $12}' <<<"$row")
    if [[ "$status" != OK || "$tuples" != "$expected_tuples" || "$iterations" != "$expected_iterations" ]]; then
        printf '%s\tFAIL\t%s\t%s\t%s\t0\t%s\t%s\t%s\t%s\t%s\t%s\n' "$workload" "$MATRIX_WORKERS" "$MATRIX_REPEAT" "$timeout_seconds" "$tuples" "$iterations" "$expected_tuples" "$expected_iterations" "$median" "$(basename "$log")" >> "$output_dir/results.tsv"
        echo "$workload oracle mismatch: status=$status tuples=$tuples iterations=$iterations" >&2
        return 1
    fi
    printf '%s\tPASS\t%s\t%s\t%s\t0\t%s\t%s\t%s\t%s\t%s\t%s\n' "$workload" "$MATRIX_WORKERS" "$MATRIX_REPEAT" "$timeout_seconds" "$tuples" "$iterations" "$expected_tuples" "$expected_iterations" "$median" "$(basename "$log")" >> "$output_dir/results.tsv"
}

while IFS=$'\t' read -r workload expected_tuples expected_iterations data_path _ provenance acquisition extra; do
    [[ -z "$workload" || "$workload" == \#* ]] && continue
    [[ -z "$extra" && -n "$provenance" && -n "$acquisition" ]] || { echo "invalid oracle row for $workload" >&2; exit 1; }
    case "$workload" in
        cspa-fast) run_workload "$workload" --data-cspa "$candidate_root/$data_path" "$expected_tuples" "$expected_iterations" ;;
        galen) run_workload "$workload" --data-galen "$candidate_root/$data_path" "$expected_tuples" "$expected_iterations" ;;
        polonius) run_workload "$workload" --data-polonius "$candidate_root/$data_path" "$expected_tuples" "$expected_iterations" ;;
        ddisasm) run_workload "$workload" --data-ddisasm "$candidate_root/$data_path" "$expected_tuples" "$expected_iterations" ;;
        crdt) run_workload "$workload" --data-crdt "$candidate_root/$data_path" "$expected_tuples" "$expected_iterations" ;;
        doop) run_workload "$workload" --data-doop "$candidate_root/$data_path" "$expected_tuples" "$expected_iterations" ;;
        *) echo "unknown workload in oracle file: $workload" >&2; exit 1 ;;
    esac
done < "$oracle_file"

run_status=PASS
echo "downstream matrix passed: candidate=$candidate_sha"
