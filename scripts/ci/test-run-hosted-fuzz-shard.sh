#!/usr/bin/env bash
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
# For commercial licenses, contact: inquiry@cleverplant.com
#
# Deterministic self-test for scripts/fuzz/run-hosted-fuzz-shard.sh.

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd -P)
helper=$repo_root/scripts/fuzz/run-hosted-fuzz-shard.sh
tmp=$(mktemp -d)
cleanup() {
    chmod -R u+w "$tmp" 2>/dev/null || :
    rm -rf "$tmp"
}
trap cleanup EXIT

build=$tmp/build
input=$tmp/input
evidence=$tmp/evidence
mkdir -p "$build/tests" "$input/nested"
printf '%s\n' seed >"$input/seed"
printf '%s\n' nested >"$input/nested/seed"

fake=$build/tests/parser_fuzz
cat >"$fake" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
corpus=$1
shift
artifact_prefix=
for arg in "$@"; do
    case "$arg" in
        -artifact_prefix=*)
            artifact_prefix=${arg#-artifact_prefix=}
            ;;
    esac
done
mode=${WIRELOG_FAKE_FUZZ_MODE:-success}
case "$mode" in
    success)
        [ -f "$corpus/seed" ]
        printf '%s\n' generated >"$corpus/generated"
        ;;
    crash)
        [ -n "$artifact_prefix" ]
        mkdir -p "$artifact_prefix"
        printf '%s\n' crash >"${artifact_prefix}crash-fixture"
        ;;
    fail)
        printf '%s\n' failing target >&2
        exit 7
        ;;
    timeout)
        sleep 5
        ;;
    *)
        printf 'unknown fake mode: %s\n' "$mode" >&2
        exit 9
        ;;
esac
EOF
chmod +x "$fake"

sha256() {
    python3 - "$1" <<'PY'
import hashlib
import sys
print(hashlib.sha256(sys.argv[1].encode("utf-8")).hexdigest())
PY
}

build_sha=$(sha256 build-fixture)
artifact_sha=$(sha256 artifact-fixture)
git_commit=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

expect_status() {
    expected=$1
    shift
    set +e
    "$@" >"$tmp/out" 2>"$tmp/err"
    actual=$?
    set -e
    if [ "$actual" -ne "$expected" ]; then
        cat "$tmp/out" "$tmp/err" >&2
        printf 'expected status %s, got %s\n' "$expected" "$actual" >&2
        exit 1
    fi
}

run_helper() {
    root=$1
    shard=$2
    artifact=$3
    corpus=${4:-$input}
    shard_count=${5:-4}
    record_output=${6:-}

    args=(
        --build-dir "$build"
        --target parser
        --input-corpus "$corpus"
        --output-root "$root"
        --duration 1s
        --campaign-id campaign-fixture
        --shard-index "$shard"
        --shard-count "$shard_count"
        --git-commit "$git_commit"
        --run-id 123456
        --run-attempt 1
        --build-id build-fixture
        --build-sha256 "$build_sha"
        --artifact-id "$artifact"
        --artifact-sha256 "$artifact_sha"
        --timeout-grace 1
    )
    if [ -n "$record_output" ]; then
        args+=(--record-output "$record_output")
    fi
    "$helper" \
        "${args[@]}"
}

expect_run_helper_status() {
    expected=$1
    mode=$2
    root=$3
    shard=$4
    artifact=$5
    shift 5

    set +e
    WIRELOG_FAKE_FUZZ_MODE=$mode run_helper "$root" "$shard" "$artifact" "$@" \
        >"$tmp/out" 2>"$tmp/err"
    actual=$?
    set -e
    if [ "$actual" -ne "$expected" ]; then
        cat "$tmp/out" "$tmp/err" >&2
        printf 'expected status %s, got %s\n' "$expected" "$actual" >&2
        exit 1
    fi
}

expect_status 2 "$helper" --build-dir "$build" --duration 0
grep -Fq 'the following arguments are required' "$tmp/err"

expect_run_helper_status 2 success "$input" 1 9010
grep -Fq 'input corpus and shard output corpus must not overlap' "$tmp/err"

overlap_root=$tmp/input-in-output-root
overlap_input=$overlap_root/corpora/campaign-fixture/parser/shard-1/output/current-input
mkdir -p "$overlap_input"
printf '%s\n' seed >"$overlap_input/seed"
expect_run_helper_status 2 success "$overlap_root" 1 9011 "$overlap_input"
grep -Fq 'input corpus and shard output corpus must not overlap' "$tmp/err"

expect_run_helper_status 2 success "$evidence/record-absolute-escape" 1 9012 \
    "$input" 4 "$tmp/escaped-record.json"
grep -Fq -- '--record-output must stay within --output-root' "$tmp/err"

expect_run_helper_status 2 success "$evidence/record-relative-escape" 1 9013 \
    "$input" 4 ../escaped-record.json
grep -Fq -- '--record-output must stay within --output-root' "$tmp/err"

success_root=$evidence/success
run_helper "$success_root" 1 9001 "$input" 4 records/success.json \
    >"$tmp/success.json"
python3 - "$repo_root" "$success_root" "$tmp/success.json" \
    "$success_root/records/success.json" <<'PY'
import importlib.util
import json
import os
import stat
import sys
from pathlib import Path

repo = Path(sys.argv[1])
root = Path(sys.argv[2]).resolve()
record = json.loads(Path(sys.argv[3]).read_text())
record_from_file = json.loads(Path(sys.argv[4]).read_text())
verifier_path = repo / "scripts" / "fuzz" / "verify-hosted-campaign.py"
spec = importlib.util.spec_from_file_location("verifier", verifier_path)
verifier = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(verifier)

assert record_from_file == record
assert verifier.validate_record(record, 1) == []
report = verifier.verify([record], root)
codes = {item["code"] for item in report["errors"]}
assert "invalid_corpus_evidence" not in codes, report
assert "corpus_hash_mismatch" not in codes, report
assert "corpus_digest_mismatch" not in codes, report

assert record["schema_version"] == 2
assert record["workflow_conclusion"] == "success"
assert record["job_conclusion"] == "success"
assert record["termination_reason"] == "completed"
assert record["exit_status"] == 0
assert record["crash_artifact_count"] == 0
assert not record["input_corpus_path"].startswith("/")
assert not record["output_corpus_path"].startswith("/")

input_snapshot = root / record["input_corpus_path"]
output_corpus = root / record["output_corpus_path"]
assert (input_snapshot / "seed").read_text() == "seed\n"
assert (output_corpus / "generated").read_text() == "generated\n"
assert not (input_snapshot.stat().st_mode & stat.S_IWUSR)
assert record["input_corpus_sha256"] == verifier.canonical_corpus_sha256(
    input_snapshot)
assert record["output_corpus_sha256"] == verifier.canonical_corpus_sha256(
    output_corpus)
assert record["input_corpus_sha256"] != record["output_corpus_sha256"]
assert root in input_snapshot.resolve().parents
assert root in output_corpus.resolve().parents
PY

handoff_root=$evidence/handoff
run_helper "$handoff_root" 1 9101 "$input" 2 >"$tmp/handoff-1.json"
handoff_first_output=$handoff_root/corpora/campaign-fixture/parser/shard-1/output
run_helper "$handoff_root" 2 9102 "$handoff_first_output" 2 \
    >"$tmp/handoff-2.json"
python3 - "$repo_root" "$handoff_root" "$tmp/handoff-1.json" \
    "$tmp/handoff-2.json" <<'PY'
import importlib.util
import json
import sys
from pathlib import Path

repo = Path(sys.argv[1])
root = Path(sys.argv[2]).resolve()
records = [json.loads(Path(path).read_text()) for path in sys.argv[3:]]
verifier_path = repo / "scripts" / "fuzz" / "verify-hosted-campaign.py"
spec = importlib.util.spec_from_file_location("verifier", verifier_path)
verifier = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(verifier)

first_output = root / records[0]["output_corpus_path"]
second_input = root / records[1]["input_corpus_path"]
assert records[0]["output_corpus_sha256"] == records[1]["input_corpus_sha256"]
assert verifier.canonical_corpus_sha256(first_output) == \
    verifier.canonical_corpus_sha256(second_input)
assert (second_input / "generated").read_text() == "generated\n"
assert all(verifier.validate_record(record, index) == []
           for index, record in enumerate(records, start=1))
report = verifier.verify(records, root)
codes = {item["code"] for item in report["errors"]}
assert "invalid_corpus_evidence" not in codes, report
assert "corpus_hash_mismatch" not in codes, report
assert "corpus_digest_mismatch" not in codes, report
PY

failure_root=$evidence/failure
expect_run_helper_status 7 fail "$failure_root" 2 9002
grep -Fq '"exit_status":7' "$tmp/out"
python3 - "$tmp/out" <<'PY'
import json
import sys
record = json.loads(open(sys.argv[1], encoding="utf-8").read())
assert record["workflow_conclusion"] == "failure"
assert record["job_conclusion"] == "failure"
assert record["termination_reason"] == "failed"
assert record["exit_status"] == 7
PY

timeout_root=$evidence/timeout
expect_run_helper_status 124 timeout "$timeout_root" 3 9003
grep -Fq '"exit_status":124' "$tmp/out"
python3 - "$tmp/out" <<'PY'
import json
import sys
record = json.loads(open(sys.argv[1], encoding="utf-8").read())
assert record["workflow_conclusion"] == "failure"
assert record["job_conclusion"] == "failure"
assert record["termination_reason"] == "timeout"
assert record["exit_status"] == 124
PY

crash_root=$evidence/crash
expect_run_helper_status 1 crash "$crash_root" 4 9004
grep -Fq '"crash_artifact_count":1' "$tmp/out"
python3 - "$crash_root" "$tmp/out" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
record = json.loads(Path(sys.argv[2]).read_text())
crash = root / "corpora/campaign-fixture/parser/shard-4/crashes/crash-fixture"
assert crash.read_text() == "crash\n"
assert record["workflow_conclusion"] == "failure"
assert record["job_conclusion"] == "failure"
assert record["termination_reason"] == "failed"
assert record["exit_status"] == 0
assert record["crash_artifact_count"] == 1
PY

echo "test-run-hosted-fuzz-shard: OK"
