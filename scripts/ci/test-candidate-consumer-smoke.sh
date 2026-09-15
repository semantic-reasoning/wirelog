#!/usr/bin/env bash
set -euo pipefail

case "$(uname -s)" in
    Linux) ;;
    *) echo 'test-candidate-consumer-smoke: SKIP: POSIX host required'; exit 77 ;;
esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
runner="$root/scripts/release/run-candidate-consumer-smoke.sh"
workflow="$root/.github/workflows/release-tag.yml"
[[ -x "$runner" ]] || { echo 'candidate consumer runner is not executable' >&2; exit 1; }
bash -n "$runner"
help=$($runner --help 2>&1)
grep -Fq -- '--output-dir DIR' <<<"$help"
grep -Fq -- '--source-root DIR' <<<"$help"
grep -Fq -- '--prefix DIR' <<<"$help"
grep -Fq -- '--expected-library FILE' <<<"$help"
! grep -Eq 'v0\.30|old-ref|upgrade-matrix' "$runner" "$workflow"
grep -Fq 'candidate-consumers:' "$workflow"
grep -Fq 'run-candidate-consumer-smoke.sh' "$workflow"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-candidate-consumer-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
good="$tmp/good"
prefix="$tmp/prefix"
"$runner" --source-root "$root" --output-dir "$good" --keep-prefix "$prefix"
grep -Fxq 'status=passed' "$good/status.txt"
grep -Fxq 'reach 1 3' "$good/easy.out"
outside="$tmp/outside-source"
mkdir -p "$outside/tests/release"
cp "$root/tests/release/"candidate_{easy,session}.c "$outside/tests/release/"
cp "$root/tests/release/candidate_program.dl" "$outside/tests/release/"
"$runner" --source-root "$outside" --candidate-sha \
    0000000000000000000000000000000000000000 --prefix "$prefix" \
    --output-dir "$tmp/outside-run"
printf 'reach 1 2\nreach 1 3\n' >"$tmp/wrong.expected"
if "$runner" --source-root "$root" --prefix "$prefix" \
    --output-dir "$tmp/wrong" \
    --expected-output "$tmp/wrong.expected"; then
    echo 'wrong expected output unexpectedly passed' >&2
    exit 1
fi
if "$runner" --source-root "$root" --prefix "$tmp/missing" \
    --output-dir "$tmp/missing-lib"; then
    echo 'missing installed library unexpectedly passed' >&2
    exit 1
fi
if CC=false "$runner" --source-root "$root" --prefix "$prefix" \
    --output-dir "$tmp/compile-failure"; then
    echo 'consumer compile failure unexpectedly passed' >&2
    exit 1
fi
if "$runner" --source-root "$root" --prefix "$prefix" \
    --output-dir "$tmp/provenance-failure" \
    --expected-library /usr/lib/libwirelog.so; then
    echo 'host-library provenance substitution unexpectedly passed' >&2
    exit 1
fi
printf 'test-candidate-consumer-smoke: all contract checks passed\n'
