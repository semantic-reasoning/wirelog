#!/bin/sh
# Build the exact event-base tree with the same production recipe/toolchain,
# compare it with the already-built tested merge checkout, and report evidence.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd -P)
head_build=${1:-}
base_sha=${2:-}
head_sha=${3:-}
report=${4:-}
pr_head_sha=${5:-}
die() { printf 'size comparison setup error: %s\n' "$1" >&2; exit 2; }
[ -n "$head_build" ] && [ -n "$base_sha" ] && [ -n "$head_sha" ] && [ -n "$report" ] && [ -n "$pr_head_sha" ] || die "usage: $0 HEAD_BUILD BASE_SHA TESTED_MERGE_SHA REPORT.json PR_HEAD_SHA"
case "$base_sha:$head_sha:$pr_head_sha" in *[!0-9a-fA-F:]*|:*|*:) die "revision identifiers must be hexadecimal commit IDs" ;; esac
[ "$(git rev-parse --verify "$base_sha^{commit}")" = "$base_sha" ] || die "event base SHA is unavailable or not a commit"
[ "$(git rev-parse --verify "$head_sha^{commit}")" = "$head_sha" ] || die "tested merge SHA is unavailable or not a commit"
[ "$(git rev-parse --verify "$pr_head_sha^{commit}")" = "$pr_head_sha" ] || die "event PR head SHA is unavailable or not a commit"
[ "$(git rev-parse HEAD)" = "$head_sha" ] || die "checkout does not match tested merge SHA; refresh/retrigger this PR"
parents=$(git rev-list --parents -n 1 "$head_sha") || die "could not inspect tested commit parents"
set -- $parents
[ "$#" -eq 3 ] || die "tested SHA is not an exact two-parent PR merge; refresh/retrigger this PR"
first_parent=$2
second_parent=$3
[ "$first_parent" = "$base_sha" ] || die "tested merge first parent differs from event base SHA; refresh/retrigger this PR"
[ "$second_parent" = "$pr_head_sha" ] || die "tested merge second parent differs from event PR head SHA; refresh/retrigger this PR"
head_source=$(git rev-parse --show-toplevel) || die "cannot locate tested source checkout"

tmp=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-size-compare.XXXXXX") || die "cannot create temporary build directory"
cleanup() { rm -rf -- "$tmp"; }
trap cleanup EXIT HUP INT TERM
mkdir -p "$tmp/base-source"
git archive --format=tar "$base_sha" | tar -xf - -C "$tmp/base-source" || die "could not extract exact event-base tree"

baseline=$(git show "$base_sha:tests/baseline_size.txt") || die "base-owned baseline is missing"
printf '%s\n' "$baseline" >"$tmp/base-baseline.txt"
case "$baseline" in ''|*[!0-9]*) die "base-owned baseline is invalid" ;; esac

git show "$head_sha:tests/baseline_size.provenance.json" >"$tmp/head-baseline.provenance.json" 2>/dev/null \
    || die "candidate baseline provenance sidecar is missing"
# Numeric edits are never authorized by candidate-authored metadata alone.
head_baseline=$(git show "$head_sha:tests/baseline_size.txt") || die "tested tree baseline is missing"
if [ "$head_baseline" != "$baseline" ]; then
    python3 "$script_dir/verify-size-baseline.py" \
        --repository "${GITHUB_REPOSITORY:-}" --base-sha "$base_sha" \
        --candidate-sha "$head_sha" --base-value "$baseline" --candidate-value "$head_baseline" \
        --provenance-file "$tmp/head-baseline.provenance.json" \
        || die "numeric baseline update is not backed by trusted reproducible CI evidence"
fi

meson setup "$tmp/base-build" "$tmp/base-source" -Dtests=true -DmbedTLS=disabled || die "event-base Meson configure failed"
meson compile -C "$tmp/base-build" wirelog || die "event-base production library build failed"
meson compile -C "$head_build" wirelog || die "tested production library build failed"
python3 "$script_dir/size-profile.py" capture --build-dir "$tmp/base-build" \
    --source-dir "$tmp/base-source" --source-sha "$base_sha" --output "$tmp/base-profile.json" || die "base production profile unavailable"
python3 "$script_dir/size-profile.py" capture --build-dir "$head_build" \
    --source-dir "$head_source" --source-sha "$head_sha" --output "$tmp/head-profile.json" || die "head production profile unavailable"
python3 "$script_dir/size-profile.py" compare "$tmp/base-profile.json" "$tmp/head-profile.json" || die "base/head production profile mismatch"

scripts="$script_dir/check-text-size.sh"
"$scripts" "$tmp/base-build/libwirelog.so" --measure-only --json "$tmp/base-size.json" --source-sha "$base_sha" --profile "$tmp/base-profile.json" || die "base library measurement failed"
"$scripts" "$head_build/libwirelog.so" --measure-only --json "$tmp/head-size.json" --source-sha "$head_sha" --profile "$tmp/head-profile.json" || die "head library measurement failed"
base_bytes=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["measured_bytes"])' "$tmp/base-size.json") || die "invalid base size output"
head_bytes=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["measured_bytes"])' "$tmp/head-size.json") || die "invalid head size output"
base_profile=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); d.pop("source_sha",None); import hashlib; print(hashlib.sha256(json.dumps(d,sort_keys=True,separators=(",",":")).encode()).hexdigest())' "$tmp/base-profile.json")
head_profile=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); d.pop("source_sha",None); import hashlib; print(hashlib.sha256(json.dumps(d,sort_keys=True,separators=(",",":")).encode()).hexdigest())' "$tmp/head-profile.json")
python3 "$script_dir/text-size-policy.py" --base-size "$base_bytes" --head-size "$head_bytes" \
    --baseline "$baseline" --base-profile "$base_profile" --head-profile "$head_profile" \
    --base-sha "$base_sha" --head-sha "$head_sha" --output "$report"
