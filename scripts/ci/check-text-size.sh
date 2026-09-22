#!/bin/sh
# Measure libwirelog's .text section and emit a stable JSON evidence record.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd -P)
threshold=5120
baseline_file="$repo_root/tests/baseline_size.txt"
json_out=
source_sha=${SOURCE_SHA:-unknown}
profile_file=${SIZE_PROFILE_FILE:-}
measure_only=no

usage() { echo "usage: $0 <library> [--baseline-file FILE] [--json FILE] [--source-sha SHA] [--profile FILE]" >&2; exit 2; }
[ "$#" -ge 1 ] || usage
library=$1; shift
while [ "$#" -gt 0 ]; do
    case "$1" in
        --baseline-file) [ "$#" -ge 2 ] || usage; baseline_file=$2; shift 2 ;;
        --json) [ "$#" -ge 2 ] || usage; json_out=$2; shift 2 ;;
        --source-sha) [ "$#" -ge 2 ] || usage; source_sha=$2; shift 2 ;;
        --profile) [ "$#" -ge 2 ] || usage; profile_file=$2; shift 2 ;;
        --measure-only) measure_only=yes; shift ;;
        *) usage ;;
    esac
done
fail() { printf 'error: %s\n' "$1" >&2; exit 2; }
[ -f "$library" ] || fail "library not found: $library"
[ "$measure_only" = yes ] || [ -f "$baseline_file" ] || fail "baseline file not found: $baseline_file"

case $(uname -s) in
    Linux) raw=$(size --format=sysv "$library") || fail "size failed for $library"; section=.text ;;
    Darwin) raw=$(size -m "$library") || fail "size failed for $library"; section=__text ;;
    *) fail "unsupported platform: $(uname -s)" ;;
esac
if [ "$section" = .text ]; then
    measured=$(printf '%s\n' "$raw" | awk '$1==".text" { n++; value=$2 } END { if (n==1) print value; else exit 2 }') || fail "expected exactly one .text section size in $library"
else
    measured=$(printf '%s\n' "$raw" | awk '/Section __text:/ { n++; value=$3 } END { if (n==1) print value; else exit 2 }') || fail "expected exactly one __text section size in $library"
fi
baseline=0
[ "$measure_only" = yes ] || baseline=$(cat "$baseline_file")
for value in "$measured" "$baseline"; do
    case "$value" in ''|*[!0-9]*) fail "invalid non-negative byte count: $value" ;; esac
    [ "${#value}" -le 18 ] || fail "byte count is out of range: $value"
done
delta=$((measured - baseline))
status=measured
[ "$measure_only" = yes ] || status=pass
[ "$measure_only" = yes ] || [ "$delta" -le "$threshold" ] || status=over-budget
printf '.text size:  %s bytes\nbaseline:    %s bytes\ndelta:       %+d bytes\nthreshold:   +%s bytes\n' "$measured" "$baseline" "$delta" "$threshold"
if [ -n "$json_out" ]; then
    python3 - "$json_out" "$measured" "$baseline" "$delta" "$threshold" "$status" "$source_sha" "$profile_file" "$library" <<'PY'
import json, os, sys
path, measured, baseline, delta, threshold, status, source, profile, library = sys.argv[1:]
data = {"schema_version": 1, "status": status, "library": os.path.basename(library),
        "measured_bytes": int(measured), "baseline_bytes": int(baseline),
        "delta_bytes": int(delta), "budget_bytes": int(threshold),
        "source_sha": source, "profile": profile or None}
with open(path, "w", encoding="utf-8") as stream:
    json.dump(data, stream, sort_keys=True, indent=2)
    stream.write("\n")
PY
fi
if [ "$measure_only" = yes ]; then
    exit 0
fi
if [ "$status" = over-budget ]; then
    printf '\nFAIL: .text growth (%+d) exceeds %s-byte budget\n' "$delta" "$threshold" >&2
    exit 1
fi
printf '\nPASS: .text delta within budget\n'
