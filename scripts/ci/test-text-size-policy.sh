#!/usr/bin/env bash
set -euo pipefail
ROOT=${1:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)}
POLICY="$ROOT/scripts/ci/text-size-policy.py"
PROFILE="$ROOT/scripts/ci/size-profile.py"
VERIFY="$ROOT/scripts/ci/verify-size-baseline.py"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/wirelog-size-policy.XXXXXX")
trap 'rm -rf -- "$TMP"' EXIT
pass=0
fail=0
check() { local name=$1 expected=$2; shift 2; local rc=0; "$@" >/dev/null 2>&1 || rc=$?; if [ "$rc" -eq "$expected" ]; then echo "ok: $name"; pass=$((pass+1)); else echo "FAIL: $name (exit $rc, expected $expected)" >&2; fail=$((fail+1)); fi; }
policy() { python3 "$POLICY" --base-size "$1" --head-size "$2" --baseline "$3" --base-profile "$4" --head-profile "$5" --base-sha base123 --head-sha head123; }

check 'exact baseline + 5120 boundary passes' 0 policy 12000 15120 10000 same same
check 'baseline + 5121 fails' 1 policy 12000 15121 10000 same same
check 'identical head passes when base is already over budget' 0 policy 15121 15121 10000 same same
check 'smaller head passes against over-budget base' 0 policy 15121 14999 10000 same same
check 'new growth above an over-budget base fails' 1 policy 15121 15122 10000 same same
check 'profile mismatch is fail-closed' 2 policy 10000 10000 10000 base-profile head-profile
check 'invalid size is fail-closed' 2 policy 10000 nope 10000 same same
check 'overflow is fail-closed' 2 policy 10000 10000 999999999999999999999999 same same

python3 - "$TMP" <<'PY'
import json, pathlib, sys
root=pathlib.Path(sys.argv[1])
base={"schema_version":1,"platform":{"system":"Linux"},"options":{"buildtype":"release","mbedTLS":"disabled"},"tools":{"c":{"id":"gcc","version":"14","linker_id":"ld.bfd","linker_version":"ld 2"}},"effective_build_commands":["cc -O3 -DWL_MBEDTLS_ENABLED=0"]}
for name, change in [("base",{}),("same",{}),("default",{"options":{"buildtype":"debugoptimized","mbedTLS":"disabled"}}),("feature",{"options":{"buildtype":"release","mbedTLS":"enabled"}}),("compiler",{"tools":{"c":{"id":"clang","version":"18","linker_id":"ld.bfd","linker_version":"ld 2"}}}),("linker",{"tools":{"c":{"id":"gcc","version":"14","linker_id":"lld","linker_version":"ld 2"}}}),("added",{"options":{"buildtype":"release","mbedTLS":"disabled","new-option":True}}),("removed",{"options":{"buildtype":"release"}})]:
    value=json.loads(json.dumps(base))
    for key,item in change.items(): value[key]=item
    (root/f"{name}.json").write_text(json.dumps(value))
PY
check 'identical normalized profiles compare' 0 python3 "$PROFILE" compare "$TMP/base.json" "$TMP/same.json"
for variant in default feature compiler linker added removed; do
    check "$variant profile change is rejected" 1 python3 "$PROFILE" compare "$TMP/base.json" "$TMP/$variant.json"
done

check 'legacy baseline is not authorized to inflate itself' 1 "$VERIFY" --repository owner/repo --base-sha base123 --candidate-sha head123 --base-value 354887 --candidate-value 999999 --provenance-file "$ROOT/tests/baseline_size.provenance.json"
if [ "$fail" -ne 0 ]; then echo "test-text-size-policy: $fail failed, $pass passed" >&2; exit 1; fi
echo "test-text-size-policy: $pass cases passed"
