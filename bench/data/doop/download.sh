#!/bin/bash
# Download DOOP (zxing) benchmark dataset
# Source: FlowLog VLDB 2026 artifact (mirrored on HuggingFace).
# The original host (pages.cs.wisc.edu/~m0riarty) is no longer available.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
URL="${DOOP_ZXING_URL:-https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/dataset/csv/zxing.zip}"
TMPZIP="/tmp/zxing_doop_$$.zip"
TMPDIR="/tmp/zxing_doop_$$"

cleanup() { rm -rf "$TMPZIP" "$TMPDIR"; }
trap cleanup EXIT

echo "Downloading zxing dataset (~112 MB) from $URL ..."
curl --fail -L "$URL" -o "$TMPZIP"

# Fail fast if the server returned HTML (e.g. 404 page) instead of a zip.
if ! unzip -tq "$TMPZIP" > /dev/null 2>&1; then
    echo "ERROR: downloaded file is not a valid zip archive." >&2
    echo "       Check connectivity and DOOP_ZXING_URL." >&2
    exit 1
fi

echo "Extracting required fact files..."
mkdir -p "$TMPDIR"
unzip -o "$TMPZIP" -d "$TMPDIR" > /dev/null

# The archive ships DOOP .facts (tab-separated, string-valued).  They are
# copied verbatim: the benchmark reads them directly, so there is no encoding
# step that could drift between refreshes.  Issue #950.
REQUIRED="DirectSuperclass DirectSuperinterface MainClass FormalParam
ComponentType AssignReturnValue ActualParam Method-Modifier Var-Type
ClassType ArrayType InterfaceType Var-DeclaringMethod ApplicationClass
ThisVar NormalHeap StringConstant AssignHeapAllocation AssignLocal AssignCast
Field StaticMethodInvocation SpecialMethodInvocation VirtualMethodInvocation
Method StoreInstanceField LoadInstanceField StoreStaticField LoadStaticField
StoreArrayIndex LoadArrayIndex Return ClassHeap MethodHandleConstant
MethodTypeConstant"

missing=""
for f in $REQUIRED; do
    [ -f "$TMPDIR/zxing/${f}.facts" ] || missing="$missing $f"
done
if [ -n "$missing" ]; then
    echo "ERROR: the archive is missing required relations:" >&2
    for f in $missing; do echo "         ${f}.facts" >&2; done
    echo "       Downloaded from: $URL" >&2
    echo "       The upstream artifact may have changed; see issue #950." >&2
    exit 1
fi

for f in $REQUIRED; do
    cp "$TMPDIR/zxing/${f}.facts" "$SCRIPT_DIR/"
done

echo "Done. $(echo $REQUIRED | wc -w) fact files copied to $SCRIPT_DIR/"
echo "Archive sha256: $(sha256sum "$TMPZIP" | cut -d" " -f1)"
