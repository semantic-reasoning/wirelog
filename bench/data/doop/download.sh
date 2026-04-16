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

echo "Extracting required CSV files..."
mkdir -p "$TMPDIR"
unzip -o "$TMPZIP" -d "$TMPDIR" > /dev/null

REQUIRED="DirectSuperclass DirectSuperinterface MainClass FormalParam
ComponentType AssignReturnValue ActualParam Method_Modifier Var_Type
HeapAllocation_Type ClassType ArrayType InterfaceType Var_DeclaringMethod
ApplicationClass ThisVar NormalHeap StringConstant AssignHeapAllocation
AssignLocal AssignCast Field StaticMethodInvocation SpecialMethodInvocation
VirtualMethodInvocation Method Method_Descriptor StoreInstanceField
LoadInstanceField StoreStaticField LoadStaticField StoreArrayIndex
LoadArrayIndex Return"

for f in $REQUIRED; do
    cp "$TMPDIR/zxing/${f}.csv" "$SCRIPT_DIR/"
done

echo "Done. 34 CSV files copied to $SCRIPT_DIR/"
