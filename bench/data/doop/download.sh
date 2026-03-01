#!/bin/bash
# Download DOOP (zxing) benchmark dataset
# Source: FlowLog artifact (VLDB 2026)
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
URL="https://pages.cs.wisc.edu/~m0riarty/dataset/csv/zxing.zip"
TMPZIP="/tmp/zxing_doop_$$.zip"
TMPDIR="/tmp/zxing_doop_$$"

echo "Downloading zxing dataset (~112 MB)..."
curl -L "$URL" -o "$TMPZIP"

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

rm -rf "$TMPDIR" "$TMPZIP"
echo "Done. 34 CSV files copied to $SCRIPT_DIR/"
