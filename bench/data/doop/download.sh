#!/bin/bash
# Download DOOP (zxing) benchmark dataset
# Source: FlowLog VLDB 2026 artifact (mirrored on HuggingFace).
# The original host (pages.cs.wisc.edu/~m0riarty) is no longer available.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
URL="${DOOP_ZXING_URL:-https://huggingface.co/datasets/NemoYuu/flowlog_benchmark/resolve/main/dataset/csv/zxing.zip}"
EXPECTED_SHA256="${DOOP_ZXING_SHA256:-154593343fefd18306d4098ba9f6286947b134b56ebcf83d8e8eae368d5867e7}"
TMPZIP="/tmp/zxing_doop_$$.zip"
TMPDIR="/tmp/zxing_doop_$$"

cleanup() { rm -rf "$TMPZIP" "$TMPDIR"; }
trap cleanup EXIT

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        echo 'ERROR: sha256sum or shasum is required' >&2
        exit 2
    fi
}

echo "Downloading zxing dataset (~112 MB) from $URL ..."
curl --fail -L "$URL" -o "$TMPZIP"

actual_sha256=$(sha256_file "$TMPZIP")
if [ "$actual_sha256" != "$EXPECTED_SHA256" ]; then
    echo "ERROR: downloaded archive checksum mismatch." >&2
    echo "       expected: $EXPECTED_SHA256" >&2
    echo "       actual:   $actual_sha256" >&2
    echo "       Set DOOP_ZXING_URL and DOOP_ZXING_SHA256 together for a" >&2
    echo "       deliberately provisioned dataset revision." >&2
    exit 1
fi

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
echo "Archive sha256: $actual_sha256"
