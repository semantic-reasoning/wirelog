#!/usr/bin/env python3
"""Fail-closed binary-size policy and report formatter."""
import argparse
import json
import sys

MAX_BYTES = 2**63 - 1

def byte_count(value, name):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer")
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a non-negative integer") from None
    if str(number) != str(value) or number < 0 or number > MAX_BYTES:
        raise ValueError(f"{name} is invalid or out of range")
    return number

def compare(base_size, head_size, baseline, base_profile, head_profile, threshold=5120):
    base_size = byte_count(base_size, "base size")
    head_size = byte_count(head_size, "head size")
    baseline = byte_count(baseline, "baseline")
    threshold = byte_count(threshold, "threshold")
    if not base_profile or base_profile != head_profile:
        raise ValueError("production profile mismatch between base and head")
    ceiling = baseline + threshold
    inherited_overage = base_size > ceiling
    allowed = base_size if inherited_overage else ceiling
    status = "pass" if head_size <= allowed else "over-budget"
    return {"schema_version": 1, "status": status, "base_bytes": base_size,
            "head_bytes": head_size, "baseline_bytes": baseline,
            "budget_bytes": threshold, "allowed_head_bytes": allowed,
            "delta_from_baseline_bytes": head_size - baseline,
            "inherited_overage": inherited_overage,
            "base_profile": base_profile, "head_profile": head_profile}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-size", required=True)
    parser.add_argument("--head-size", required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--base-profile", required=True)
    parser.add_argument("--head-profile", required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--threshold", default="5120")
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        if not args.base_sha or not args.head_sha or args.base_sha == "unknown" or args.head_sha == "unknown":
            raise ValueError("base/head source SHA is missing")
        report = compare(args.base_size, args.head_size, args.baseline,
                         args.base_profile, args.head_profile, args.threshold)
        report.update({"base_sha": args.base_sha, "head_sha": args.head_sha})
        text = json.dumps(report, sort_keys=True, indent=2) + "\n"
        if args.output:
            with open(args.output, "w", encoding="utf-8") as stream:
                stream.write(text)
        print(text, end="")
        if report["inherited_overage"]:
            print("inherited over-budget base; head may not exceed measured base size", file=sys.stderr)
        return 0 if report["status"] == "pass" else 1
    except (OSError, ValueError) as exc:
        print(f"text-size-policy: {exc}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
