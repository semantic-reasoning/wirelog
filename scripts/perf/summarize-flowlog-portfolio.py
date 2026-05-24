#!/usr/bin/env python3
"""Emit a current-run Markdown summary for flowlog portfolio artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return None, f"missing {path}"
    except json.JSONDecodeError as exc:
        return None, f"malformed JSON in {path}: {exc}"
    except OSError as exc:
        return None, f"cannot read {path}: {exc}"
    if not isinstance(data, dict):
        return None, f"{path} JSON root is not an object"
    return data, None


def load_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return [], [f"missing {path}"]
    except OSError as exc:
        return [], [f"cannot read {path}: {exc}"]

    for line_no, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"{path}:{line_no}: malformed JSONL record: {exc}")
            continue
        if not isinstance(item, dict):
            errors.append(f"{path}:{line_no}: JSONL record is not an object")
            continue
        records.append(item)
    return records, errors


def load_tsv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
    except FileNotFoundError:
        return [], [f"missing {path}"]
    except OSError as exc:
        return [], [f"cannot read {path}: {exc}"]

    if not rows:
        return [], [f"{path} has no data rows"]

    records: list[dict[str, Any]] = []
    for row in rows:
        summary = {
            "median_ms": row.get("median_ms") or None,
            "tuples": row.get("tuples") or None,
            "iterations": row.get("iterations") or None,
            "peak_rss_kb": row.get("peak_rss_kb") or None,
        }
        records.append(
            {
                "status": row.get("status") or "",
                "reason": row.get("reason") or None,
                "workload": row.get("workload") or "",
                "workers": row.get("workers") or "",
                "repeat": row.get("repeat") or "",
                "return_code": row.get("return_code") or None,
                "summary": summary,
            }
        )
    return records, []


def table_escape(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def fmt_number(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def manifest_value(manifest: dict[str, Any] | None, path: list[str], default: Any = "") -> Any:
    data: Any = manifest
    for key in path:
        if not isinstance(data, dict) or key not in data:
            return default
        data = data[key]
    return data


def status_counts(records: list[dict[str, Any]], manifest: dict[str, Any] | None) -> dict[str, int]:
    counts = Counter(str(record.get("status") or "unknown") for record in records)
    if not counts and isinstance(manifest, dict):
        manifest_counts = manifest.get("status_counts")
        if isinstance(manifest_counts, dict):
            for key, value in manifest_counts.items():
                try:
                    counts[str(key)] = int(value)
                except (TypeError, ValueError):
                    counts[str(key)] = 0
    return {key: counts.get(key, 0) for key in ["ok", "skip", "fail", "unknown"]}


def reason_counts(records: list[dict[str, Any]]) -> Counter[tuple[str, str]]:
    counts: Counter[tuple[str, str]] = Counter()
    for record in records:
        status = str(record.get("status") or "unknown")
        if status == "ok":
            continue
        reason = str(record.get("reason") or "unspecified")
        counts[(status, reason)] += 1
    return counts


def records_from_artifacts(artifact_dir: Path) -> tuple[list[dict[str, Any]], list[str]]:
    jsonl_records, jsonl_errors = load_jsonl(artifact_dir / "portfolio.jsonl")
    if jsonl_records:
        return jsonl_records, jsonl_errors

    tsv_records, tsv_errors = load_tsv(artifact_dir / "portfolio.tsv")
    if tsv_records:
        return tsv_records, jsonl_errors + ["using portfolio.tsv fallback"] + tsv_errors
    return [], jsonl_errors + tsv_errors


def emit_summary(
    artifact_dir: Path,
    artifact_name: str | None,
    manifest: dict[str, Any] | None,
    records: list[dict[str, Any]],
    warnings: list[str],
) -> str:
    lines: list[str] = []
    lines.append("## Flowlog Portfolio")
    lines.append("")
    if artifact_name:
        lines.append(f"Artifact: `{artifact_name}`")
    lines.append(f"Artifact path: `{artifact_dir}`")
    lines.append("")

    if manifest is None and not records:
        lines.append("Portfolio artifacts are unavailable or unreadable for this run.")
    else:
        tier = manifest_value(manifest, ["args", "tier"], "unknown")
        workers = manifest_value(manifest, ["args", "workers"], "unknown")
        repeat = manifest_value(manifest, ["args", "repeat"], "unknown")
        record_count = len(records) if records else manifest_value(manifest, ["record_count"], 0)
        if isinstance(workers, list):
            workers_text = ",".join(str(item) for item in workers)
        else:
            workers_text = str(workers)

        lines.append(
            f"Tier `{tier}`, workers `{workers_text}`, repeat `{repeat}`, "
            f"records `{record_count}`."
        )
        lines.append("")

        counts = status_counts(records, manifest)
        lines.append(
            "Status counts: "
            f"ok `{counts['ok']}`, skip `{counts['skip']}`, "
            f"fail `{counts['fail']}`, unknown `{counts['unknown']}`."
        )
        lines.append("")

        reasons = reason_counts(records)
        if reasons:
            lines.append("| Status | Reason | Count |")
            lines.append("|--------|--------|-------|")
            for (status, reason), count in sorted(reasons.items()):
                lines.append(
                    f"| {table_escape(status)} | {table_escape(reason)} | {count} |"
                )
            lines.append("")

        if records:
            lines.append("| Workload | Workers | Status | Median ms | Tuples | Iterations | Reason |")
            lines.append("|----------|---------|--------|-----------|--------|------------|--------|")
            for record in records:
                summary = record.get("summary") if isinstance(record.get("summary"), dict) else {}
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            table_escape(record.get("workload")),
                            table_escape(record.get("workers")),
                            table_escape(record.get("status")),
                            table_escape(fmt_number(summary.get("median_ms"))),
                            table_escape(fmt_number(summary.get("tuples"))),
                            table_escape(fmt_number(summary.get("iterations"))),
                            table_escape(record.get("reason")),
                        ]
                    )
                    + " |"
                )
            lines.append("")

    if warnings:
        lines.append("<details><summary>Portfolio artifact warnings</summary>")
        lines.append("")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a Markdown summary for current flowlog portfolio artifacts."
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("perf-artifacts/portfolio"),
        help="Directory containing manifest.json and portfolio artifacts.",
    )
    parser.add_argument(
        "--artifact-name",
        default=None,
        help="Uploaded artifact name to mention in the summary.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact_dir = args.artifact_dir
    warnings: list[str] = []

    manifest, manifest_error = load_json(artifact_dir / "manifest.json")
    if manifest_error:
        warnings.append(manifest_error)

    records, record_errors = records_from_artifacts(artifact_dir)
    warnings.extend(record_errors)

    print(emit_summary(artifact_dir, args.artifact_name, manifest, records, warnings), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
