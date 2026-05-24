#!/usr/bin/env python3
"""Summarize recent flowlog portfolio artifact history."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


STATUS_KEYS = ("ok", "skip", "fail", "unknown")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
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


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    warnings: list[str] = []
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
            warnings.append(f"{path}:{line_no}: malformed JSONL record: {exc}")
            continue
        if not isinstance(item, dict):
            warnings.append(f"{path}:{line_no}: JSONL record is not an object")
            continue
        records.append(item)
    return records, warnings


def read_tsv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
    except FileNotFoundError:
        return [], [f"missing {path}"]
    except OSError as exc:
        return [], [f"cannot read {path}: {exc}"]

    records: list[dict[str, Any]] = []
    for row in rows:
        records.append(
            {
                "status": row.get("status") or "unknown",
                "reason": row.get("reason") or None,
                "workload": row.get("workload") or "unknown",
                "workers": row.get("workers") or "unknown",
            }
        )
    if not records:
        return [], [f"{path} has no portfolio records"]
    return records, []


def find_artifact_dirs(root: Path) -> list[Path]:
    dirs: list[Path] = []
    seen: set[Path] = set()
    for manifest in root.rglob("manifest.json"):
        directory = manifest.parent
        if directory in seen:
            continue
        if (directory / "portfolio.jsonl").exists() or (directory / "portfolio.tsv").exists():
            dirs.append(directory)
            seen.add(directory)
    return sorted(dirs)


def infer_artifact_name(path: Path) -> str:
    for part in reversed(path.parts):
        if part.startswith("perf-portfolio-"):
            return part
    return path.name


def infer_os_compiler(artifact_name: str) -> tuple[str, str]:
    prefix = "perf-portfolio-"
    if not artifact_name.startswith(prefix):
        return "unknown", "unknown"
    rest = artifact_name[len(prefix) :]
    if "-" not in rest:
        return rest or "unknown", "unknown"
    os_name, compiler = rest.rsplit("-", 1)
    return os_name or "unknown", compiler or "unknown"


def normalize_status(value: Any) -> str:
    status = str(value or "unknown").lower()
    return status if status in STATUS_KEYS else "unknown"


def count_record(records: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(normalize_status(record.get("status")) for record in records)
    return {key: counts.get(key, 0) for key in STATUS_KEYS}


def empty_counts() -> dict[str, int]:
    return {key: 0 for key in STATUS_KEYS}


def add_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key in STATUS_KEYS:
        target[key] += source.get(key, 0)


def skip_rate(counts: dict[str, int]) -> float:
    total = sum(counts.get(key, 0) for key in STATUS_KEYS)
    return (counts.get("skip", 0) / total) if total else 0.0


def load_artifact(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    warnings: list[str] = []
    manifest, error = read_json(path / "manifest.json")
    if error:
        warnings.append(error)
        manifest = {}

    records, record_warnings = read_jsonl(path / "portfolio.jsonl")
    warnings.extend(record_warnings)
    if not records:
        tsv_records, tsv_warnings = read_tsv(path / "portfolio.tsv")
        records = tsv_records
        warnings.extend(tsv_warnings)
        if records:
            warnings.append(f"{path}: used portfolio.tsv fallback")

    artifact_name = infer_artifact_name(path)
    os_name, compiler = infer_os_compiler(artifact_name)
    started = parse_time(manifest.get("started_at")) if isinstance(manifest, dict) else None
    ended = parse_time(manifest.get("ended_at")) if isinstance(manifest, dict) else None
    timestamp = ended or started
    counts = count_record(records)

    return (
        {
            "path": str(path),
            "artifact_name": artifact_name,
            "os": os_name,
            "compiler": compiler,
            "started_at": manifest.get("started_at") if isinstance(manifest, dict) else None,
            "ended_at": manifest.get("ended_at") if isinstance(manifest, dict) else None,
            "timestamp": timestamp.isoformat(timespec="seconds") if timestamp else None,
            "records": records,
            "record_count": len(records),
            "counts": counts,
            "skip_rate": skip_rate(counts),
        },
        warnings,
    )


def group_artifacts(artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    totals = empty_counts()
    by_platform: dict[tuple[str, str], dict[str, int]] = defaultdict(empty_counts)
    by_workload: dict[tuple[str, str], dict[str, int]] = defaultdict(empty_counts)
    by_platform_workload: dict[tuple[str, str, str, str], dict[str, int]] = defaultdict(empty_counts)
    reasons: Counter[tuple[str, str]] = Counter()

    for artifact in artifacts:
        counts = artifact["counts"]
        add_counts(totals, counts)
        platform_key = (artifact["os"], artifact["compiler"])
        add_counts(by_platform[platform_key], counts)
        for record in artifact["records"]:
            record_counts = empty_counts()
            status = normalize_status(record.get("status"))
            record_counts[status] = 1
            workload_key = (str(record.get("workload") or "unknown"), str(record.get("workers") or "unknown"))
            add_counts(by_workload[workload_key], record_counts)
            platform_workload_key = (
                artifact["os"],
                artifact["compiler"],
                workload_key[0],
                workload_key[1],
            )
            add_counts(by_platform_workload[platform_workload_key], record_counts)
            if status != "ok":
                reasons[(status, str(record.get("reason") or "unspecified"))] += 1

    def rows_from_counts(grouped: dict[tuple[Any, ...], dict[str, int]], key_names: tuple[str, ...]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, counts in sorted(grouped.items()):
            total = sum(counts.values())
            rows.append(
                {
                    **{name: key[index] for index, name in enumerate(key_names)},
                    "record_count": total,
                    **counts,
                    "skip_rate": skip_rate(counts),
                }
            )
        return rows

    return {
        "totals": {
            "record_count": sum(totals.values()),
            **totals,
            "skip_rate": skip_rate(totals),
        },
        "by_platform": rows_from_counts(by_platform, ("os", "compiler")),
        "by_workload_workers": rows_from_counts(by_workload, ("workload", "workers")),
        "by_platform_workload_workers": rows_from_counts(
            by_platform_workload,
            ("os", "compiler", "workload", "workers"),
        ),
        "reasons": [
            {"status": status, "reason": reason, "count": count}
            for (status, reason), count in sorted(reasons.items())
        ],
    }


def table_escape(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def pct(value: float) -> str:
    return f"{value * 100.0:.1f}%"


def counts_table(rows: list[dict[str, Any]], first_cols: list[str]) -> list[str]:
    headings = [*first_cols, "Records", "OK", "Skip", "Fail", "Unknown", "Skip rate"]
    lines = ["| " + " | ".join(headings) + " |"]
    lines.append("|" + "|".join(["---"] * len(headings)) + "|")
    for row in rows:
        values = [
            *(table_escape(row.get(col)) for col in first_cols),
            str(row.get("record_count", 0)),
            str(row.get("ok", 0)),
            str(row.get("skip", 0)),
            str(row.get("fail", 0)),
            str(row.get("unknown", 0)),
            pct(float(row.get("skip_rate", 0.0))),
        ]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def build_summary(result: dict[str, Any]) -> str:
    metadata = result["metadata"]
    grouped = result["groups"]
    totals = grouped["totals"]
    lines: list[str] = []
    lines.append("## Flowlog Portfolio SKIP-Rate History")
    lines.append("")
    lines.append(
        f"Requested window: `{metadata['requested_window_days']}` days; "
        f"observed artifacts: `{metadata['observed_artifact_count']}`; "
        f"observed records: `{metadata['observed_record_count']}`."
    )
    if metadata["observed_artifact_count"] < metadata["requested_window_days"]:
        lines.append(
            f"Observed artifact count is below the requested day window "
            f"({metadata['observed_artifact_count']} < {metadata['requested_window_days']}); "
            "summary uses currently available artifacts."
        )
    lines.append("")
    lines.append(
        f"Totals: ok `{totals['ok']}`, skip `{totals['skip']}`, "
        f"fail `{totals['fail']}`, unknown `{totals['unknown']}`, "
        f"skip rate `{pct(float(totals['skip_rate']))}`."
    )
    lines.append("")

    if grouped["by_platform"]:
        lines.append("### By OS / Compiler")
        lines.extend(counts_table(grouped["by_platform"], ["os", "compiler"]))
        lines.append("")

    if grouped["by_workload_workers"]:
        lines.append("### By Workload / Workers")
        lines.extend(counts_table(grouped["by_workload_workers"], ["workload", "workers"]))
        lines.append("")

    if grouped["by_platform_workload_workers"]:
        lines.append("### By OS / Compiler / Workload / Workers")
        lines.extend(
            counts_table(
                grouped["by_platform_workload_workers"],
                ["os", "compiler", "workload", "workers"],
            )
        )
        lines.append("")

    if grouped["reasons"]:
        lines.append("### Non-OK Reasons")
        lines.append("| Status | Reason | Count |")
        lines.append("|---|---|---|")
        for row in grouped["reasons"]:
            lines.append(
                f"| {table_escape(row['status'])} | {table_escape(row['reason'])} | {row['count']} |"
            )
        lines.append("")

    warnings = metadata["warnings"]
    if warnings:
        lines.append("<details><summary>History artifact warnings</summary>")
        lines.append("")
        for warning in warnings:
            lines.append(f"- {warning}")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def summarize(root: Path, window_days: int, now: datetime) -> dict[str, Any]:
    cutoff = now - timedelta(days=window_days)
    warnings: list[str] = []
    artifacts: list[dict[str, Any]] = []

    if not root.exists():
        warnings.append(f"history root does not exist: {root}")
        artifact_dirs: list[Path] = []
    else:
        artifact_dirs = find_artifact_dirs(root)
        if not artifact_dirs:
            warnings.append(f"no perf portfolio artifacts found under {root}")

    for artifact_dir in artifact_dirs:
        artifact, artifact_warnings = load_artifact(artifact_dir)
        warnings.extend(artifact_warnings)
        if artifact is None:
            continue
        timestamp = parse_time(artifact.get("timestamp"))
        if timestamp is not None and timestamp < cutoff:
            continue
        artifacts.append(artifact)

    grouped = group_artifacts(artifacts)
    result = {
        "metadata": {
            "generated_at": now.isoformat(timespec="seconds"),
            "requested_window_days": window_days,
            "history_root": str(root),
            "observed_artifact_count": len(artifacts),
            "observed_record_count": grouped["totals"]["record_count"],
            "warnings": warnings,
        },
        "artifacts": [
            {
                key: value
                for key, value in artifact.items()
                if key not in {"records"}
            }
            for artifact in artifacts
        ],
        "groups": grouped,
    }
    result["markdown"] = build_summary(result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize last-N/current-available flowlog portfolio SKIP rates."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("perf-artifacts/history"),
        help="Root containing downloaded/extracted perf-portfolio-* artifact directories.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Requested history window in days; available artifacts are summarized.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Optional path for machine-readable JSON output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    window_days = max(args.window_days, 1)
    result = summarize(args.root, window_days, utc_now())
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(result["markdown"], end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
