#!/usr/bin/env python3
"""Prove a sharded `meson test --slice K/N` lane ran every test exactly once.

Issue #2397 splits the Linux Clang PR suite into N jobs that each run
`meson test --slice K/N`.  Meson's slice is ``tests[K-1::N]`` over the same
filtered list that `meson test --list` prints, so the N slices partition the
default run by construction -- but only if every shard saw the same list and
actually executed its whole slice.  This script turns that into evidence that
fails closed:

  plan       before the suite: record the full list, this shard's slice and a
             manifest (shard, count, commit SHA, run attempt, Meson version).
  verify     after the suite: check that ``testlog.json`` names every assigned
             test exactly once and nothing else, and keep a trimmed
             ``results.jsonl`` (name/result/starttime/duration/returncode
             only; the raw log carries each test's full environment and
             output and is never copied into the evidence).
  aggregate  in the collecting job: require exactly shards 1..N for one SHA
             and count, identical non-empty duplicate-free full lists, each
             slice equal to ``full[K-1::N]``, pairwise-disjoint slices whose
             union is the full list, each shard's executed set equal to its
             slice, and no failing result.  Prints a per-shard table and
             appends it to ``--summary`` when given.

Exit status is 0 (proven), 1 (not proven) or 2 (usage error); there is no
skip outcome (77).
Every CI-provided value is an explicit argument -- the script reads no
GitHub environment variable -- so its selftest cannot leak into a real job.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

KEPT_FIELDS = ("name", "result", "starttime", "duration", "returncode")
PASSING_RESULTS = {"OK", "SKIP", "EXPECTEDFAIL"}


class NotProven(Exception):
    pass


def read_lines(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise NotProven(f"cannot read {path}: {exc}") from exc
    return [line for line in text.splitlines() if line]


def write_lines(path: Path, lines: list[str]) -> None:
    path.write_text("".join(line + "\n" for line in lines), encoding="utf-8",
                    newline="")


def trim(row: dict) -> dict:
    return {key: row.get(key) for key in KEPT_FIELDS}


def read_jsonl(path: Path, label: str) -> list[dict]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise NotProven(f"cannot read {label} {path}: {exc}") from exc
    rows = []
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError as exc:
            raise NotProven(f"{label} {path} line {number} is not JSON: {exc}") from exc
        if not isinstance(row, dict) or not isinstance(row.get("name"), str):
            raise NotProven(f"{label} {path} line {number} has no test name")
        rows.append(row)
    return rows


def check_count(shard: int, count: int) -> None:
    if count < 1 or not 1 <= shard <= count:
        raise NotProven(f"invalid shard {shard}/{count}")


def check_execution(label: str, assigned: list[str], executed: list[str]) -> None:
    seen: dict[str, int] = {}
    for name in executed:
        seen[name] = seen.get(name, 0) + 1
    problems = []
    twice = sorted(name for name, n in seen.items() if n > 1)
    if twice:
        problems.append(f"ran more than once: {twice}")
    extra = sorted(set(seen) - set(assigned))
    if extra:
        problems.append(f"ran tests not assigned to it: {extra}")
    missing = [name for name in assigned if name not in seen]
    if missing:
        problems.append(f"{len(missing)} assigned tests not executed: {missing[:20]}")
    if problems:
        raise NotProven(f"{label}: " + "; ".join(problems))


def run_meson(meson: list[str], args: list[str]) -> list[str]:
    proc = subprocess.run(meson + args, capture_output=True, text=True,
                          encoding="utf-8")
    if proc.returncode != 0:
        raise NotProven(f"{' '.join(meson + args)} exited {proc.returncode}: "
                        f"{proc.stderr.strip()}")
    return [line for line in proc.stdout.splitlines() if line.strip()]


def cmd_plan(args: argparse.Namespace) -> None:
    check_count(args.shard, args.count)
    meson = args.meson or ["meson"]
    version = run_meson(meson, ["--version"])
    base = ["test", "-C", str(args.builddir), "--no-rebuild", "--list"]
    full = run_meson(meson, base)
    if args.count > len(full):
        raise NotProven(f"shard count {args.count} exceeds the {len(full)} "
                        "registered tests")
    assigned = run_meson(meson, base + ["--slice", f"{args.shard}/{args.count}"])
    if assigned != full[args.shard - 1::args.count]:
        raise NotProven("meson --slice no longer selects full[K-1::N]; the "
                        "coverage proof does not hold for this Meson")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    write_lines(out / "full.txt", full)
    write_lines(out / "slice.txt", assigned)
    manifest = {"shard": args.shard, "count": args.count, "sha": args.sha,
                "run_attempt": args.run_attempt,
                "meson_version": version[0] if version else "",
                "full_tests": len(full), "slice_tests": len(assigned)}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n",
                                       encoding="utf-8")
    print(f"check-test-shards: shard {args.shard}/{args.count}: "
          f"{len(assigned)} of {len(full)} tests (meson {manifest['meson_version']})")


def cmd_verify(args: argparse.Namespace) -> None:
    out = Path(args.evidence)
    assigned = read_lines(out / "slice.txt")
    rows = read_jsonl(Path(args.testlog), "testlog")
    # Keep what was measured even when the proof fails: a partial log after a
    # step timeout is the diagnostic the collecting job and humans need.
    write_lines(out / "results.jsonl", [json.dumps(trim(r)) for r in rows])
    check_execution("shard", assigned, [r["name"] for r in rows])
    print(f"check-test-shards: executed exactly the {len(assigned)} assigned tests")


def load_shard(directory: Path) -> dict:
    try:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise NotProven(f"{directory}: unreadable manifest.json: {exc}") from exc
    shard = manifest.get("shard")
    if not isinstance(shard, int):
        raise NotProven(f"{directory}: manifest has no integer shard")
    suffix = re.search(r"-(\d+)$", directory.name)
    if suffix and int(suffix[1]) != shard:
        raise NotProven(f"folder {directory.name} holds manifest of shard {shard}")
    return {"dir": directory, "manifest": manifest, "shard": shard,
            "full": read_lines(directory / "full.txt"),
            "slice": read_lines(directory / "slice.txt"),
            "results": read_jsonl(directory / "results.jsonl", "results.jsonl")}


def cmd_aggregate(args: argparse.Namespace) -> None:
    root = Path(args.root)
    if args.count < 1:
        raise NotProven(f"invalid shard count {args.count}")
    dirs = sorted(p.parent for p in root.glob("*/manifest.json"))
    loaded = [load_shard(d) for d in dirs]
    indices = sorted(s["shard"] for s in loaded)
    if indices != list(range(1, args.count + 1)):
        raise NotProven(f"expected shards 1..{args.count}, found {indices}")
    loaded.sort(key=lambda s: s["shard"])
    full = loaded[0]["full"]
    if not full:
        raise NotProven("empty full test list")
    if len(set(full)) != len(full):
        raise NotProven("duplicate names in the full test list")
    union: list[str] = []
    for s in loaded:
        label = f"shard {s['shard']}"
        manifest = s["manifest"]
        if manifest.get("count") != args.count:
            raise NotProven(f"{label}: manifest count {manifest.get('count')} != {args.count}")
        if manifest.get("sha") != args.sha:
            raise NotProven(f"{label}: manifest sha {manifest.get('sha')} != {args.sha}")
        if s["full"] != full:
            raise NotProven(f"{label}: full test list differs from shard 1")
        if s["slice"] != full[s["shard"] - 1::args.count]:
            raise NotProven(f"{label}: slice is not full[{s['shard'] - 1}::{args.count}]")
        check_execution(label, s["slice"], [r["name"] for r in s["results"]])
        bad = sorted(f"{r['name']}={r.get('result')}" for r in s["results"]
                     if r.get("result") not in PASSING_RESULTS)
        if bad:
            raise NotProven(f"{label}: failing results {bad}")
        union.extend(s["slice"])
    # Implied by shards 1..N each holding full[K-1::N]; stated directly so the
    # claim this script exists to prove is also the last thing it checks.
    if len(union) != len(set(union)) or sorted(union) != sorted(full):
        raise NotProven("slices are not a disjoint cover of the full test list")

    lines = [f"Linux Clang shards: {len(full)} tests in {args.count} shards, "
             "each executed exactly once.", "",
             "| shard | assigned | executed | summed test s | elapsed s |",
             "| --- | --- | --- | --- | --- |"]
    for s in loaded:
        rows = s["results"]
        summed = sum(float(r.get("duration") or 0) for r in rows)
        starts = [float(r["starttime"]) for r in rows if r.get("starttime") is not None]
        ends = [float(r["starttime"]) + float(r.get("duration") or 0)
                for r in rows if r.get("starttime") is not None]
        elapsed = max(ends) - min(starts) if starts else 0.0
        lines.append(f"| {s['shard']} | {len(s['slice'])} | {len(rows)} | "
                     f"{summed:.0f} | {elapsed:.0f} |")
    report = "\n".join(lines) + "\n"
    print(report, end="")
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as handle:
            handle.write(report)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan")
    plan.add_argument("--builddir", required=True)
    plan.add_argument("--shard", type=int, required=True)
    plan.add_argument("--count", type=int, required=True)
    plan.add_argument("--sha", required=True)
    plan.add_argument("--run-attempt", default="")
    plan.add_argument("--out", required=True)
    plan.add_argument("--meson", action="append",
                      help="meson command token (repeat; default: meson)")
    verify = sub.add_parser("verify")
    verify.add_argument("--evidence", required=True)
    verify.add_argument("--testlog", required=True)
    aggregate = sub.add_parser("aggregate")
    aggregate.add_argument("--count", type=int, required=True)
    aggregate.add_argument("--sha", required=True)
    aggregate.add_argument("--summary")
    aggregate.add_argument("root")
    args = parser.parse_args(argv)
    try:
        {"plan": cmd_plan, "verify": cmd_verify,
         "aggregate": cmd_aggregate}[args.command](args)
    except NotProven as exc:
        print(f"check-test-shards: FAIL: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
