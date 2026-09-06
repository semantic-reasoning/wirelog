#!/usr/bin/env python3
"""Audit actual TDD selection/execution, not scalability (#1378).

Each measurement is a separate repeat=1 process. Preserve full logs, validate
the release tuple pins, and never classify absent/failed evidence as rejection.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys

WORKLOADS = ("cspa-fast", "galen", "polonius", "ddisasm", "crdt", "doop")
TIMINGS = ("total_ms", "kfusion_ms", "exchange_ms", "tdd_ms", "worker_min_ms",
           "worker_max_ms", "worker_sum_ms", "wait_ms", "merge_ms")
COUNTS = ("idx", "recursive", "use_tdd", "requested_workers", "selected_workers",
          "submitted_tasks", "completed_rounds", "worker_delta_rows", "replay_rc", "rc")
DECISION_FLAGS = ("recursive", "snapshot", "exchange", "safe", "global_read_candidate",
                  "self_join", "single_key_aligned", "use_tdd")
DECISION_COUNTS = ("idb_atoms", "segments", "segment_seed", "segment_global_read",
                   "segment_unsafe", "segment_max_idb", "segment_max_joins")
FALLBACKS = ("none", "non_recursive", "snapshot_ineligible", "no_exchange", "unsafe_plan", "adaptive_workers")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value):
    result = float(value)
    require(math.isfinite(result) and result >= 0, f"invalid nonnegative number: {value}")
    return result


def fields(line, skip=2):
    result = {}
    for token in line.split()[skip:]:
        key, value = token.split("=", 1)
        require(key not in result, f"duplicate field: {key}")
        result[key] = value
    return result


def parse_strata(stderr, workers, require_full=True):
    require(stderr.endswith("\n"), "missing/truncated stderr")
    decisions, strata = [], []
    begin, complete, pending = None, None, False
    for line in stderr.splitlines():
        if line.startswith("TDD snapshot begin "):
            require(begin is None and complete is None, "duplicate/nested frame")
            begin = fields(line, 3)
        elif line.startswith("TDD snapshot complete "):
            require(begin is not None and complete is None and not pending, "unexpected completion")
            complete = fields(line, 3)
        elif line.startswith("TDD snapshot "):
            raise ValueError("unknown snapshot marker")
        elif line.startswith("TDD decision "):
            require(begin is not None and complete is None and not pending, "out-of-frame/reordered decision")
            decisions.append(fields(line))
            pending = True
        elif line.startswith("TDD stratum "):
            require(begin is not None and complete is None and pending, "out-of-frame/reordered profile")
            strata.append(fields(line))
            pending = False
    require(begin is not None and complete is not None and not pending, "incomplete snapshot frame")
    require(all(k in begin for k in ("plan_count", "expected_count", "scope", "affected_mask", "requested_workers"))
            and all(k in complete for k in ("evaluated_count", "rc")), "missing frame fields")
    plan_count, expected = int(begin["plan_count"]), int(begin["expected_count"])
    mask = int(begin["affected_mask"], 16)
    require(0 <= plan_count <= 0xffffffff and 0 <= mask <= 0xffffffffffffffff, "invalid frame bounds")
    require(int(begin["requested_workers"]) == workers and int(complete["rc"]) == 0, "frame width/error mismatch")
    require(begin["scope"] in ("full", "affected", "stable"), "unknown frame scope")
    require(not require_full or begin["scope"] == "full", "audit requires an initial full evaluation")
    if begin["scope"] == "stable":
        require(expected == 0 and mask == 0, "invalid stable frame")
        indices = []
    else:
        require((mask == 0xffffffffffffffff) == (begin["scope"] == "full"), "inconsistent scope/mask")
        count = (mask & ((1 << min(plan_count, 64)) - 1)).bit_count() + max(0, plan_count - 64)
        require(count == expected == len(strata), "missing complete stratum pairs")
        indices = [i for i in range(plan_count) if i >= 64 or mask & (1 << i)]
    require(expected == int(complete["evaluated_count"]) == len(strata) == len(decisions), "incomplete stratum inventory")
    for index, row, decision in zip(indices, strata, decisions):
        require(all(k in decision for k in DECISION_FLAGS + DECISION_COUNTS + ("rel", "fallback")),
                "missing admission-analysis field")
        require(all(int(decision[k]) in (0, 1) for k in DECISION_FLAGS), "invalid decision boolean")
        require(all(int(decision[k]) >= 0 for k in DECISION_COUNTS), "invalid decision count")
        require(decision["rel"] and decision["fallback"] in FALLBACKS, "unknown admission fallback")
        require(all(k in row for k in COUNTS + TIMINGS), "missing profile field")
        for key in COUNTS:
            row[key] = int(row[key])
            require(row[key] >= 0, f"negative {key}")
        for key in TIMINGS:
            row[key] = number(row[key])
        require(row["idx"] == index, "duplicate/missing/reordered stratum")
        require(row["requested_workers"] == workers and row["rc"] == 0,
                "wrong width or failed stratum")
        require(row["recursive"] in (0, 1) and row["use_tdd"] in (0, 1), "invalid boolean")
        for key in ("rel", "recursive", "use_tdd", "fallback"):
            require(str(row.get(key)) == decision.get(key), "decision/profile mismatch")
        width, tasks, rounds = (row[k] for k in
                                ("selected_workers", "submitted_tasks", "completed_rounds"))
        require(width <= workers and tasks == rounds * width, "incomplete submissions")
        if not rounds:
            require(row["worker_delta_rows"] == 0 and all(row[k] == 0 for k in
                    ("worker_min_ms", "worker_max_ms", "worker_sum_ms")), "worker evidence without a barrier")
        require(row.get("replay") in ("none", "owner_tiny_frontier", "global_read_overflow"),
                "unknown replay")
        require(row.get("strategy") in ("none", "owner", "global_read", "bdx", "aligned", "replicate"),
                "unknown strategy")
        if not row["use_tdd"]:
            require(width == tasks == rounds == 0 and row["strategy"] == "none"
                    and row["replay"] == "none", "stale rejected-stratum execution")
            disposition = "not_eligible" if row["recursive"] else "non_recursive"
        else:
            require(width >= 1 and row["strategy"] != "none" and row["recursive"] == 1,
                    "missing selected strategy")
            require(width > 1 or tasks == rounds == 0, "serial width claimed dispatch")
            disposition = ("adaptively_narrowed" if width == 1 else
                           "tdd_executed" if rounds else "admitted_no_dispatch")
            if row["replay"] != "none":
                disposition = "tdd_then_serial_replay" if rounds else "serial_replay_before_dispatch"
        require(row["replay_rc"] == 0 or row["replay"] == "global_read_overflow",
                "unexplained triggering error")
        if row["replay"] == "owner_tiny_frontier":
            require(row["strategy"] == "owner" and rounds > 0, "invalid owner replay")
        if row["replay"] == "global_read_overflow":
            require(row["strategy"] == "global_read" and row["replay_rc"] > 0, "invalid overflow replay")
        row.update(disposition=disposition, dispatch_width=width if rounds else 0,
                   decision=decision)
    return strata


def parse_result(stdout, workload, workers, tuples, w1_iterations):
    alias = "cspa" if workload == "cspa-fast" else workload
    if stdout.lstrip().startswith("{"):
        row = json.loads(stdout)
        row["median_ms"] = row["wall_time_ms"]["median"]
    else:
        lines = [line.split("\t") for line in stdout.splitlines()
                 if line and not line.startswith("workload\t")]
        require(len(lines) == 1 and len(lines[0]) == 12, "expected exactly one result row")
        name, _, _, width, repeat, _, median, _, rss, count, iterations, status = lines[0]
        require(status == "OK", "benchmark reported failure")
        row = dict(workload=name, workers=int(width), repeat=int(repeat), tuples=int(count),
                   iterations=int(iterations), median_ms=number(median), peak_rss_kb=int(rss))
    require(row["workload"] == alias, "wrong workload (cspa-fast emits cspa)")
    require(row["workers"] == workers and row["repeat"] == 1, "wrong invocation")
    require(row["tuples"] == tuples and row["iterations"] >= 0, "incorrect result")
    require(workers != 1 or row["iterations"] == w1_iterations, "W1 iteration oracle mismatch")
    number(row["median_ms"])
    number(row["peak_rss_kb"])
    return row


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def data_manifest(directory, doop=False):
    paths = directory.glob("*.facts") if doop else directory.rglob("*")
    files = {str(p.relative_to(directory)): sha(p) for p in
             sorted((p for p in paths if p.is_file()), key=lambda p: os.fsencode(str(p)))}
    require(files, f"no input files: {directory}")
    payload = "".join(f"{digest}  {name}\n" for name, digest in files.items())
    return hashlib.sha256(payload.encode()).hexdigest(), files


def capture(command, cwd):
    return subprocess.run(command, cwd=cwd, check=True, capture_output=True,
                          text=True, timeout=30).stdout.strip()


def memory_observation():
    # Keep raw capacity evidence, including cgroup ancestry. No inference that
    # physical MemAvailable guarantees a long-running workload will fit.
    result = {}
    for name in ("/proc/meminfo", "/proc/self/cgroup"):
        path = Path(name)
        if path.exists():
            result[name] = path.read_text()
    for line in result.get("/proc/self/cgroup", "").splitlines():
        if line.startswith("0::"):
            root = Path("/sys/fs/cgroup")
            location = root / line[3:].lstrip("/")
            while location == root or root in location.parents:
                for name in ("memory.max", "memory.current", "cpuset.cpus.effective"):
                    path = location / name
                    if path.is_file():
                        result[str(path)] = path.read_text().strip()
                if location == root:
                    break
                location = location.parent
    return result


def run_process(command, env, prefix, timeout, cwd):
    record = dict(command=command, started_utc=datetime.now(timezone.utc).isoformat())
    with prefix.with_suffix(".stdout").open("w") as stdout, prefix.with_suffix(".stderr").open("w") as stderr:
        try:
            proc = subprocess.run(command, env=env, cwd=cwd, stdout=stdout,
                                  stderr=stderr, timeout=timeout, check=False)
            record.update(returncode=proc.returncode,
                          disposition="completed" if proc.returncode == 0 else "execution_error")
        except subprocess.TimeoutExpired:
            record.update(returncode=None, disposition="timeout", timeout_seconds=timeout)
        except OSError as error:
            record.update(returncode=None, disposition="execution_error", error=str(error))
    record["logs"] = {suffix: {"file": prefix.with_suffix(suffix).name,
                               "sha256": sha(prefix.with_suffix(suffix)),
                               "text": prefix.with_suffix(suffix).read_text(errors="replace")}
                      for suffix in (".stdout", ".stderr")}
    return record


def validate_record(record, workload, workers, tuples, iterations, config):
    if record["disposition"] != "completed":
        return
    try:
        record["result"] = parse_result(record["logs"][".stdout"]["text"], workload,
                                        workers, tuples, iterations)
        if "k_fusion" in record["result"]:
            require(record["result"]["k_fusion"] == (config == "default"), "wrong binary configuration")
        record["strata"] = parse_strata(record["logs"][".stderr"]["text"], workers)
        record["disposition"] = "measured_verified"
    except (ValueError, KeyError, TypeError) as error:
        record.update(disposition="invalid_evidence", error=str(error))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new directory; never overwrite evidence")
    parser.add_argument("--timeout", type=int, default=3600, help="seconds per invocation")
    doop = parser.add_mutually_exclusive_group(required=True)
    doop.add_argument("--include-doop", action="store_true")
    doop.add_argument("--defer-doop", metavar="REASON")
    args = parser.parse_args()
    require(args.timeout > 0, "timeout must be positive")
    require(args.include_doop or args.defer_doop.strip(), "DOOP deferral requires a reason")
    root = Path(__file__).resolve().parents[2]
    build = args.build_dir.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith("WIRELOG_")}
    env.update(WIRELOG_TDD_DECISION_DEBUG="1", WIRELOG_TDD_STRATUM_PROFILE="1", LC_ALL="C")
    info = build / "meson-info"
    inventory = dict(schema=1, observed_utc=datetime.now(timezone.utc).isoformat(),
                     collector_sha256=sha(Path(__file__)),
                     commit=capture(["git", "rev-parse", "HEAD"], root),
                     tree=capture(["git", "rev-parse", "HEAD^{tree}"], root),
                     dirty_paths=capture(["git", "status", "--porcelain"], root),
                     runtime_patch=capture(["git", "diff", "HEAD", "--", "wirelog", "bench",
                                            "meson.build", "meson_options.txt", "subprojects"], root),
                     platform=platform.platform(), cpu_count=os.cpu_count(),
                     cpu=capture(["lscpu"], root) if sys.platform.startswith("linux") else platform.processor(),
                     memory=memory_observation(),
                     environment={k: v for k, v in env.items() if k.startswith(
                         ("WIRELOG_", "OMP_", "MALLOC_", "ASAN_", "UBSAN_")) or k == "LC_ALL"},
                     build_options=json.loads((info / "intro-buildoptions.json").read_text()),
                     compilers=json.loads((info / "intro-compilers.json").read_text()),
                     compile_commands_sha256=sha(build / "compile_commands.json"),
                     binaries={}, data={}, runs=[], controls=[])
    inventory["runtime_patch_sha256"] = hashlib.sha256(inventory["runtime_patch"].encode()).hexdigest()
    for config, name in (("default", "bench_flowlog"), ("unfused", "bench_flowlog_seq")):
        binary = build / "bench" / name
        inventory["binaries"][config] = dict(path=str(binary), sha256=sha(binary))
    pins = root / "scripts/release/downstream-matrix-oracles.tsv"
    inventory["oracles_sha256"] = sha(pins)
    rows = [line.split("\t") for line in pins.read_text().splitlines() if line and not line.startswith("#")]
    require(tuple(row[0] for row in rows) == WORKLOADS, "unexpected workload pins")
    failed = False
    for workload, tuples, iterations, data_path, expected, provenance, acquisition in rows:
        if workload == "doop" and not args.include_doop:
            inventory["runs"].append(dict(workload=workload, disposition="not_measured",
                                          reason=args.defer_doop, configurations=["default", "unfused"],
                                          requested_workers=[1, 8]))
            continue
        try:
            manifest, files = data_manifest(root / data_path, workload == "doop")
            wanted = expected.split(";files:")[-1]
            require(manifest == wanted, f"{workload} input manifest mismatch")
        except (OSError, ValueError) as error:
            inventory["runs"].append(dict(workload=workload, disposition="data_error", reason=str(error)))
            failed = True
            continue
        inventory["data"][workload] = dict(path=data_path, sha256=manifest, files=files,
                                           provenance=provenance, acquisition=acquisition)
        for config in inventory["binaries"]:
            for workers in (1, 8):
                name = f"{workload}-{config}-w{workers}"
                prefix = output / name
                command = [inventory["binaries"][config]["path"], "--workload", workload,
                           "--data-" + ("cspa" if workload == "cspa-fast" else workload),
                           str(root / data_path), "--workers", str(workers), "--repeat", "1",
                           "--format", "json"]
                record = run_process(command, env, prefix, args.timeout, root)
                record.update(workload=workload, configuration=config, requested_workers=workers)
                validate_record(record, workload, workers, int(tuples), int(iterations), config)
                failed |= record["disposition"] != "measured_verified"
                inventory["runs"].append(record)
                (output / "inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
                print(name, record["disposition"], flush=True)
    # Separate synthetic controls, never counted as additional portfolio data.
    graph = root / "bench/data/graph_100.csv"
    inventory["control_data_sha256"] = sha(graph)
    for config, binary in inventory["binaries"].items():
        command = [binary["path"], "--workload", "tdd-bdx", "--data", str(graph),
                   "--workers", "8", "--repeat", "1", "--format", "json"]
        control_env = dict(env, WIRELOG_TDD_MIN_ROWS_PER_WORKER="1")
        record = run_process(command, control_env, output / f"control-{config}", args.timeout, root)
        record.update(configuration=config, environment_override={"WIRELOG_TDD_MIN_ROWS_PER_WORKER": "1"})
        validate_record(record, "tdd-bdx", 8, 4950, 7, config)
        if record["disposition"] == "measured_verified":
            recursive = [r for r in record["strata"] if r["recursive"]]
            if not (len(recursive) == 1 and recursive[0]["dispatch_width"] == 8
                    and recursive[0]["disposition"] == "tdd_executed"):
                record.update(disposition="invalid_evidence", error="control did not dispatch width eight")
        inventory["controls"].append(record)
        failed |= record["disposition"] != "measured_verified"
    for binary in inventory["binaries"].values():
        binary["unchanged_after_audit"] = sha(Path(binary["path"])) == binary["sha256"]
        failed |= not binary["unchanged_after_audit"]
    inventory["iteration_comparisons"] = []
    for workload in WORKLOADS:
        for workers in (1, 8):
            runs = [r for r in inventory["runs"] if r["workload"] == workload
                    and r.get("requested_workers") == workers and r["disposition"] == "measured_verified"]
            if len(runs) == 2:
                values = {r["configuration"]: r["result"]["iterations"] for r in runs}
                inventory["iteration_comparisons"].append(dict(workload=workload, workers=workers,
                    iterations=values, equal=len(set(values.values())) == 1))
                failed |= len(set(values.values())) != 1
    (output / "inventory.json").write_text(json.dumps(inventory, indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
