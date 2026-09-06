#!/usr/bin/env python3
import json
import os
import subprocess
import sys


def extract_json(stdout):
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start < 0 or end < start:
        raise AssertionError("bench_flowlog did not print a JSON object")
    return json.loads(stdout[start : end + 1])


def main():
    if len(sys.argv) != 3:
        print("usage: validate_bench_flowlog_tdd_json.py BENCH_FLOWLOG DATA")
        return 2

    env = os.environ.copy()
    for key in list(env):
        if key.startswith("WIRELOG_TDD_"):
            del env[key]
    env.pop("WIRELOG_RADIX_BENCH_LOG", None)
    env["WIRELOG_TDD_MIN_ROWS_PER_WORKER"] = "1"
    env["WIRELOG_TDD_STRATUM_PROFILE"] = "1"

    proc = subprocess.run(
        [
            sys.argv[1],
            "--workload",
            "tdd-bdx",
            "--data",
            sys.argv[2],
            "--workers",
            "8",
            "--repeat",
            "1",
            "--format",
            "json",
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        timeout=20,
    )
    if proc.returncode != 0:
        print(proc.stdout, end="")
        print(proc.stderr, end="", file=sys.stderr)
        return proc.returncode

    if "[radix-bench" in proc.stderr:
        raise AssertionError("radix benchmark logs should be opt-in")

    row = extract_json(proc.stdout)
    assert row["tuples"] == 4950, row
    records = [dict(token.split("=", 1) for token in line.split()[2:])
               for line in proc.stderr.splitlines()
               if line.startswith("TDD stratum ")]
    recursive = [r for r in records if r["recursive"] == "1"]
    assert len(recursive) == 1, records
    record = recursive[0]
    assert record.get("strategy") == "bdx", record
    assert record["selected_workers"] == "8", record
    assert int(record["completed_rounds"]) > 0, record
    assert int(record["submitted_tasks"]) == 8 * int(record["completed_rounds"]), record
    assert record["replay"] == "none" and record["rc"] == "0", record

    # The ordinary benchmark must keep identical results without diagnostics.
    quiet_env = env.copy()
    del quiet_env["WIRELOG_TDD_STRATUM_PROFILE"]
    quiet = subprocess.run(proc.args, capture_output=True, text=True,
                           env=quiet_env, timeout=20, check=True)
    assert "TDD stratum " not in quiet.stderr, quiet.stderr
    assert extract_json(quiet.stdout)["tuples"] == 4950

    adaptive_env = env.copy()
    adaptive_env["WIRELOG_TDD_MIN_ROWS_PER_WORKER"] = "1000000000"
    adaptive = subprocess.run(proc.args, capture_output=True, text=True,
                              env=adaptive_env, timeout=20, check=True)
    assert extract_json(adaptive.stdout)["tuples"] == 4950
    assert "selected_workers=1 submitted_tasks=0 completed_rounds=0" in adaptive.stderr
    tdd_phase = row.get("tdd_phase_ms")
    exchange_phase = row.get("tdd_exchange_phase_ms")
    if not isinstance(tdd_phase, dict):
        raise AssertionError("missing tdd_phase_ms object")
    if not isinstance(exchange_phase, dict):
        raise AssertionError("missing tdd_exchange_phase_ms object")

    exchange = tdd_phase.get("exchange")
    if not isinstance(exchange, (int, float)) or exchange < 0:
        raise AssertionError("tdd_phase_ms.exchange must be nonnegative")

    keys = ("matrix", "coordinator", "scatter", "gather", "broadcast")
    subtotal = 0.0
    positive = False
    for key in keys:
        value = exchange_phase.get(key)
        if not isinstance(value, (int, float)):
            raise AssertionError(f"missing numeric exchange phase: {key}")
        if value < 0:
            raise AssertionError(f"negative exchange phase: {key}")
        subtotal += float(value)
        positive = positive or value > 0

    if exchange > 0.01 and not positive:
        raise AssertionError("exchange sub-counters are all zero")

    # The parent exchange timer has small unclassified overhead around the
    # sub-counters.  Printed millisecond precision can also round tiny runs.
    tolerance = max(1.0, exchange * 0.25)
    if subtotal > exchange + tolerance:
        raise AssertionError(
            f"exchange sub-counters exceed parent: {subtotal} > {exchange}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
