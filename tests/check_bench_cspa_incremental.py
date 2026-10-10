#!/usr/bin/env python3
"""Issue #2114: run the CSPA incremental bench and check its models.

Both cspa_incr and cspa_incr_overlap compare the snapshot after an
incremental insert with a fresh full evaluation. cspa_incr prints FAIL
when the two differ. The overlap row goes through the portfolio parser,
which rejects a FAIL status, a differing tuple count or digest, and a
missing row. Speedup is not checked.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "perf" / "run-flowlog-portfolio.py"


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: check_bench_cspa_incremental.py BENCH_FLOWLOG CSPA_DIR WORKERS")
        return 2
    spec = importlib.util.spec_from_file_location("flowlog_portfolio", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    portfolio = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(portfolio)

    proc = subprocess.run(
        [
            sys.argv[1],
            "--workload",
            "cspa",
            "--data-cspa",
            sys.argv[2],
            "--workers",
            sys.argv[3],
            "--repeat",
            "1",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    sys.stderr.write(proc.stderr[-4000:])
    if proc.returncode != 0:
        print(f"bench_flowlog exited {proc.returncode}")
        print(proc.stdout)
        return 1
    rows = [line.split("\t") for line in proc.stdout.splitlines() if line.startswith("cspa_incr\t")]
    # cspa_incr prints SLOW below a 2x speedup and FAIL only when its model
    # differs from full evaluation or the run failed. Speed is not checked here.
    if len(rows) != 1 or rows[0][-1] not in ("OK", "SLOW"):
        print("cspa_incr row missing or failed")
        print(proc.stdout)
        return 1
    overlap, error = portfolio.parse_cspa_overlap_tsv(proc.stdout)
    if error is not None:
        print(f"invalid cspa_incr_overlap row: {error}")
        print(proc.stdout)
        return 1
    print(f"ok: cspa_incr={rows[0][-1]} overlap_tuples={overlap['tuples_incr']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
