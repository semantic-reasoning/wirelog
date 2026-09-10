#!/usr/bin/env python3
"""Check that WL_MEM_REPORT is diagnostic-only and byte fields are stable."""
import os
from pathlib import Path
import re
import subprocess
import sys


def run(binary, enabled, batch_bytes=None):
    env = os.environ.copy()
    env.pop("WL_MEM_REPORT", None)
    env.pop("WIRELOG_JOIN_BATCH_BYTES", None)
    if enabled:
        env["WL_MEM_REPORT"] = "1"
    if batch_bytes is not None:
        env["WIRELOG_JOIN_BATCH_BYTES"] = str(batch_bytes)
    return subprocess.run([binary], capture_output=True, text=True,
                          env=env, timeout=300, check=False)


def main():
    if len(sys.argv) < 2:
        print("usage: test_mem_report_contract.py BINARY...", file=sys.stderr)
        return 2
    for name in sys.argv[1:]:
        binary = str(Path(name))
        off = run(binary, False)
        on = run(binary, True)
        on_batch = run(binary, True, 4096)
        if off.returncode != on.returncode or off.stdout != on.stdout:
            raise AssertionError(f"WL_MEM_REPORT changed result output: {binary}")
        if "[wirelog mem]" in off.stderr:
            raise AssertionError(f"report leaked into off-mode stderr: {binary}")
        if "[wirelog mem]" not in on.stderr:
            raise AssertionError(f"report missing in on-mode stderr: {binary}")
        for field in ("budget_bytes=", "current_bytes=", "peak_bytes="):
            if field not in on.stderr:
                raise AssertionError(f"missing {field}: {binary}")
        if "join_batch " in on.stderr:
            raise AssertionError(f"disabled join report leaked into output: {binary}")
        if on_batch.returncode != on.returncode or on_batch.stdout != on.stdout:
            raise AssertionError(f"join report changed result output: {binary}")
        join_lines = [line for line in on_batch.stderr.splitlines()
                      if line.startswith("[wirelog mem] join_batch ")]
        if not join_lines or any(not re.search(
                r"fallback_count=\d+ last_reason=\S+ rows_per_batch=\d+$",
                line) for line in join_lines):
            raise AssertionError(f"invalid bounded join report: {binary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
