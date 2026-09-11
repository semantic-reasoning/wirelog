#!/usr/bin/env python3
"""Check that WL_MEM_REPORT is diagnostic-only and byte fields are stable."""
import os
from pathlib import Path
import subprocess
import sys
import time


class ContractFailure(Exception):
    """An expected child-process or report-contract failure."""


def output_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def diagnostic(binary, enabled, status, stdout=None, stderr=None):
    mode = "on" if enabled else "off"
    return (f"{binary} WL_MEM_REPORT={mode}: {status}\n"
            f"stdout:\n{output_text(stdout)}\n"
            f"stderr:\n{output_text(stderr)}")


def run(binary, enabled):
    env = os.environ.copy()
    env.pop("WL_MEM_REPORT", None)
    if enabled:
        env["WL_MEM_REPORT"] = "1"
    started = time.monotonic()
    try:
        result = subprocess.run([binary], capture_output=True, text=True,
                                env=env, timeout=300, check=False)
    except subprocess.TimeoutExpired as exc:
        raise ContractFailure(diagnostic(
            binary, enabled,
            f"timeout after {exc.timeout}s (elapsed {time.monotonic() - started:.2f}s)",
            exc.stdout, exc.stderr)) from None
    except OSError as exc:
        raise ContractFailure(diagnostic(
            binary, enabled,
            f"execution failed: {exc} (elapsed {time.monotonic() - started:.2f}s)")) from None
    if result.returncode != 0:
        raise ContractFailure(diagnostic(
            binary, enabled,
            f"rc={result.returncode} (elapsed {time.monotonic() - started:.2f}s)",
            result.stdout, result.stderr))
    return result


def check_contract(binary):
    off = run(binary, False)
    on = run(binary, True)
    reason = None
    if off.stdout != on.stdout:
        reason = "WL_MEM_REPORT changed result output"
    elif "[wirelog mem]" in off.stderr:
        reason = "report leaked into off-mode stderr"
    elif "[wirelog mem]" not in on.stderr:
        reason = "report missing in on-mode stderr"
    else:
        for field in ("budget_bytes=", "current_bytes=", "peak_bytes="):
            if field not in on.stderr:
                reason = f"missing {field}"
                break
    if reason:
        raise ContractFailure(
            f"{reason}\n"
            + diagnostic(binary, False, f"rc={off.returncode}", off.stdout, off.stderr)
            + "\n"
            + diagnostic(binary, True, f"rc={on.returncode}", on.stdout, on.stderr))


def main():
    if len(sys.argv) < 2:
        print("usage: test_mem_report_contract.py BINARY...", file=sys.stderr)
        return 2
    try:
        for name in sys.argv[1:]:
            check_contract(str(Path(name)))
    except ContractFailure as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
