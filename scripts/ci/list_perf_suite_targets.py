#!/usr/bin/env python3
"""List executable targets for Meson tests in the perf suite."""

import json
from pathlib import Path
import subprocess
import sys


def perf_suite_targets(tests: list[dict], targets: list[dict]) -> list[str]:
    available = {target.get("name") for target in targets}
    names = set()
    for test in tests:
        suites = test.get("suite", [])
        if any(suite.rsplit(":", 1)[-1] == "perf" for suite in suites):
            command = test.get("cmd", [])
            if command:
                names.add(Path(command[0]).name)
    names.add("bench_flowlog")
    return sorted(names & available)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} BUILDDIR", file=sys.stderr)
        return 2
    builddir = sys.argv[1]
    tests = json.loads(subprocess.check_output(
        ["meson", "introspect", builddir, "--tests"], text=True,
        encoding="utf-8"))
    targets = json.loads(subprocess.check_output(
        ["meson", "introspect", builddir, "--targets"], text=True,
        encoding="utf-8"))
    for target in perf_suite_targets(tests, targets):
        print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
