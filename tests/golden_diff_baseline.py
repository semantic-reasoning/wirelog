#!/usr/bin/env python3
# golden_diff_baseline.py - Golden-output comparator for baseline fixtures.
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
#
# Usage: golden_diff_baseline.py <executable> <program.dl> <expected.txt>
#
# Runs the executable with the .dl file as argument, captures stdout, and
# compares it line-by-line against the expected file.  On mismatch, prints
# a unified diff and exits non-zero.  Line endings are normalized to LF.

import difflib
import subprocess
import sys


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: golden_diff_baseline.py <executable> <program.dl> "
              "<expected.txt>", file=sys.stderr)
        return 2

    exe, dl_path, expected_path = sys.argv[1], sys.argv[2], sys.argv[3]

    result = subprocess.run(
        [exe, dl_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(
            "executable exited with code %d\n" % result.returncode)
        sys.stderr.write(result.stderr)
        return result.returncode

    actual = result.stdout.replace("\r\n", "\n").splitlines(keepends=True)
    with open(expected_path, "r", encoding="utf-8", newline="") as fh:
        expected = fh.read().replace("\r\n", "\n").splitlines(keepends=True)

    if actual == expected:
        return 0

    sys.stdout.writelines(
        difflib.unified_diff(
            expected, actual,
            fromfile="expected",
            tofile="actual",
        )
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
