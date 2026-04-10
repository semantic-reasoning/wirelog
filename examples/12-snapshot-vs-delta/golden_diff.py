#!/usr/bin/env python3
# golden_diff.py - Portable golden-output comparator for example 12.
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
#
# Usage: golden_diff.py <executable> <expected.txt>
#
# Runs the executable, captures stdout, and compares it line-by-line
# against the expected file.  On mismatch, prints a unified diff and
# exits non-zero.  Trailing whitespace on each line is preserved; line
# endings are normalized to LF so Windows builds are not spuriously
# flagged.

import difflib
import subprocess
import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: golden_diff.py <executable> <expected.txt>",
              file=sys.stderr)
        return 2

    exe, expected_path = sys.argv[1], sys.argv[2]

    result = subprocess.run(
        [exe],
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
            fromfile="expected.txt",
            tofile="actual",
            lineterm="",
        )
    )
    sys.stdout.write("\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
