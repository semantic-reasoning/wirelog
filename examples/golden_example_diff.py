#!/usr/bin/env python3
"""Run an example's Datalog program and diff its outputs against the goldens.

The examples address their inputs and outputs with paths relative to the
repository root -- `.input record(filename="examples/04-hash-functions/...")`
-- which is why they were never wired into the `examples` test suite: running
one in place rewrites the very `*_output.csv` files that are supposed to be
the expected values, so a regression would silently update its own golden and
pass.

This runner copies the example directory into a temporary tree that mirrors
the repository layout, deletes the outputs from the copy, runs the CLI with
that tree as its working directory, and diffs what the run produced against
the committed files.  The source tree is only ever read.

Usage:
  golden_example_diff.py --cli PATH --root PATH --dir examples/NN-name
                         --program NAME.dl --golden OUT.csv [--golden ...]
                         [--same A.csv:B.csv]

  --golden  Output file that must match the committed copy of itself.
  --same    Two files in the produced tree that must match each other; used
            for the `diff unique_records_output.csv expected_deduplicated.csv`
            check the example's README documents.
"""

import argparse
import filecmp
import os
import shutil
import subprocess
import sys
import tempfile


def fail(msg):
    print("FAIL: %s" % msg, file=sys.stderr)
    return 1


def diff(expected, actual):
    """Print a unified diff of two text files, best effort."""
    import difflib
    try:
        with open(expected, "r", encoding="utf-8", errors="replace") as f:
            a = f.readlines()
        with open(actual, "r", encoding="utf-8", errors="replace") as f:
            b = f.readlines()
    except OSError as exc:
        print("  (could not read for diff: %s)" % exc, file=sys.stderr)
        return
    for line in difflib.unified_diff(a, b, fromfile=expected,
                                     tofile=actual, lineterm="\n"):
        sys.stderr.write(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cli", required=True)
    ap.add_argument("--root", required=True)
    ap.add_argument("--dir", required=True,
                    help="example directory, relative to --root")
    ap.add_argument("--program", required=True,
                    help="Datalog program, relative to --dir")
    ap.add_argument("--golden", action="append", default=[],
                    help="output file, relative to --dir")
    ap.add_argument("--same", action="append", default=[],
                    help="A:B, both relative to --dir")
    args = ap.parse_args()

    src_dir = os.path.join(args.root, args.dir)
    if not os.path.isdir(src_dir):
        return fail("no such example directory: %s" % src_dir)
    for name in args.golden:
        if not os.path.isfile(os.path.join(src_dir, name)):
            return fail("golden %s is missing from %s" % (name, args.dir))

    with tempfile.TemporaryDirectory(prefix="wl-example-") as tmp:
        work_dir = os.path.join(tmp, args.dir)
        os.makedirs(os.path.dirname(work_dir), exist_ok=True)
        shutil.copytree(src_dir, work_dir)

        # Remove the goldens from the working copy so a program that writes
        # nothing fails instead of inheriting the committed answer.
        for name in args.golden:
            os.remove(os.path.join(work_dir, name))

        proc = subprocess.run(
            [args.cli, os.path.join(args.dir, args.program)],
            cwd=tmp, capture_output=True, text=True)
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout)
            sys.stderr.write(proc.stderr)
            return fail("%s exited %d" % (args.program, proc.returncode))

        failures = 0
        for name in args.golden:
            produced = os.path.join(work_dir, name)
            expected = os.path.join(src_dir, name)
            if not os.path.isfile(produced):
                failures += fail("%s was not written" % name)
                continue
            if not filecmp.cmp(expected, produced, shallow=False):
                failures += fail("%s does not match the committed golden"
                                 % name)
                diff(expected, produced)

        for pair in args.same:
            a_name, _, b_name = pair.partition(":")
            a = os.path.join(work_dir, a_name)
            b = os.path.join(work_dir, b_name)
            if not (os.path.isfile(a) and os.path.isfile(b)):
                failures += fail("%s: one side is missing" % pair)
                continue
            if not filecmp.cmp(a, b, shallow=False):
                failures += fail("%s and %s differ" % (a_name, b_name))
                diff(b, a)

        if failures:
            return 1

    print("OK: %s reproduced %d golden file(s)"
          % (args.dir, len(args.golden)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
