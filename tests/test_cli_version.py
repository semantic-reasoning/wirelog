#!/usr/bin/env python3
# test_cli_version.py - Regression test for wirelog_cli --version / -v.
#
# Copyright (C) CleverPlant
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Usage: test_cli_version.py <wirelog_cli_executable> <project_version_string>
#
# Asserts (issue #801 acceptance criteria):
#   * --version       exits 0 and stdout contains <version> tagged with
#                     "wirelog_cli"; stderr is empty.
#   * -v              same contract as --version.
#   * --version  and  -v produce byte-identical stdout.
#   * --help          exits 0 and surfaces the version somewhere in its
#                     output (stdout or stderr; --help currently goes to
#                     stderr — that pre-existing behavior is preserved).
#   * --version short-circuits ahead of --workers / file-required checks:
#     "wirelog_cli --version --workers 4 nonexistent.dl" exits 0 with no
#     file read, no plugin load.
#   * Unknown options still fail non-zero with empty stdout (regression
#     guard: the new branch must not turn into a permissive catch-all).
#
# Line endings are normalized to LF so the test passes on Windows runners.

import subprocess
import sys


def _normalize(s: str) -> str:
    return s.replace("\r\n", "\n")


def _run(exe, *args):
    # timeout=30 fails closed if a future regression makes a flag stop
    # short-circuiting and instead block on stdin (e.g. via watch mode).
    return subprocess.run(
        [exe, *args], capture_output=True, text=True, timeout=30)


def _fail(msg):
    sys.stderr.write(msg if msg.endswith("\n") else msg + "\n")
    return 1


def main() -> int:
    if len(sys.argv) != 3:
        return _fail(
            "usage: test_cli_version.py <executable> <project_version>")

    exe, version = sys.argv[1], sys.argv[2]
    version_outputs = {}

    for flag in ("--version", "-v"):
        r = _run(exe, flag)
        if r.returncode != 0:
            return _fail(
                "%s: exit %d (expected 0); stderr=%r"
                % (flag, r.returncode, r.stderr))
        stdout = _normalize(r.stdout)
        if "wirelog_cli" not in stdout:
            return _fail(
                "%s: stdout missing 'wirelog_cli' tag; got %r"
                % (flag, stdout))
        if version not in stdout:
            return _fail(
                "%s: stdout missing version %r; got %r"
                % (flag, version, stdout))
        if r.stderr.strip() != "":
            return _fail(
                "%s: stderr should be empty; got %r" % (flag, r.stderr))
        version_outputs[flag] = stdout

    if version_outputs["--version"] != version_outputs["-v"]:
        return _fail(
            "--version and -v produced different stdout:\n"
            "  --version: %r\n"
            "  -v:        %r"
            % (version_outputs["--version"], version_outputs["-v"]))

    # --help must surface the version too (acceptance criterion #3).
    r = _run(exe, "--help")
    if r.returncode != 0:
        return _fail("--help: exit %d (expected 0)" % r.returncode)
    help_out = _normalize(r.stdout) + _normalize(r.stderr)
    if version not in help_out:
        return _fail(
            "--help: version %r not found in output; "
            "stdout=%r stderr=%r"
            % (version, r.stdout, r.stderr))

    # --version must short-circuit before file-required / --workers checks.
    r = _run(exe, "--version", "--workers", "4", "nonexistent-file.dl")
    if r.returncode != 0:
        return _fail(
            "--version with extra args: exit %d (expected 0); stderr=%r"
            % (r.returncode, r.stderr))
    if version not in _normalize(r.stdout):
        return _fail(
            "--version with extra args: stdout missing version; got %r"
            % r.stdout)

    # Unknown options must still fail non-zero with empty stdout.
    r = _run(exe, "--definitely-not-a-real-flag")
    if r.returncode == 0:
        return _fail(
            "unknown option: expected non-zero exit, got 0; stdout=%r"
            % r.stdout)
    if r.stdout.strip() != "":
        return _fail(
            "unknown option: stdout should be empty, got %r" % r.stdout)

    return 0


if __name__ == "__main__":
    sys.exit(main())
