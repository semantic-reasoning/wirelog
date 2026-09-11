#!/usr/bin/env python3
"""Exercise the memory-report wrapper without running native workloads."""
import contextlib
import io
import os
import subprocess
import unittest
from unittest import mock

import test_mem_report_contract as contract


REPORT = "[wirelog mem] budget_bytes=1 current_bytes=0 peak_bytes=1\n"


def child(rc=0, stdout="result\n", stderr=""):
    return subprocess.CompletedProcess(["fused"], rc, stdout, stderr)


class MemReportContractTest(unittest.TestCase):
    def invoke(self, outcomes, binaries=("fused",)):
        stderr = io.StringIO()
        with mock.patch.object(contract.sys, "argv", ["wrapper", *binaries]), \
                mock.patch.object(contract.subprocess, "run",
                                  side_effect=outcomes) as run, \
                contextlib.redirect_stderr(stderr):
            rc = contract.main()
        return rc, stderr.getvalue(), run

    def assert_failure(self, outcomes, mode, *details):
        rc, stderr, run = self.invoke(outcomes)
        self.assertEqual(rc, 1)
        for detail in ("fused", mode, *details):
            self.assertIn(detail, stderr)
        self.assertNotIn("Traceback", stderr)
        return run

    def test_normal_four_invocations_and_environment(self):
        environment = {"WL_MEM_REPORT": "inherited", "KEEP_ME": "value"}
        with mock.patch.dict(os.environ, environment, clear=True):
            rc, stderr, run = self.invoke(
                [child(), child(stderr=REPORT)] * 2, ("fused", "unfused"))
            self.assertEqual(dict(os.environ), environment)
        self.assertEqual((rc, stderr), (0, ""))
        self.assertEqual(run.call_count, 4)
        for call, binary, enabled in zip(
                run.call_args_list, ("fused", "fused", "unfused", "unfused"),
                (False, True, False, True)):
            expected_env = {"KEEP_ME": "value"}
            if enabled:
                expected_env["WL_MEM_REPORT"] = "1"
            self.assertEqual(call, mock.call(
                [binary], capture_output=True, text=True, env=expected_env,
                timeout=300, check=False))

    def test_timeout_partial_output(self):
        for mode in ("off", "on"):
            for value in (b"partial\xff", "partial text", None):
                with self.subTest(mode=mode, value=value):
                    error = subprocess.TimeoutExpired(
                        ["fused"], 300, output=value, stderr=value)
                    outcomes = ([child()] if mode == "on" else []) + [error]
                    details = ["timeout", "300", "stdout", "stderr"]
                    if value is not None:
                        details.append("partial")
                    self.assert_failure(outcomes, mode, *details)

    def test_failed_child(self):
        for mode in ("off", "on"):
            with self.subTest(mode=mode):
                outcomes = ([child()] if mode == "on" else []) + [
                    child(7, "partial result", "child error")]
                self.assert_failure(outcomes, mode, "rc=7", "partial result",
                                    "child error")

    def test_equal_nonzero_is_failure(self):
        self.assert_failure([child(7), child(7, stderr=REPORT)], "off", "rc=7")

    def test_missing_report(self):
        self.assert_failure([child(), child()], "on", "report missing", "rc=0")

    def test_missing_fields(self):
        for field in ("budget_bytes=", "current_bytes=", "peak_bytes="):
            with self.subTest(field=field):
                self.assert_failure(
                    [child(), child(stderr=REPORT.replace(field, "other="))],
                    "on", "missing", field, "rc=0")

    def test_stdout_mismatch(self):
        self.assert_failure([child(stdout="before"),
                             child(stdout="after", stderr=REPORT)],
                            "on", "changed result output", "before", "after")

    def test_off_report(self):
        self.assert_failure([child(stderr=REPORT), child(stderr=REPORT)],
                            "off", "report leaked", "rc=0")

    def test_oserror(self):
        for mode in ("off", "on"):
            with self.subTest(mode=mode):
                outcomes = ([child()] if mode == "on" else []) + [
                    OSError("cannot execute")]
                self.assert_failure(outcomes, mode, "cannot execute")


if __name__ == "__main__":
    unittest.main()
