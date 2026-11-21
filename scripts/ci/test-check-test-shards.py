#!/usr/bin/env python3
"""Selftest for scripts/ci/check-test-shards.py (#2397).

The checker is the only evidence that a sharded `meson test --slice K/N`
lane ran every registered test exactly once, so each rejection below is a
way a shard could silently drop or duplicate suite work.  The checker takes
every CI-provided value (commit SHA, run attempt, step-summary path) as an
explicit argument and reads no GitHub environment variable, so this test --
which itself runs inside CI jobs -- cannot write into a real job summary or
depend on the hosting run.  The Meson stand-in is a Python script invoked
through sys.executable, never a shebang, so the test runs on the MSVC leg.
"""

from pathlib import Path
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parent / "check-test-shards.py"
SPEC = importlib.util.spec_from_file_location("check_test_shards", SCRIPT)
shards = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shards)

SHA = "0123456789abcdef0123456789abcdef01234567"
FULL = ["unit - wirelog:alpha", "abi - wirelog:beta", "wirelog:gamma",
        "unit+tsan - wirelog:delta", "tidy - wirelog:epsilon",
        "abi - wirelog:zeta", "unit - wirelog:eta"]

STUB_MESON = r'''
import sys
args = sys.argv[1:]
full = [line for line in open(sys.argv[1], encoding="utf-8").read().splitlines()]
args = args[1:]
if "--version" in args:
    print("1.10.1")
    sys.exit(0)
assert args[:1] == ["test"] and "--list" in args, args
names = full
if "--slice" in args:
    k, n = map(int, args[args.index("--slice") + 1].split("/"))
    names = full[k - 1::n]
for name in names:
    print(name)
'''


def temp_directory():
    # CI fixtures follow the perf helpers' HOME temp policy on every platform.
    parent = (Path.home() / ".tmp").resolve()
    parent.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix="ci-test-shards-", dir=parent)


def write_lines(path, lines):
    path.write_text("".join(line + "\n" for line in lines), encoding="utf-8",
                    newline="")


def result_row(name, start, result="OK", duration=2.0):
    return {"name": name, "result": result, "starttime": start,
            "duration": duration, "returncode": 0, "command": ["x"],
            "env": {"SECRET": "must-not-be-copied"}, "stdo": "noise"}


class Fixture:
    """Build an N-shard evidence tree as the workflow would leave it."""

    def __init__(self, root, count=3, full=FULL):
        self.root = Path(root)
        self.count = count
        self.full = list(full)

    def shard_dir(self, k):
        return self.root / "evidence" / f"linux-clang-shard-{k}"

    def build(self, mutate=None):
        for k in range(1, self.count + 1):
            out = self.shard_dir(k)
            out.mkdir(parents=True)
            assigned = self.full[k - 1::self.count]
            write_lines(out / "full.txt", self.full)
            write_lines(out / "slice.txt", assigned)
            manifest = {"shard": k, "count": self.count, "sha": SHA,
                        "run_attempt": "1", "meson_version": "1.10.1",
                        "full_tests": len(self.full),
                        "slice_tests": len(assigned)}
            (out / "manifest.json").write_text(json.dumps(manifest),
                                               encoding="utf-8")
            rows = [shards.trim(result_row(n, 1000.0 + 3 * i))
                    for i, n in enumerate(assigned)]
            write_lines(out / "results.jsonl", [json.dumps(r) for r in rows])
        if mutate:
            mutate(self)
        return self.root / "evidence"


def run_cli(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *map(str, args)],
                          capture_output=True, text=True, encoding="utf-8",
                          env=clean_env())


def clean_env():
    import os
    env = dict(os.environ)
    for key in ("GITHUB_STEP_SUMMARY", "GITHUB_SHA", "GITHUB_RUN_ATTEMPT"):
        env.pop(key, None)
    return env


class AggregateTests(unittest.TestCase):
    def setUp(self):
        self._tmp = temp_directory()
        self.tmp = Path(self._tmp.name)
        self.fixture = Fixture(self.tmp)

    def tearDown(self):
        self._tmp.cleanup()

    def aggregate(self, evidence, count=3, sha=SHA, summary=None):
        args = ["aggregate", "--count", count, "--sha", sha]
        if summary is not None:
            args += ["--summary", summary]
        return run_cli(*args, evidence)

    def assert_rejected(self, mutate, message, count=3):
        evidence = self.fixture.build(mutate)
        proc = self.aggregate(evidence, count=count)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn(message, proc.stderr)

    def test_complete_partition_passes_and_reports(self):
        evidence = self.fixture.build()
        summary = self.tmp / "summary.md"
        summary.write_text("previous\n", encoding="utf-8")
        proc = self.aggregate(evidence, summary=summary)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("7 tests in 3 shards", proc.stdout)
        self.assertIn("| 1 | 3 | 3 |", proc.stdout)
        self.assertIn("| 3 | 2 | 2 |", proc.stdout)
        text = summary.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("previous\n"), "summary must be appended")
        self.assertIn("| 1 | 3 | 3 |", text)

    def test_no_summary_written_without_argument(self):
        evidence = self.fixture.build()
        proc = self.aggregate(evidence)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir()), ["evidence"])

    def test_test_missing_from_every_shard(self):
        def drop(f):
            sd = f.shard_dir(2)
            write_lines(sd / "slice.txt", f.full[1::3][:-1])
        self.assert_rejected(drop, "slice")

    def test_missing_execution_is_rejected(self):
        def drop(f):
            sd = f.shard_dir(1)
            rows = (sd / "results.jsonl").read_text(encoding="utf-8").splitlines()
            write_lines(sd / "results.jsonl", rows[:-1])
        self.assert_rejected(drop, "not executed")

    def test_duplicate_execution_is_rejected(self):
        def dup(f):
            sd = f.shard_dir(1)
            rows = (sd / "results.jsonl").read_text(encoding="utf-8").splitlines()
            write_lines(sd / "results.jsonl", rows + rows[:1])
        self.assert_rejected(dup, "more than once")

    def test_extra_execution_is_rejected(self):
        def extra(f):
            sd = f.shard_dir(3)
            rows = (sd / "results.jsonl").read_text(encoding="utf-8").splitlines()
            rows.append(json.dumps(shards.trim(result_row(f.full[0], 5.0))))
            write_lines(sd / "results.jsonl", rows)
        self.assert_rejected(extra, "not assigned")

    def test_test_assigned_to_two_shards(self):
        def overlap(f):
            sd = f.shard_dir(2)
            write_lines(sd / "slice.txt", f.full[1::3] + [f.full[0]])
        self.assert_rejected(overlap, "slice")

    def test_shards_disagree_on_full_list(self):
        def disagree(f):
            write_lines(f.shard_dir(3) / "full.txt", f.full[:-1])
        self.assert_rejected(disagree, "full test list differs")

    def test_duplicate_full_names(self):
        self.fixture.full = FULL + [FULL[0]]
        self.assert_rejected(None, "duplicate")

    def test_empty_full_list(self):
        self.fixture.full = []
        self.fixture.count = 3
        self.assert_rejected(None, "empty")

    def test_missing_shard_directory(self):
        import shutil
        self.assert_rejected(lambda f: shutil.rmtree(f.shard_dir(2)),
                             "expected shards 1..3")

    def test_extra_shard_directory(self):
        self.assert_rejected(None, "expected shards 1..2", count=2)

    def test_folder_suffix_must_match_manifest(self):
        def swap(f):
            a, b, t = f.shard_dir(1), f.shard_dir(2), f.root / "evidence" / "tmp"
            a.rename(t)
            b.rename(a)
            t.rename(b)
        self.assert_rejected(swap, "folder")

    def test_count_mismatch(self):
        def recount(f):
            path = f.shard_dir(1) / "manifest.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["count"] = 4
            path.write_text(json.dumps(data), encoding="utf-8")
        self.assert_rejected(recount, "count")

    def test_sha_mismatch(self):
        def resha(f):
            path = f.shard_dir(3) / "manifest.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            data["sha"] = "f" * 40
            path.write_text(json.dumps(data), encoding="utf-8")
        self.assert_rejected(resha, "sha")

    def test_failed_result_is_rejected(self):
        def fail(f):
            sd = f.shard_dir(2)
            rows = [json.loads(r) for r in
                    (sd / "results.jsonl").read_text(encoding="utf-8").splitlines()]
            rows[0]["result"] = "TIMEOUT"
            write_lines(sd / "results.jsonl", [json.dumps(r) for r in rows])
        self.assert_rejected(fail, "TIMEOUT")

    def test_missing_results_is_rejected(self):
        self.assert_rejected(lambda f: (f.shard_dir(1) / "results.jsonl").unlink(),
                             "results.jsonl")

    def test_corrupt_results_is_rejected(self):
        def corrupt(f):
            (f.shard_dir(1) / "results.jsonl").write_text("{not json\n",
                                                          encoding="utf-8")
        self.assert_rejected(corrupt, "results.jsonl")


class PlanVerifyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = temp_directory()
        self.tmp = Path(self._tmp.name)
        self.listing = self.tmp / "listing.txt"
        write_lines(self.listing, FULL)
        self.stub = self.tmp / "meson_stub.py"
        self.stub.write_text(STUB_MESON, encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def meson(self):
        return ["--meson", sys.executable, "--meson", self.stub,
                "--meson", self.listing]

    def plan(self, k, count=3, out=None):
        out = out or self.tmp / f"shard-{k}"
        return run_cli("plan", "--builddir", self.tmp, "--shard", k,
                       "--count", count, "--sha", SHA, "--run-attempt", "2",
                       "--out", out, *self.meson()), out

    def write_testlog(self, names, extra=None):
        path = self.tmp / "testlog.json"
        rows = [result_row(n, 10.0 + i) for i, n in enumerate(names)]
        write_lines(path, [json.dumps(r) for r in rows] + (extra or []))
        return path

    def test_plan_records_slice_and_manifest(self):
        proc, out = self.plan(2)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual((out / "slice.txt").read_text(encoding="utf-8").splitlines(),
                         FULL[1::3])
        self.assertEqual((out / "full.txt").read_text(encoding="utf-8").splitlines(),
                         FULL)
        manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["shard"], 2)
        self.assertEqual(manifest["count"], 3)
        self.assertEqual(manifest["sha"], SHA)
        self.assertEqual(manifest["run_attempt"], "2")
        self.assertEqual(manifest["meson_version"], "1.10.1")
        self.assertIn("shard 2/3: 2 of 7 tests", proc.stdout)

    def test_plan_rejects_bad_shard_index(self):
        for k, n in ((0, 3), (4, 3), (1, 0)):
            with self.subTest(k=k, n=n):
                proc, _ = self.plan(k, count=n, out=self.tmp / f"bad-{k}-{n}")
                self.assertNotEqual(proc.returncode, 0)

    def test_plan_rejects_more_shards_than_tests(self):
        proc, _ = self.plan(1, count=len(FULL) + 1)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("exceeds", proc.stderr)

    def test_verify_accepts_exact_execution_and_trims_log(self):
        _, out = self.plan(1)
        log = self.write_testlog(FULL[0::3])
        proc = run_cli("verify", "--evidence", out, "--testlog", log)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        rows = [json.loads(r) for r in
                (out / "results.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual([r["name"] for r in rows], FULL[0::3])
        self.assertEqual(set(rows[0]), set(shards.KEPT_FIELDS))
        self.assertNotIn("must-not-be-copied",
                         (out / "results.jsonl").read_text(encoding="utf-8"))

    def test_verify_rejects_partial_log_but_keeps_evidence(self):
        _, out = self.plan(1)
        log = self.write_testlog(FULL[0::3][:-1])
        proc = run_cli("verify", "--evidence", out, "--testlog", log)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("not executed", proc.stderr)
        self.assertEqual(len((out / "results.jsonl").read_text(
            encoding="utf-8").splitlines()), 2)

    def test_verify_rejects_missing_log(self):
        _, out = self.plan(1)
        proc = run_cli("verify", "--evidence", out, "--testlog",
                       self.tmp / "absent.json")
        self.assertEqual(proc.returncode, 1)
        self.assertIn("testlog", proc.stderr)

    def test_verify_rejects_corrupt_log(self):
        _, out = self.plan(1)
        log = self.write_testlog(FULL[0::3], extra=["{truncated"])
        proc = run_cli("verify", "--evidence", out, "--testlog", log)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("line 4", proc.stderr)

    def test_end_to_end_partition(self):
        evidence = self.tmp / "evidence"
        for k in (1, 2, 3):
            out = evidence / f"linux-clang-shard-{k}"
            proc, _ = self.plan(k, out=out)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            log = self.write_testlog(FULL[k - 1::3])
            proc = run_cli("verify", "--evidence", out, "--testlog", log)
            self.assertEqual(proc.returncode, 0, proc.stderr)
        proc = run_cli("aggregate", "--count", 3, "--sha", SHA, evidence)
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
