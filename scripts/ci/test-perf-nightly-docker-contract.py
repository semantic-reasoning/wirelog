#!/usr/bin/env python3
"""Protect the strict local-image perf-nightly owner contract."""

from pathlib import Path
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (ROOT / ".github/workflows/perf-nightly.yml").read_text(encoding="utf-8")
RUNNER = (ROOT / "scripts/ci/run-perf-nightly-linux.sh").read_text(encoding="utf-8")
CHECKER = (ROOT / "scripts/ci/check-nightly-crdt-cspa.sh").read_text(encoding="utf-8")
CAPTURE = (ROOT / "scripts/ci/capture-nightly-perf-host.sh").read_text(encoding="utf-8")
REQUIRED = (ROOT / ".github/workflows/perf-suite-required.yml").read_text(encoding="utf-8")
sys.path.insert(0, str(ROOT / "scripts/ci"))
from list_perf_suite_targets import perf_suite_targets


class PerfNightlyDockerContract(unittest.TestCase):
    @unittest.skipUnless(sys.platform.startswith("linux"),
                         "Linux perf wrappers require Linux host telemetry")
    def test_both_wrappers_launch_explicit_host_network_and_propagate_failure(self):
        if not shutil.which("bash") or not shutil.which("jq"):
            self.skipTest("wrapper runtime contract requires bash and jq")
        home_tmp = Path.home() / ".tmp"
        home_tmp.mkdir(parents=True, exist_ok=True)
        image_id = "sha256:" + "a" * 64
        with tempfile.TemporaryDirectory(prefix="perf-docker-contract-",
                                         dir=home_tmp) as directory:
            fixture = Path(directory)
            fake_bin = fixture / "bin"
            fake_bin.mkdir()
            fake_git = fake_bin / "git"
            fake_git.write_text(
                f"#!{sys.executable}\n"
                "import os, sys\n"
                "arg = sys.argv[-1]\n"
                "print(os.environ['FAKE_REPO'] if arg == '--show-toplevel' "
                "else 'b' * 40)\n", encoding="utf-8")
            fake_git.chmod(0o755)
            fake_docker = fake_bin / "docker"
            fake_docker.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "with open(os.environ['DOCKER_CAPTURE'], 'a') as output:\n"
                "    output.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "if sys.argv[1:3] == ['image', 'inspect']:\n"
                "    print(json.dumps([{'Id': os.environ['FAKE_IMAGE_ID']}]))\n"
                "elif sys.argv[1] == 'run':\n"
                "    sys.exit(int(os.environ['DOCKER_EXIT']))\n"
                "else:\n"
                "    sys.exit(99)\n", encoding="utf-8")
            fake_docker.chmod(0o755)
            for wrapper in ("nightly", "stable"):
                for exit_code in (0, 125):
                    with self.subTest(wrapper=wrapper, exit_code=exit_code):
                        case = fixture / f"{wrapper}-{exit_code}"
                        repo = case / "repo"
                        home = case / "home"
                        repo.mkdir(parents=True)
                        home.mkdir()
                        capture = case / "docker.jsonl"
                        artifacts = (case / "artifacts" if wrapper == "nightly"
                                     else repo / "perf-artifacts/doop")
                        env = dict(os.environ, HOME=str(home),
                                   TMPDIR=str(home / ".tmp"),
                                   PATH=str(fake_bin) + os.pathsep + os.environ["PATH"],
                                   DOCKER_BIN=str(fake_docker),
                                   DOCKER_CAPTURE=str(capture), FAKE_REPO=str(repo),
                                   FAKE_IMAGE_ID=image_id, DOCKER_EXIT=str(exit_code),
                                   PERF_ARTIFACT_DIR=str(artifacts))
                        script = f"scripts/ci/run-perf-{wrapper}-linux.sh"
                        result = subprocess.run(["bash", str(ROOT / script)],
                                                cwd=repo, env=env,
                                                capture_output=True, encoding="utf-8",
                                                timeout=30)
                        self.assertEqual(result.returncode, exit_code, result.stderr)
                        calls = [json.loads(line) for line in
                                 capture.read_text(encoding="utf-8").splitlines()]
                        self.assertEqual(calls[0],
                                         ["image", "inspect", "semantic-reasoning:ubuntu26"])
                        self.assertEqual(len(calls), 2, "inspect then one run; no retry")
                        args = calls[1]
                        self.assertEqual(args[0], "run")
                        self.assertEqual(args.count("--network=host"), 1)
                        self.assertIn("--pull=never", args)
                        self.assertIn("--rm", args)
                        self.assertEqual(args[args.index("--user") + 1],
                                         f"{os.getuid()}:{os.getgid()}")
                        mounts = [args[index + 1] for index, arg in enumerate(args)
                                  if arg == "-v"]
                        self.assertEqual(mounts, [f"{repo}:/workspace",
                                                 f"{artifacts}:/artifacts",
                                                 f"{home / '.tmp'}:/home/perf"])
                        container_env = [args[index + 1]
                                         for index, arg in enumerate(args) if arg == "-e"]
                        self.assertIn("HOME=/home/perf", container_env)
                        self.assertIn("TMPDIR=/home/perf/" +
                                      ("tmp" if wrapper == "nightly" else ".tmp"),
                                      container_env)
                        workdir = args.index("-w")
                        self.assertEqual(args[workdir + 1:workdir + 3],
                                         ["/workspace", image_id])
                        self.assertEqual(args[workdir + 3:workdir + 6],
                                         ["bash", script, "--inside-image"])
                        self.assertFalse(any(arg.startswith(("--privileged", "--cap-add"))
                                             for arg in args))
                        identity = (artifacts / "image-identity.txt").read_text(
                            encoding="utf-8")
                        self.assertEqual(identity,
                                         f"tag=semantic-reasoning:ubuntu26\n"
                                         f"image_id={image_id}\ndocker_network=host\n")

    def test_linux_owner_is_guarded_and_serialized_on_perf_label(self):
        perf_job = WORKFLOW.split("\n  perf:\n", 1)[1].split("\n  portfolio-skip-rate:", 1)[0]
        self.assertIn("github.repository == 'semantic-reasoning/wirelog'", perf_job)
        self.assertIn("github.ref == 'refs/heads/main'", perf_job)
        self.assertIn('runner: \'["self-hosted", "Linux", "X64", "perf"]\'', perf_job)
        self.assertIn("group: wirelog-perf-${{ matrix.os }}", perf_job)
        self.assertIn("queue: max", perf_job)
        self.assertIn("cancel-in-progress: false", perf_job)
        self.assertIn("run-perf-nightly-linux.sh", perf_job)
        self.assertIn("strict_smoke", WORKFLOW)
        self.assertIn("runner.os == 'Windows' && !inputs.strict_smoke", perf_job)
        self.assertNotIn("apt-get install", perf_job.split("\n      - name:", 1)[0])
        self.assertNotIn("sudo apt-get", perf_job)

    def test_other_linux_perf_owner_shares_main_guard_and_queue(self):
        stable = WORKFLOW.split("\n  perf-stable:\n", 1)[1].split("\n  stress:", 1)[0]
        self.assertIn("github.repository == 'semantic-reasoning/wirelog'", stable)
        self.assertIn("github.ref == 'refs/heads/main'", stable)
        self.assertIn("runs-on: [self-hosted, Linux, X64, perf]", stable)
        self.assertIn("group: wirelog-perf-linux", stable)
        self.assertIn("queue: max", stable)
        self.assertIn("cancel-in-progress: false", stable)
        self.assertIn("run-perf-stable-linux.sh", stable)
        self.assertNotIn("sudo apt-get", stable)
        self.assertIn("inputs.strict_smoke", stable.split("\n    runs-on:", 1)[0])

    def test_local_image_execution_and_strict_profile(self):
        self.assertIn("semantic-reasoning:ubuntu26", RUNNER)
        self.assertIn('image inspect "$image"', RUNNER)
        self.assertIn('run --pull=never', RUNNER)
        self.assertIn('--user "$(id -u):$(id -g)"', RUNNER)
        self.assertIn('PORTFOLIO_TIER="${PORTFOLIO_TIER:-readme-full}"', RUNNER)
        self.assertIn('PERF_STRICT_SMOKE="${PERF_STRICT_SMOKE:-false}"', RUNNER)
        self.assertIn("-Dwirelog_log_max_level=error", RUNNER)
        self.assertIn("meson==1.12.0", RUNNER)
        self.assertIn("uv venv", RUNNER)
        self.assertIn("crdt_perf_gate cspa_w1_gate", RUNNER)
        self.assertIn("WIRELOG_PERF_REQUIRE=1", RUNNER)
        self.assertIn("acquisition_status=", RUNNER)
        self.assertIn("performance_verdict=", RUNNER)
        self.assertIn("coverage_status=partial_smoke", RUNNER)
        self.assertIn("trace_test=not_run", RUNNER)
        self.assertIn("gate-before.txt", RUNNER)
        self.assertIn("gate-after.txt", RUNNER)
        self.assertIn("check-nightly-host-telemetry.sh", RUNNER)
        self.assertIn('"$image_id"', RUNNER)
        self.assertIn("tag=$image", RUNNER)
        self.assertIn("image_id=$image_id", RUNNER)
        self.assertIn('-w /workspace "$image_id"', RUNNER)
        stable = (ROOT / "scripts/ci/run-perf-stable-linux.sh").read_text(encoding="utf-8")
        self.assertIn('-w /workspace "$image_id"', stable)

    def test_cgroup_cpu_stat_preserves_kernel_whitespace_schema(self):
        self.assertIn(r"$1 !~ /^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)*$/", CAPTURE)
        self.assertIn('printf \'cpu_stat=%s\\n\' "$cpu_stat"', CAPTURE)
        self.assertIn('cpu_stat=usage_usec=20 user_usec=10 system_usec=1',
                      (ROOT / "scripts/ci/test-nightly-crdt-cspa.sh").read_text(encoding="utf-8"))

    def test_repeated_runs_clear_only_the_owned_meson_environment(self):
        stable = (ROOT / "scripts/ci/run-perf-stable-linux.sh").read_text(encoding="utf-8")
        for runner, parent in ((RUNNER, "$tmp_root"), (stable, "$TMPDIR")):
            with self.subTest(parent=parent):
                self.assertIn(f'python_env="{parent}/meson-venv"', runner)
                commands = [line.strip() for line in runner.splitlines()
                            if line.strip().startswith("uv venv ")]
                self.assertEqual(commands,
                                 ['uv venv --clear --python python3 "$python_env"'])

    def test_meson_introspection_normalizes_project_suite_namespace(self):
        tests = [
            {"name": "strict gate", "suite": ["wirelog:perf"],
             "cmd": ["/build/tests/test_crdt_perf_gate"]},
            {"name": "other suite", "suite": ["wirelog:abi"],
             "cmd": ["/build/tests/test_abi"]},
        ]
        targets = [{"name": "test_crdt_perf_gate"},
                   {"name": "bench_flowlog"}, {"name": "test_abi"}]
        self.assertEqual(perf_suite_targets(tests, targets),
                         ["bench_flowlog", "test_crdt_perf_gate"])

    def test_postcompile_mesontests_disable_implicit_rebuilds(self):
        stable = (ROOT / "scripts/ci/run-perf-stable-linux.sh").read_text(encoding="utf-8")
        self.assertIn("test_cspa_correctness bench_flowlog", stable)
        self.assertGreaterEqual(RUNNER.count("--no-rebuild"), 2)
        self.assertGreaterEqual(stable.count("--no-rebuild"), 2)

    def test_required_pr_gate_remains_hosted_and_unlabeled(self):
        job = REQUIRED.split("\n  perf-gate:\n", 1)[1]
        self.assertIn("runs-on: ubuntu-latest", job.split("\n    steps:", 1)[0])
        self.assertNotIn("self-hosted", job)
        self.assertNotIn("perf]", job)

    def test_checker_requires_samples_correctness_and_cov_but_tracks_misses(self):
        for required in ("exactly nine raw samples", "CoV exceeds the 3%",
                         "104851 \\(expected 104851\\)", "20381/20,381",
                         "iterations[[:space:]]*=[[:space:]]*6/6",
                         "acquisition_status=complete",
                         "performance_verdict=fail", "target_miss"):
            self.assertIn(required, CHECKER)
        self.assertIn("build-perf-nightly-trace --suite perf", RUNNER)
        self.assertIn("run-flowlog-portfolio.py", RUNNER)


if __name__ == "__main__":
    unittest.main()
