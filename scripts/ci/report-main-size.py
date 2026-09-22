#!/usr/bin/env python3
"""Always-run advisory main-branch size monitor and durable report writer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--library", required=True)
    p.add_argument("--build-dir", required=True)
    p.add_argument("--source-dir", required=True)
    p.add_argument("--report", required=True)
    p.add_argument("--sha", required=True)
    p.add_argument("--run-url", required=True)
    p.add_argument("--runner-os", required=True)
    p.add_argument("--compiler", required=True)
    p.add_argument("--phase", default="measurement")
    p.add_argument("--baseline-file")
    p.add_argument("--configure-status", default="unknown")
    p.add_argument("--build-status", default="unknown")
    p.add_argument("--test-status", default="unknown")
    a = p.parse_args()
    output = Path(a.report)
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {"schema_version": 1, "status": "monitoring-error", "commit_sha": a.sha,
              "run_url": a.run_url, "run_id": os.environ.get("GITHUB_RUN_ID"),
              "job": os.environ.get("GITHUB_JOB"), "runner_os": a.runner_os,
              "compiler": a.compiler, "phase": a.phase, "measured_bytes": None,
              "workflow_steps": {"configure": a.configure_status, "build": a.build_status,
                                 "test": a.test_status},
              "baseline_bytes": None, "delta_bytes": None, "budget_bytes": 5120,
              "profile": None, "provenance": "legacy-unverified"}
    failure = None
    try:
        if not Path(a.library).is_file():
            raise RuntimeError(f"library unavailable: {a.library}")
        if not Path(a.build_dir).is_dir():
            raise RuntimeError(f"Meson profile unavailable: {a.build_dir}")
        profile_path = output.with_suffix(".profile.json")
        profile_cmd = [sys.executable, str(Path(__file__).with_name("size-profile.py")), "capture",
                       "--build-dir", a.build_dir, "--source-dir", a.source_dir,
                       "--source-sha", a.sha, "--output", str(profile_path)]
        subprocess.run(profile_cmd, check=True, capture_output=True, text=True)
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        measured = subprocess.check_output(["size", "--format=sysv", a.library], text=True)
        values = [line.split()[1] for line in measured.splitlines()
                  if line.split() and line.split()[0] == ".text"]
        if len(values) != 1 or not values[0].isdigit():
            raise RuntimeError("could not extract exactly one .text size")
        baseline_path = Path(a.baseline_file) if a.baseline_file else Path(a.source_dir) / "tests/baseline_size.txt"
        raw_baseline = baseline_path.read_text(encoding="ascii").strip()
        if not raw_baseline.isdigit():
            raise RuntimeError("baseline size is missing or malformed")
        size = int(values[0]); baseline = int(raw_baseline)
        report.update({"measured_bytes": size, "baseline_bytes": baseline,
                       "delta_bytes": size - baseline, "profile": profile,
                       "profile_sha256": hashlib.sha256(json.dumps(
                           {k: v for k, v in profile.items() if k != "source_sha"},
                           sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                       "library_sha256": hashlib.sha256(Path(a.library).read_bytes()).hexdigest(),
                       "status": "within-budget" if size - baseline <= 5120 else "over-budget"})
        if a.configure_status != "success" or a.build_status != "success":
            failed = [name for name, outcome in (("configure", a.configure_status), ("build", a.build_status))
                      if outcome != "success"]
            report["status"] = "monitoring-error"
            report["phase"] = ",".join(failed)
            report["error"] = "production build step failed: " + ",".join(failed)
            failure = report["error"]
        if report["status"] == "over-budget":
            failure = f"binary size is {size} bytes, {size-baseline:+d} from baseline {baseline} (budget +5120)"
    except Exception as exc:  # This job reports monitoring failures but stays advisory.
        failure = str(exc)
        report["error"] = failure
    output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    status = report["status"]
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write(f"## Binary size monitor: {status}\n\n")
            stream.write(f"- Commit: `{a.sha}`\n- Run: {a.run_url}\n")
            stream.write(f"- Runner profile: `{a.runner_os}` / `{a.compiler}`\n")
            stream.write(f"- Baseline: {report['baseline_bytes']} bytes\n")
            stream.write(f"- Measured: {report['measured_bytes']} bytes\n")
            stream.write(f"- Delta: {report['delta_bytes']} bytes; budget: +5120 bytes\n")
            stream.write(f"- Profile: {report['profile'] and report['profile'].get('tools')}\n")
            stream.write(f"- Configure/build/test outcomes: {report['workflow_steps']}\n")
            if failure:
                stream.write(f"- Monitoring detail: {failure}\n")
    profile_data = report.get("profile") or {}
    compiler = profile_data.get("tools", {})
    profile_id = report.get("profile_sha256", "unavailable")
    detail = failure or f"binary size {report['measured_bytes']} bytes; baseline {report['baseline_bytes']} bytes; delta {report['delta_bytes']}"
    detail += f"; profile {profile_id}; compiler/linker {compiler or 'unavailable'}"
    annotation = f"commit {a.sha}; run {a.run_url}; {detail}"
    if status in ("over-budget", "monitoring-error"):
        print(f"::warning title=Binary size monitor ({status})::{annotation}")
    else:
        print(annotation)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
