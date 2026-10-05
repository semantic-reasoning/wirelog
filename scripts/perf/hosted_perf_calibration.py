#!/usr/bin/env python3
"""Capture and classify hosted, read-only PR performance evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess


ORDINALS = {
    "01": ("base", "head"), "02": ("head", "base"),
    "03": ("base", "head"), "04": ("head", "base"),
    "05": ("base", "base"), "06": ("head", "head"),
}
GATES = ("crdt", "cspa")
TRIALS = 9
PROFILE = {
    "cc": "gcc",
    "meson": "1.12.0",
    "meson_args": ["--buildtype=release", "-Dwirelog_log_max_level=trace",
                   "-Dtests=true", "-DmbedTLS=disabled"],
    "gates": {"crdt": "crdt_perf_gate", "cspa": "cspa_w1_gate"},
    "trials": TRIALS,
    "affinity_cpu": 0,
    "governor": "best-effort performance request; gate may skip if unavailable",
}
PROFILE_SHA256 = hashlib.sha256(
    json.dumps(PROFILE, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
RAW_RE = re.compile(
    r"test_(crdt|cspa)_perf_gate: raw_ms =((?:\s+[0-9]+(?:\.[0-9]+)?){1,})")
TARGET_RE = re.compile(r"median_ms\s*=\s*[0-9]+(?:\.[0-9]+)?\s+\(target\s+(\d+)\)")
MEDIAN_RE = re.compile(r"median_ms\s*=\s*([0-9]+(?:\.[0-9]+)?)")
COV_RE = re.compile(r"CoV\s+([0-9]+(?:\.[0-9]+)?)%")


def run_text(command: list[str], *, timeout: int = 10) -> str | None:
    try:
        result = subprocess.run(command, text=True, capture_output=True,
                                check=False, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return (result.stdout + result.stderr).strip()


def read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def cpu_model() -> str | None:
    text = read_text(Path("/proc/cpuinfo"))
    if not text:
        return None
    for line in text.splitlines():
        if line.startswith(("model name", "Hardware")) and ":" in line:
            return line.split(":", 1)[1].strip()
    return None


def capture_host() -> dict:
    cpu0 = Path("/sys/devices/system/cpu/cpu0/cpufreq")
    cgroup_text = read_text(Path("/proc/self/cgroup"))
    cgroup_dir = Path("/sys/fs/cgroup")
    if cgroup_text:
        for line in cgroup_text.splitlines():
            fields = line.split(":", 2)
            if len(fields) == 3 and fields[0] == "0" and not fields[1]:
                cgroup_dir = cgroup_dir / fields[2].lstrip("/")
                break
    cgroup: dict[str, str | None] = {
        "membership": cgroup_text,
        "resolved_directory": str(cgroup_dir),
    }
    for name in ("cpu.max", "cpu.stat", "memory.max", "memory.current",
                 "memory.events"):
        cgroup[name] = read_text(cgroup_dir / name)
    return {
        "runner_image": "/".join(filter(None, (os.getenv("ImageOS"),
                                                  os.getenv("ImageVersion")))) or None,
        "os": platform.platform(),
        "architecture": platform.machine(),
        "cpu_model": cpu_model(),
        "cpu_count": os.cpu_count(),
        "kernel": platform.release(),
        "gcc": run_text(["gcc", "--version"]),
        "meson": run_text(["meson", "--version"]),
        "affinity": run_text(["taskset", "-pc", str(os.getpid())]),
        "requested_affinity_cpu": 0,
        "governor": read_text(cpu0 / "scaling_governor"),
        "frequency_khz": read_text(cpu0 / "scaling_cur_freq"),
        "frequency_min_khz": read_text(cpu0 / "scaling_min_freq"),
        "frequency_max_khz": read_text(cpu0 / "scaling_max_freq"),
        "loadavg": read_text(Path("/proc/loadavg")),
        "psi_cpu": read_text(Path("/proc/pressure/cpu")),
        "psi_memory": read_text(Path("/proc/pressure/memory")),
        "psi_io": read_text(Path("/proc/pressure/io")),
        "cgroup": cgroup,
    }


def parse_gate(gate: str, exit_code: int, output: str) -> dict:
    if gate not in GATES:
        raise ValueError(f"unknown gate: {gate}")
    test_name = f"test_{gate}_perf_gate"
    raw_match = RAW_RE.search(output)
    raw_values = ([float(value) for value in raw_match[2].split()]
                  if raw_match and raw_match[1] == gate else [])
    raw_complete = len(raw_values) == TRIALS and all(value > 0 for value in raw_values)
    target_match = TARGET_RE.search(output)
    target_ms = int(target_match[1]) if target_match else None
    trials_match = re.search(rf"{re.escape(test_name)}: trials=(\d+) workers=1", output)
    exit_match = re.search(r"(?m)^result:\s+exit status (\d+)$", output)
    test_exit_code = int(exit_match[1]) if exit_match else exit_code
    if gate == "crdt":
        correctness = re.search(
            r"result\s*=\s*(\d+)\s+\(expected\s+(\d+)\)", output)
        correctness_values = ([int(correctness[1]), int(correctness[2])]
                              if correctness else None)
        correctness_ok = bool(correctness_values == [104851, 104851])
    else:
        tuples = re.search(r"tuples\s*=\s*(\d+)/20,381", output)
        iterations = re.search(r"iterations\s*=\s*(\d+)/6", output)
        correctness_values = ([int(tuples[1]), 20381, int(iterations[1]), 6]
                              if tuples and iterations else None)
        correctness_ok = bool(tuples and iterations
                              and tuples[1] == "20381" and iterations[1] == "6")
    target_miss = (test_exit_code == 1 and raw_complete and correctness_ok
                   and f"{test_name}: FAIL: median " in output
                   and " exceeds target " in output)
    if test_exit_code == 77 or "SKIP:" in output:
        status = "no_measurement"
    elif target_miss:
        status = "target_miss"
    elif (exit_code == 0 and raw_complete and correctness_ok
          and f"{test_name} OK" in output):
        status = "pass"
    elif "FAIL:" in output and not target_miss:
        status = "correctness_failed"
    else:
        status = "no_measurement"
    return {
        "gate": gate,
        "status": status,
        "exit_code": test_exit_code,
        "meson_exit_code": exit_code,
        "raw_ms": raw_values,
        "raw_trial_count": len(raw_values),
        "trials": int(trials_match[1]) if trials_match else None,
        "target_ms": target_ms,
        "correctness_ok": correctness_ok,
        "correctness_values": correctness_values,
        "median_ms": (float(MEDIAN_RE.search(output)[1])
                      if MEDIAN_RE.search(output) else None),
        "cov_percent": [float(value) for value in COV_RE.findall(output)],
    }


def classify_campaign(expected: dict, current: dict,
                      arms: dict[str, dict | None]) -> dict:
    missing = [ordinal for ordinal in ORDINALS
               if arms.get(ordinal) is None]
    reasons: list[str] = []
    if missing:
        stale = current != expected
        if stale:
            reasons.append("PR base/head/merge identity changed during campaign")
        for ordinal, arm in arms.items():
            if arm is None:
                continue
            if (not isinstance(arm, dict) or arm.get("ordinal") != ordinal
                    or arm.get("identity") != expected):
                stale = True
                reasons.append(f"present arm {ordinal} identity is stale or malformed")
                continue
            runs = arm.get("runs")
            if not isinstance(runs, list) or len(runs) != 2:
                stale = True
                reasons.append(f"present arm {ordinal} run structure is malformed")
                continue
            for run_index, run in enumerate(runs):
                if not isinstance(run, dict):
                    stale = True
                    reasons.append(f"present arm {ordinal} run {run_index + 1} is malformed")
                    continue
                side = ORDINALS[ordinal][run_index]
                expected_sha = expected.get(
                    "base_sha" if side == "base" else "merge_sha")
                if (run.get("side") != side
                        or run.get("source_sha") != expected_sha):
                    stale = True
                    reasons.append(
                        f"present arm {ordinal} run {run_index + 1} SHA is stale")
        return {
            "classification": "incomplete", "complete": False,
            "eligible": False, "stale": stale,
            "reasons": sorted(set(reasons + [
                "missing arm artifacts: " + ",".join(missing)])),
            "arms": arms,
        }

    stale = current != expected
    profiles: set[str | None] = set()
    host_profiles: set[tuple | None] = set()
    targets: dict[str, set[int | None]] = {gate: set() for gate in GATES}
    eligible = True
    complete = True
    for ordinal, arm in arms.items():
        if not isinstance(arm, dict):
            complete = False
            reasons.append(f"arm {ordinal} artifact is malformed")
            continue
        runs = arm.get("runs")
        if (arm.get("ordinal") != ordinal or not isinstance(runs, list)
                or len(runs) != 2 or arm.get("identity") != expected):
            stale = True
            complete = False
            reasons.append(f"arm {ordinal} identity or two-run structure is invalid")
            continue
        for run_index, run in enumerate(runs):
            if not isinstance(run, dict):
                complete = False
                stale = True
                reasons.append(f"arm {ordinal} run {run_index + 1} is malformed")
                continue
            side = ORDINALS[ordinal][run_index]
            expected_revision = expected.get(
                "base_sha" if side == "base" else "merge_sha")
            if (run.get("side") != side
                    or run.get("source_sha") != expected_revision):
                stale = True
                reasons.append(f"arm {ordinal} run {run_index + 1} SHA is stale")
            if run.get("source_status") != "":
                eligible = False
                reasons.append(f"arm {ordinal} run {run_index + 1} source checkout is dirty")
            host = run.get("host")
            if not isinstance(host, dict):
                profiles.add(None)
                host_profiles.add(None)
                eligible = False
                reasons.append(f"arm {ordinal} run {run_index + 1} host profile is missing")
            else:
                profile_sha = host.get("profile_sha256")
                profiles.add(profile_sha if isinstance(profile_sha, str) else None)
                host_profile = host.get("host_profile")
                if (isinstance(host_profile, (list, tuple))
                        and len(host_profile) == 6
                        and all(isinstance(value, str) and value
                                for value in host_profile)):
                    host_profiles.add(tuple(host_profile))
                else:
                    host_profiles.add(None)
                    eligible = False
                    reasons.append(
                        f"arm {ordinal} run {run_index + 1} host profile is malformed")
            gates = run.get("gates")
            if not isinstance(gates, dict) or any(gate not in gates for gate in GATES):
                eligible = False
                complete = False
                reasons.append(f"arm {ordinal} run {run_index + 1} lacks gate evidence")
                continue
            for gate in GATES:
                result = gates[gate]
                if not isinstance(result, dict):
                    eligible = False
                    complete = False
                    reasons.append(
                        f"arm {ordinal} run {run_index + 1} {gate} evidence is malformed")
                    continue
                targets[gate].add(result.get("target_ms"))
                if not result.get("evidence_complete"):
                    complete = False
                    reasons.append(
                        f"arm {ordinal} run {run_index + 1} {gate} logs or host samples missing")
                if result.get("status") not in ("pass", "target_miss"):
                    eligible = False
                    reasons.append(
                        f"arm {ordinal} run {run_index + 1} {gate} status is "
                        f"{result.get('status')}")
                if (result.get("raw_trial_count") != TRIALS
                        or result.get("trials") != TRIALS
                        or not isinstance(result.get("raw_ms"), list)
                        or len(result["raw_ms"]) != TRIALS
                        or not result.get("correctness_ok")
                        or result.get("median_ms") is None
                        or not result.get("cov_percent")):
                    eligible = False
                    reasons.append(
                        f"arm {ordinal} run {run_index + 1} {gate} lacks nine correct raw trials")

    if profiles != {PROFILE_SHA256}:
        eligible = False
        reasons.append("build profile hash differs or is missing across arms")
    if len(host_profiles) != 1 or None in host_profiles:
        eligible = False
        reasons.append("host image or CPU profile differs or is missing across runs")
    for gate, values in targets.items():
        if len(values) != 1 or None in values:
            eligible = False
            reasons.append(f"{gate} target differs or is missing across arms")

    if stale:
        eligible = False
        reasons.append("PR base/head/merge identity changed during campaign")
    if not complete:
        eligible = False
    return {
        "classification": ("incomplete" if not complete else
                           "eligible" if eligible else "ineligible"),
        "complete": complete,
        "eligible": eligible,
        "stale": stale,
        "reasons": sorted(set(reasons)),
        "identity": expected,
        "profile_sha256": next(iter(profiles)) if len(profiles) == 1 else None,
        "arms": arms,
    }


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


def command_capture_host(args: argparse.Namespace) -> None:
    host = capture_host()
    host["profile"] = PROFILE
    host["profile_sha256"] = (
        PROFILE_SHA256 if host["meson"] == PROFILE["meson"]
        and host["gcc"] and host["gcc"].startswith("gcc ") else "toolchain-mismatch")
    write_json(args.output, host)


def parse_run(evidence: Path, source: Path, side: str, identity: dict,
              run_number: int) -> dict:
    source_sha = run_text(["git", "-C", str(source), "rev-parse", "HEAD"])
    source_tree = run_text(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"])
    source_status = read_text(evidence / "source-status.txt")
    gates = {}
    profile_hashes = set()
    runner_images = set()
    host_profiles = set()
    for gate in GATES:
        folder = evidence / gate
        stdout = read_text(folder / "stdout.log") or ""
        stderr = read_text(folder / "stderr.log") or ""
        meson_log = read_text(folder / "meson-testlog.txt") or ""
        try:
            exit_code = int((folder / "exit").read_text(encoding="ascii"))
        except (OSError, ValueError):
            exit_code = 255
        parsed = parse_gate(gate, exit_code,
                            stdout + "\n" + stderr + "\n" + meson_log)
        for snapshot in ("host-before.json", "host-after.json"):
            try:
                host_sample = json.loads(
                    (folder / snapshot).read_text(encoding="utf-8"))
                parsed[snapshot.removesuffix(".json")] = host_sample
                profile_hashes.add(host_sample.get("profile_sha256"))
                runner_images.add(host_sample.get("runner_image"))
                host_profiles.add((host_sample.get("runner_image"),
                                   host_sample.get("architecture"),
                                   host_sample.get("cpu_model"),
                                   host_sample.get("gcc"),
                                   host_sample.get("meson"),
                                   host_sample.get("kernel")))
            except (OSError, json.JSONDecodeError):
                parsed[snapshot.removesuffix(".json")] = None
        parsed["evidence_complete"] = all(
            (folder / name).is_file() for name in
            ("stdout.log", "stderr.log", "meson-testlog.txt", "exit",
             "host-before.json", "host-after.json"))
        gates[gate] = parsed
    host_profile = (next(iter(host_profiles)) if len(host_profiles) == 1 else None)
    if host_profile and any(not value for value in host_profile):
        host_profile = None
    return {
        "run_number": run_number,
        "side": side,
        "source_ref": "base" if side == "base" else "pull-merge",
        "source_sha": source_sha,
        "source_tree": source_tree,
        "source_status": source_status,
        "identity": identity,
        "host": {
            "profile": PROFILE,
            "profile_sha256": (next(iter(profile_hashes))
                               if len(profile_hashes) == 1 else None),
            "runner_image": (next(iter(runner_images))
                             if len(runner_images) == 1 else None),
            "host_profile": host_profile,
        },
        "binary_sha256": read_text(evidence / "binary-sha256.txt"),
        "gates": gates,
    }


def command_capture_ordinal(args: argparse.Namespace) -> None:
    identity = json.loads(args.identity.read_text(encoding="utf-8"))
    sides = ORDINALS[args.ordinal]
    runs = [parse_run(args.evidence / f"run-{number:02d}",
                      args.source / f"run-{number:02d}", side, identity, number)
            for number, side in enumerate(sides, start=1)]
    write_json(args.output, {"ordinal": args.ordinal, "identity": identity,
                             "runs": runs})


def load_arms(evidence: Path) -> dict[str, dict | None]:
    arms: dict[str, dict | None] = {}
    for ordinal in ORDINALS:
        candidates = list(evidence.rglob(f"{ordinal}.json"))
        if len(candidates) != 1:
            arms[ordinal] = None
            continue
        try:
            arms[ordinal] = json.loads(candidates[0].read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            arms[ordinal] = None
    return arms


def command_aggregate(args: argparse.Namespace) -> int:
    expected = json.loads(args.identity.read_text(encoding="utf-8"))
    current = json.loads(args.current_identity.read_text(encoding="utf-8"))
    arms = load_arms(args.evidence)
    result = classify_campaign(expected, current, arms)
    write_json(args.output, result)
    print(json.dumps({key: result[key] for key in
                      ("classification", "complete", "eligible", "stale", "reasons")},
                     sort_keys=True))
    return 0 if result["classification"] == "eligible" else 1


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    host = commands.add_parser("capture-host")
    host.add_argument("--output", type=Path, required=True)
    host.set_defaults(function=command_capture_host)
    arm = commands.add_parser("capture-ordinal")
    arm.add_argument("--ordinal", choices=tuple(ORDINALS), required=True)
    arm.add_argument("--identity", type=Path, required=True)
    arm.add_argument("--source", type=Path, required=True)
    arm.add_argument("--evidence", type=Path, required=True)
    arm.add_argument("--output", type=Path, required=True)
    arm.set_defaults(function=command_capture_ordinal)
    aggregate = commands.add_parser("aggregate")
    aggregate.add_argument("--identity", type=Path, required=True)
    aggregate.add_argument("--current-identity", type=Path, required=True)
    aggregate.add_argument("--evidence", type=Path, required=True)
    aggregate.add_argument("--output", type=Path, required=True)
    aggregate.set_defaults(function=command_aggregate)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    result = args.function(args)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
