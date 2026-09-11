#!/usr/bin/env python3
"""Bounded, serial evidence collection for the five #1575 ARM reproducers."""
import argparse
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

TARGETS = {
    "test_compaction": 30,
    "test_diff_join": 30,
    "test_col_rel_deep_copy": 30,
    "test_tdd_decision_stats": 300,
    "test_tdd_decision_stats_nofusion": 300,
}
MESON = {"b_sanitize": "address,undefined", "b_lundef": False,
         "tests": True, "buildtype": "debugoptimized"}


def observer_env():
    # An explicit small environment: no sanitized loader paths or secret dump.
    return {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}


def child_env(build):
    env = observer_env()
    env["LD_LIBRARY_PATH"] = ":".join(str(path) for path in (
        build, build / "subprojects/nanoarrow", build / "subprojects/xxHash-0.8.3"))
    env["ASAN_OPTIONS"] = "abort_on_error=1:halt_on_error=1:print_stacktrace=1"
    env["UBSAN_OPTIONS"] = "abort_on_error=1:halt_on_error=1:print_stacktrace=1"
    return env


def manifest(build):
    sha = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                         text=True, check=True, timeout=5,
                         env=observer_env()).stdout.strip()
    return {"sha": sha, "platform": platform.platform(),
            "python": platform.python_version(), "build": str(build),
            "meson": MESON, "targets": TARGETS,
            "child_config": child_env(build),
            "observer_config": observer_env()}


def observe_command(command, path, env, timeout=2):
    with path.open("w") as output:
        try:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT,
                                    env=env, timeout=timeout, check=False)
            return {"status": "ok" if result.returncode == 0 else "fail",
                    "rc": result.returncode}
        except subprocess.TimeoutExpired:
            return {"status": "timeout"}
        except OSError as exc:
            print(exc, file=output)
            return {"status": "unavailable"}


def snapshot_proc(pid, path):
    status = "ok"
    with path.open("w") as output:
        # These fields do not include the process environment or command line.
        for name in ("status", "stat", "wchan", "stack"):
            print(f"--- {name} ---", file=output)
            try:
                print(Path(f"/proc/{pid}/{name}").read_text(), file=output)
            except OSError as exc:
                status = "unavailable"
                print(exc, file=output)
    return {"status": status}


def observe(pid, deadline, directory):
    """May block in the OS; the parent independently enforces the deadline."""
    env = observer_env()
    gdb_at = deadline - min(10, max(0, (deadline - time.monotonic()) / 2))
    gdb_done = False
    count = 0
    with (directory / "observer.jsonl").open("w", buffering=1) as events:
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            if not gdb_done and time.monotonic() >= gdb_at:
                event = observe_command([
                    "gdb", "--nx", "--batch", "-p", str(pid),
                    "-ex", "set pagination off", "-ex", "thread apply all bt",
                    "-ex", "detach"], directory / "gdb.log", env,
                    timeout=max(0.01, min(5, deadline - time.monotonic())))
                events.write(json.dumps({"observer": "gdb", **event}) + "\n")
                gdb_done = True
            event = snapshot_proc(pid, directory / f"proc-{count}.log")
            events.write(json.dumps({"observer": "proc", **event}) + "\n")
            event = observe_command([
                "ps", "-L", "-p", str(pid), "-o",
                "pid,tid,ppid,stat,pcpu,pmem,etime,wchan:32,comm"],
                directory / f"ps-{count}.log", env)
            events.write(json.dumps({"observer": "ps", **event}) + "\n")
            count += 1
            time.sleep(max(0, min(2, deadline - time.monotonic(),
                                 gdb_at - time.monotonic() if not gdb_done else 2)))


def observer_command(pid, deadline, directory):
    return [sys.executable, str(Path(__file__).resolve()), "--observe",
            str(pid), str(deadline), str(directory)]


def kill_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def run_child(command, timeout, directory, env, observer_environment):
    directory.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    deadline = started + timeout
    observer = None
    result = {"command": command, "deadline_seconds": timeout,
              "observer_status": "unavailable"}
    with (directory / "stdout.log").open("w") as stdout, \
            (directory / "stderr.log").open("w") as stderr, \
            (directory / "observer.log").open("w") as observer_log:
        try:
            child = subprocess.Popen(command, stdout=stdout, stderr=stderr,
                                     env=env, start_new_session=True)
        except OSError as exc:
            result.update(status="fail", rc=None, error=str(exc))
        else:
            result["pid"] = child.pid
            try:
                try:
                    observer = subprocess.Popen(
                        observer_command(child.pid, deadline, directory),
                        stdout=observer_log, stderr=subprocess.STDOUT,
                        env=observer_environment, start_new_session=True)
                except OSError as exc:
                    print(exc, file=observer_log)
                try:
                    rc = child.wait(timeout=max(0, deadline - time.monotonic()))
                    result.update(status="pass" if rc == 0 else "fail", rc=rc)
                except subprocess.TimeoutExpired:
                    kill_group(child)
                    result.update(status="timeout", rc=child.returncode)
            finally:
                # Also clean descendants if the leader exited before them.
                kill_group(child)
                if observer is not None:
                    rc = observer.poll()
                    result["observer_status"] = (
                        "interrupted" if rc is None else "ok" if rc == 0 else "fail")
                    kill_group(observer)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    events = directory / "observer.jsonl"
    result["observer_events"] = []
    if events.exists():
        for line in events.read_text().splitlines():
            try:
                result["observer_events"].append(json.loads(line))
            except json.JSONDecodeError:
                result["observer_events"].append({"status": "interrupted"})
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    if len(sys.argv) == 5 and sys.argv[1] == "--observe":
        observe(int(sys.argv[2]), float(sys.argv[3]), Path(sys.argv[4]))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, default=Path("builddir-san"))
    parser.add_argument("--output", type=Path, default=Path("diagnostics-1575"))
    args = parser.parse_args()
    build, output = args.build.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    try:
        (output / "manifest.json").write_text(json.dumps(manifest(build), indent=2) + "\n")
        results = []
        for name, timeout in TARGETS.items():
            results.append(run_child([str(build / "tests" / name)], timeout,
                                     output / name, child_env(build), observer_env()))
        (output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
        return int(any(result["status"] != "pass" for result in results))
    except (OSError, subprocess.SubprocessError) as exc:
        (output / "harness-error.log").write_text(str(exc) + "\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
