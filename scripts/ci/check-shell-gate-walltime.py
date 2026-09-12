#!/usr/bin/env python3
"""Measure shell-gate wall time against its own timeout after the suite (#1487).

Every shell-seeded test carries an explicit timeout (check-shell-gate-timeouts.py,
#1464), but a timeout is a hang detector and is deliberately useless for
detecting drift: a gate that quietly goes from 3 s to 110 s passes everything.
This gate closes that gap.  It runs as a POST-SUITE CI step -- after
``meson test`` -- and it fails every shell-seeded test whose measured duration
exceeds a fraction of its own timeout (50% by default, ``--fraction`` to
override).  Both numbers are printed for every offender so a drift is visible
at a glance.

Why a CI step and not a meson test: the measurement input,
``<builddir>/meson-logs/testlog.json``, exists only after ``meson test``
finishes, so a suite-registered gate would always skip in a fresh CI job (the
#1301 always-skip shape).  Hence the workflow step in ci-pr.yml and
release-tag.yml.  ``WIRELOG_WALLTIME_REQUIRED=1`` (which CI sets on the jobs
where shell gates are expected) turns every would-be skip into a failure, so a
gate that asserts nothing is loud instead of green.

Two inputs are joined, and the join key matters:

* ``meson-info/intro-tests.json`` gives the population -- the shell-seeded
  tests (same ``shell_seed_from_cmd`` classifier as the timeout gate, shared
  with check-bash-constructs.py) and each one's registered ``timeout``.
* ``meson-logs/testlog.json`` gives the measured ``duration`` for the tests
  that actually ran.  Meson writes it as JSON Lines: one object per line, not
  a single document.

The join is on the test ``command`` (``cmd`` in the introspection, ``command``
in the log) -- identical in both files and stable across Meson versions.  It
deliberately is NOT the ``name`` field: Meson 1.12 records a *pretty* name in
the log (``abi - wirelog:source_access_contract``) that never equals the plain
introspection name (``source_access_contract``), so a name join silently
measures nothing and the gate would pass on every drifted gate.

Verdicts:
  * no shell seeds in the introspection          -> SKIP (escalatable)
  * shell seeds exist but none were measured     -> SKIP (escalatable)
  * a measured gate exceeds the budget           -> FAIL, by name, both numbers
  * otherwise                                    -> PASS
Corrupt/unreadable input is a failure -- the gate cannot decide, so it must
not pass.

Usage: check-shell-gate-walltime.py <builddir> [--fraction 0.5]
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path

DEFAULT_FRACTION = 0.5
REQUIRED_ENV = "WIRELOG_WALLTIME_REQUIRED"
SCRIPT_DIR = Path(__file__).resolve().parent


def fail(message: str) -> int:
    print(f"check-shell-gate-walltime: FAIL: {message}", file=sys.stderr)
    return 1


def skip(message: str) -> int:
    if os.environ.get(REQUIRED_ENV) == "1":
        return fail(f"gate required but would skip: {message}")
    print(f"check-shell-gate-walltime: SKIP: {message}")
    return 77


def load_classifier():
    """importlib-load the shared gate for ``shell_seed_from_cmd``.

    Same dance as check-shell-gate-timeouts.py: the hyphenated filename cannot
    be imported by name, and one classifier must serve both gates so the
    measured set cannot drift from the timeout rule."""
    path = SCRIPT_DIR / "check-bash-constructs.py"
    spec = importlib.util.spec_from_file_location("_bash_constructs", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load sibling gate {path}")
    module = importlib.util.module_from_spec(spec)
    # Register before executing: the sibling declares a dataclass, which
    # resolves its module through sys.modules at class-creation time.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.shell_seed_from_cmd


def _cmd_key(cmd: object) -> tuple[str, ...] | None:
    """Normalise a test command to a hashable join key, or None."""
    if not isinstance(cmd, list) or not cmd:
        return None
    return tuple(str(c) for c in cmd)


def shell_seeds(intro: Path) -> list[tuple[str, str, object, tuple[str, ...]]]:
    """Return (name, suite, timeout, cmd_key) for every shell-seeded test."""
    data = json.loads(intro.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("introspection is not a list of tests")
    classify = load_classifier()
    seeds: list[tuple[str, str, object, tuple[str, ...]]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if classify(item.get("cmd")) is None:
            continue
        name = str(item.get("name", "?"))
        suites = item.get("suite")
        suite = ",".join(str(s) for s in suites) if isinstance(suites, list) else "?"
        key = _cmd_key(item.get("cmd"))
        seeds.append((name, suite, item.get("timeout"), key))
    return seeds


def durations_from_testlog(testlog: Path) -> dict[tuple[str, ...], list[float]]:
    """Map command key -> measured durations from Meson's JSON-Lines log.

    testlog.json holds one object per line for the tests that RAN in this
    build directory; a registered test absent here was not executed by the
    suite and therefore has no runtime to budget.  A command that ran more
    than once keeps every duration so the worst run is the one that is judged."""
    durations: dict[tuple[str, ...], list[float]] = {}
    with testlog.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if not isinstance(entry, dict):
                continue
            key = _cmd_key(entry.get("command"))
            duration = entry.get("duration")
            if key is None or not isinstance(duration, (int, float)) or duration < 0:
                continue
            durations.setdefault(key, []).append(float(duration))
    return durations


def budget_offenders(
    seeds: list[tuple[str, str, object, tuple[str, ...]]],
    durations: dict[tuple[str, ...], list[float]],
    fraction: float,
) -> tuple[int, list[str]]:
    """Return (measured shell-gate count, offender lines) over the budget."""
    measured = 0
    offenders: list[str] = []
    for name, suite, timeout, key in seeds:
        runs = durations.get(key) if key is not None else None
        if not runs:
            continue  # registered but not executed in this suite
        measured += 1
        try:
            limit = float(timeout)
        except (TypeError, ValueError):
            offenders.append(
                f"{name} (suite {suite}): timeout {timeout!r} is not a number;"
                " its runtime cannot be bounded")
            continue
        if limit <= 0:
            offenders.append(
                f"{name} (suite {suite}): timeout {limit} disables the walltime"
                " budget; set a positive timeout: in tests/meson.build")
            continue
        worst = max(runs)
        ratio = worst / limit
        if ratio > fraction:
            offenders.append(
                f"{name} (suite {suite}): duration {worst:.2f}s is"
                f" {ratio * 100:.1f}% of its {limit:.0f}s timeout"
                f" (budget {fraction * 100:.0f}%)")
    return measured, offenders


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="check-shell-gate-walltime")
    parser.add_argument("builddir")
    parser.add_argument("--fraction", type=float, default=DEFAULT_FRACTION,
                        help=f"fail when a duration exceeds this fraction of the"
                             f" timeout (default {DEFAULT_FRACTION})")
    args = parser.parse_args(argv[1:])
    if not (0 < args.fraction <= 1):
        return fail(f"--fraction must be in (0, 1], got {args.fraction}")
    build = Path(args.builddir).resolve()
    if not build.is_dir():
        return skip(f"meson build directory missing: {build}")
    testlog = build / "meson-logs" / "testlog.json"
    intro = build / "meson-info" / "intro-tests.json"
    if not testlog.is_file():
        return skip(f"testlog missing: {testlog} (post-suite gate; `meson test`"
                    " has not produced it)")
    if not intro.is_file():
        return skip(f"meson introspection unavailable: {intro} missing")
    try:
        seeds = shell_seeds(intro)
        durations = durations_from_testlog(testlog)
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        return fail(f"cannot evaluate the walltime budget: {exc}")
    if not seeds:
        return skip("introspection contained no shell-seeded tests; nothing to"
                    " measure (the classifier or tests/meson.build changed shape)")
    measured, offenders = budget_offenders(seeds, durations, args.fraction)
    if measured == 0:
        return skip(f"{len(seeds)} shell-seeded tests are registered but none"
                    " ran in this suite; nothing to measure")
    if offenders:
        return fail("\n".join(offenders))
    print(f"check-shell-gate-walltime: ok {measured} executed shell-seeded tests"
          f" are within {args.fraction * 100:.0f}% of their timeout budgets")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
