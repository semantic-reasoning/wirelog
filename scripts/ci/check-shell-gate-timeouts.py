#!/usr/bin/env python3
"""Enforce the shell-gate timeout rule over Meson's introspected tests (#1464).

Every test whose command runs a shell script (argv[0] is a ``.sh`` file, or
argv[0] is ``bash``/``sh`` and argv[1] is a ``.sh`` file) must carry an
explicit, positive ``timeout:``.  Meson only exposes the *effective* value
through ``meson-info/intro-tests.json`` and does not record whether the kwarg
was written, so the gate asserts a bright line: a shell-seeded test may not
sit at Meson's 30 s default, and may not disable its timeout with a
non-positive value.  A gate that genuinely wants 30 s writes another value and
justifies it in tests/meson.build.  Timeouts are hang detectors; this gate
never measures wall time (that gap is recorded separately).

The rule is suite-agnostic on purpose: suite membership is not a proxy for
platform exposure, and the #1462 outage was a bash gate inheriting the default
on the Windows runner.  Between them the Linux and macOS introspections see
every shell registration in tests/meson.build (Linux covers the
``if not is_windows`` block, macOS the ``darwin`` block); the gate SKIPs
(exit 77) on other platforms and when the build directory or its
introspection is missing.  ``WIRELOG_ABI_REQUIRED=1`` turns every skip into a
failure.  Tests seeded by python3 or any other non-shell interpreter are
outside the rule by construction.  The classifier is shared with
check-bash-constructs.py so both gates agree on which tests are shell-seeded.

Usage: check-shell-gate-timeouts.py <builddir>
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

# Meson's default test timeout in seconds.  It cannot be read back from the
# introspection, so the constant is pinned here and in the self-test.
MESON_DEFAULT_TIMEOUT = 30

SCRIPT_DIR = Path(__file__).resolve().parent


def fail(message: str) -> int:
    print(f"check-shell-gate-timeouts: FAIL: {message}", file=sys.stderr)
    return 1


def skip(message: str) -> int:
    if os.environ.get("WIRELOG_ABI_REQUIRED") == "1":
        return fail(f"gate required but would skip: {message}")
    print(f"check-shell-gate-timeouts: SKIP: {message}")
    return 77


def load_classifier():
    """importlib-load the sibling gate for ``shell_seed_from_cmd``.

    The hyphenated filename cannot be imported by name; loading it keeps one
    classifier for both gates instead of a copy that can drift."""
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


def shell_seeds(intro: Path) -> list[tuple[dict, str]]:
    """Return (test entry, shell script) for every shell-seeded test."""
    data = json.loads(intro.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("introspection is not a list of tests")
    classify = load_classifier()
    seeds: list[tuple[dict, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        script = classify(item.get("cmd"))
        if script is not None:
            seeds.append((item, script))
    return seeds


def offenders_from_intro(intro: Path) -> tuple[int, list[str]]:
    """Return (shell-seeded test count, offender lines) for ``intro``."""
    checked = 0
    offenders: list[str] = []
    for item, _ in shell_seeds(intro):
        checked += 1
        name = str(item.get("name", "?"))
        suites = item.get("suite")
        suite = ",".join(str(s) for s in suites) if isinstance(suites, list) else "?"
        timeout = item.get("timeout")
        try:
            value = int(timeout)
        except (TypeError, ValueError):
            offenders.append(
                f"{name} (suite {suite}): timeout {timeout!r} is not a number;"
                " add timeout: to tests/meson.build")
            continue
        if value == MESON_DEFAULT_TIMEOUT:
            offenders.append(
                f"{name} (suite {suite}): timeout {value} is Meson's default;"
                " add timeout: to tests/meson.build")
        elif value <= 0:
            offenders.append(
                f"{name} (suite {suite}): timeout {value} disables the hang"
                " detector; set a positive timeout: in tests/meson.build")
    return checked, offenders


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        return fail("usage: check-shell-gate-timeouts.py <builddir>")
    build = Path(argv[1]).resolve()
    if sys.platform not in ("linux", "darwin"):
        return skip(f"introspection is checked on Linux and macOS only: {sys.platform}")
    if not build.is_dir():
        return skip(f"meson introspection unavailable: build directory missing: {build}")
    intro = build / "meson-info" / "intro-tests.json"
    if not intro.is_file():
        return skip(f"meson introspection unavailable: {intro} missing")
    try:
        checked, offenders = offenders_from_intro(intro)
    except (OSError, ValueError, RuntimeError) as exc:
        return fail(f"cannot read {intro}: {exc}")
    if checked == 0:
        # "Nothing to check" and "the scan no longer matches registrations"
        # are the same result unless the count is floored.
        return fail("introspection contained no shell-seeded tests; the "
                    "classifier or tests/meson.build has changed shape")
    if offenders:
        return fail("\n".join(offenders))
    print(f"check-shell-gate-timeouts: ok {checked} shell-seeded tests carry "
          f"an explicit positive timeout (none at the {MESON_DEFAULT_TIMEOUT}s default)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
