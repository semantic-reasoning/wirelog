#!/usr/bin/env python3
"""Enforce timeout coverage for process-spawning Meson tests (#1464/#1488).

Every test whose command spawns a process must carry an explicit, positive
``timeout:``.  Three registration shapes are in the closure: a shell test
(argv[0] is a ``.sh`` file, or argv[0] is ``bash``/``sh`` and argv[1] is a
``.sh`` file); a PowerShell test (argv[0] is a ``.ps1`` file, or argv[0] is
``pwsh``/``powershell`` and ``-File`` names a ``.ps1``); and a Python test
whose script's AST contains a ``subprocess`` or ``os`` process call.  Meson
only exposes the *effective* value through ``meson-info/intro-tests.json``
and does not record whether the kwarg was written, so the gate asserts a
bright line: such a test may not sit at Meson's 30 s default, and may not
disable its timeout with a non-positive value.  A gate that genuinely
wants 30 s writes another value and justifies it in tests/meson.build.
Timeouts are hang detectors; this gate never measures wall time (that
gap is recorded separately).

The rule is suite-agnostic on purpose: suite membership is not a proxy for
platform exposure.  It covers every shape above on every platform, including
the PowerShell registrations that exist only under
``host_machine.system() == 'windows'`` (#1489); pure in-process Python tests
remain outside the rule.  The gate SKIPs (exit 77) when the build directory
or its introspection is missing.  ``WIRELOG_ABI_REQUIRED=1`` turns every skip
into a failure.  The *shell* third of the classifier is shared with
check-bash-constructs.py so both gates agree on which tests are shell-seeded;
the PowerShell and Python shapes are this gate's alone and deliberately stay
outside that ratchet's closure.

Usage: check-shell-gate-timeouts.py <builddir>
"""

from __future__ import annotations

import importlib.util
import ast
import json
import os
import sys
from pathlib import Path

# Meson's default test timeout in seconds.  It cannot be read back from the
# introspection, so the constant is pinned here and in the self-test.
MESON_DEFAULT_TIMEOUT = 30

SCRIPT_DIR = Path(__file__).resolve().parent
SOURCE_ROOT = SCRIPT_DIR.parents[1]
PYTHON_PROCESS_CALLS = frozenset({"run", "Popen", "call", "check_call", "check_output"})
OS_PROCESS_CALLS = frozenset({"system", "popen", "spawn", "spawnv", "spawnve"})


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


def python_process_seed_from_cmd(cmd: object, root: Path = SOURCE_ROOT) -> str | None:
    """Return the Python script when its AST contains a process launch.

    Python tests which only exercise functions in-process stay outside this
    rule.  Looking for actual calls, rather than merely an ``import
    subprocess``, also keeps self-tests that patch subprocess objects out of
    the process-spawning set.
    """
    if not isinstance(cmd, list) or len(cmd) < 2:
        return None
    interpreter = Path(str(cmd[0])).name.lower()
    if interpreter not in {"python", "python.exe", "python3", "python3.exe", "py.exe"} and not interpreter.startswith("python3."):
        return None
    script = next((str(arg) for arg in cmd[1:] if str(arg).endswith(".py")), None)
    if script is None:
        return None
    path = Path(script)
    if not path.is_absolute():
        path = root / path
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr in PYTHON_PROCESS_CALLS and isinstance(node.func.value, ast.Name) and node.func.value.id == "subprocess":
            return str(path)
        if node.func.attr in OS_PROCESS_CALLS and isinstance(node.func.value, ast.Name) and node.func.value.id == "os":
            return str(path)
    return None


def powershell_seed_from_cmd(cmd: object) -> str | None:
    """Return the PowerShell script an introspected test command runs, or None.

    A test is PowerShell-seeded when argv[0] is a ``.ps1`` file or when argv[0]
    is ``pwsh``/``powershell`` and ``-File`` names a ``.ps1``.  Deliberately
    *not* shared with check-bash-constructs.py: a ``.ps1`` in that gate's
    closure would be lexed as Bash (#1489).

    Only the spelled-out ``-File``/``--file`` is recognised.  PowerShell also
    accepts unambiguous prefix abbreviations (``-f``, ``-Fi``), so a
    registration written that way would escape this gate; no such registration
    exists today.
    """
    if not isinstance(cmd, list) or not cmd:
        return None
    first = Path(str(cmd[0])).name.lower()
    if first.endswith(".ps1"):
        return str(cmd[0])
    if first not in {"pwsh", "pwsh.exe", "powershell", "powershell.exe"}:
        return None
    for index, arg in enumerate(cmd[:-1]):
        if str(arg).lower() in {"-file", "--file"} and str(cmd[index + 1]).lower().endswith(".ps1"):
            return str(cmd[index + 1])
    return None


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


def python_process_seeds(intro: Path) -> list[tuple[dict, str]]:
    """Return process-spawning Python tests from Meson's introspection."""
    data = json.loads(intro.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("introspection is not a list of tests")
    seeds: list[tuple[dict, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        script = python_process_seed_from_cmd(item.get("cmd"))
        if script is not None:
            seeds.append((item, script))
    return seeds


def powershell_seeds(intro: Path) -> list[tuple[dict, str]]:
    """Return (test entry, PowerShell script) for every PowerShell-seeded test."""
    data = json.loads(intro.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("introspection is not a list of tests")
    return [(item, script) for item in data if isinstance(item, dict)
            for script in [powershell_seed_from_cmd(item.get("cmd"))] if script is not None]


def offenders_from_intro(intro: Path) -> tuple[int, list[str]]:
    """Return (process-spawning test count, offender lines) for ``intro``."""
    checked = 0
    offenders: list[str] = []
    seeds = shell_seeds(intro) + powershell_seeds(intro) + python_process_seeds(intro)
    for item, _ in seeds:
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
    if sys.platform not in ("linux", "darwin", "win32"):
        return skip(f"introspection is checked on Linux, macOS, and Windows only: {sys.platform}")
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
        return fail("introspection contained no process-spawning tests "
                    "(shell, PowerShell, Python); the classifier or "
                    "tests/meson.build has changed shape")
    if offenders:
        return fail("\n".join(offenders))
    print(f"check-shell-gate-timeouts: ok {checked} process-spawning tests "
          "(shell, PowerShell, Python) carry "
          f"an explicit positive timeout (none at the {MESON_DEFAULT_TIMEOUT}s default)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
