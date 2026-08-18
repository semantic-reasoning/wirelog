#!/usr/bin/env python3
"""clang-tidy ratchet: files already clean must stay clean (Issue #1100).

No CI job has run clang-tidy since `efbde27` removed the job, so the only
thing keeping the tree lint-clean has been reviewer attention.  It does not
hold: `wirelog/ir/program.c` was clean, regressed after `f1d2a2c`, and now
reports `bugprone-branch-clone` with nothing failing.

This gate is a ratchet over the `libwirelog.so` translation units in the
build's `compile_commands.json`:

  - `scripts/ci/clang-tidy-allowlist.txt` lists the files that are clean
    today.  Every one of them is analysed on every run; a single diagnostic
    fails the build.
  - `scripts/ci/clang-tidy-backlog.txt` lists the files that are not clean
    yet.  They are not analysed.  The backlog carries no diagnostic counts:
    counts are toolchain-version-dependent, so a count would turn a
    toolchain bump into a spurious failure.
  - The two lists must partition the selected translation units exactly.
    That assertion is what stops a regression being made to disappear by
    deleting the allowlist line.  Demotion -- moving a line from the
    allowlist to the backlog -- keeps the partition exact, so it is caught
    separately by `scripts/ci/check-clang-tidy-backlog-monotonic.sh`.

Detection detail that matters: clang-tidy writes diagnostics to STDOUT and
exits 0 even when it reports them.  The exit code is therefore useless as a
pass/fail signal.  A file fails when any stdout line matches
`<path>:<line>:<col>: warning|error:` and `<path>` resolves under
`wirelog/`.  A non-zero exit with no such line is a harness error (missing
file, unparseable database) and is never reported as a pass.

Calibration guards (the allowlist was derived on one toolchain and one
build configuration; 9 of the 13 backlog diagnostics are path-sensitive
`clang-analyzer-*` results that move between LLVM releases).  The gate
SKIPs, with a named reason, unless all of the following hold:

  - `clang-tidy --version` reports a major listed in
    `scripts/ci/clang-tidy-supported-majors.txt`;
  - the host is Linux on x86_64;
  - the selected database entries carry no `-fsanitize=` and no MSVC-style
    command line.

The architecture guard is not redundant with the OS one: `build-arm`
(`.github/workflows/ci-pr.yml`, ubuntu-24.04-arm) is Linux, uninstrumented
and not MSVC, and runs an unfiltered `meson test`.  It is protected today
only by clang-tidy 22 being absent from that image, which is an accident of
the runner rather than a guarantee.  `clang-analyzer-*` results are
target-dependent, so an image bump could otherwise yield an aarch64 verdict
against an x86_64-calibrated allowlist.

Environment:

  WIRELOG_TIDY_REQUIRED=1  turn every skip into a failure (CI sets this)
  WIRELOG_TIDY_SKIP=1      force a skip, so the gate can be stood down
                           without being deleted
  WIRELOG_TIDY_JOBS=N      override the parallelism (default min(8, ncpu))
  CLANG_TIDY               override the clang-tidy executable

The ratchet also scans the alternate preprocessor configurations used by the
bench targets for the two allowlisted sources that have configuration-gated
code.  These scans are synthesized from the canonical `libwirelog.so`
commands, not copied from arbitrary bench or test targets, so the allowlist
continues to describe one source-level verdict.  A diagnostic in either
configuration fails that source and is reported with its configuration label.

Exit codes:
   0 - every allowlisted file is clean
   1 - a diagnostic was reported, a list is out of sync, or a harness error
  77 - skipped (meson SKIP), with the reason on stdout
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import pathlib
import platform
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

SKIP_EXIT = 77

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
ALLOWLIST = REPO_ROOT / "scripts/ci/clang-tidy-allowlist.txt"
BACKLOG = REPO_ROOT / "scripts/ci/clang-tidy-backlog.txt"
SUPPORTED_MAJORS = REPO_ROOT / "scripts/ci/clang-tidy-supported-majors.txt"
CONFIG_FILE = REPO_ROOT / ".clang-tidy"
FIXTURE = REPO_ROOT / "tests/clang_tidy_fixture.c"
HEADER_FIXTURE = REPO_ROOT / "wirelog/clang_tidy_sensitivity_fixture.h"
BASELINE_DIR = REPO_ROOT / "scripts/ci/clang-tidy-baselines"

# A parse that silently selected nothing would make the partition assertion
# trivially true -- the exact shape of failure this gate exists to catch.
# Demand a plausible floor rather than merely non-empty, in the spirit of
# scripts/ci/check-doop-catalogue.sh.
MIN_ENTRIES = 50

# clang-tidy writes these to stdout.  Anchored at the start of the line so
# that a diagnostic quoted inside a note's source snippet cannot match.
DIAG_RE = re.compile(r"^(?P<path>.+?):(?P<line>\d+):(?P<col>\d+): "
                     r"(?P<severity>warning|error): (?P<message>.*)$")
CHECK_RE = re.compile(r"\[([A-Za-z0-9_.,-]+)\]\s*$")
VERSION_RE = re.compile(r"LLVM version (\d+)\.")
NOLINT_RE = re.compile(
    r"Suppressed\s+\d+\s+warnings?\s+\((?P<categories>[^)]*)\)\."
)
NOLINT_CATEGORY_RE = re.compile(r"(?P<count>\d+)\s+NOLINT(?:NEXTLINE)?")

# The marker comment in tests/clang_tidy_fixture.c that pins the line the
# sensitivity check expects its one diagnostic on.
FIXTURE_MARKER = "WL_TIDY_FIXTURE_EXPECT"
HEADER_FIXTURE_MARKER = "WL_TIDY_HEADER_FIXTURE_EXPECT"
FIXTURE_CHECK = "bugprone-integer-division"


class GateError(Exception):
    """A harness problem: the gate could not decide clean vs. dirty."""


class SkipGate(Exception):
    """A calibration guard rejected this environment."""


def baseline_path(version: str, suffix: str) -> pathlib.Path:
    return BASELINE_DIR / f"llvm-{version}.{suffix}.txt"


# ---------------------------------------------------------------------------
# List files
# ---------------------------------------------------------------------------

def read_list(path: pathlib.Path) -> list[str]:
    """Read a `#`-commented, one-path-per-line register."""
    if not path.is_file():
        raise GateError(f"list file missing: {path}")
    entries: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        entries.append(line)
    duplicates = sorted({e for e in entries if entries.count(e) > 1})
    if duplicates:
        raise GateError(
            f"{path.name} has duplicate entries: {', '.join(duplicates)}")
    return entries


# ---------------------------------------------------------------------------
# Compilation database
# ---------------------------------------------------------------------------

def entry_command(entry: dict) -> list[str]:
    """Return an entry's command line as a token list."""
    if "arguments" in entry:
        return list(entry["arguments"])
    if "command" in entry:
        return shlex.split(entry["command"])
    raise GateError("compilation database entry has neither 'command' nor "
                    "'arguments'")


def entry_source(entry: dict) -> pathlib.Path:
    """Resolve an entry's `file` against its `directory`.

    The in-database form is relative (`../wirelog/ir/program.c`); diagnostic
    paths and list entries are not.  Everything is normalised here so the
    three can be compared.
    """
    return (pathlib.Path(entry["directory"]) / entry["file"]).resolve()


def select_entries(build_dir: pathlib.Path) -> list[dict]:
    """Pick one canonical entry per libwirelog.so translation unit."""
    db_path = build_dir / "compile_commands.json"
    if not db_path.is_file():
        raise SkipGate(f"no compilation database at {db_path}")
    try:
        db = json.loads(db_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise GateError(f"cannot parse {db_path}: {exc}") from exc

    selected: dict[pathlib.Path, dict] = {}
    for entry in db:
        if "libwirelog.so" not in entry.get("output", ""):
            continue
        source = entry_source(entry)
        # Deterministic pick if a future build grows more than one shared
        # library target for the same source.
        previous = selected.get(source)
        if previous is None or entry["output"] < previous["output"]:
            selected[source] = entry

    if len(selected) < MIN_ENTRIES:
        raise GateError(
            f"selected only {len(selected)} libwirelog.so translation "
            f"unit(s) from {db_path} (expected at least {MIN_ENTRIES}). "
            "The database shrank drastically or this selector broke; "
            "either way the partition assertion below would be meaningless.")
    return [selected[key] for key in sorted(selected)]


def rel_to_repo(path: pathlib.Path) -> str:
    """Repo-relative POSIX path, or the absolute path when outside."""
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def prune_db(entries: list[dict], workdir: pathlib.Path) -> pathlib.Path:
    """Write the selected entries to a private compilation database.

    Never into the build directory, where the pruned copy could shadow the
    real one.  Each entry's `directory` is preserved verbatim so its
    relative `-I` flags keep resolving.
    """
    db_path = workdir / "compile_commands.json"
    db_path.write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
    return db_path


# ---------------------------------------------------------------------------
# Calibration guards
# ---------------------------------------------------------------------------

def supported_majors() -> list[str]:
    majors = read_list(SUPPORTED_MAJORS)
    if not majors:
        raise GateError(f"{SUPPORTED_MAJORS.name} lists no versions")
    return majors


def resolve_clang_tidy() -> str:
    # Versioned names first: an unsuffixed `clang-tidy` is whatever the
    # distribution happens to default to, so preferring it would SKIP for a
    # developer who has clang-tidy-20 as the default and a supported
    # clang-tidy-22 installed alongside it.  CI is unaffected either way --
    # it exports CLANG_TIDY explicitly.
    override = os.environ.get("CLANG_TIDY")
    candidates = [override] if override else []
    for major in supported_majors():
        candidates.append(f"clang-tidy-{major}")
    candidates.append("clang-tidy")
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise SkipGate("clang-tidy not found on PATH")


def check_version(exe: str) -> str:
    try:
        proc = subprocess.run([exe, "--version"], capture_output=True,
                              text=True, check=False)
    except OSError as exc:
        raise SkipGate(f"cannot run {exe}: {exc}") from exc
    match = VERSION_RE.search(proc.stdout)
    if match is None:
        raise SkipGate(f"cannot parse a version out of `{exe} --version`")
    major = match.group(1)
    allowed = supported_majors()
    if major not in allowed:
        raise SkipGate(
            f"clang-tidy major {major} is not in the supported set "
            f"({', '.join(allowed)}); the allowlist was calibrated against "
            f"those and clang-analyzer results move between releases. "
            f"Update {rel_to_repo(SUPPORTED_MAJORS)} once the lists have "
            "been re-derived on the new toolchain.")
    return match.group(0).removeprefix("LLVM version ").rstrip(".")


SUPPORTED_ARCHES = ("x86_64", "amd64")


def check_platform() -> None:
    # Mirrors the `if not is_windows` guard the ABI gates already use in
    # tests/meson.build.  The allowlist was derived on Linux only.
    if not sys.platform.startswith("linux"):
        raise SkipGate(f"host platform is {sys.platform}, not Linux; the "
                       "allowlist was calibrated on Linux only")
    # Not redundant with the OS check: the build-arm job is Linux,
    # uninstrumented and not MSVC, and runs an unfiltered `meson test`.
    # clang-analyzer results are target-dependent, so an aarch64 verdict
    # against an x86_64-calibrated allowlist would be noise either way it
    # came out.
    machine = platform.machine()
    if machine not in SUPPORTED_ARCHES:
        raise SkipGate(f"host arch is {machine}; the allowlist was "
                       "calibrated on x86_64 only")


def check_configuration(entries: list[dict]) -> None:
    for entry in entries:
        tokens = entry_command(entry)
        for token in tokens:
            if token.startswith("-fsanitize="):
                raise SkipGate(
                    "the build is instrumented "
                    f"({token} on {rel_to_repo(entry_source(entry))}); the "
                    "allowlist was calibrated against an uninstrumented "
                    "default build")
        compiler = pathlib.PurePath(tokens[0]).name.lower() if tokens else ""
        if compiler in ("cl", "cl.exe"):
            raise SkipGate("the database records MSVC-style command lines, "
                           "which clang-tidy cannot consume as-is")


def normalized_config_lines(text: str) -> set[str]:
    """Return stable config lines, excluding YAML framing and the host user."""
    return {
        line.rstrip()
        for line in text.splitlines()
        if line.strip() not in ("---", "...")
        and not line.startswith("User:")
        and not line.lstrip().startswith("#")
        and line.strip()
    }


def check_config_baseline(exe: str, version: str) -> None:
    """Fail if a recorded clang-tidy config value was weakened or removed."""
    path = baseline_path(version, "dump-config")
    if not path.is_file():
        raise GateError(f"clang-tidy config baseline missing: {path}")
    proc = subprocess.run(
        [exe, f"--config-file={CONFIG_FILE}", "--dump-config"],
        cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise GateError(f"{exe} --dump-config failed with exit "
                        f"{proc.returncode}: {proc.stderr.strip()}")
    expected = normalized_config_lines(path.read_text(encoding="utf-8"))
    actual = normalized_config_lines(proc.stdout)
    missing = sorted(expected - actual)
    added = sorted(actual - expected)
    if missing:
        print("check-clang-tidy-ratchet: FAIL: clang-tidy configuration "
              "baseline no longer matches:", file=sys.stderr)
        for line in missing:
            print(f"  missing or changed: {line}", file=sys.stderr)
        raise GateError("clang-tidy configuration baseline mismatch")
    if added:
        print("check-clang-tidy-ratchet: NOTICE: clang-tidy emitted "
              f"{len(added)} unrecorded config line(s); review on toolchain "
              "updates", file=sys.stderr)


# ---------------------------------------------------------------------------
# Running clang-tidy
# ---------------------------------------------------------------------------

class Result:
    def __init__(self, source: pathlib.Path, returncode: int,
                 stdout: str, stderr: str, diagnostics: list[str],
                 configuration: str = "default", nolint_count: int = 0):
        self.source = source
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.diagnostics = diagnostics
        self.configuration = configuration
        self.nolint_count = nolint_count


def parse_diagnostics(stdout: str, cwd: pathlib.Path) -> list[str]:
    """Keep the stdout diagnostics whose path lands under wirelog/.

    Paths arrive in both relative (`../wirelog/ir/program.c`) and absolute
    form depending on how the file entered the invocation, so both are
    resolved against the invocation's working directory before filtering.
    Without this, a file could be scanned and have its diagnostics
    attributed to nothing.
    """
    wirelog_dir = REPO_ROOT / "wirelog"
    kept: list[str] = []
    for line in stdout.splitlines():
        match = DIAG_RE.match(line)
        if match is None:
            continue
        raw = pathlib.Path(match.group("path"))
        resolved = raw if raw.is_absolute() else (cwd / raw)
        try:
            resolved = resolved.resolve()
        except OSError:
            continue
        if not resolved.is_relative_to(wirelog_dir):
            continue
        kept.append("{}:{}:{}: {}: {}".format(
            rel_to_repo(resolved), match.group("line"), match.group("col"),
            match.group("severity"), match.group("message")))
    return kept


def parse_all_diagnostics(stdout: str, cwd: pathlib.Path) -> list[str]:
    """Normalize diagnostics for the sensitivity fixture, including tests."""
    diagnostics = []
    for line in stdout.splitlines():
        match = DIAG_RE.match(line)
        if match is None:
            continue
        raw = pathlib.Path(match.group("path"))
        resolved = raw if raw.is_absolute() else cwd / raw
        diagnostics.append("{}:{}:{}: {}: {}".format(
            rel_to_repo(resolved.resolve()), match.group("line"),
            match.group("col"), match.group("severity"),
            match.group("message")))
    return diagnostics


def parse_nolint_count(stderr: str) -> int:
    """Extract clang-tidy's NOLINT suppression count from stderr."""
    return sum(int(category.group("count"))
               for match in NOLINT_RE.finditer(stderr)
               for category in NOLINT_CATEGORY_RE.finditer(
                   match.group("categories")))


def run_clang_tidy(exe: str, db_dir: pathlib.Path, entry: dict,
                   configuration: str = "default") -> Result:
    source = entry_source(entry)
    cwd = pathlib.Path(entry["directory"])
    argv = [exe, f"--config-file={CONFIG_FILE}", "-p", str(db_dir),
            str(source)]
    proc = subprocess.run(argv, cwd=str(cwd), capture_output=True, text=True,
                          check=False)
    return Result(source, proc.returncode, proc.stdout, proc.stderr,
                  parse_diagnostics(proc.stdout, cwd), configuration,
                  parse_nolint_count(proc.stderr))


def job_count() -> int:
    override = os.environ.get("WIRELOG_TIDY_JOBS")
    if override:
        try:
            value = int(override)
        except ValueError as exc:
            raise GateError(
                f"WIRELOG_TIDY_JOBS={override!r} is not an integer") from exc
        if value < 1:
            raise GateError(f"WIRELOG_TIDY_JOBS={value} must be >= 1")
        return value
    return min(8, os.cpu_count() or 1)


def run_all(exe: str, db_dir: pathlib.Path,
            entries: list[dict]) -> list[Result]:
    jobs = job_count()
    with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(run_clang_tidy, exe, db_dir, entry)
                   for entry in entries]
        return [future.result() for future in futures]


ALTERNATE_CONFIGS = {
    "wirelog/exec_plan_gen.c":
        ("ENABLE_K_FUSION=0", "ENABLE_K_FUSION", "0"),
    "wirelog/columnar/relation.c":
        ("WL_RADIX_BENCH=1", "WL_RADIX_BENCH", "1"),
}


def replace_macro(tokens: list[str], macro: str, value: str) -> list[str]:
    """Replace all command-line definitions of *macro* with one value."""
    result: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in ("-D", "-U") and index + 1 < len(tokens):
            definition = tokens[index + 1]
            if definition.split("=", 1)[0] == macro:
                index += 2
                continue
        if token.startswith(("-D", "-U")):
            definition = token[2:]
            if definition.split("=", 1)[0] == macro:
                index += 1
                continue
        result.append(token)
        index += 1
    result.append(f"-D{macro}={value}")
    return result


def alternate_entry(entry: dict, macro: str, value: str,
                    stem: str) -> dict:
    """Clone one canonical entry for one isolated alternate scan."""
    tokens = replace_macro(entry_command(entry), macro, value)
    output: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in ("-o", "-MQ", "-MF") and index + 1 < len(tokens):
            output.extend((token, stem + (".d" if token == "-MF" else ".o")))
            index += 2
            continue
        output.append(token)
        index += 1
    return {
        "directory": entry["directory"],
        "command": shlex.join(output),
        "file": entry["file"],
        "output": stem + ".o",
    }


def run_alternate_configs(exe: str, entries: list[dict],
                          allowed: set[str] | None = None) -> list[Result]:
    """Run only required alternate configs from canonical entries."""
    by_source = {rel_to_repo(entry_source(entry)): entry for entry in entries}
    specs = []
    for source, (label, macro, value) in ALTERNATE_CONFIGS.items():
        if source in by_source and (allowed is None or source in allowed):
            specs.append((source, label, macro, value, by_source[source]))

    def run(spec: tuple) -> Result:
        source, label, macro, value, entry = spec
        with tempfile.TemporaryDirectory(prefix="wl-tidy-alt-") as tmp:
            workdir = pathlib.Path(tmp)
            synthetic = alternate_entry(
                entry, macro, value, "wl_tidy_alt_" + source.replace("/", "_"))
            prune_db([synthetic], workdir)
            return run_clang_tidy(exe, workdir, synthetic, label)

    with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(specs), job_count()) or 1) as pool:
        return list(pool.map(run, specs))


def nolint_key(result: Result) -> str:
    label = (f" [{result.configuration}]"
             if result.configuration != "default" else "")
    return f"{rel_to_repo(result.source)}{label}"


def read_nolint_baseline(version: str) -> dict[str, int]:
    path = baseline_path(version, "nolint-counts")
    if not path.is_file():
        raise GateError(f"clang-tidy NOLINT baseline missing: {path}")
    baseline: dict[str, int] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, count = line.rpartition(" ")
        if not separator or not key or not count.isdigit():
            raise GateError(f"invalid NOLINT baseline line: {raw!r}")
        if key in baseline:
            raise GateError(f"duplicate NOLINT baseline entry: {key}")
        baseline[key] = int(count)
    return baseline


def check_nolint_baseline(results: list[Result], version: str) -> int:
    expected = read_nolint_baseline(version)
    observed = {nolint_key(result): result.nolint_count for result in results}
    increased = []
    decreased = []
    for key, count in sorted(observed.items()):
        previous = expected.get(key, 0)
        if count > previous:
            increased.append((key, previous, count))
        elif count < previous:
            decreased.append((key, previous, count))
    if increased:
        print("check-clang-tidy-ratchet: FAIL: NOLINT suppression count "
              "increased:", file=sys.stderr)
        for key, previous, count in increased:
            print(f"  {key}: baseline {previous}, observed {count}",
                  file=sys.stderr)
        return 1
    if decreased:
        print("check-clang-tidy-ratchet: NOTICE: NOLINT suppression count "
              "decreased for " f"{len(decreased)} scan(s)", file=sys.stderr)
    return 0


def report_failures(results: list[Result]) -> int:
    """Print the failures; return the process exit code."""
    dirty = [r for r in results if r.diagnostics]
    # A non-zero exit with no diagnostics on stdout means clang-tidy could
    # not do its job.  Never report that as a pass.
    broken = [r for r in results if r.returncode != 0 and not r.diagnostics]

    if broken:
        print("check-clang-tidy-ratchet: FAIL: clang-tidy failed to run "
              "cleanly (harness error, not a lint result):", file=sys.stderr)
        for result in broken:
            label = (f" [{result.configuration}]"
                     if result.configuration != "default" else "")
            print(f"  {rel_to_repo(result.source)}{label}: "
                  f"exit {result.returncode}", file=sys.stderr)
            for line in result.stderr.splitlines():
                print(f"    {line}", file=sys.stderr)
        return 1

    if not dirty:
        return 0

    print("check-clang-tidy-ratchet: FAIL: allowlisted file(s) are no "
          "longer clang-tidy clean:", file=sys.stderr)
    print("", file=sys.stderr)
    for result in dirty:
        label = (f" [{result.configuration}]"
                 if result.configuration != "default" else "")
        print(f"  {rel_to_repo(result.source)}{label}", file=sys.stderr)
        for diagnostic in result.diagnostics:
            print(f"    {diagnostic}", file=sys.stderr)
        if result.stderr.strip():
            for line in result.stderr.splitlines():
                print(f"    [stderr] {line}", file=sys.stderr)
        print("", file=sys.stderr)
    print("Fix the diagnostics.  Do NOT move the file to "
          f"{rel_to_repo(BACKLOG)}: the backlog is monotonically "
          "non-increasing and check-clang-tidy-backlog-monotonic.sh will "
          "reject the demotion.", file=sys.stderr)
    return 1


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def mode_ratchet(exe: str, entries: list[dict], version: str) -> int:
    selected = {rel_to_repo(entry_source(e)): e for e in entries}
    allow = read_list(ALLOWLIST)
    backlog = read_list(BACKLOG)

    fixture_rel = rel_to_repo(FIXTURE)
    if fixture_rel in allow or fixture_rel in backlog:
        raise GateError(
            f"{fixture_rel} is a sensitivity fixture and must not appear in "
            "either list; it is deliberately not part of any build target")

    overlap = sorted(set(allow) & set(backlog))
    missing = sorted(set(selected) - set(allow) - set(backlog))
    unknown = sorted((set(allow) | set(backlog)) - set(selected))

    if overlap or missing or unknown:
        print("check-clang-tidy-ratchet: FAIL: the allowlist and the backlog "
              "no longer partition the libwirelog.so sources.", file=sys.stderr)
        if overlap:
            print("  listed in BOTH files:", file=sys.stderr)
            for path in overlap:
                print(f"    {path}", file=sys.stderr)
        if missing:
            print("  built but listed in NEITHER file (add a new file to the "
                  "allowlist if it is clean, to the backlog if it is not):",
                  file=sys.stderr)
            for path in missing:
                print(f"    {path}", file=sys.stderr)
        if unknown:
            print("  listed but no longer built (a deleted or renamed "
                  "source, or an allowlist line dropped to hide a "
                  "regression):", file=sys.stderr)
            for path in unknown:
                print(f"    {path}", file=sys.stderr)
        return 1

    targets = [selected[path] for path in allow]
    with tempfile.TemporaryDirectory(prefix="wl-tidy-") as tmp:
        workdir = pathlib.Path(tmp)
        prune_db([selected[p] for p in sorted(selected)], workdir)
        results = run_all(exe, workdir, targets)
    results += run_alternate_configs(exe, entries, set(allow))

    rc = report_failures(results)
    rc = max(rc, check_nolint_baseline(results, version))
    if rc == 0:
        print(f"check-clang-tidy-ratchet: OK; {len(allow)} file(s) clean, "
              f"{len(backlog)} in backlog, "
              f"{len(ALTERNATE_CONFIGS)} alternate configuration(s) checked")
    return rc


def rewrite_for_fixture(entry: dict, fixture: pathlib.Path,
                        stem: str) -> dict:
    """Retarget a donor database entry at the fixture.

    Six things describe the translation unit and all six must move together:
    `file`, `output`, `-o`, `-MQ`, `-MF` and the trailing `-c <source>`.
    Rewriting only `file` leaves clang-tidy analysing the DONOR source and
    reporting the fixture clean at exit 0 -- a silently vacuous check.
    """
    tokens = entry_command(entry)
    obj = f"{stem}.o"
    out: list[str] = []
    index = 0
    seen = {"-o": False, "-MQ": False, "-MF": False, "-c": False}
    while index < len(tokens):
        token = tokens[index]
        if token in ("-o", "-MQ", "-MF", "-c") and index + 1 < len(tokens):
            seen[token] = True
            out.append(token)
            out.append(str(fixture) if token == "-c"
                       else (obj + ".d" if token == "-MF" else obj))
            index += 2
            continue
        out.append(token)
        index += 1
    unrewritten = sorted(flag for flag, hit in seen.items() if not hit)
    if unrewritten:
        raise GateError(
            "donor compilation entry has no "
            f"{', '.join(unrewritten)}; refusing to build a fixture command "
            "that might still point at the donor source")
    return {
        "directory": entry["directory"],
        "command": shlex.join(out),
        "file": str(fixture),
        "output": obj,
    }


def fixture_expected_line(path: pathlib.Path, marker: str) -> int:
    if not path.is_file():
        raise GateError(f"sensitivity fixture missing: {path}")
    lines = path.read_text(encoding="utf-8").splitlines()
    hits = [n for n, text in enumerate(lines, 1) if marker in text]
    if len(hits) != 1:
        raise GateError(
            f"expected exactly one {marker} marker in "
            f"{rel_to_repo(path)}, found {len(hits)}")
    return hits[0]


def mode_sensitivity(exe: str, entries: list[dict], _version: str) -> int:
    """Prove the gate can still see a diagnostic it is supposed to see.

    A ratchet that reports everything clean because it silently analysed the
    wrong file, or lost its configuration, passes just as loudly as one that
    works.  This is the control: a fixture that is never compiled by any
    meson target, carrying one deliberate `bugprone-integer-division`.
    Zero diagnostics, a different check, a different line, or a compile
    error all fail.
    """
    expected_line = fixture_expected_line(FIXTURE, FIXTURE_MARKER)
    expected_header_line = fixture_expected_line(
        HEADER_FIXTURE, HEADER_FIXTURE_MARKER)
    donor = entries[0]

    with tempfile.TemporaryDirectory(prefix="wl-tidy-fixture-") as tmp:
        workdir = pathlib.Path(tmp)
        synthetic = rewrite_for_fixture(donor, FIXTURE, "wl_tidy_fixture")
        prune_db([synthetic], workdir)
        result = run_clang_tidy(exe, workdir, synthetic)

    diagnostics = parse_all_diagnostics(result.stdout,
                                        pathlib.Path(synthetic["directory"]))
    problems: list[str] = []
    if len(diagnostics) != 2:
        problems.append(f"expected 2 diagnostics, got {len(diagnostics)}")
    else:
        expected = {
            rel_to_repo(FIXTURE): expected_line,
            rel_to_repo(HEADER_FIXTURE): expected_header_line,
        }
        for diagnostic in diagnostics:
            match = DIAG_RE.match(diagnostic)
            assert match is not None
            path = match.group("path")
            check = CHECK_RE.search(match.group("message"))
            name = check.group(1) if check else "<no check name>"
            if name != FIXTURE_CHECK:
                problems.append(f"expected check {FIXTURE_CHECK}, got {name}")
            if path not in expected:
                problems.append(f"unexpected diagnostic path {path}")
            elif int(match.group("line")) != expected[path]:
                problems.append(f"expected {path} at line {expected[path]}, "
                                f"got {match.group('line')}")
            if match.group("severity") != "warning":
                problems.append(
                    f"expected a warning, got {match.group('severity')}")

    if not problems:
        print("check-clang-tidy-ratchet: OK; sensitivity fixtures report "
              f"{FIXTURE_CHECK} in source and header")
        return 0

    print("check-clang-tidy-ratchet: FAIL: the sensitivity fixture did not "
          "behave as expected, so a clean ratchet run proves nothing:",
          file=sys.stderr)
    for problem in problems:
        print(f"  {problem}", file=sys.stderr)
    print(f"  clang-tidy exit {result.returncode}", file=sys.stderr)
    print("  --- stdout ---", file=sys.stderr)
    for line in result.stdout.splitlines():
        print(f"  {line}", file=sys.stderr)
    print("  --- stderr ---", file=sys.stderr)
    for line in result.stderr.splitlines():
        print(f"  {line}", file=sys.stderr)
    return 1


def mode_regenerate(exe: str, entries: list[dict], _version: str) -> int:
    """Maintainer helper: print the clean/dirty split of every selected TU.

    Not wired into CI.  Run it after a toolchain bump to re-derive the two
    lists, then review the move of every single line by hand.
    """
    with tempfile.TemporaryDirectory(prefix="wl-tidy-") as tmp:
        workdir = pathlib.Path(tmp)
        prune_db(entries, workdir)
        results = run_all(exe, workdir, entries)
    results += run_alternate_configs(exe, entries)
    for result in sorted(results, key=lambda r: r.source):
        state = "DIRTY" if result.diagnostics else "clean"
        print(f"{state} {rel_to_repo(result.source)} "
              f"[{result.configuration}] ({len(result.diagnostics)})")
    return 0


MODES = {
    "ratchet": mode_ratchet,
    "sensitivity": mode_sensitivity,
    "regenerate": mode_regenerate,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--build-dir", required=True,
                        help="meson build directory holding "
                             "compile_commands.json")
    parser.add_argument("--mode", choices=sorted(MODES), default="ratchet")
    args = parser.parse_args()

    required = os.environ.get("WIRELOG_TIDY_REQUIRED") == "1"

    try:
        if os.environ.get("WIRELOG_TIDY_SKIP") == "1":
            raise SkipGate("WIRELOG_TIDY_SKIP=1 in the environment")
        check_platform()
        if not CONFIG_FILE.is_file():
            raise GateError(f"clang-tidy config missing: {CONFIG_FILE}")
        exe = resolve_clang_tidy()
        version = check_version(exe)
        check_config_baseline(exe, version)
        entries = select_entries(pathlib.Path(args.build_dir).resolve())
        check_configuration(entries)
        print(f"check-clang-tidy-ratchet: using {exe} (LLVM {version}), "
              f"{len(entries)} libwirelog.so translation unit(s), "
              f"{job_count()} job(s)")
        return MODES[args.mode](exe, entries, version)
    except SkipGate as skip:
        if required:
            print(f"check-clang-tidy-ratchet: FAIL: {skip} "
                  "(WIRELOG_TIDY_REQUIRED=1 forbids skipping)",
                  file=sys.stderr)
            return 1
        print(f"check-clang-tidy-ratchet: SKIP: {skip}")
        return SKIP_EXIT
    except GateError as err:
        print(f"check-clang-tidy-ratchet: FAIL: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
