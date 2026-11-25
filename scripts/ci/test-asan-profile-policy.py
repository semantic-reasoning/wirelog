#!/usr/bin/env python3
"""Keep main, PR and release Tier-1 ASan/UBSan on one optimized debug profile.

The grown lifecycle matrices time out in main's unoptimized debug profile.
This contract reads the canonical folded/literal Meson setup commands, like
our TSan policy; it is deliberately not a general YAML or shell parser.
Actual resolved compiler flags and runtime outcomes are validated separately.
"""
from pathlib import Path
import re
import shlex
import unittest

ROOT = Path(__file__).resolve().parents[2]
BUILDS = {"ci-main.yml": "builddir", "ci-pr.yml": "builddir-san",
          "tier1-sanitizers.yml": "builddir-san"}
JOBS = {"ci-main.yml": "sanitizer-matrix", "ci-pr.yml": "sanitizer-matrix",
        "tier1-sanitizers.yml": "asan-ubsan"}
REQUIRED = ("-Db_sanitize=address,undefined", "-Db_lundef=false",
            "-Db_lto=false", "-Dtests=true", "-Dexamples=false",
            "--buildtype=debugoptimized")
OPTIONAL = {"-Doptimization": "2", "-Ddebug": "true", "-Db_ndebug": "false"}


def sanitizer_job(text, job):
    jobs = re.findall(r"^  " + re.escape(job) + r":\n(.*?)(?=^  [\w-]+:\n|\Z)",
                      text, re.MULTILINE | re.DOTALL)
    assert len(jobs) == 1, f"expected exactly one {job} job"
    return jobs[0]


def setup_commands(job):
    lines = job.splitlines()
    commands = []
    for index, line in enumerate(lines):
        match = re.search(r"\bmeson setup\b", line.split("#", 1)[0])
        if not match:
            continue
        parts = [line[match.start():].strip().rstrip("\\")]
        for following in lines[index + 1:]:
            flag = following.strip()
            if flag.startswith("#"):
                continue
            if not flag.startswith(("-D", "--")):
                break
            parts.append(flag.rstrip("\\"))
        commands.append(shlex.split(" ".join(parts), comments=True))
    return commands


def validate(sources):
    assert set(sources) == set(BUILDS), "expected all known ASan workflows"
    for name, build in BUILDS.items():
        commands = setup_commands(sanitizer_job(sources[name], JOBS[name]))
        assert len(commands) == 1, f"{name}: expected one sanitizer setup"
        tokens = commands[0]
        assert tokens[:3] == ["meson", "setup", build], (name, tokens)
        for required in REQUIRED:
            key = required.split("=", 1)[0]
            actual = [t for t in tokens[3:] if t.split("=", 1)[0] == key]
            assert actual == [required], f"{name}: requires {required}: {actual}"
        for key, value in OPTIONAL.items():
            actual = [t for t in tokens[3:] if t.split("=", 1)[0] == key]
            assert actual in ([], [key + "=" + value]), (
                f"{name}: contradictory debug/optimization/assert option: {actual}")
        assert not any("NDEBUG" in t or "-fno-sanitize" in t or
                       re.search(r"(?:^|[= ])(?:-O[0-9sgz]|-Ofast|-g0)(?:$| )", t)
                       for t in tokens[3:]), (
            f"{name}: compiler arguments must not override the sanitizer profile")


class AsanProfilePolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sources = {n: (ROOT / ".github/workflows" / n).read_text(encoding="utf-8")
                       for n in BUILDS}

    def mutated(self, name, transform):
        sources = dict(self.sources)
        old = sanitizer_job(sources[name], JOBS[name])
        # Mutate commands, never an explanatory comment containing a flag.
        commands = "".join(line for line in old.splitlines(keepends=True)
                           if not line.lstrip().startswith("#"))
        new = transform(commands)
        self.assertNotEqual(commands, new, "mutation must change the actual job")
        sources[name] = sources[name].replace(old, new, 1)
        return sources

    def test_actual_workflows(self):
        validate(self.sources)

    def test_folded_and_literal_commands(self):
        for style in (">", "|"):
            sources = {name: "  " + JOBS[name] + ":\n    steps:\n      - run: " + style +
                       "\n          meson setup " + build + "\n          " +
                       "\n          ".join(REQUIRED) + "\n"
                       for name, build in BUILDS.items()}
            validate(sources)

    def test_explicit_compatible_options(self):
        for name in BUILDS:
            sources = self.mutated(name, lambda job: job.replace(
                "--buildtype=debugoptimized", "--buildtype=debugoptimized " +
                " ".join(k + "=" + v for k, v in OPTIONAL.items()), 1))
            validate(sources)

    def test_missing_workflow_or_job(self):
        with self.assertRaises(AssertionError):
            validate({"ci-main.yml": self.sources["ci-main.yml"]})
        for name in BUILDS:
            sources = dict(self.sources)
            sources[name] = sources[name].replace("  " + JOBS[name] + ":", "  removed:", 1)
            with self.subTest(name=name), self.assertRaises(AssertionError):
                validate(sources)

    def test_duplicate_job_rejected(self):
        for name in BUILDS:
            sources = dict(self.sources)
            sources[name] += "\n  " + JOBS[name] + ":\n    steps: []\n"
            with self.subTest(name=name), self.assertRaises(AssertionError):
                validate(sources)

    def test_tier1_exact_job_boundary(self):
        name = "tier1-sanitizers.yml"
        for decoy in ("sanitizer-matrix", "tsan", "msan-fuzz"):
            sources = dict(self.sources)
            # A valid-looking other job must not replace the missing ASan job.
            sources[name] = sources[name].replace("  asan-ubsan:", "  removed:", 1)
            sources[name] += ("\n  " + decoy + ":\n    steps:\n      - run: |\n" +
                              "          meson setup builddir-san " +
                              " ".join(REQUIRED) + "\n")
            with self.subTest(decoy=decoy), self.assertRaises(AssertionError):
                validate(sources)
        # An unrelated bad profile is ignored when the real job is present.
        sources = dict(self.sources)
        sources[name] += ("\n  sanitizer-matrix:\n    steps:\n      - run: |\n" +
                          "          meson setup decoy --buildtype=debug\n")
        validate(sources)

    def test_missing_or_duplicate_setup(self):
        for name in BUILDS:
            for transform in (lambda job: job.replace("meson setup", "echo setup", 1),
                              lambda job: job + "\n          meson setup extra\n"):
                sources = self.mutated(name, transform)
                with self.subTest(name=name), self.assertRaises(AssertionError):
                    validate(sources)

    def test_each_required_option_missing_or_duplicated(self):
        for name in BUILDS:
            for option in REQUIRED:
                for replacement in ("", option + " " + option):
                    sources = self.mutated(name, lambda job: job.replace(
                        option, replacement, 1))
                    with self.subTest(name=name, option=option, replacement=replacement):
                        with self.assertRaises(AssertionError):
                            validate(sources)

    def test_old_profile_rejected_in_each_workflow(self):
        for name in BUILDS:
            sources = self.mutated(name, lambda job: job.replace(
                "--buildtype=debugoptimized", "--buildtype=debug", 1).replace(
                "-Db_lto=false", "", 1))
            with self.subTest(name=name), self.assertRaises(AssertionError):
                validate(sources)

    def test_profile_overrides_rejected_in_each_workflow(self):
        for name in BUILDS:
            for extra in ("-Doptimization=0", "-Doptimization=1", "-Ddebug=false",
                          "-Db_ndebug=true", "-Db_ndebug=if-release", "-Db_lto=true",
                          "-Dc_args=-DNDEBUG", "-Dc_args=-O0", "-Dc_args=-g0",
                          "-Dc_args=-fno-sanitize=address"):
                sources = self.mutated(name, lambda job: job.replace(
                    "--buildtype=debugoptimized", "--buildtype=debugoptimized " + extra, 1))
                with self.subTest(name=name, extra=extra), self.assertRaises(AssertionError):
                    validate(sources)

    def test_instrumentation_removal_rejected(self):
        for name in BUILDS:
            for value in ("none", "address", "undefined"):
                sources = self.mutated(name, lambda job: job.replace(
                    "-Db_sanitize=address,undefined", "-Db_sanitize=" + value, 1))
                with self.subTest(name=name, value=value), self.assertRaises(AssertionError):
                    validate(sources)


if __name__ == "__main__":
    unittest.main()
