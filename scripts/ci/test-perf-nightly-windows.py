#!/usr/bin/env python3
"""Pin the nightly Windows toolchain wiring; native CI verifies MSVC execution.

This deliberately checks the workflow's existing block-scalar layout, not
general YAML. No YAML package is needed on the platforms running the ABI suite.
"""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/perf-nightly.yml"
CONFIGURE = "Configure release + trace ceiling (Windows)"
BUILD = "Build (Windows)"
TEST = "Run perf suite (Windows)"


def step(text: str, name: str) -> str:
    # Comments must not stand in for active options or environment settings.
    text = "\n".join(line for line in text.splitlines()
                     if line.strip() and not line.lstrip().startswith("#"))
    matches = re.findall(
        rf"^      - name: {re.escape(name)}\n(.*?)(?=^      - |^  \S|\Z)",
        text, re.MULTILINE | re.DOTALL)
    assert len(matches) == 1, f"expected exactly one step: {name}"
    return matches[0] + "\n"


def field(body: str, key: str) -> str:
    match = re.search(rf"^        {key}:(?: \|)?\n((?:          [^\n]*\n)+)",
                      body, re.MULTILINE)
    assert match, f"missing {key} block"
    return match[1]


def validate(text: str) -> None:
    assert re.search(r"- os: windows-latest\n\s+compiler: msvc\n\s+cc: cl\n", text), \
        "Windows matrix must select cl"
    bodies = {name: step(text, name) for name in (CONFIGURE, BUILD, TEST)}
    for name, body in bodies.items():
        assert "        if: runner.os == 'Windows'\n" in body, name
        assert "        shell: cmd\n" in body, name
        assert "vcvars" not in body, f"hardcoded activation in {name}"

    configure = bodies[CONFIGURE]
    command = field(configure, "run").strip().split()
    assert command[:3] == ["meson", "setup", "build-perf"], "Windows setup command"
    for option in ("--vsenv", "--buildtype=release", "-Dwirelog_log_max_level=trace",
                   "-Dtests=true", "-DmbedTLS=disabled"):
        assert option in command, f"Windows setup must include {option}"
    env = field(configure, "env")
    for variable in ("CC", "CXX"):
        assert f"          {variable}: ${{{{ matrix.cc }}}}\n" in env, \
            f"Windows configure must set {variable}"

    # Separate processes must use the configured build tree via Meson, which
    # restores the persisted --vsenv environment before invoking Ninja/tests.
    assert field(bodies[BUILD], "run").strip() == "meson compile -C build-perf", \
        "Windows build must use Meson"
    assert field(bodies[TEST], "run").strip() == (
        "meson test -C build-perf --suite perf --print-errorlogs --num-processes 1"
    ), "Windows test must use Meson and retain the perf suite"
    assert "          WIRELOG_PERF_GATE: '1'\n" in field(bodies[TEST], "env"), \
        "Windows perf gate must remain enabled"


class WindowsWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_actual_workflow(self):
        validate(self.text)

    def test_removing_activation_fails(self):
        with self.assertRaisesRegex(AssertionError, "must include --vsenv"):
            validate(self.text.replace(" --vsenv", ""))

    def test_comment_cannot_supply_activation(self):
        mutated = self.text.replace(" --vsenv", "")
        mutated = mutated.replace("        run: |", "        # --vsenv\n        run: |")
        with self.assertRaisesRegex(AssertionError, "must include --vsenv"):
            validate(mutated)

    def test_removing_compiler_selection_fails(self):
        for variable in ("CC", "CXX"):
            with self.subTest(variable=variable):
                line = f"          {variable}: ${{{{ matrix.cc }}}}\n"
                with self.assertRaisesRegex(AssertionError, f"must set {variable}"):
                    validate(self.text.replace(line, ""))

    def test_compiler_selection_in_wrong_step_fails(self):
        line = "          CC: ${{ matrix.cc }}\n"
        mutated = self.text.replace(line, "")
        mutated = mutated.replace(f"      - name: {BUILD}\n",
                                  f"      - name: {BUILD}\n        env:\n{line}")
        with self.assertRaisesRegex(AssertionError, "must set CC"):
            validate(mutated)

    def test_comment_cannot_supply_compiler_selection(self):
        with self.assertRaisesRegex(AssertionError, "must set CC"):
            validate(self.text.replace("          CC:", "          # CC:"))

    def test_activation_in_wrong_step_fails(self):
        mutated = self.text.replace(" --vsenv", "")
        mutated = mutated.replace("meson compile -C build-perf",
                                  "meson compile -C build-perf --vsenv")
        with self.assertRaisesRegex(AssertionError, "must include --vsenv"):
            validate(mutated)

    def test_gcc_matrix_fails(self):
        with self.assertRaisesRegex(AssertionError, "matrix must select cl"):
            validate(self.text.replace("            cc: cl", "            cc: gcc"))

    def test_bypassing_meson_build_fails(self):
        with self.assertRaisesRegex(AssertionError, "build must use Meson"):
            validate(self.text.replace("meson compile -C build-perf", "ninja -C build-perf"))

    def test_disabling_perf_gate_fails(self):
        with self.assertRaisesRegex(AssertionError, "gate must remain enabled"):
            validate(self.text.replace("WIRELOG_PERF_GATE: '1'", "WIRELOG_PERF_GATE: '0'"))


if __name__ == "__main__":
    unittest.main()
