#!/usr/bin/env python3
"""Execute the production Meson workaround block with compiler-query fixtures."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
BEGIN = "# BEGIN ARM ASan strict-align workaround"
END = "# END ARM ASan strict-align workaround"


class SelectionTest(unittest.TestCase):
    def configure(self, compiler="gcc", version="13.3.0", cpu="aarch64",
                  sanitizer="address,undefined", compile_support=True,
                  link_support=True, selected=True):
        source = (ROOT / "meson.build").read_text()
        self.assertEqual(source.count(BEGIN), 1)
        self.assertEqual(source.count(END), 1)
        block = source.split(BEGIN, 1)[1].split(END, 1)[0]
        # Substitute only platform/compiler queries. Meson itself evaluates
        # the production condition, version comparison, errors and flag calls.
        queries = {
            "cc.get_id()": repr(compiler),
            "cc.version()": repr(version),
            "host_machine.cpu_family()": repr(cpu),
            "get_option('b_sanitize')": repr(sanitizer),
            "cc.has_argument('-mstrict-align')": str(compile_support).lower(),
            "cc.has_link_argument('-mstrict-align')": str(link_support).lower(),
        }
        for query, value in queries.items():
            self.assertIn(query, block)
            block = block.replace(query, value)
        meson = shutil.which("meson")
        self.assertIsNotNone(meson, "Meson is required for selection fixtures")
        with tempfile.TemporaryDirectory(prefix="arm-asan-selection-") as tmp:
            root = Path(tmp)
            (root / "meson.build").write_text(
                "project('selection', 'c', default_options: ['b_lto=true'])\n"
                + block + "\nexecutable('probe', 'probe.c')\n")
            (root / "probe.c").write_text("int main(void) { return 0; }\n")
            # Do not inherit a parent's target compiler or cross-file choice.
            env = os.environ.copy()
            env.pop("CC", None)
            result = subprocess.run(
                [meson, "setup", str(root / "build"), str(root), "--backend=ninja"],
                env=env, capture_output=True, text=True, timeout=30)
            output = result.stdout + result.stderr
            if selected and not (compile_support and link_support):
                self.assertNotEqual(result.returncode, 0, output)
                self.assertIn("requires compiler and linker support for -mstrict-align", output)
                return
            self.assertEqual(result.returncode, 0, output)
            commands = json.loads((root / "build/compile_commands.json").read_text())
            self.assertEqual("-mstrict-align" in commands[0]["command"], selected)
            ninja = (root / "build/build.ninja").read_text()
            link_lines = [line for line in ninja.splitlines()
                          if line.strip().startswith("LINK_ARGS =")]
            self.assertTrue(link_lines)
            self.assertEqual(any("-mstrict-align" in line for line in link_lines), selected)
            self.assertEqual("Applying ARM GCC ASan FakeStack workaround" in output, selected)

    def test_affected_versions(self):
        for version in ("13.3.0", "14.2.0"):
            with self.subTest(version=version):
                self.configure(version=version)

    def test_unaffected_configurations(self):
        cases = [dict(version="14.3.0"), dict(version="15.1.0"),
                 dict(cpu="x86_64"), dict(compiler="clang"),
                 dict(compiler="msvc"), dict(sanitizer="none"),
                 dict(sanitizer="thread")]
        for case in cases:
            with self.subTest(**case):
                self.configure(**case, selected=False,
                               compile_support=False, link_support=False)

    def test_required_support(self):
        for compile_support, link_support in ((False, True), (True, False),
                                              (False, False)):
            with self.subTest(compile=compile_support, link=link_support):
                self.configure(compile_support=compile_support,
                               link_support=link_support)


if __name__ == "__main__":
    unittest.main()
