#!/usr/bin/env python3
"""Self-test for check-program-lifetime.py (#1471).

Drives the gate over synthetic C sources in a temporary tree so every
verdict (offender shapes, allowed shapes, vacuity, skip, required
escalation) is exercised, plus one case over the real repository that must
report no offender and a sane number of scanned helpers and files.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
GATE = SCRIPT_DIR / "check-program-lifetime.py"
REPO_ROOT = SCRIPT_DIR.parents[1]


def load_gate():
    spec = importlib.util.spec_from_file_location("_program_lifetime", GATE)
    assert spec is not None and spec.loader is not None, GATE
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


HEADER = (
    "#include \"../wirelog/wirelog.h\"\n"
    "#include \"../wirelog/exec_plan_gen.h\"\n"
    "#include \"../wirelog/session.h\"\n\n"
)

# A caller that creates a session from the helper's plan, so a helper's
# escaping plan is what the guard must reason about.
CALLER = (
    "static int\n"
    "run(const char *src)\n"
    "{\n"
    "    wl_plan_t *plan = build_plan(src);\n"
    "    wl_session_t *sess = NULL;\n"
    "    if (!plan || wl_session_create(wl_backend_columnar(), plan, 1,\n"
    "        &sess) != 0)\n"
    "        return 1;\n"
    "    wl_session_destroy(sess);\n"
    "    wl_plan_free(plan);\n"
    "    return 0;\n"
    "}\n"
)

OFFENDER_RETURN = (
    "static wl_plan_t *\n"
    "build_plan(const char *src)\n"
    "{\n"
    "    wirelog_error_t err;\n"
    "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
    "    wl_plan_t *plan = NULL;\n"
    "    int rc = wl_plan_from_program(prog, &plan);\n"
    "    wirelog_program_free(prog);\n"
    "    if (rc != 0)\n"
    "        return NULL;\n"
    "    return plan;\n"
    "}\n"
)

HELD = (
    "static wl_plan_t *\n"
    "build_plan(const char *src)\n"
    "{\n"
    "    wirelog_error_t err;\n"
    "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
    "    wl_plan_t *plan = NULL;\n"
    "    int rc = wl_plan_from_program(prog, &plan);\n"
    "    if (rc != 0) {\n"
    "        wirelog_program_free(prog);\n"
    "        return NULL;\n"
    "    }\n"
    "    plan_fixture_hold(prog);\n"
    "    return plan;\n"
    "}\n"
)


class GateCase(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = load_gate()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "tests").mkdir()
        (self.root / "bench").mkdir()
        self.environ = dict(os.environ)
        os.environ.pop("WIRELOG_ABI_REQUIRED", None)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self.environ)
        self.tmp.cleanup()

    def write(self, name: str, body: str, sub: str = "tests") -> None:
        (self.root / sub / name).write_text(HEADER + body + CALLER,
                                            encoding="utf-8")

    def run_gate(self, root: Path | None = None) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = self.gate.main(["gate", str(root or self.root)])
        return rc, out.getvalue(), err.getvalue()

    def assert_offender(self, name: str, function: str, line: int) -> None:
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1, err)
        self.assertIn(f"tests/{name}:{function}:{line}:", err)

    def assert_clean(self, helpers: int = 1) -> None:
        rc, out, err = self.run_gate()
        self.assertEqual(rc, 0, err)
        self.assertIn(f"ok {helpers} plan-building helpers", out)

    # ---- offenders -----------------------------------------------------

    def test_free_before_returning_plan(self) -> None:
        self.write("a.c", OFFENDER_RETURN)
        self.assert_offender("a.c", "build_plan", 12)

    def test_free_before_session_create(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "build_plan_and_run(const char *src)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    wl_session_t *sess = NULL;\n"
                   "    if (wl_plan_from_program(prog, &plan) != 0) {\n"
                   "        wirelog_program_free(prog);\n"
                   "        return 1;\n"
                   "    }\n"
                   "    wirelog_program_free(prog);\n"
                   "    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0)\n"
                   "        return 1;\n"
                   "    wl_session_destroy(sess);\n"
                   "    return 0;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_offender("a.c", "build_plan_and_run", 16)

    def test_free_after_out_parameter_store(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "build_plan_into(const char *src, wl_plan_t **out_plan)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    if (wl_plan_from_program(prog, &plan) != 0) {\n"
                   "        wirelog_program_free(prog);\n"
                   "        return -1;\n"
                   "    }\n"
                   "    *out_plan = plan;\n"
                   "    wirelog_program_free(prog);\n"
                   "    return 0;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_offender("a.c", "build_plan_into", 16)

    def test_free_and_return_on_one_line(self) -> None:
        self.write("a.c",
                   "static wl_plan_t *\n"
                   "build_plan(const char *src)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    (void)wl_plan_from_program(prog, &plan);\n"
                   "    wirelog_program_free(prog); return plan;\n"
                   "}\n")
        self.assert_offender("a.c", "build_plan", 12)

    def test_create_before_free_with_conditional_destroy(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "build_plan_and_run(const char *src)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    wl_session_t *sess = NULL;\n"
                   "    if (wl_plan_from_program(prog, &plan) != 0) {\n"
                   "        wirelog_program_free(prog);\n"
                   "        return 1;\n"
                   "    }\n"
                   "    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);\n"
                   "    if (sess)\n"
                   "        wl_session_destroy(sess);\n"
                   "    wirelog_program_free(prog);\n"
                   "    return rc == 0 && plan != NULL ? 0 : 1;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_offender("a.c", "build_plan_and_run", 19)

    def test_with_snapshot_variant_is_scanned(self) -> None:
        self.write("a.c", OFFENDER_RETURN.replace(
            "wl_plan_from_program(prog, &plan)",
            "wl_plan_from_program_with_snapshot(prog, NULL, &plan)"))
        self.assert_offender("a.c", "build_plan", 12)

    def test_bench_tree_is_scanned(self) -> None:
        self.write("b.c", OFFENDER_RETURN, sub="bench")
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("bench/b.c:build_plan:12:", err)

    # ---- allowed shapes ------------------------------------------------

    def test_held_program_is_clean(self) -> None:
        self.write("a.c", HELD)
        self.assert_clean()

    def test_held_helper_does_not_exempt_sibling(self) -> None:
        self.write("a.c", HELD + OFFENDER_RETURN.replace("build_plan",
                                                         "build_other"))
        self.assert_offender("a.c", "build_other", 26)

    def test_failure_branch_free_braced_and_braceless(self) -> None:
        self.write("a.c",
                   "static wl_plan_t *\n"
                   "build_plan(const char *src)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    int rc = wl_plan_from_program(prog, &plan);\n"
                   "    if (rc != 0)\n"
                   "        wirelog_program_free(prog);\n"
                   "    if (rc != 0) {\n"
                   "        wirelog_program_free(prog);\n"
                   "        return NULL;\n"
                   "    }\n"
                   "    plan_fixture_hold(prog);\n"
                   "    return plan;\n"
                   "}\n")
        self.assert_clean()

    def test_free_after_plan_free_is_clean(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "roundtrip(const char *src)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    wl_session_t *sess = NULL;\n"
                   "    if (wl_plan_from_program(prog, &plan) != 0) {\n"
                   "        wirelog_program_free(prog);\n"
                   "        return 1;\n"
                   "    }\n"
                   "    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);\n"
                   "    if (sess)\n"
                   "        wl_session_destroy(sess);\n"
                   "    wl_plan_free(plan);\n"
                   "    wirelog_program_free(prog);\n"
                   "    return rc;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_clean()

    def test_free_after_unconditional_destroy_is_clean(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "roundtrip(const char *src)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    wl_session_t *sess = NULL;\n"
                   "    if (wl_plan_from_program(prog, &plan) != 0) {\n"
                   "        wirelog_program_free(prog);\n"
                   "        return 1;\n"
                   "    }\n"
                   "    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {\n"
                   "        wl_plan_free(plan);\n"
                   "        wirelog_program_free(prog);\n"
                   "        return 1;\n"
                   "    }\n"
                   "    wl_session_destroy(sess);\n"
                   "    wirelog_program_free(prog);\n"
                   "    wl_plan_free(plan);\n"
                   "    return 0;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_clean()

    def test_plan_variable_reuse_after_release_is_clean(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "twice(const char *src)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    if (wl_plan_from_program(prog, &plan) != 0)\n"
                   "        return 1;\n"
                   "    wl_plan_free(plan);\n"
                   "    wirelog_program_free(prog);\n"
                   "    prog = wirelog_parse_string(src, &err);\n"
                   "    if (wl_plan_from_program(prog, &plan) != 0)\n"
                   "        return 1;\n"
                   "    wl_plan_free(plan);\n"
                   "    wirelog_program_free(prog);\n"
                   "    return 0;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_clean()

    def test_plan_call_in_loop_with_continue_is_clean(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "many(const char *const *srcs, int n)\n"
                   "{\n"
                   "    for (int i = 0; i < n; i++) {\n"
                   "        wirelog_error_t err;\n"
                   "        wirelog_program_t *prog = wirelog_parse_string(srcs[i], &err);\n"
                   "        wl_plan_t *plan = NULL;\n"
                   "        if (wl_plan_from_program(prog, &plan) != 0) {\n"
                   "            wirelog_program_free(prog);\n"
                   "            continue;\n"
                   "        }\n"
                   "        wl_plan_free(plan);\n"
                   "        wirelog_program_free(prog);\n"
                   "    }\n"
                   "    return 0;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_clean()

    def test_strings_and_comments_do_not_skew_depth(self) -> None:
        # Three lines of brace-carrying literals and comments precede an
        # offender, whose line must still be reported at the right place.
        self.write("a.c",
                   "static const char *SRC = \"rule(x) :- { // not a comment {\";\n"
                   "static const char BRACE = '{';\n"
                   "/* { a brace in a comment */\n"
                   + OFFENDER_RETURN.replace("wirelog_error_t err;",
                                             "wirelog_error_t err; // {"))
        self.assert_offender("a.c", "build_plan", 15)

    @unittest.expectedFailure
    def test_goto_label_free_is_a_documented_gap(self) -> None:
        self.write("a.c",
                   "static int\n"
                   "build_plan_into(const char *src, wl_plan_t **out_plan)\n"
                   "{\n"
                   "    wirelog_error_t err;\n"
                   "    wirelog_program_t *prog = wirelog_parse_string(src, &err);\n"
                   "    wl_plan_t *plan = NULL;\n"
                   "    int rc = wl_plan_from_program(prog, &plan);\n"
                   "    if (rc != 0)\n"
                   "        goto done;\n"
                   "    *out_plan = plan;\n"
                   "    goto done;\n"
                   "done:\n"
                   "    wirelog_program_free(prog);\n"
                   "    return rc;\n"
                   "}\n"
                   "static wl_plan_t *build_plan(const char *s) { (void)s; return NULL; }\n")
        self.assert_offender("a.c", "build_plan_into", 17)

    # ---- gate mechanics --------------------------------------------------

    def test_vacuity_floor(self) -> None:
        (self.root / "tests" / "a.c").write_text(HEADER + CALLER,
                                                 encoding="utf-8")
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("no plan-building helper", err)

    def test_missing_tree_skips(self) -> None:
        rc, out, _ = self.run_gate(self.root / "absent")
        self.assertEqual(rc, 77)
        self.assertIn("SKIP", out)

    def test_required_turns_skip_into_failure(self) -> None:
        os.environ["WIRELOG_ABI_REQUIRED"] = "1"
        rc, _, err = self.run_gate(self.root / "absent")
        self.assertEqual(rc, 1)
        self.assertIn("gate required but would skip", err)

    def test_usage(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = self.gate.main(["gate"])
        self.assertEqual(rc, 1)
        self.assertIn("usage", err.getvalue())

    def test_repository_is_clean(self) -> None:
        """The real tree must have no offender and a sane scan extent."""
        helpers, files, offenders = self.gate.scan_tree(REPO_ROOT)
        self.assertEqual(offenders, [])
        self.assertGreater(helpers, 100)
        self.assertGreaterEqual(files, 26)


if __name__ == "__main__":
    unittest.main()
