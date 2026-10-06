#!/usr/bin/env python3
"""Self-test classify-perf-surface.py (#2084) against a fixture repository.

Each case commits one kind of change on top of a fixed base and checks the
verdict.  Every case that must run the strict suite is also a mutation
witness for the rule that catches it: weakening that rule flips the case to
`applicable=false` and fails it here.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ci/classify-perf-surface.py"
WORKFLOW = ROOT / ".github/workflows/perf-suite-required.yml"

spec = importlib.util.spec_from_file_location("classify_perf_surface", SCRIPT)
classifier = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = classifier
spec.loader.exec_module(classifier)

OPS_C = """\
/* ops.c fixture */
#include "columnar/internal.h"

#define OPS_WIDTH(x) \\
    ((x) * 2)

static int ops_counter;

struct ops_pair {
    int left;
};

static const int ops_table[] = {
    1, 2,
};

/* A file-scope comment. */
int
col_op_fixture(int value)
{
    const char *text = "{ not a brace }";
    char quote = '}';
    if (value > 0) {
        ops_counter += value;   /* { comment brace */
    }
    return OPS_WIDTH(value) + (int)text[0] + quote;
}

static int
col_op_other(void)
{
    return ops_counter;
}
"""

MERGE_C = """\
#include "columnar/internal.h"

int
col_rel_compact_runs(void *rel)
{
    return rel != 0;
}
"""


def run(cwd: Path, *args: str) -> str:
    result = subprocess.run(list(args), cwd=cwd, capture_output=True,
                            text=True, encoding="utf-8", check=False)
    if result.returncode != 0:
        raise AssertionError(f"{args} failed: {result.stderr}")
    return result.stdout


class ClassifierTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="perf-surface-"))
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        run(self.repo, "git", "init", "-q", "-b", "main")
        run(self.repo, "git", "config", "user.email", "fixture@example.invalid")
        run(self.repo, "git", "config", "user.name", "fixture")
        run(self.repo, "git", "config", "commit.gpgsign", "false")
        run(self.repo, "git", "config", "core.autocrlf", "false")
        self.write("wirelog/columnar/ops.c", OPS_C)
        self.write("wirelog/columnar/merge.c", MERGE_C)
        self.write("wirelog/columnar/lftj.c", "int lftj(void) { return 0; }\n")
        self.write("docs/notes.md", "notes\n")
        self.write("scripts/ci/classify-perf-surface.py", "# classifier\n")
        self.write(".github/workflows/perf-suite-required.yml", "name: x\n")
        self.base = self.commit("base")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, path: str, text: str) -> None:
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def commit(self, message: str) -> str:
        run(self.repo, "git", "add", "-A")
        run(self.repo, "git", "commit", "-q", "--allow-empty", "-m", message)
        return run(self.repo, "git", "rev-parse", "HEAD").strip()

    def edit_ops(self, old: str, new: str) -> None:
        text = (self.repo / "wirelog/columnar/ops.c").read_text(encoding="utf-8")
        self.assertEqual(text.count(old), 1, old)
        self.write("wirelog/columnar/ops.c", text.replace(old, new))

    def verdict(self, base: str | None = None, head: str | None = None):
        head = head or self.commit("change")
        return classifier.classify(self.repo, base or self.base, head)

    def expect(self, applicable: bool, reason: str | None = None,
               base: str | None = None, head: str | None = None) -> None:
        try:
            verdict = self.verdict(base, head)
            got, reasons = verdict.applicable, verdict.reasons
        except classifier.Uncertain as error:
            got, reasons = True, [f"uncertain: {error}"]
        self.assertEqual(got, applicable, reasons)
        if reason is not None:
            self.assertTrue(any(reason in r for r in reasons),
                            f"{reason!r} not in {reasons}")

    # --- not applicable ------------------------------------------------
    def test_ops_body_edit_is_unrelated(self) -> None:
        self.edit_ops("ops_counter += value;", "ops_counter += value * 2;")
        self.expect(False)

    def test_ops_body_edit_beside_unrelated_file_is_unrelated(self) -> None:
        self.edit_ops("return ops_counter;", "return ops_counter + 1;")
        self.write("wirelog/columnar/lftj.c", "int lftj(void) { return 1; }\n")
        self.expect(False)

    def test_ops_comment_only_edit_is_unrelated(self) -> None:
        self.edit_ops("/* A file-scope comment. */", "/* A reworded comment. */")
        self.expect(False)

    def test_braces_in_strings_and_comments_do_not_count(self) -> None:
        self.edit_ops('char quote = \'}\';', 'char quote = \'{\';')
        self.expect(False)

    # --- applicable: protected surface ---------------------------------
    def test_merge_c_edit_runs_the_suite(self) -> None:
        self.write("wirelog/columnar/merge.c", MERGE_C.replace("!=", "=="))
        self.expect(True, "protected wirelog/columnar/merge.c")

    def test_mixed_ops_and_merge_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;", "ops_counter -= value;")
        self.write("wirelog/columnar/merge.c", MERGE_C + "\n")
        self.expect(True, "merge.c")

    def test_rename_out_of_protected_path_runs_the_suite(self) -> None:
        run(self.repo, "git", "mv", "wirelog/columnar/merge.c",
            "wirelog/columnar/compact.c")
        self.expect(True, "protected wirelog/columnar/merge.c")

    def test_rename_into_protected_path_runs_the_suite(self) -> None:
        run(self.repo, "git", "rm", "-q", "wirelog/columnar/merge.c")
        self.commit("drop merge.c")
        base = run(self.repo, "git", "rev-parse", "HEAD").strip()
        run(self.repo, "git", "mv", "wirelog/columnar/lftj.c",
            "wirelog/columnar/merge.c")
        self.expect(True, "protected wirelog/columnar/merge.c", base=base)

    def test_deleting_merge_c_runs_the_suite(self) -> None:
        run(self.repo, "git", "rm", "-q", "wirelog/columnar/merge.c")
        self.expect(True, "D touches protected")

    def test_classifier_change_runs_the_suite(self) -> None:
        self.write("scripts/ci/classify-perf-surface.py", "# weakened\n")
        self.expect(True, "classify-perf-surface.py")

    def test_workflow_change_runs_the_suite(self) -> None:
        self.write(".github/workflows/perf-suite-required.yml", "name: y\n")
        self.expect(True, "perf-suite-required.yml")

    def test_protected_glob_matches_nested_data(self) -> None:
        self.write("bench/data/crdt/sub/edges.csv", "1,2\n")
        self.expect(True, "bench/data/crdt/sub/edges.csv")

    # --- applicable: ops.c edits that are not provably unrelated -------
    def test_ops_include_edit_runs_the_suite(self) -> None:
        self.edit_ops('#include "columnar/internal.h"',
                      '#include "columnar/internal.h"\n#include <stdio.h>')
        self.expect(True, "preprocessor line")

    def test_ops_macro_continuation_edit_runs_the_suite(self) -> None:
        self.edit_ops("    ((x) * 2)", "    ((x) * 3)")
        self.expect(True, "preprocessor line")

    def test_ops_file_scope_declaration_edit_runs_the_suite(self) -> None:
        self.edit_ops("static int ops_counter;", "static long ops_counter;")
        self.expect(True, "outside a function body")

    def test_ops_signature_edit_runs_the_suite(self) -> None:
        self.edit_ops("col_op_fixture(int value)", "col_op_fixture(long value)")
        self.expect(True, "outside a function body")

    def test_ops_body_naming_compaction_runs_the_suite(self) -> None:
        self.edit_ops("return ops_counter;",
                      "return ops_counter + col_rel_compact_runs(0);")
        self.expect(True, "protected compaction symbol")

    def test_ops_rename_runs_the_suite(self) -> None:
        run(self.repo, "git", "mv", "wirelog/columnar/ops.c",
            "wirelog/columnar/ops2.c")
        self.expect(True, "only an in-place edit")

    def test_ops_deletion_runs_the_suite(self) -> None:
        run(self.repo, "git", "rm", "-q", "wirelog/columnar/ops.c")
        self.expect(True, "only an in-place edit")

    @unittest.skipIf(os.name == "nt", "needs POSIX symlinks")
    def test_ops_type_change_runs_the_suite(self) -> None:
        ops = self.repo / "wirelog/columnar/ops.c"
        ops.unlink()
        os.symlink("merge.c", ops)
        self.expect(True, "only an in-place edit")

    @unittest.skipIf(os.name == "nt", "needs an executable file mode")
    def test_ops_mode_change_runs_the_suite(self) -> None:
        (self.repo / "wirelog/columnar/ops.c").chmod(0o755)
        self.expect(True, "no textual change")

    def test_ops_unbalanced_braces_run_the_suite(self) -> None:
        self.edit_ops("    return ops_counter;\n}", "    return ops_counter;\n")
        self.expect(True, "braces do not balance")

    def test_ops_unterminated_comment_runs_the_suite(self) -> None:
        self.edit_ops("    return ops_counter;\n}\n",
                      "    return ops_counter;\n}\n/* trailing\n")
        self.expect(True, "unterminated block comment")

    # --- applicable: review #2084 bypasses --------------------------------
    def test_function_closed_mid_line_runs_the_suite(self) -> None:
        self.edit_ops("    return ops_counter;\n}",
                      "    return ops_counter; } int memmove_hook(void) {\n"
                      "    return 0;\n}")
        self.expect(True, "outside a function body")

    def test_comment_continued_by_backslash_runs_the_suite(self) -> None:
        # The compiler folds the `{` into the comment, so the visible `}`
        # that follows closes col_op_fixture early and the next line lands
        # at file scope.  A scanner that did not splice would count the
        # hidden brace and judge every line below a body edit.
        self.edit_ops("        ops_counter += value;   /* { comment brace */\n",
                      "        ops_counter += value;   // hide \\\n"
                      "        {\n"
                      "    }\n"
                      "    }\n"
                      "int smuggled_global = 1;\n"
                      "static void smuggled(void) {\n"
                      "    {\n")
        self.expect(True, "outside a function body")

    def test_inline_assembly_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;",
                      'ops_counter += value; __asm__(".globl memmove");')
        self.expect(True, "assembly, an attribute or a pragma")

    def test_body_static_in_a_load_section_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;",
                      "static void (*hook)(void) __attribute__((used, "
                      'section(".init_array"))) = 0;\n        ops_counter += value;')
        self.expect(True, "assembly, an attribute or a pragma")

    def test_short_attribute_spelling_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;",
                      "static void (*hook)(void) __attribute((used, "
                      'section(".init_array"))) = 0;\n        ops_counter += value;')
        self.expect(True, "assembly, an attribute or a pragma")

    def test_standard_attribute_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;",
                      '[[gnu::used, gnu::section(".init_array")]] '
                      "static void (*hook)(void) = 0;\n        ops_counter += value;")
        self.expect(True, "assembly, an attribute or a pragma")

    def test_body_pragma_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;",
                      '_Pragma("GCC optimize(\\"O0\\")") ops_counter += value;')
        self.expect(True, "assembly, an attribute or a pragma")

    def test_edit_that_nests_the_next_function_runs_the_suite(self) -> None:
        # Both changed lines stay inside bodies, but together they move the
        # unchanged header of col_op_other into col_op_fixture's body.
        self.edit_ops("        ops_counter += value;   /* { comment brace */",
                      "        if (value) {")
        self.edit_ops("    return ops_counter;\n}", "    }\n}")
        self.expect(True, "is unchanged but moves")

    def test_summary_escapes_untrusted_paths(self) -> None:
        verdict = classifier.Verdict()
        verdict.require("M touches protected a`b<script>")
        text = classifier.summary_text(verdict, "`base`", "head\nnext")
        self.assertNotIn("<script>", text)
        self.assertNotIn("a`b", text)
        self.assertNotIn("head\nnext", text)

    def test_digraph_directive_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;", "%:define OPS_X 1\n        ops_counter += value;")
        self.expect(True, "digraph")

    def test_trigraph_directive_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;", "??=define OPS_X 1\n        ops_counter += value;")
        self.expect(True, "trigraph")

    def test_directive_after_comment_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;", "/**/ #define OPS_X 1\n        ops_counter += value;")
        self.expect(True, "preprocessor line")

    def test_split_protected_name_runs_the_suite(self) -> None:
        self.edit_ops("return ops_counter;",
                      "(void)col_rel_compact\\\n_runs(0);\n    return ops_counter;")
        self.expect(True, "protected compaction symbol")

    def test_continuation_off_the_end_runs_the_suite(self) -> None:
        self.edit_ops("    return ops_counter;\n}\n",
                      "    return ops_counter;\n}\n\\")
        self.expect(True, "runs off the end")

    def test_unchanged_pairs_follow_insertions_and_deletions(self) -> None:
        # git -U0: an insertion after old line 2, a deletion of old line 2,
        # an insertion at the top and a deletion of the first line.
        pairs = classifier.unchanged_pairs
        self.assertEqual(list(pairs([(2, 0, 3, 1)], 3, 4)),
                         [(1, 1), (2, 2), (3, 4)])
        self.assertEqual(list(pairs([(2, 1, 1, 0)], 3, 2)), [(1, 1), (3, 2)])
        self.assertEqual(list(pairs([(0, 0, 1, 1)], 2, 3)), [(1, 2), (2, 3)])
        self.assertEqual(list(pairs([(1, 1, 0, 0)], 2, 1)), [(2, 1)])
        with self.assertRaises(classifier.Uncertain):
            list(pairs([(2, 1, 2, 1)], 3, 4))

    def test_glob_star_stays_within_a_directory(self) -> None:
        self.assertIsNone(classifier.glob_regex("a/*.h").match("a/b/c.h"))
        self.assertIsNotNone(classifier.glob_regex("a/**/*.h").match("a/c.h"))
        self.assertIsNotNone(classifier.glob_regex("a/**/*.h").match("a/b/c.h"))
        self.assertIsNotNone(classifier.glob_regex("a/**").match("a/b/c"))

    def test_lone_carriage_return_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;", "ops_counter += value\r+0;")
        self.expect(True, "carriage return")

    def test_crlf_file_body_edit_is_unrelated(self) -> None:
        self.write("wirelog/columnar/ops.c", OPS_C.replace("\n", "\r\n"))
        base = self.commit("crlf")
        text = (self.repo / "wirelog/columnar/ops.c").read_bytes()
        self.write("wirelog/columnar/ops.c",
                   text.decode().replace("+= value;", "+= value * 5;"))
        self.expect(False, base=base)

    def test_file_scope_struct_edit_runs_the_suite(self) -> None:
        self.edit_ops("    int left;", "    long left;")
        self.expect(True, "outside a function body")

    def test_file_scope_initializer_edit_runs_the_suite(self) -> None:
        self.edit_ops("    1, 2,", "    1, 3,")
        self.expect(True, "outside a function body")

    def test_deleted_include_runs_the_suite(self) -> None:
        self.edit_ops('#include "columnar/internal.h"\n', "")
        self.expect(True, "(old) changes a preprocessor line")

    def test_base_branch_progress_is_not_part_of_the_diff(self) -> None:
        run(self.repo, "git", "checkout", "-q", "-b", "topic")
        self.edit_ops("ops_counter += value;", "ops_counter += 6;")
        head = self.commit("topic body edit")
        run(self.repo, "git", "checkout", "-q", "main")
        self.write("wirelog/columnar/merge.c", MERGE_C + "/* later */\n")
        self.edit_ops("static int ops_counter;",
                      "static int ops_counter;\nstatic int ops_main_only;")
        moved = self.commit("main moves on")
        self.expect(False, base=moved, head=head)

    # --- applicable: history and input ---------------------------------
    def test_unrelated_only_change_runs_the_suite(self) -> None:
        self.write("docs/notes.md", "changed\n")
        self.expect(True, "path filter fired")

    def test_unknown_head_runs_the_suite(self) -> None:
        self.expect(True, "not in the local history", head="0" * 40)

    def test_unknown_base_runs_the_suite(self) -> None:
        self.edit_ops("ops_counter += value;", "ops_counter += 3;")
        head = self.commit("change")
        self.expect(True, "not in the local history", base="1" * 40,
                    head=head)

    def test_malformed_sha_runs_the_suite(self) -> None:
        self.expect(True, "not a full hexadecimal", head="HEAD")

    def test_unrelated_histories_run_the_suite(self) -> None:
        run(self.repo, "git", "checkout", "-q", "--orphan", "other")
        run(self.repo, "git", "rm", "-rqf", ".")
        self.write("wirelog/columnar/ops.c", OPS_C.replace("+= value", "+= 9"))
        self.expect(True, "merge-base")

    # --- command line ----------------------------------------------------
    def test_cli_prints_one_line_and_writes_summary(self) -> None:
        self.edit_ops("ops_counter += value;", "ops_counter += 4;")
        head = self.commit("change")
        summary = self.tmp / "summary.md"
        out = run(ROOT, sys.executable, str(SCRIPT), "--repo", str(self.repo),
                  "--base", self.base, "--head", head, "--summary", str(summary))
        self.assertEqual(out, "applicable=false\n")
        text = summary.read_text(encoding="utf-8")
        self.assertIn(self.base, text)
        self.assertIn(head, text)
        self.assertIn("not applicable", text)

    def test_unknown_change_type_runs_the_suite(self) -> None:
        # git does not emit these for commit-to-commit diffs, so the parser
        # is stubbed to deliver one.
        head = self.commit("empty")
        with mock.patch.object(classifier, "name_status",
                               return_value=[("X", ["wirelog/columnar/ops.c"])]):
            self.expect(True, "unknown change type", head=head)

    def test_unexpected_exception_fails_closed(self) -> None:
        out = io.StringIO()
        with mock.patch.object(classifier, "classify",
                               side_effect=RuntimeError("boom")), \
                contextlib.redirect_stdout(out):
            rc = classifier.main(["--repo", str(self.repo), "--base", self.base,
                                  "--head", self.base])
        self.assertEqual((rc, out.getvalue()), (0, "applicable=true\n"))

    def test_cli_fails_closed_when_git_fails(self) -> None:
        out = run(ROOT, sys.executable, str(SCRIPT), "--repo",
                  str(self.tmp / "missing"), "--base", self.base,
                  "--head", self.base)
        self.assertEqual(out, "applicable=true\n")


class WorkflowContractTest(unittest.TestCase):
    def test_path_filter_matches_the_classifier(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        block = text.split("\n    paths:\n", 1)[1].split("\n\n", 1)[0]
        paths = re.findall(r"(?m)^      - '([^']+)'\s*$", block)
        self.assertEqual(sorted(paths),
                         sorted(classifier.PROTECTED + (classifier.CANDIDATE,)))

    def test_workflow_runs_the_base_classifier(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        step = text.split("      - name: Classify the change", 1)[1]
        step = step.split("\n      - name:", 1)[0]
        self.assertIn('git show "$BASE_SHA:scripts/ci/classify-perf-surface.py"'
                      ' > "$trusted"', step)
        self.assertIn('python3 "$trusted"', step)
        self.assertNotIn("HEAD_SHA:scripts", step)
        self.assertIn("fetch-depth: 0", text)

    def test_every_suite_step_is_gated(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        for name in ("Install dependencies", "Set up Meson 1.12.0",
                     "Set cpufreq governor (best-effort)",
                     "Configure release + trace ceiling", "Build",
                     "Run perf suite"):
            block = text.split(f"      - name: {name}\n", 1)[1]
            block = block.split("\n      - name:", 1)[0]
            self.assertIn("if: steps.classify.outputs.applicable == 'true'",
                          block, name)

    def test_include_closure_is_protected(self) -> None:
        # merge.c and both gates, through every in-repo include.  A header
        # meson generates must have a protected template; the only other
        # unresolved include allowed is the nanoarrow subproject's.
        external = {"nanoarrow/nanoarrow.h"}
        seen = set()
        stack = ["wirelog/columnar/merge.c", "tests/test_crdt_perf_gate.c",
                 "tests/test_cspa_perf_gate.c"]
        include = re.compile(r'(?m)^\s*#\s*include\s*"([^"]+)"')
        while stack:
            path = stack.pop()
            if path in seen:
                continue
            seen.add(path)
            text = (ROOT / path).read_text(encoding="utf-8", errors="replace")
            for name in include.findall(text):
                for base in (os.path.dirname(path), "wirelog", "."):
                    candidate = os.path.normpath(os.path.join(base, name))
                    if (ROOT / candidate).is_file():
                        stack.append(candidate.replace(os.sep, "/"))
                        break
                    if (ROOT / (candidate + ".in")).is_file():
                        seen.add((candidate + ".in").replace(os.sep, "/"))
                        break
                else:
                    self.assertIn(name, external, f"{path} includes {name}")
        self.assertGreater(len(seen), 10)
        self.assertIn("tests/test_perf_util.h", seen)
        self.assertIn("bench/bench_crdt_workload.h", seen)
        missing = sorted(p for p in seen if not classifier.is_protected(p))
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
