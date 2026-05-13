#!/usr/bin/env python3
"""check-test-parity.py - Issue #785 cross-facade test-parity gate.

Asserts that every `test_*` function in `tests/test_wirelog_easy.c`
is either:

  (a) **Paired** -- a function with the same name exists in
      `tests/test_wirelog_advanced.c`, OR
  (b) **Annotated** -- the immediately-preceding line(s) carry a
      `/* PARITY: ... */` block-comment naming the reason no
      advanced analogue exists (typically because the easy facade
      surface has no advanced counterpart -- `*_sym` variadics,
      `WIRELOG_EASY_OPEN_OPTS_INIT`, `wirelog_easy_print_delta`,
      etc.).

The rule is per-test (no ratio threshold) so future easy-side
additions cannot silently regress parity coverage; either pair the
new test on the advanced side, or annotate the easy declaration
with the structural reason.

Source-of-truth comments:

  - Easy test file: `tests/test_wirelog_easy.c`.
  - Advanced test file: `tests/test_wirelog_advanced.c`.
  - PARITY-annotation format: `/* PARITY: <text> */` on the line
    immediately preceding `static void test_X(void)`.

Exit codes:

  0 -- every easy test is paired or annotated.
  1 -- at least one easy test is unpaired and unannotated.

Run as a lint-stage script under `meson test --suite abi:test_parity`.
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
EASY = REPO_ROOT / "tests" / "test_wirelog_easy.c"
ADVANCED = REPO_ROOT / "tests" / "test_wirelog_advanced.c"

NAME_RE = re.compile(r"^test_([A-Za-z0-9_]+)\(void\)$")
PARITY_RE = re.compile(r"/\*\s*PARITY\s*:")


def die(msg: str) -> None:
    print(f"check-test-parity: {msg}", file=sys.stderr)
    sys.exit(1)


def collect_test_names(path: pathlib.Path) -> dict[str, int]:
    """Return {test_name: 1-based_line_no} for every `test_*(void)`
    in the file."""
    out: dict[str, int] = {}
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        m = NAME_RE.match(line)
        if m:
            out[f"test_{m.group(1)}"] = lineno
    return out


def has_parity_annotation(lines: list[str], decl_lineno: int) -> bool:
    """Check whether the lines immediately preceding the `test_X(void)`
    declaration at decl_lineno (1-based) carry a PARITY annotation.
    The annotation can sit either on the line above the declaration
    or on the line above `static void` (the previous line).  Look
    back up to 5 lines."""
    start = max(0, decl_lineno - 6)
    window = "\n".join(lines[start : decl_lineno - 1])
    return bool(PARITY_RE.search(window))


def main() -> int:
    for p in (EASY, ADVANCED):
        if not p.is_file():
            die(f"missing test file: {p}")

    easy_names = collect_test_names(EASY)
    advanced_names = set(collect_test_names(ADVANCED).keys())
    easy_lines = EASY.read_text().splitlines()

    missing: list[tuple[str, int]] = []
    for name, lineno in easy_names.items():
        if name in advanced_names:
            continue
        if has_parity_annotation(easy_lines, lineno):
            continue
        missing.append((name, lineno))

    if missing:
        print(
            f"check-test-parity: FAIL: {len(missing)} easy test(s) "
            f"unpaired AND unannotated:",
            file=sys.stderr,
        )
        for name, lineno in missing:
            rel = EASY.relative_to(REPO_ROOT)
            print(f"  {rel}:{lineno}: {name}", file=sys.stderr)
        print(
            "",
            "Fix one of:",
            "  (a) Add the same-named function to "
            "tests/test_wirelog_advanced.c, OR",
            "  (b) Prepend a `/* PARITY: facade-only -- <reason> */`",
            "      block-comment on the line(s) immediately above the",
            "      `static void test_X(void)` declaration in easy.",
            sep="\n",
            file=sys.stderr,
        )
        return 1

    paired = sum(1 for n in easy_names if n in advanced_names)
    annotated = len(easy_names) - paired - len(missing)
    print(
        f"check-test-parity: OK; {paired} paired, {annotated} annotated "
        f"({len(easy_names)} easy test(s) total; "
        f"{len(advanced_names)} advanced test(s))"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
