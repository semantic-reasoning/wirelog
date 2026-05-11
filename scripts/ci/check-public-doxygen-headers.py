#!/usr/bin/env python3
"""check-public-doxygen-headers.py - Issue #780 / Epic #680 exit gate.

File-level Doxygen marker gate.  Asserts that every installed public
header carries both ``@file`` and ``@brief`` Doxygen tags inside a
``/** ... */`` documentation block placed near the top of the file
(after the SPDX/copyright C-comment, before or just after the
``#ifndef`` guard).

Why both markers must be present
--------------------------------

Doxygen will only emit a file-level documentation page for a header
when it finds a ``@file`` directive inside a JavaDoc-style block
(``/**`` or ``/*!``).  A bare ``@file`` inside a plain ``/*`` block
is silently ignored.  ``@brief`` is what shows up in the file-index
tooltip and in IDE hover; without it, the index entry is blank.

Source of truth for the header list
-----------------------------------

This script does NOT maintain its own list of public headers.  It
imports ``parse_meson_sot`` from the sibling lint at
``scripts/ci/check-public-header-surface.py``, which authoritatively
parses ``meson.build``'s ``wirelog_public_headers`` plus the
standalone ``install_headers('wirelog/io/io_adapter.h', ...)`` call.
Any header newly added to (or removed from) the install set is
picked up automatically -- there is no second list to keep in sync.

Generated headers (``wirelog/wirelog-version.h`` via
``configure_file``) are not source-tracked and are not returned by
``parse_meson_sot``; they are intentionally exempt and checked
elsewhere (``check-version-sync.py``).

Detection rules (deliberately strict)
-------------------------------------

A header passes when, within its first 80 lines:

  1. A ``/**`` token opens a comment block.
  2. Inside that block (before its closing ``*/``), a line matches
     ``^\\s*\\*\\s*@file\\b`` -- the leading-asterisk + word-boundary
     anchor distinguishes the file-level marker from per-parameter
     ``@filename:`` annotations that appear in GTK-Doc-style
     function blocks elsewhere in the same files.
  3. Inside the same block, a line matches
     ``^\\s*\\*\\s*@brief\\b``.

Exit codes mirror the sibling lints:

  0 -- every public header carries both markers in a Doxygen block.
  1 -- at least one header is missing one or both markers.

See also
--------

  - ``scripts/ci/check-public-prefix.py``        (#761 prefix gate)
  - ``scripts/ci/check-public-header-surface.py`` (#705 SoT gate)
  - Doxygen exemplar: ``wirelog/wirelog-advanced.h``
"""

from __future__ import annotations

import importlib.util
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
HEAD_LINES = 80

FILE_RE = re.compile(r"^\s*\*\s*@file\b")
BRIEF_RE = re.compile(r"^\s*\*\s*@brief\b")
OPEN_RE = re.compile(r"/\*\*")
CLOSE_RE = re.compile(r"\*/")


def _load_surface_module():
    """Load the sibling lint as a module (hyphenated filename requires
    importlib).  Reused for ``parse_meson_sot``.
    """
    path = SCRIPT_DIR / "check-public-header-surface.py"
    spec = importlib.util.spec_from_file_location("_surface_sot", path)
    if spec is None or spec.loader is None:
        die(f"unable to load sibling lint {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def die(msg: str) -> None:
    print(f"check-public-doxygen-headers: {msg}", file=sys.stderr)
    sys.exit(1)


def find_doxygen_block(lines: list[str]) -> tuple[int, int] | None:
    """Return (open_idx, close_idx) of the first ``/** ... */`` block
    inside the head window, or ``None`` if no JavaDoc block is found.
    Indexes are 0-based and refer to ``lines``.  A single-line ``/** ...
    */`` block is permitted.
    """
    head = lines[:HEAD_LINES]
    open_idx: int | None = None
    for i, line in enumerate(head):
        if open_idx is None:
            if OPEN_RE.search(line):
                open_idx = i
                if CLOSE_RE.search(line[OPEN_RE.search(line).end():]):
                    return (i, i)
        else:
            if CLOSE_RE.search(line):
                return (open_idx, i)
    return None


def check_header(path: pathlib.Path) -> list[str]:
    """Return a list of human-readable missing-marker strings for one
    header.  Empty list == header passes.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    block = find_doxygen_block(lines)
    if block is None:
        return [
            "no Doxygen /** ... */ block found in first "
            f"{HEAD_LINES} lines",
        ]
    open_idx, close_idx = block
    body = lines[open_idx : close_idx + 1]
    missing: list[str] = []
    if not any(FILE_RE.match(ln) for ln in body):
        missing.append("@file")
    if not any(BRIEF_RE.match(ln) for ln in body):
        missing.append("@brief")
    return missing


def main() -> int:
    surface = _load_surface_module()
    headers = sorted(surface.parse_meson_sot(REPO_ROOT))
    if not headers:
        die("parse_meson_sot returned no headers")

    failures: list[tuple[str, list[str]]] = []
    for rel in headers:
        path = REPO_ROOT / rel
        if not path.is_file():
            failures.append((rel, ["file missing on disk"]))
            continue
        missing = check_header(path)
        if missing:
            failures.append((rel, missing))

    if failures:
        print(
            "check-public-doxygen-headers: FAIL: "
            f"{len(failures)} of {len(headers)} public header(s) missing "
            "file-level Doxygen markers",
            file=sys.stderr,
        )
        for rel, missing in failures:
            print(f"  {rel}: missing {', '.join(missing)}", file=sys.stderr)
        print(
            "",
            "To fix, add a /** ... */ block between the SPDX C-comment "
            "and the include guard, for example:",
            "",
            "    /**",
            "     * @file <basename>.h",
            "     * @brief <one-line description>",
            "     */",
            "",
            "See wirelog/wirelog-advanced.h for the exemplar.",
            sep="\n",
            file=sys.stderr,
        )
        return 1

    print(
        "check-public-doxygen-headers: OK; "
        f"{len(headers)} public headers carry @file + @brief"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
