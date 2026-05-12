#!/usr/bin/env python3
"""check-public-api-macro.py - Issue #782 backstop.

Asserts that every public-API prototype on the 8 installed prototype
headers uses the canonical `WIRELOG_API` attribute macro, not its
backward-compat alias `WIRELOG_PUBLIC`.  The alias is allowed (and
required) only inside `wirelog/wirelog-export.h`, where the platform
`#if` ladder *defines* `WIRELOG_PUBLIC` and `WIRELOG_API` is itself
defined as `#define WIRELOG_API WIRELOG_PUBLIC`.

Source of truth for the public-headers list
-------------------------------------------

This lint imports `PUBLIC_HEADERS` from the sibling lint at
`scripts/ci/check-public-prefix.py` (which authoritatively mirrors
`wirelog_public_headers` in `meson.build` plus the standalone
`install_headers('wirelog/io/io_adapter.h', ...)` call).  Any change
to the installed-header set updates both lints automatically.

Detection (text scan, deliberately conservative)
------------------------------------------------

Each header is scanned line by line.  Block comments (`/* ... */`)
and line comments (`//`) are stripped before scanning so that
docstring references to `WIRELOG_PUBLIC` do not produce false
positives.  Anything else is searched for `\\bWIRELOG_PUBLIC\\b`.
Hits anywhere in `wirelog/wirelog-export.h` are exempt (the macro
itself lives there).  Hits in any other header are violations.

Run as a lint-stage script: no configured build dir required.
"""

from __future__ import annotations

import importlib.util
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent

EXEMPT_HEADER = "wirelog/wirelog-export.h"

VIOLATION_RE = re.compile(r"\bWIRELOG_PUBLIC\b")
BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _load_prefix_module():
    """Load the sibling lint as a module (hyphenated filename
    requires importlib); reused for `PUBLIC_HEADERS`."""
    path = SCRIPT_DIR / "check-public-prefix.py"
    spec = importlib.util.spec_from_file_location("_public_prefix", path)
    if spec is None or spec.loader is None:
        print(
            f"check-public-api-macro: unable to load sibling lint {path}",
            file=sys.stderr,
        )
        sys.exit(1)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def strip_comments(src: str) -> str:
    return BLOCK_COMMENT_RE.sub(" ", src)


def scan_header(path: pathlib.Path) -> list[tuple[int, str]]:
    raw = path.read_text(encoding="utf-8")
    stripped = strip_comments(raw)
    hits: list[tuple[int, str]] = []
    for lineno, line in enumerate(stripped.splitlines(), start=1):
        code = line.split("//", 1)[0]
        if VIOLATION_RE.search(code):
            hits.append((lineno, line.rstrip()))
    return hits


def main() -> int:
    prefix = _load_prefix_module()
    headers = prefix.PUBLIC_HEADERS
    violations: list[tuple[str, int, str]] = []
    for rel in headers:
        if rel == EXEMPT_HEADER:
            continue
        path = REPO_ROOT / rel
        if not path.exists():
            print(
                f"check-public-api-macro: missing public header: {rel}",
                file=sys.stderr,
            )
            return 1
        for lineno, line in scan_header(path):
            violations.append((rel, lineno, line))

    if not violations:
        print(
            "check-public-api-macro: OK; no WIRELOG_PUBLIC tokens outside "
            f"{EXEMPT_HEADER} "
            f"({len(headers) - 1} prototype headers scanned)"
        )
        return 0

    print(
        f"check-public-api-macro: FAIL; {len(violations)} WIRELOG_PUBLIC "
        f"token(s) on prototype headers (must use WIRELOG_API per "
        f"AGENTS.md and #782):",
        file=sys.stderr,
    )
    for rel, lineno, line in violations:
        print(f"  {rel}:{lineno}: {line}", file=sys.stderr)
    print(
        "\nPublic prototypes must use WIRELOG_API.  WIRELOG_PUBLIC is "
        "retained as a backward-compatibility alias only inside "
        f"{EXEMPT_HEADER}; new code on the installed prototype surface "
        "must use WIRELOG_API.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
