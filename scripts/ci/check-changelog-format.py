#!/usr/bin/env python3
"""check-changelog-format.py - Issue #471 CHANGELOG format gate.

Asserts that `CHANGELOG.md`'s `[Unreleased]` section conforms to the
project's Keep-a-Changelog-style conventions documented in
`CONTRIBUTING.md`:

  - Every bullet entry sits under one of the allowed category headings:
      Added, Changed, Fixed, Performance, Documentation, Removed,
      Security
  - Every bullet entry references at least one PR or issue
    (`#N` token where N is a positive integer).
  - Every released section header has shape
    `## [X.Y.Z] - YYYY-MM-DD`.
  - The file starts with `# Changelog` and contains exactly one
    `## [Unreleased]` heading.

Run as a lint-stage script under `meson test --suite abi`.

Usage:
    scripts/ci/check-changelog-format.py [path/to/CHANGELOG.md]

If no path is given, defaults to `<repo_root>/CHANGELOG.md`.
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

ALLOWED_CATEGORIES = {
    "Added",
    "Changed",
    "Fixed",
    "Performance",
    "Documentation",
    "Removed",
    "Security",
    "Deprecated",
}

# Category heading: `### <Category>`
CATEGORY_RE = re.compile(r"^###\s+(\S+)\s*$")
# Bullet entry start: `-` or `*`, possibly indented
BULLET_RE = re.compile(r"^\s*[-*]\s+\S")
# Section header: `## [...] - YYYY-MM-DD` or `## [Unreleased]`
SECTION_RE = re.compile(r"^##\s+\[([^\]]+)\](?:\s*-\s*(\d{4}-\d{2}-\d{2}))?\s*$")
# Issue / PR reference: `#N` token
REF_RE = re.compile(r"#\d+")


class Diag:
    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self.errors: list[str] = []

    def err(self, lineno: int, msg: str) -> None:
        self.errors.append(f"{self.path}:{lineno}: {msg}")


def main() -> int:
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    path = pathlib.Path(arg) if arg else (REPO_ROOT / "CHANGELOG.md")
    if not path.exists():
        print(
            f"check-changelog-format: FAIL: {path} does not exist",
            file=sys.stderr,
        )
        return 1

    text = path.read_text(encoding="utf-8")
    lines = text.split("\n")
    diag = Diag(path)

    if not lines or not lines[0].startswith("# Changelog"):
        diag.err(1, "first line must be `# Changelog`")

    in_unreleased = False
    saw_unreleased = False
    current_category: str | None = None
    # Accumulate the current bullet across continuation lines so the
    # PR/issue-reference check sees the whole entry.
    bullet_buf: list[str] = []
    bullet_start_lineno = 0

    def flush_bullet() -> None:
        nonlocal bullet_buf, bullet_start_lineno
        if not bullet_buf:
            return
        full = " ".join(bullet_buf)
        if not REF_RE.search(full):
            diag.err(
                bullet_start_lineno,
                "[Unreleased] entry has no #N or PR-#N reference: "
                f"{full[:80]!r}",
            )
        bullet_buf = []
        bullet_start_lineno = 0

    for i, line in enumerate(lines, start=1):
        m_section = SECTION_RE.match(line)
        if m_section:
            flush_bullet()
            tag, date = m_section.group(1), m_section.group(2)
            if tag == "Unreleased":
                if saw_unreleased:
                    diag.err(i, "duplicate [Unreleased] section")
                saw_unreleased = True
                in_unreleased = True
                current_category = None
            else:
                in_unreleased = False
                # Versioned section header must have a date.
                if not date:
                    diag.err(
                        i,
                        f"versioned section [{tag}] missing - YYYY-MM-DD date",
                    )
            continue

        if not in_unreleased:
            continue

        m_cat = CATEGORY_RE.match(line)
        if m_cat:
            flush_bullet()
            cat = m_cat.group(1)
            if cat not in ALLOWED_CATEGORIES:
                diag.err(
                    i,
                    f"unknown category {cat!r}; "
                    f"allowed: {sorted(ALLOWED_CATEGORIES)}",
                )
            current_category = cat
            continue

        if BULLET_RE.match(line):
            flush_bullet()
            if current_category is None:
                diag.err(
                    i,
                    "bullet outside any category heading "
                    f"(must follow ### Added|Changed|...): "
                    f"{line.strip()[:80]!r}",
                )
            bullet_buf = [line.strip().lstrip("-*").strip()]
            bullet_start_lineno = i
            continue

        # Continuation line: append to current bullet if non-empty.
        if bullet_buf and line.strip():
            bullet_buf.append(line.strip())
        elif not line.strip() and bullet_buf:
            # Blank line ends a bullet.
            flush_bullet()

    flush_bullet()

    if not saw_unreleased:
        diag.err(0, "missing `## [Unreleased]` section")

    if diag.errors:
        print(
            f"check-changelog-format: FAIL ({len(diag.errors)} issues)",
            file=sys.stderr,
        )
        for e in diag.errors:
            print(f"  {e}", file=sys.stderr)
        print(
            "\nFix per CONTRIBUTING.md > Changelog Conventions:\n"
            f"  - categories: {sorted(ALLOWED_CATEGORIES)}\n"
            "  - every bullet must reference a PR or issue "
            "(`#N`).\n"
            "  - versioned sections need `## [X.Y.Z] - YYYY-MM-DD`.",
            file=sys.stderr,
        )
        return 1

    print("check-changelog-format: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
