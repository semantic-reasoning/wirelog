#!/usr/bin/env python3
"""check-public-prefix.py - Issue #761 backstop for the v0.40 API audit.

Public-API prefix CI lint: forbids `wl_*` / `WL_*` symbol prefixes on
installed public headers.  AGENTS.md:17-20 declares public APIs use
`wirelog_*` (functions / types) and `WIRELOG_*` (macros / enums);
internal `wl_*` / `WL_*` prefixes leaking through a public installed
header is a 1.0 ABI violation.

Sibling rename issues (#762 / #756 / #757 / #758 / #759 / #760) close
out the existing leaks at v0.40; this lint prevents future drift.  An
inline allow-list is supported for deliberately deferred entries; no
allow-list entries exist at the time of landing.

Source of truth for the public-headers list
-------------------------------------------

  1. `meson.build` `wirelog_public_headers` list (bulk install).
  2. `meson.build` standalone `install_headers('wirelog/io/io_adapter.h',
     ...)` call (subdir-specific install).

The configure-time generated header `wirelog/wirelog-version.h` is
exempt: it is produced via `configure_file()` and tracked separately
by `check-version-sync.py`.

Symbol detection (text scan, deliberately conservative)
-------------------------------------------------------

Each header is scanned line by line.  Lines that are purely comment
(starting with `*`, `//`, or that sit inside a `/* ... */` block) are
skipped.  Anything else is searched for tokens matching
`\\b(wl_|WL_)[A-Za-z0-9_]+\\b`.  Each hit is reported as a violation
unless explicitly allow-listed.

Run as a lint-stage script: no configured build dir is required.
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# The public-headers list authoritatively declared by meson.build.  Kept
# in sync with check-public-header-surface.py / AGENTS.md:23.
PUBLIC_HEADERS = (
    "wirelog/wirelog.h",
    "wirelog/wirelog-types.h",
    "wirelog/wirelog-ir.h",
    "wirelog/wirelog-parser.h",
    "wirelog/wirelog-optimizer.h",
    "wirelog/wirelog-export.h",
    "wirelog/wirelog-easy.h",
    "wirelog/wirelog-advanced.h",
    "wirelog/io/io_adapter.h",
)

# Allow-list: tokens matching `\\b(wl_|WL_)[A-Za-z0-9_]+\\b` that are
# allowed to appear on the public surface for a documented reason.
# Empty at the time of landing.  Each entry must include a tracking
# issue or a written justification in this file.
ALLOW_LIST: tuple[tuple[str, str], ...] = (
    # Format: (token, header_relpath, justification)
    #
    # `struct wl_intern` is the internal struct tag forward-declared
    # in wirelog/wirelog.h solely so the public typedef
    # `typedef struct wl_intern wirelog_intern_t;` can name the type.
    # The struct itself is internal; the typedef *name* on the public
    # surface is the conformant `wirelog_intern_t`.  Per #760 design.
    ("wl_intern", "wirelog/wirelog.h"),
)

VIOLATION_RE = re.compile(r"\b(?:wl_|WL_)[A-Za-z0-9_]+\b")


def strip_block_comments(src: str) -> str:
    """Remove /* ... */ block comments (including multi-line) so that
    docstring references like `* See wl_intern_create()` do not produce
    false positives.  Line comments (`//`) are stripped by the per-line
    scan below."""
    return re.sub(r"/\*.*?\*/", " ", src, flags=re.DOTALL)


def scan_header(path: pathlib.Path) -> list[tuple[int, str, str]]:
    """Return a list of (line_number, line_text, token) for every
    violation in the given header.  Tokens that match an entry in
    ALLOW_LIST are silently skipped."""
    raw = path.read_text(encoding="utf-8")
    stripped = strip_block_comments(raw)
    lines = stripped.splitlines()
    # Normalize path separator so Windows backslashes match the
    # ALLOW_LIST tuples (which always use forward slashes).
    rel = path.relative_to(REPO_ROOT).as_posix()
    allow_set = {tok for (tok, hdr) in ALLOW_LIST if hdr == rel}
    hits: list[tuple[int, str, str]] = []
    for lineno, line in enumerate(lines, start=1):
        # Strip everything after `//` for line-comment hygiene.
        code = line.split("//", 1)[0]
        for match in VIOLATION_RE.finditer(code):
            tok = match.group(0)
            if tok in allow_set:
                continue
            hits.append((lineno, line.rstrip(), tok))
    return hits


def main() -> int:
    violations: list[tuple[str, int, str, str]] = []
    for rel in PUBLIC_HEADERS:
        path = REPO_ROOT / rel
        if not path.exists():
            print(
                f"check-public-prefix: missing public header: {rel}",
                file=sys.stderr,
            )
            return 1
        for lineno, line, tok in scan_header(path):
            violations.append((rel, lineno, line, tok))

    if not violations:
        print(
            "check-public-prefix: OK; no wl_*/WL_* tokens on public surface "
            f"({len(PUBLIC_HEADERS)} headers scanned)"
        )
        return 0

    print(
        f"check-public-prefix: FAIL; {len(violations)} wl_*/WL_* tokens "
        "leaking through public installed headers (AGENTS.md:17-20):",
        file=sys.stderr,
    )
    for rel, lineno, line, tok in violations:
        print(f"  {rel}:{lineno}: '{tok}' in: {line}", file=sys.stderr)
    print(
        "\nPublic typedefs / functions must use the 'wirelog_' prefix; "
        "public macros / enums must use the 'WIRELOG_' prefix.\n"
        "Add a documented exception to ALLOW_LIST in this script ONLY if a "
        "rename is deliberately deferred (cite a tracking issue).",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
