#!/usr/bin/env python3
"""check-public-header-surface.py - Issue #705 / Blocker B2 cross-check.

Public-header surface single-source-of-truth gate.  The list of headers
installed under ``${includedir}/wirelog/`` is enumerated in several
in-tree locations; any drift between them is a 1.0-blocker risk
(consumers ``#include`` an internal header, the next minor breaks ABI).

This script declares ``meson.build`` the single source of truth and
fails when any of the in-tree mirrors disagree.  It runs in lint stage
(no configured build dir required); the parser is deliberately a static
text scan so that a renamed meson DSL helper or a regenerated build dir
cannot mask drift.

Source of truth
---------------

  1. ``meson.build`` ``wirelog_public_headers`` list  (the bulk install)
  2. ``meson.build`` standalone ``install_headers('wirelog/io/io_adapter.h',
     subdir: 'wirelog/io')`` call  (the subdir-specific install)

Combined, these enumerate every source-tree header installed to
``${includedir}/wirelog/``.

Mirrors that must agree with the SoT
------------------------------------

  3. ``stable-release-plan.md`` ``### 3.1`` table
  4. ``AGENTS.md`` ``**Public API headers**`` enumeration
  5. ``wirelog/wirelog.h`` umbrella ``#include`` block + the doc comment
     listing sub-headers
  6. ``scripts/check_log_header_not_public.sh`` ``PUBLIC_HEADERS`` array

Generated headers are explicitly exempt
---------------------------------------

``wirelog/wirelog-version.h`` is produced via ``configure_file()`` at
``wirelog/meson.build`` and is not source-tracked; it is checked
separately by ``check-version-sync.py`` and is omitted from this gate.

Anchors that must NOT be parsed
-------------------------------

The frozen verbatim appendix at the end of ``stable-release-plan.md``
contains a historical header enumeration that is preserved without
modification.  This script anchors on ``### 3.1`` exactly so that
appendix is never read.
"""

from __future__ import annotations

import pathlib
import re
import sys


def die(msg: str) -> None:
    print(f"check-public-header-surface: {msg}", file=sys.stderr)
    sys.exit(1)


HEADER_TOKEN = re.compile(r"`(wirelog/[A-Za-z0-9_./-]+\.h)`")


def parse_meson_sot(repo_root: pathlib.Path) -> set[str]:
    """Extract the canonical public-header set from meson.build.

    Picks up two install paths:
      - the named ``wirelog_public_headers`` list (subdir 'wirelog');
      - the standalone ``install_headers('...', subdir: '...')`` calls
        whose path begins with ``wirelog/``.
    """
    text = (repo_root / "meson.build").read_text(encoding="utf-8")

    list_match = re.search(
        r"wirelog_public_headers\s*=\s*\[(.*?)\]", text, re.DOTALL
    )
    if not list_match:
        die("meson.build: wirelog_public_headers list not found")
    bulk = set(re.findall(r"'(wirelog/[^']+\.h)'", list_match.group(1)))

    standalone = set(
        re.findall(
            r"install_headers\(\s*'(wirelog/[^']+\.h)'", text, re.DOTALL
        )
    )

    headers = bulk | standalone
    if len(headers) < 5:
        die(
            f"meson.build: parsed only {len(headers)} headers; list shape "
            "may have changed (update parser)"
        )
    return headers


def parse_plan_section(repo_root: pathlib.Path) -> set[str] | None:
    """Extract headers from stable-release-plan.md §3.1.

    Anchors on ``### 3.1`` exactly; stops at the next ``###`` heading
    so the frozen verbatim appendix at the document end is never
    traversed.

    Returns ``None`` if the plan file is not yet checked into the repo
    (the plan is tracked separately from the source tree).  Returns the
    parsed set otherwise.  If the plan IS present but malformed (anchor
    missing, section empty), this fails loudly.
    """
    path = repo_root / "stable-release-plan.md"
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    start = text.find("### 3.1")
    if start < 0:
        die(f"{path.name}: '### 3.1' anchor not found")
    rest = text[start:]
    end = rest.find("\n### ", 1)
    section = rest if end < 0 else rest[:end]
    headers = set(HEADER_TOKEN.findall(section))
    if not headers:
        die(f"{path.name} §3.1: no `wirelog/*.h` tokens found")
    return headers


def parse_agents_md(repo_root: pathlib.Path) -> set[str]:
    """Extract headers from AGENTS.md.

    Anchors on the bold ``**Public API headers**`` marker; reads tokens
    from every following line until the first blank line.
    """
    path = repo_root / "AGENTS.md"
    text = path.read_text(encoding="utf-8")
    marker = "**Public API headers**"
    start = text.find(marker)
    if start < 0:
        die(f"{path.name}: '{marker}' anchor not found")
    block_start = text.find("\n", start) + 1
    block_end = text.find("\n\n", block_start)
    if block_end < 0:
        block_end = len(text)
    headers = set(HEADER_TOKEN.findall(text[block_start:block_end]))
    if not headers:
        die(f"{path.name}: no `wirelog/*.h` tokens after marker")
    return headers


def parse_umbrella_header(repo_root: pathlib.Path) -> set[str]:
    """Extract sub-header includes from wirelog/wirelog.h.

    Reads quoted ``#include "wirelog/..."`` lines.  Excludes the
    generated ``wirelog-version.h`` (covered by check-version-sync) and
    the umbrella's own ``wirelog/wirelog.h`` self-reference.
    """
    path = repo_root / "wirelog" / "wirelog.h"
    text = path.read_text(encoding="utf-8")
    raw = set(
        re.findall(
            r'^#include\s+"(wirelog/[^"]+\.h)"', text, re.MULTILINE
        )
    )
    raw.discard("wirelog/wirelog-version.h")
    raw.discard("wirelog/wirelog.h")
    return raw


def parse_log_header_check(repo_root: pathlib.Path) -> set[str]:
    """Extract PUBLIC_HEADERS array from scripts/check_log_header_not_public.sh."""
    path = repo_root / "scripts" / "check_log_header_not_public.sh"
    text = path.read_text(encoding="utf-8")
    arr_match = re.search(r"PUBLIC_HEADERS=\(([^)]*)\)", text)
    if not arr_match:
        die(f"{path.name}: PUBLIC_HEADERS=( ... ) array not found")
    return set(re.findall(r"(wirelog/[A-Za-z0-9_./-]+\.h)", arr_match.group(1)))


def diff_report(
    canonical: set[str], other: set[str], label: str
) -> list[str]:
    missing = sorted(canonical - other)
    extra = sorted(other - canonical)
    msgs: list[str] = []
    if missing:
        msgs.append(f"  {label}: missing {missing}")
    if extra:
        msgs.append(f"  {label}: extra {extra}")
    return msgs


def main(argv: list[str]) -> int:
    if len(argv) > 2:
        die("usage: check-public-header-surface.py [<repo_root>]")
    repo_root = (
        pathlib.Path(argv[1]).resolve()
        if len(argv) == 2
        else pathlib.Path(__file__).resolve().parents[2]
    )

    canonical = parse_meson_sot(repo_root)
    # The umbrella header lists every public sub-header except itself
    # (an umbrella does not include itself), so compare against the
    # SoT minus the umbrella entry.
    canonical_for_umbrella = canonical - {"wirelog/wirelog.h"}
    plan_set = parse_plan_section(repo_root)

    mirrors: list[tuple[str, set[str], set[str]]] = [
        ("AGENTS.md", canonical, parse_agents_md(repo_root)),
        (
            "wirelog/wirelog.h umbrella",
            canonical_for_umbrella,
            parse_umbrella_header(repo_root),
        ),
        (
            "scripts/check_log_header_not_public.sh PUBLIC_HEADERS",
            canonical,
            parse_log_header_check(repo_root),
        ),
    ]
    if plan_set is not None:
        mirrors.insert(
            0, ("stable-release-plan.md §3.1", canonical, plan_set)
        )
    else:
        print(
            "check-public-header-surface: stable-release-plan.md absent, "
            "skipping §3.1 cross-check (will activate when plan is "
            "checked in)",
            file=sys.stderr,
        )

    drift: list[str] = []
    for label, expected, observed in mirrors:
        drift.extend(diff_report(expected, observed, label))

    if drift:
        print("check-public-header-surface: drift detected", file=sys.stderr)
        print(
            f"  meson.build (SoT): {sorted(canonical)}", file=sys.stderr
        )
        for line in drift:
            print(line, file=sys.stderr)
        return 1

    print(f"check-public-header-surface: OK ({len(canonical)} headers)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
