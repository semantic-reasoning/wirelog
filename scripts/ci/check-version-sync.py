#!/usr/bin/env python3
"""check-version-sync.py - Issue #688 / #704 version-sync tripwire.

Cross-checks three sources of truth for the wirelog version:

  1. meson.build's `version:` field (the authoritative input).
  2. The generated wirelog-version.h in the build tree (what consumers
     actually see at compile time).
  3. wirelog/wirelog-version.h.in (the template; placeholder tokens must
     still be present).

Pass the meson build directory as the single argument.  Suite: abi.
"""

from __future__ import annotations

import pathlib
import re
import sys


def die(msg: str) -> None:
    print(f"check-version-sync: {msg}", file=sys.stderr)
    sys.exit(1)


def parse_meson_version(path: pathlib.Path) -> str:
    text = path.read_text(encoding="utf-8")
    m = re.search(r"version:\s*'([^']+)'", text)
    if not m:
        die(f"no `version:` line in {path}")
    return m.group(1)


def parse_header_macros(path: pathlib.Path) -> dict[str, int]:
    text = path.read_text(encoding="utf-8")
    out: dict[str, int] = {}
    for name in ("MAJOR", "MINOR", "PATCH"):
        m = re.search(rf"#define\s+WIRELOG_VERSION_{name}\s+(\d+)", text)
        if not m:
            die(f"WIRELOG_VERSION_{name} missing in {path}")
        out[name] = int(m.group(1))
    return out


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        die("usage: check-version-sync.py <meson_build_dir>")
    build_dir = pathlib.Path(argv[1]).resolve()
    repo_root = pathlib.Path(__file__).resolve().parents[2]

    meson_version = parse_meson_version(repo_root / "meson.build")
    release = meson_version.split("-", 1)[0]
    parts = release.split(".")
    if len(parts) != 3:
        die(f"project_version must be MAJOR.MINOR.PATCH (got: {meson_version!r})")
    try:
        expected = {
            "MAJOR": int(parts[0]),
            "MINOR": int(parts[1]),
            "PATCH": int(parts[2]),
        }
    except ValueError:
        die(f"non-integer component in version triple: {parts!r}")

    header = build_dir / "wirelog-version.h"
    if not header.exists():
        die(f"generated header not found: {header}")
    actual = parse_header_macros(header)

    if expected != actual:
        die(
            f"version drift: meson.build={meson_version} -> {expected}, "
            f"{header} -> {actual}"
        )

    template = repo_root / "wirelog" / "wirelog-version.h.in"
    if not template.exists():
        die(f"template missing: {template}")
    template_text = template.read_text(encoding="utf-8")
    for token in (
        "@WIRELOG_VERSION_MAJOR@",
        "@WIRELOG_VERSION_MINOR@",
        "@WIRELOG_VERSION_PATCH@",
        "@WIRELOG_VERSION_STRING@",
    ):
        if token not in template_text:
            die(
                f"{template} missing {token} placeholder; was it hand-edited?"
            )

    print("check-version-sync: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
