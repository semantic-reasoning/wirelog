#!/usr/bin/env python3
"""Ensure installed WIRELOG_API function declarations are exported.

The ABI symbol allowlist must not drift behind the installed public
headers.  This static lint compares every WIRELOG_API function prototype
on public headers with abi/libwirelog-1.0.symbols.
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
WIRELOG_API_PROTO_RE = re.compile(r"\bWIRELOG_API\b\s+([^;]*?)\s*;", re.DOTALL)
FUNCTION_NAME_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def parse_meson_public_headers() -> set[str]:
    text = (REPO_ROOT / "meson.build").read_text(encoding="utf-8")
    list_match = re.search(
        r"wirelog_public_headers\s*=\s*\[(.*?)\]", text, re.DOTALL
    )
    if not list_match:
        print(
            "check-public-api-exports: meson.build wirelog_public_headers "
            "list not found",
            file=sys.stderr,
        )
        sys.exit(1)

    headers = set(re.findall(r"'(wirelog/[^']+\.h)'", list_match.group(1)))
    headers |= set(
        re.findall(
            r"install_headers\(\s*'(wirelog/[^']+\.h)'", text, re.DOTALL
        )
    )
    if len(headers) < 5:
        print(
            "check-public-api-exports: parsed too few public headers from "
            "meson.build",
            file=sys.stderr,
        )
        sys.exit(1)
    return headers


def strip_comments(src: str) -> str:
    stripped = BLOCK_COMMENT_RE.sub(" ", src)
    return "\n".join(line.split("//", 1)[0] for line in stripped.splitlines())


def public_api_functions(header: pathlib.Path) -> list[str]:
    src = strip_comments(header.read_text(encoding="utf-8"))
    names: list[str] = []
    for match in WIRELOG_API_PROTO_RE.finditer(src):
        prototype = " ".join(match.group(1).split())
        if "(" not in prototype:
            continue
        candidates = FUNCTION_NAME_RE.findall(prototype)
        if candidates:
            names.append(candidates[-1])
    return names


def main() -> int:
    expected: dict[str, str] = {}
    for rel in sorted(parse_meson_public_headers()):
        path = REPO_ROOT / rel
        if not path.exists():
            print(f"check-public-api-exports: missing header {rel}", file=sys.stderr)
            return 1
        for name in public_api_functions(path):
            expected[name] = rel

    symbols_path = REPO_ROOT / "abi/libwirelog-1.0.symbols"
    exported = set(
        line.strip()
        for line in symbols_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    )

    missing = sorted(name for name in expected if name not in exported)
    if not missing:
        print(
            "check-public-api-exports: OK; "
            f"{len(expected)} WIRELOG_API function(s) covered by ABI symbols"
        )
        return 0

    print(
        "check-public-api-exports: FAIL; public WIRELOG_API declarations "
        "missing from abi/libwirelog-1.0.symbols:",
        file=sys.stderr,
    )
    for name in missing:
        print(f"  {expected[name]}: {name}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
