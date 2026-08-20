#!/usr/bin/env python3
"""Validate the documented PSIRT and supported-version policy."""

from __future__ import annotations

import pathlib
import re
import sys


def fail(message: str) -> None:
    print(f"check-support-policy: {message}", file=sys.stderr)
    raise SystemExit(1)


def read(root: pathlib.Path, name: str) -> str:
    path = root / name
    if not path.is_file():
        fail(f"missing {name}")
    return path.read_text(encoding="utf-8")


def main() -> None:
    root = pathlib.Path(__file__).resolve().parents[2]
    security = read(root, "SECURITY.md")
    readme = read(root, "README.md")
    policy = read(root, "docs/SUPPORT_POLICY.md")
    release = read(root, "docs/RELEASE_PROCESS.md")

    required = {
        "SECURITY.md": (
            security,
            "inquiry@cleverplant.com",
            "3 business days",
            "7 calendar days",
            "14 calendar days",
            "90 calendar days",
            "fix or mitigation target",
            "docs/SUPPORT_POLICY.md",
            "CVE",
        ),
        "README.md": (readme, "Supported versions", "docs/SUPPORT_POLICY.md"),
        "docs/SUPPORT_POLICY.md": (
            policy,
            "18 months",
            "12 months",
            "no separate LTS designation",
            "What supported means",
            "End of life",
        ),
        "docs/RELEASE_PROCESS.md": (
            release,
            "docs/SUPPORT_POLICY.md",
            "#753",
            "#1144",
        ),
    }
    for name, (text, *needles) in required.items():
        for needle in needles:
            if needle not in text:
                fail(f"{name}: missing required text {needle!r}")

    if "#753" in security and "deferred" in security.lower():
        fail("SECURITY.md still describes #753 as deferred")

    for source in (root / "SECURITY.md", root / "README.md", root / "docs/RELEASE_PROCESS.md", root / "docs/SUPPORT_POLICY.md"):
        text = source.read_text(encoding="utf-8")
        for target in re.findall(r"\]\(([^)#]+)(?:#[^)]+)?\)", text):
            if "://" in target:
                continue
            target_path = (source.parent / target).resolve()
            if not target_path.is_file():
                fail(f"{source.relative_to(root)}: broken link {target!r}")

    print("check-support-policy: OK")


if __name__ == "__main__":
    main()
