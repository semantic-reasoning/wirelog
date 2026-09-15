#!/usr/bin/env python3
"""Shared helpers for the hosted libFuzzer campaign tools."""

from __future__ import annotations

import hashlib
from pathlib import Path


TARGETS = ("parser", "csv_reader", "intern", "compound_arena")


def corpus_digest(root: Path) -> str:
    """Hash a corpus as a sorted sequence of relative names and file bytes."""

    root = root.resolve()
    if not root.is_dir():
        raise ValueError(f"corpus is not a directory: {root}")

    digest = hashlib.sha256()
    files = sorted(path for path in root.rglob("*") if path.is_file())
    for path in files:
        if path.is_symlink():
            raise ValueError(f"corpus contains a symlink: {path}")
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()
