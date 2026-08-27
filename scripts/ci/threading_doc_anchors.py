#!/usr/bin/env python3
"""Build and resolve the stable anchors used by the threading audit gate."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ATOMIC = re.compile(
    r"\batomic_(?:load|store|fetch_add|fetch_sub|fetch_or|fetch_and|fetch_xor|"
    r"compare_exchange_weak|compare_exchange_strong|exchange|init|thread_fence)"
    r"(?:_explicit)?\s*\("
)
FUNCTION = re.compile(
    r"\b([A-Za-z_]\w*)\s*\([^;{}]*\)\s*"
    r"(?:__[A-Za-z_]\w*\s*)*\{"
)
DEFINE = re.compile(r"^\s*#\s*define\s+([A-Za-z_]\w*)")
CONTROL = {"if", "for", "while", "switch", "catch", "sizeof"}


@dataclass(frozen=True)
class Site:
    file: str
    line: int
    symbol: str
    operation: str
    anchor: str
    offset: int


def _mask_comments_and_strings(text: str) -> str:
    out = list(text)
    i = 0
    state = "code"
    while i < len(out):
        c = out[i]
        n = out[i + 1] if i + 1 < len(out) else ""
        if state == "block":
            if c == "*" and n == "/":
                out[i] = out[i + 1] = " "
                i += 2
                state = "code"
                continue
            if c != "\n":
                out[i] = " "
            i += 1
            continue
        if state in {"string", "char"}:
            if c == "\\":
                if c != "\n":
                    out[i] = " "
                if i + 1 < len(out) and out[i + 1] != "\n":
                    out[i + 1] = " "
                i += 2
                continue
            if (state == "string" and c == '"') or (
                state == "char" and c == "'"
            ):
                out[i] = " "
                state = "code"
            elif c != "\n":
                out[i] = " "
            i += 1
            continue
        if c == "/" and n == "/":
            out[i] = out[i + 1] = " "
            i += 2
            while i < len(out) and out[i] != "\n":
                out[i] = " "
                i += 1
            continue
        if c == "/" and n == "*":
            out[i] = out[i + 1] = " "
            i += 2
            state = "block"
            continue
        if c == '"':
            out[i] = " "
            state = "string"
        elif c == "'":
            out[i] = " "
            state = "char"
        i += 1
    return "".join(out)


def _line_starts(text: str) -> list[int]:
    starts = [0]
    starts.extend(i + 1 for i, c in enumerate(text) if c == "\n")
    return starts


def _brace_end(text: str, opening: int) -> int:
    depth = 0
    for i in range(opening, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return i
    return len(text)


def inventory(source: Path) -> list[Site]:
    raw = source.read_text(encoding="utf-8")
    clean = _mask_comments_and_strings(raw)
    starts = _line_starts(clean)
    basename = source.name
    ranges: list[tuple[int, int, str]] = []
    for match in FUNCTION.finditer(clean):
        symbol = match.group(1)
        if symbol in CONTROL:
            continue
        opening = match.end() - 1
        ranges.append((opening, _brace_end(clean, opening), symbol))

    macro_lines: set[int] = set()
    macro_ranges: list[tuple[int, int, str]] = []
    raw_lines = raw.splitlines(keepends=True)
    line = 0
    while line < len(raw_lines):
        match = DEFINE.match(raw_lines[line])
        if not match:
            line += 1
            continue
        end = line
        while raw_lines[end].rstrip("\n").rstrip().endswith("\\"):
            if end + 1 >= len(raw_lines):
                break
            end += 1
        macro_ranges.append((line, end, match.group(1)))
        macro_lines.update(range(line, end + 1))
        line = end + 1

    sites: list[Site] = []
    counts: dict[str, int] = {}

    def add(match: re.Match[str], line_number: int, symbol: str) -> None:
        operation = match.group(0).split("(", 1)[0].strip()
        key = symbol
        counts[key] = counts.get(key, 0) + 1
        suffix = "" if counts[key] == 1 else f"#{counts[key]}"
        anchor = f"{basename}:{symbol}{suffix}"
        sites.append(
            Site(basename, line_number, symbol, operation, anchor, match.start())
        )

    for first, last, symbol in macro_ranges:
        if symbol.startswith("atomic_"):
            continue
        for number in range(first, last + 1):
            start = starts[number]
            end = starts[number + 1] if number + 1 < len(starts) else len(clean)
            for match in ATOMIC.finditer(clean[start:end]):
                add(match, number + 1, symbol)

    for number, _raw_line in enumerate(raw_lines):
        if number in macro_lines:
            continue
        start = starts[number]
        end = starts[number + 1] if number + 1 < len(starts) else len(clean)
        symbol = "<global>"
        containing = [r for r in ranges if r[0] <= start < r[1]]
        if containing:
            symbol = min(containing, key=lambda r: r[1] - r[0])[2]
        for match in ATOMIC.finditer(clean[start:end]):
            add(match, number + 1, symbol)
    return sites


def inventory_tree(root: Path) -> list[Site]:
    sites: list[Site] = []
    for source in sorted(root.rglob("*")):
        if source.suffix in {".c", ".h"} and source.is_file():
            sites.extend(inventory(source))
    return sites


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--dump", action="store_true")
    args = parser.parse_args()
    root = args.root
    sites = inventory_tree(root / "wirelog")
    if args.dump:
        for site in sites:
            print(
                f"{site.file}\t{site.line}\t{site.symbol}\t"
                f"{site.operation}\t{site.anchor}"
            )
        return 0
    print(len(sites))
    return 0


if __name__ == "__main__":
    sys.exit(main())
