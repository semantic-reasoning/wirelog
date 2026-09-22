#!/usr/bin/env python3
"""Reject locale-dependent text I/O in CI-registered Python gates (#1650).

The CI gates process repository files and subprocess evidence.  Every text
operation must name its encoding so a runner's locale cannot change a verdict
or corrupt an evidence artifact.  This AST check covers the Python gates and
test helpers registered from this repository, plus the performance audit
collector used by the release evidence workflow.
"""
from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    column: int
    message: str

    def format(self, root: Path) -> str:
        return f"{self.path.relative_to(root).as_posix()}:{self.line}:{self.column}: {self.message}"


def _constant_string(node: ast.AST | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _keyword(node: ast.Call, name: str) -> ast.keyword | None:
    return next((item for item in node.keywords if item.arg == name), None)


def _has_explicit_encoding(node: ast.Call) -> bool:
    keyword = _keyword(node, "encoding")
    return keyword is not None and not (
        isinstance(keyword.value, ast.Constant) and keyword.value.value is None
    )


def _is_true(node: ast.AST | None) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


def _text_mode(node: ast.Call, mode_index: int) -> bool:
    mode_node = node.args[mode_index] if len(node.args) > mode_index else None
    if mode_node is None:
        mode_keyword = _keyword(node, "mode")
        mode_node = mode_keyword.value if mode_keyword else None
    mode = _constant_string(mode_node)
    return mode is None or "b" not in mode


class EncodingVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.violations: list[Violation] = []
        self.subprocess_names = {"subprocess"}
        self.subprocess_calls: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "subprocess":
                self.subprocess_names.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "subprocess":
            for alias in node.names:
                if alias.name in {"run", "check_output", "check_call", "Popen"}:
                    self.subprocess_calls.add(alias.asname or alias.name)
        self.generic_visit(node)

    def add(self, node: ast.AST, message: str) -> None:
        self.violations.append(Violation(self.path, node.lineno, node.col_offset + 1, message))

    def visit_Call(self, node: ast.Call) -> None:
        function = node.func
        name = function.id if isinstance(function, ast.Name) else None
        attr = function.attr if isinstance(function, ast.Attribute) else None

        if attr in {"read_text", "write_text"} and not _has_explicit_encoding(node):
            self.add(node, f"Path.{attr}() must specify encoding=\"utf-8\"")
        elif (attr == "open" and not _has_explicit_encoding(node)
              and not (isinstance(function.value, ast.Call)
                       and isinstance(function.value.func, ast.Name)
                       and function.value.func.id == "__import__"
                       and function.value.args
                       and _constant_string(function.value.args[0]) in {"tarfile", "zipfile"})
              and not (isinstance(function.value, ast.Name)
                       and function.value.id in {"tarfile", "zipfile"})
              and _text_mode(node, 0)):
            self.add(node, "text Path.open() must specify encoding=\"utf-8\"")
        elif name == "open" and not _has_explicit_encoding(node) and _text_mode(node, 1):
            self.add(node, "text open() must specify encoding=\"utf-8\"")

        # Only subprocess calls are relevant; qualified-name matching keeps
        # helper functions named run() out of the contract.
        text_keyword = _keyword(node, "text") or _keyword(node, "universal_newlines")
        is_subprocess = (isinstance(function, ast.Attribute)
                         and function.attr in {"run", "check_output", "check_call", "Popen"}
                         and isinstance(function.value, ast.Name)
                         and function.value.id in self.subprocess_names) \
            or (isinstance(function, ast.Name) and function.id in self.subprocess_calls)
        if is_subprocess and text_keyword is not None and _is_true(text_keyword.value):
            if not _has_explicit_encoding(node):
                self.add(node, "subprocess text output must specify encoding=\"utf-8\"")
        self.generic_visit(node)


def scan_file(path: Path) -> list[Violation]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError) as error:
        return [Violation(path, 1, 1, f"cannot parse Python source: {error}")]
    visitor = EncodingVisitor(path)
    visitor.visit(tree)
    return visitor.violations


def targets(root: Path) -> list[Path]:
    paths = sorted((root / "scripts" / "ci").glob("*.py"))
    paths += sorted((root / "scripts" / "perf").glob("*.py"))
    paths += sorted((root / "scripts" / "fuzz").glob("*.py"))
    paths += sorted((root / "tests").glob("*.py"))
    return [path for path in paths if path.is_file() and path.name != Path(__file__).name]


def scan(root: Path) -> list[Violation]:
    return [violation for path in targets(root) for violation in scan_file(path)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=REPO_ROOT)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    violations = scan(root)
    if violations:
        print(f"check-python-encoding: FAIL; {len(violations)} locale-dependent text operation(s):", file=sys.stderr)
        for violation in violations:
            print(f"  {violation.format(root)}", file=sys.stderr)
        return 1
    print(f"check-python-encoding: OK; {len(targets(root))} Python gate/test files scanned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
