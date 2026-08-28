#!/usr/bin/env python3
"""Check the repository contract for the Issue #912 design proposal."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs/design/scalar-function-addon.md"

REQUIRED = (
    "## Current implementation boundary",
    "## Recommended direction",
    "## Candidate value and callback contract",
    "## Reentrancy, threading, and determinism",
    "## Optimizer and serialization policy",
    "## Future public ABI boundary and non-goals",
    "## Implementation decision gate",
    "lexer/parser -> AST -> IR -> serialized plan expression -> columnar evaluator",
    "WIRELOG_FUNCTION_ABI_VERSION",
    "WIRELOG_IO_ABI_VERSION",
    "WIRELOG_API",
    "wirelog_public_headers",
    "non-normative pseudocode",
    "raw function pointer",
    "registry snapshot",
    "snapshot/unregister/unload",
    "WIRELOG_ERR_EXEC",
    "common-subexpression elimination",
    "predicate pushdown",
    "I/O adapter registry is not reused",
    "no runtime or public API",
)

SECTION_REQUIREMENTS = {
    "## Recommended direction": (
        "Built-in names win over addon names",
        "compile-time resolution is mandatory",
        "never a process-local address",
        "unregister only affects future snapshots",
        "incompatible descriptor fails before execution",
    ),
    "## Candidate value and callback contract": (
        "successful scalar result, a false predicate result, and a callback error",
        "values are deferred",
        "interned-string result must either be copied",
        "host-provided allocator",
        "`FILTER` callback errors are hard errors",
        "session delta is not published",
        "must not throw, `longjmp`, terminate the process",
    ),
    "## Reentrancy, threading, and determinism": (
        "non-reentrant",
        "thread-safe callback concurrently",
        "trusted process-local code, not a sandbox",
    ),
    "## Optimizer and serialization policy": (
        "unknown or unverified capability disables",
        "Function pointers, user-data addresses",
        "Generic addon calls",
        "otherwise opaque",
    ),
}


def section_text(document: str, heading: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(heading)}\s*$\n(.*?)(?=^## |\Z)", document
    )
    return match.group(1) if match else ""


def check_links(path: Path, expected_label: str,
                expected_target: str) -> list[str]:
    document = path.read_text(encoding="utf-8")
    links = re.findall(r"\[([^\]]+)\]\(([^)]+)\)", document)
    errors = []
    if not any(
        label.strip("`") == expected_label and target == expected_target
        for label, target in links
    ):
        errors.append(f"{path.name}: missing exact design link")
    for _, target in links:
        if target.startswith(("http://", "https://", "#")):
            continue
        target_path = target.split("#", 1)[0]
        if target_path and not (path.parent / target_path).resolve().is_file():
            errors.append(f"{path.name}: broken local link {target}")
    return errors


def validate_document(text: str) -> list[str]:
    missing = [item for item in REQUIRED if item not in text]
    for heading, terms in SECTION_REQUIREMENTS.items():
        body = section_text(text, heading)
        missing.extend(
            f"{heading}: {term}" for term in terms if term not in body
        )
    for path in (ROOT / "README.md", ROOT / "docs/SEMANTICS.md",
                 ROOT / "docs/INTERNALS.md"):
        expected = ("docs/design/scalar-function-addon.md"
                    if path.name == "README.md"
                    else "design/scalar-function-addon.md")
        missing.extend(check_links(path, "docs/design/scalar-function-addon.md",
                                   expected))
    return missing


def main() -> int:
    if not DOC.is_file():
        print(f"FAIL: missing {DOC}")
        return 1
    missing = validate_document(DOC.read_text(encoding="utf-8"))
    if missing:
        print("FAIL: missing design contract: " + ", ".join(missing))
        return 1
    print("scalar function addon design contract: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
