#!/usr/bin/env python3
"""Check the repository contract for the Issue #913 design proposal."""

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs/design/weighted-semiring-addon.md"


REQUIRED = (
    "## Current normative baseline",
    "## Problem and boundary with scalar addons",
    "## Options and decision",
    "## Algebra and retraction contract",
    "## Candidate future ABI (non-normative)",
    "## Optimizer and execution legality",
    "## Decision gate and non-goals",
    "signed-integer Z-set/differential algebra",
    "separate weighted backend",
    "external higher-layer inference",
    "additive-inverse algebra",
    "replacement/recomputation",
    "provenance/derivation tracking",
    "WIRELOG_WEIGHT_ABI_VERSION",
    "WIRELOG_IO_ABI_VERSION",
    "WIRELOG_API",
    "wirelog_public_headers",
    "non-normative pseudocode",
    "NaN",
    "infinity",
    "callback reentrancy",
    "deterministic antijoin",
    "probabilistic semantics",
    "No probabilistic semantics",
    "public weight API",
)

SECTION_REQUIREMENTS = {
    "## Algebra and retraction contract": (
        "most semirings",
        "have no additive inverse",
        "an additive-inverse algebra",
        "replacement/recomputation",
        "provenance/derivation tracking",
        "leaves the prior snapshot unchanged",
        "Partial callback output is discarded",
    ),
    "## Candidate future ABI (non-normative)": (
        "WIRELOG_WEIGHT_ABI_VERSION",
        "size, alignment, construction/destruction",
        "versioned, deterministic serialization format",
        "callback reentrancy",
        "unload",
        "invalid values, overflow, underflow, NaN, infinity",
    ),
    "## Optimizer and execution legality": (
        "algebraic laws it actually satisfies",
        "disabled, not guessed",
        "Approximate or floating-point arithmetic",
        "each worker width and replay order",
    ),
}


def section_text(document: str, heading: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(heading)}\s*$\n(.*?)(?=^## |\Z)", document
    )
    return match.group(1) if match else ""


def check_links(path: Path, expected: tuple[str, str]) -> list[str]:
    """Parse local Markdown links and validate the expected design link."""
    document = path.read_text(encoding="utf-8")
    links = re.findall(r"\[([^\]]+)\]\(([^)]+)\)", document)
    errors = []
    expected_label, expected_target = expected
    if not any(
        label.strip("`") == expected_label and target == expected_target
        for label, target in links
    ):
        errors.append(f"{path.name}: missing exact design link")
    for label, target in links:
        if target.startswith(("http://", "https://", "#")):
            continue
        target_path = target.split("#", 1)[0]
        if not target_path:
            continue
        resolved = (path.parent / target_path).resolve()
        if not resolved.is_file():
            errors.append(f"{path.name}: broken local link {target}")
    return errors


def main() -> int:
    if not DOC.is_file():
        print(f"FAIL: missing {DOC}")
        return 1
    text = DOC.read_text(encoding="utf-8")
    missing = [item for item in REQUIRED if item not in text]
    for heading, terms in SECTION_REQUIREMENTS.items():
        body = section_text(text, heading)
        missing.extend(
            f"{heading}: {term}" for term in terms if term not in body
        )
    for path, expected in (
        (ROOT / "README.md", (
            "docs/design/weighted-semiring-addon.md",
            "docs/design/weighted-semiring-addon.md",
        )),
        (ROOT / "docs/SEMANTICS.md", (
            "docs/design/weighted-semiring-addon.md",
            "design/weighted-semiring-addon.md",
        )),
        (ROOT / "docs/INTERNALS.md", (
            "docs/design/weighted-semiring-addon.md",
            "design/weighted-semiring-addon.md",
        )),
    ):
        missing.extend(check_links(path, expected))
    for relative in ("docs/design/weighted-semiring-addon.md",):
        if not (ROOT / relative).is_file():
            missing.append(f"missing link target: {relative}")
    if missing:
        print("FAIL: missing design contract: " + ", ".join(missing))
        return 1
    print("weighted semiring design contract: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
