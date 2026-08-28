#!/usr/bin/env python3
"""Negative fixtures for the scalar-addon design link checker."""

import importlib.util
from pathlib import Path
import tempfile


ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts/ci/check-scalar-function-addon-design.py"
SPEC = importlib.util.spec_from_file_location("scalar_design_checker", CHECKER)
CHECKER_MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECKER_MODULE)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="wirelog-scalar-design-") as name:
        root = Path(name)
        target = root / "design.md"
        target.write_text("ok\n", encoding="utf-8")

        good = root / "good.md"
        good.write_text("[design.md](design.md)\n", encoding="utf-8")
        assert CHECKER_MODULE.check_links(good, "design.md", "design.md") == []

        broken = root / "broken.md"
        broken.write_text("[design.md](missing.md)\n", encoding="utf-8")
        errors = CHECKER_MODULE.check_links(broken, "design.md", "missing.md")
        assert any("broken local link" in error for error in errors)

        missing = root / "missing.md"
        missing.write_text("[other](design.md)\n", encoding="utf-8")
        errors = CHECKER_MODULE.check_links(missing, "design.md", "design.md")
        assert any("missing exact design link" in error for error in errors)

        original = CHECKER_MODULE.DOC.read_text(encoding="utf-8")
        misplaced = original.replace(
            "Built-in names win over addon names; duplicate addon names\n",
            "duplicate addon names\n",
        ) + "\n## Other\n\nBuilt-in names win over addon names; duplicate addon names\n"
        errors = CHECKER_MODULE.validate_document(misplaced)
        assert any(
            "## Recommended direction: Built-in names win over addon names"
            in error for error in errors
        )
    print("scalar function addon design checker fixtures: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
