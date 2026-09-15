#!/usr/bin/env python3
"""Contract tests for honest release perf/RSS availability reporting."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RELEASE = (ROOT / ".github/workflows/release-tag.yml").read_text()
NIGHTLY = (ROOT / ".github/workflows/perf-nightly.yml").read_text()
DOCS = (ROOT / "docs/RELEASE_PROCESS.md").read_text()
DOCS_FLAT = " ".join(DOCS.split())


def require(needle: str, haystack: str) -> None:
    if needle not in haystack:
        raise AssertionError(f"missing contract text: {needle!r}")


def main() -> int:
    require("perf-rss-status:", RELEASE)
    require("authoritative-runner-unavailable", RELEASE)
    require("Administration:read", RELEASE)
    require("gh api --paginate --slurp", RELEASE)
    require('status == \"online\" and .busy == false', RELEASE)
    require("needs: [default, abi, sbom, fuzz, mbedtls, sanitizers, downstream, perf-rss-status]", RELEASE)
    require('test "${#statuses[@]}" -eq 8', RELEASE)
    require("hosted perf remains diagnostic", RELEASE)

    require("name: Perf hosted runner / correctness + DOOP", NIGHTLY)
    require("runs-on: ubuntu-latest", NIGHTLY)
    require("WIRELOG_PERF_REQUIRE: '1'", NIGHTLY)
    require("WIRELOG_DOOP_PERF_MODE: required-hosted", NIGHTLY)
    require("continue-on-error: true", NIGHTLY)

    require("Hosted perf/RSS evidence is diagnostic", DOCS_FLAT)
    require("30 consecutive stable-runner nights", DOCS_FLAT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
