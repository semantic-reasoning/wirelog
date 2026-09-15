#!/usr/bin/env python3
"""Contract tests for honest release perf/RSS availability reporting."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RELEASE = (ROOT / ".github/workflows/release-tag.yml").read_text()
DOCS = (ROOT / "docs/RELEASE_PROCESS.md").read_text()
DOCS_FLAT = " ".join(DOCS.split())


def require(needle: str, haystack: str) -> None:
    if needle not in haystack:
        raise AssertionError(f"missing contract text: {needle!r}")


def refuse(needle: str, haystack: str, why: str) -> None:
    if needle in haystack:
        raise AssertionError(f"forbidden text {needle!r}: {why}")


def main() -> int:
    require("perf-rss-status:", RELEASE)
    require("authoritative-runner-unavailable", RELEASE)
    require("Administration:read", RELEASE)
    require("gh api --paginate --slurp", RELEASE)
    require('status == \"online\" and .busy == false', RELEASE)

    # The needs: list and its hardcoded `-eq N` are NOT asserted here. Both are
    # covered more strongly by scripts/ci/test-required-gates.sh, which derives
    # the expected set from the jobs actually declared in the file and ties the
    # count to the list length; a literal copy here would only re-rot the next
    # time a job is added. What that gate cannot see is the wiring below: the
    # aggregate must read this job's output and fail closed on anything else.
    require(
        "PERF_RSS_STATUS: ${{ needs['perf-rss-status'].outputs.status }}",
        RELEASE)
    require("missing explicit perf/RSS evidence status", RELEASE)

    # Backticks inside a double-quoted bash string are command substitution,
    # not markdown quoting, so the echo form executes the status value and
    # prints an empty status. The positive assertion is what makes the
    # refusal meaningful -- a refusal alone is satisfied by deleting the line.
    printf_status = """printf -- '- status: `%s`\\n' "$status\""""
    printf_detail = """printf -- '- detail: `%s`\\n' "$detail\""""
    require(printf_status, RELEASE)
    require(printf_detail, RELEASE)
    refuse("`$status`", RELEASE,
           "backticks in a double-quoted string are command substitution, "
           "not markdown quoting; the status value would be executed")
    refuse("`$detail`", RELEASE,
           "same defect one line over: the detail value would be executed")

    # The single safety property this job exists to hold. Only an online, idle,
    # correctly labelled runner may flip the status to available -- widening
    # this to "the API answered" is how an API result becomes a false
    # stable-runner claim. It survived every gate until it was pinned here.
    require('if [ "$detail" = matching-runner-online-and-idle ]; then',
            RELEASE)
    require("status=authoritative-runner-available", RELEASE)

    # detail is a closed vocabulary written by this script. If any of these
    # stops being a fixed literal, server-controlled text could reach
    # $GITHUB_OUTPUT.
    for token in ("no-matching-runner",
                  "matching-runner-online-and-idle",
                  "matching-runner-present-but-busy",
                  "matching-runner-present-but-offline",
                  "runner-inventory-response-unreadable",
                  "runner-inventory-api-unavailable"):
        require(token, RELEASE)
    require("detail: ${{ steps.status.outputs.detail }}", RELEASE)

    # The gh error body is uncontrolled content on a public repository: it goes
    # to the job log, never to the 35-day artifact. The upload publishes
    # status.txt alone.
    require("2>runner-api-error.txt", RELEASE)
    require("path: release-perf-rss-status/status.txt", RELEASE)
    refuse("release-perf-rss-status/runner-api-error.txt", RELEASE,
           "the gh error body must not be written into the uploaded directory")

    # `administration` is not a settable GITHUB_TOKEN permission scope. An
    # unknown key in a permissions: block makes the whole workflow unparseable,
    # which would take release signing down on tag day, and nothing in CI lints
    # this file. See the comment in the perf-rss-status job.
    refuse("administration: read", RELEASE,
           "administration is not a valid GITHUB_TOKEN permission scope; "
           "it makes release-tag.yml unparseable")

    # Hosted correctness and RSS are REQUIRED by release-perf; only timing is
    # advisory. A blanket "hosted perf/RSS evidence is diagnostic" claim would
    # now be false, so the doc contract pins the narrower, true statement.
    require("does not satisfy the B8 burn-in", DOCS_FLAT)
    require("30 consecutive stable-runner nights", DOCS_FLAT)
    # The job cannot read the runner inventory with GITHUB_TOKEN, so the docs
    # must not imply the availability query is live today.
    require("cannot read the runner inventory API", DOCS_FLAT)
    refuse("release performance/RSS gating is deferred until 0.80", DOCS_FLAT,
           "the tag workflow requires hosted correctness and RSS evidence; "
           "the deferral claim was removed from main")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
