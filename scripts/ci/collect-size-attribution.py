"""Collect reproducible, read-only size and linker-map evidence for reviewed PRs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

REPOSITORY = "semantic-reasoning/wirelog"
PR_NUMBER = 1903
ADMISSION_PR_NUMBER = 1945
# Main-owned eligibility record for the documentation-only #1937 experiment PR.
SIZE_REDUCTION_PR_NUMBER = 1956
CURRENT_CANDIDATE_PR_NUMBERS = {PR_NUMBER, ADMISSION_PR_NUMBER, SIZE_REDUCTION_PR_NUMBER}
CURRENT_TWO_TREE_PR_NUMBERS = {PR_NUMBER, SIZE_REDUCTION_PR_NUMBER}
BASE_SHA = "8d91c2da2b188b94af5b9f1da21c569d80ccb387"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
POLICY_FILES = (
    ".github/workflows/ci-pr.yml",
    ".github/actions/setup-meson/action.yml",
    "meson.build",
    "meson_options.txt",
    "scripts/ci/size-profile.py",
    "scripts/ci/check-text-size.sh",
)
WORKFLOW_CODE_FILES = (
    ".github/workflows/size-attribution.yml",
    "scripts/ci/collect-size-attribution.py",
    "scripts/ci/test-size-attribution.py",
)
TOKEN_KEYS = ("GH_TOKEN", "GITHUB_TOKEN", "ACTIONS_RUNTIME_TOKEN", "ACTIONS_ID_TOKEN_REQUEST_TOKEN")


class DiagnosticError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run(command: list[str], *, cwd: Path | None = None,
        env: dict[str, str] | None = None, stdout: Any = subprocess.PIPE) -> str:
    result = subprocess.run(command, cwd=cwd, env=env, text=True, encoding="utf-8",
                            stdout=stdout, stderr=subprocess.PIPE, check=False)
    if result.returncode:
        rendered = " ".join(command)
        raise DiagnosticError(
            f"command failed ({result.returncode}): {rendered}\n{result.stderr.strip()}"
        )
    return result.stdout if isinstance(result.stdout, str) else ""


def clean_build_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    # The PR tree is untrusted executable input. Give Meson only the basic
    # process environment it needs; in particular, do not forward GitHub's
    # per-step command-file paths, credentials, or caller build overrides.
    allowed = ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE", "TZ")
    env = {key: os.environ[key] for key in allowed if key in os.environ}
    if extra:
        env.update(extra)
    return env


def require_os_ubuntu_2404() -> dict[str, str]:
    release = Path("/etc/os-release").read_text(encoding="utf-8")
    fields = re.findall(r"(?m)^([A-Z_]+)=(?:\"([^\"]*)\"|([^\n]*))$", release)
    normalized = {key: quoted or plain for key, quoted, plain in fields}
    if normalized.get("ID") != "ubuntu" or normalized.get("VERSION_ID") != "24.04":
        raise DiagnosticError("runner must be Ubuntu 24.04; refusing non-matching size evidence")
    return {
        "id": normalized.get("ID", "unknown"),
        "version_id": normalized.get("VERSION_ID", "unknown"),
        "image_os": os.environ.get("ImageOS", "unavailable"),
        "image_version": os.environ.get("ImageVersion", "unavailable"),
        "machine": platform.machine(),
        "system": platform.platform(),
    }


def validate_sha(value: str, name: str) -> None:
    if not SHA_RE.fullmatch(value):
        raise DiagnosticError(f"{name} must be a full lowercase 40-character commit SHA")


def read_policy_identity(repo: Path) -> dict[str, str]:
    identities: dict[str, str] = {}
    for relative in POLICY_FILES:
        path = repo / relative
        if not path.is_file():
            raise DiagnosticError(f"required CI policy file is missing: {relative}")
        identities[relative] = sha256_file(path)

    workflow = (repo / ".github/workflows/ci-pr.yml").read_text(encoding="utf-8")
    meson = (repo / "meson.build").read_text(encoding="utf-8")
    options = (repo / "meson_options.txt").read_text(encoding="utf-8")
    setup = (repo / ".github/actions/setup-meson/action.yml").read_text(encoding="utf-8")
    required = (
        ("runs-on: ubuntu-latest", workflow),
        ("meson setup builddir -Dtests=true -DmbedTLS=disabled", workflow),
        ("meson compile -C builddir wirelog", workflow),
        ("buildtype=release", meson),
        ("optimization=s", meson),
        ("b_lto=true", meson),
        ("b_sanitize=none", meson),
        ("value: 'trace'", options),
        ("meson==1.12.0", setup),
    )
    missing = [snippet for snippet, contents in required if snippet not in contents]
    if missing:
        raise DiagnosticError("CI size profile drifted; missing required policy: " + ", ".join(missing))
    return identities


def read_workflow_code_identity(repo: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in WORKFLOW_CODE_FILES:
        path = repo / relative
        if not path.is_file():
            raise DiagnosticError(f"diagnostic workflow file is missing: {relative}")
        result[relative] = sha256_file(path)
    return result


def compare_policy_identities(current: dict[str, str], base: dict[str, str],
                              candidate: dict[str, str]) -> list[str]:
    return sorted(path for path in set(current) | set(base) | set(candidate)
                  if current.get(path) != base.get(path) or
                  current.get(path) != candidate.get(path))


def validate_pr1945_request(repository: str, pr_number: int, base_sha: str,
                            candidate_sha: str, reference_sha: str,
                            workflow_ref: str, remote_main_sha: str,
                            remote_head_sha: str, merge_parents: list[str],
                            pull_request: dict[str, Any]) -> None:
    validate_current_candidate_request(
        repository, pr_number, base_sha, candidate_sha, reference_sha,
        workflow_ref, remote_main_sha, remote_head_sha, merge_parents,
        pull_request, allowed_pr_numbers={ADMISSION_PR_NUMBER})


def validate_current_candidate_request(repository: str, pr_number: int,
                                       base_sha: str, candidate_sha: str,
                                       reference_sha: str, workflow_ref: str,
                                       remote_main_sha: str, remote_head_sha: str,
                                       merge_parents: list[str],
                                       pull_request: dict[str, Any],
                                       allowed_pr_numbers: set[int] | None = None) -> None:
    if allowed_pr_numbers is None:
        allowed_pr_numbers = {PR_NUMBER}
    if repository != REPOSITORY:
        raise DiagnosticError(f"unexpected repository {repository!r}")
    if pr_number not in allowed_pr_numbers:
        raise DiagnosticError("the current-candidate attribution mode does not allow this PR")
    if pull_request.get("number") != pr_number:
        raise DiagnosticError(f"GitHub PR metadata number does not match PR #{pr_number}")
    if pull_request.get("base_repository") != REPOSITORY:
        raise DiagnosticError(f"PR #{pr_number} must target this repository")
    if workflow_ref != "refs/heads/main":
        raise DiagnosticError("diagnostics must be dispatched from refs/heads/main")
    for name, value in (("base SHA", base_sha), ("candidate SHA", candidate_sha),
                        ("reference SHA", reference_sha),
                        ("remote main SHA", remote_main_sha),
                        ("remote PR head SHA", remote_head_sha)):
        validate_sha(value, name)
    if base_sha != remote_main_sha:
        raise DiagnosticError("base SHA must match freshly fetched origin/main")
    if reference_sha == base_sha:
        raise DiagnosticError("reference SHA must identify a PR candidate commit after the base")
    if reference_sha == candidate_sha:
        raise DiagnosticError("reference SHA must be the unreduced candidate before the current head")
    if candidate_sha != remote_head_sha:
        raise DiagnosticError("candidate SHA does not match the current remote PR head")
    if merge_parents != [base_sha, candidate_sha]:
        raise DiagnosticError(f"PR #{pr_number} merge ref does not match the requested main base and head")
    if pull_request.get("state") != "open":
        raise DiagnosticError(f"PR #{pr_number} must remain open during attribution")
    if pull_request.get("base_ref") != "main":
        raise DiagnosticError(f"PR #{pr_number} must target main")
    if pull_request.get("base_sha") != base_sha:
        raise DiagnosticError(f"PR #{pr_number} API base SHA differs from the requested current main SHA")
    if pull_request.get("head_sha") != candidate_sha:
        raise DiagnosticError(f"PR #{pr_number} API head SHA differs from the current remote head")
    if pull_request.get("head_repository") != REPOSITORY:
        raise DiagnosticError(f"PR #{pr_number} must originate from this repository")


def fetch_pull_request_metadata(repository: str, pr_number: int) -> dict[str, Any]:
    if repository != REPOSITORY or pr_number not in CURRENT_CANDIDATE_PR_NUMBERS:
        raise DiagnosticError("pull request metadata is restricted to allowlisted same-repository PRs")
    url = f"https://api.github.com/repos/{REPOSITORY}/pulls/{pr_number}"
    request = urllib.request.Request(
        url, headers={"Accept": "application/vnd.github+json",
                      "User-Agent": "wirelog-size-attribution"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise DiagnosticError(f"could not verify public PR #{pr_number} metadata: {exc}") from exc
    if not isinstance(payload, dict):
        raise DiagnosticError(f"GitHub returned malformed PR #{pr_number} metadata")
    if payload.get("number") != pr_number:
        raise DiagnosticError(f"GitHub returned metadata for a different PR than #{pr_number}")
    base = payload.get("base")
    head = payload.get("head")
    if not isinstance(base, dict) or not isinstance(head, dict):
        raise DiagnosticError(f"GitHub returned malformed PR #{pr_number} base/head metadata")
    base_repo = base.get("repo")
    if not isinstance(base_repo, dict):
        raise DiagnosticError(f"GitHub returned malformed PR #{pr_number} base repository metadata")
    if base_repo.get("full_name") != REPOSITORY:
        raise DiagnosticError(f"PR #{pr_number} must target this repository")
    head_repo = head.get("repo")
    if head_repo is not None and not isinstance(head_repo, dict):
        raise DiagnosticError(f"GitHub returned malformed PR #{pr_number} head repository metadata")
    head_repo = head_repo or {}
    return {"number": payload.get("number"),
            "state": payload.get("state"), "base_ref": base.get("ref"),
            "base_sha": base.get("sha"), "head_sha": head.get("sha"),
            "base_repository": base_repo.get("full_name"),
            "head_repository": head_repo.get("full_name")}


def validate_current_baseline_request(repository: str, pr_number: int,
                                      base_sha: str, candidate_sha: str,
                                      workflow_ref: str, remote_main_sha: str,
                                      remote_head_sha: str, merge_parents: list[str],
                                      pull_request: dict[str, Any],
                                      allowed_pr_numbers: set[int] | None = None) -> None:
    if allowed_pr_numbers is None:
        allowed_pr_numbers = {PR_NUMBER}
    if repository != REPOSITORY or pr_number not in allowed_pr_numbers:
        raise DiagnosticError("current two-tree baseline attribution does not allow this PR")
    if pull_request.get("number") != pr_number:
        raise DiagnosticError(f"GitHub PR metadata number does not match PR #{pr_number}")
    if pull_request.get("base_repository") != REPOSITORY:
        raise DiagnosticError(f"PR #{pr_number} must target this repository")
    if workflow_ref != "refs/heads/main":
        raise DiagnosticError("diagnostics must be dispatched from refs/heads/main")
    for name, value in (("base SHA", base_sha), ("candidate SHA", candidate_sha),
                        ("remote main SHA", remote_main_sha),
                        ("remote PR head SHA", remote_head_sha)):
        validate_sha(value, name)
    if base_sha != remote_main_sha:
        raise DiagnosticError("base SHA must match freshly fetched origin/main")
    if candidate_sha != remote_head_sha:
        raise DiagnosticError("candidate SHA does not match the current remote PR head")
    if merge_parents != [base_sha, candidate_sha]:
        raise DiagnosticError(f"PR #{pr_number} merge ref does not match the requested main base and head")
    if pull_request.get("state") != "open":
        raise DiagnosticError(f"PR #{pr_number} must remain open during attribution")
    if pull_request.get("base_ref") != "main" or pull_request.get("base_sha") != base_sha:
        raise DiagnosticError(f"PR #{pr_number} must target the requested current main SHA")
    if pull_request.get("head_sha") != candidate_sha:
        raise DiagnosticError(f"PR #{pr_number} API head SHA differs from the current remote head")
    if pull_request.get("head_repository") != REPOSITORY:
        raise DiagnosticError(f"PR #{pr_number} must originate from this repository")


def verify_repository(repo: Path, repository: str, pr_number: int,
                      base_sha: str, candidate_sha: str, workflow_ref: str,
                      reference_sha: str | None = None) -> dict[str, str]:
    if pr_number in CURRENT_TWO_TREE_PR_NUMBERS and not reference_sha:
        if pr_number != PR_NUMBER or base_sha != BASE_SHA:
            return verify_current_baseline_repository(
                repo, repository, pr_number, base_sha, candidate_sha, workflow_ref)
        if repository != REPOSITORY:
            raise DiagnosticError(f"unexpected repository {repository!r}")
        if workflow_ref != "refs/heads/main":
            raise DiagnosticError("diagnostics must be dispatched from refs/heads/main")
        validate_sha(base_sha, "base SHA")
        validate_sha(candidate_sha, "candidate SHA")
        if base_sha != BASE_SHA:
            raise DiagnosticError(f"base SHA must match the measured CI base {BASE_SHA}")

        origin = run(["git", "remote", "get-url", "origin"], cwd=repo).strip()
        if not re.fullmatch(r"(?:https://github\.com/semantic-reasoning/wirelog(?:\.git)?|git@github\.com:semantic-reasoning/wirelog(?:\.git)?)", origin):
            raise DiagnosticError(f"unexpected origin URL {origin!r}")
        run(["git", "fetch", "--no-tags", "origin", "refs/heads/main:refs/remotes/origin/main"], cwd=repo)
        remote_head = run(["git", "ls-remote", "origin", f"refs/pull/{PR_NUMBER}/head"], cwd=repo).split()
        if len(remote_head) != 2 or remote_head[0] != candidate_sha:
            raise DiagnosticError("candidate SHA does not match the current remote PR head")
        run(["git", "fetch", "--no-tags", "origin", f"refs/pull/{PR_NUMBER}/head:refs/diagnostic/pr-{PR_NUMBER}-head"], cwd=repo)
        fetched_head = run(["git", "rev-parse", f"refs/diagnostic/pr-{PR_NUMBER}-head"], cwd=repo).strip()
        if fetched_head != candidate_sha:
            raise DiagnosticError("fetched PR head differs from candidate SHA")
        for name, sha in (("base", base_sha), ("candidate", candidate_sha)):
            resolved = run(["git", "rev-parse", "--verify", f"{sha}^{{commit}}"], cwd=repo).strip()
            if resolved != sha:
                raise DiagnosticError(f"{name} SHA did not resolve to the requested commit")
        run(["git", "merge-base", "--is-ancestor", base_sha, candidate_sha], cwd=repo)
        run(["git", "merge-base", "--is-ancestor", base_sha, "refs/remotes/origin/main"], cwd=repo)
        return {"origin": origin, "remote_pr_head": remote_head[0]}

    if pr_number not in CURRENT_CANDIDATE_PR_NUMBERS:
        raise DiagnosticError("only allowlisted same-repository admission PRs are accepted")
    if not reference_sha:
        raise DiagnosticError(f"PR #{pr_number} current-candidate attribution requires an immutable reference SHA")
    if repository != REPOSITORY:
        raise DiagnosticError(f"unexpected repository {repository!r}")
    if workflow_ref != "refs/heads/main":
        raise DiagnosticError("diagnostics must be dispatched from refs/heads/main")
    validate_sha(base_sha, "base SHA")
    validate_sha(candidate_sha, "candidate SHA")
    validate_sha(reference_sha, "reference SHA")
    origin = run(["git", "remote", "get-url", "origin"], cwd=repo).strip()
    if not re.fullmatch(r"(?:https://github\.com/semantic-reasoning/wirelog(?:\.git)?|git@github\.com:semantic-reasoning/wirelog(?:\.git)?)", origin):
        raise DiagnosticError(f"unexpected origin URL {origin!r}")
    run(["git", "fetch", "--no-tags", "origin", "refs/heads/main:refs/remotes/origin/main"], cwd=repo)
    remote_main = run(["git", "rev-parse", "refs/remotes/origin/main"], cwd=repo).strip()
    pull_request = fetch_pull_request_metadata(repository, pr_number)
    remote_head = run(["git", "ls-remote", "origin", f"refs/pull/{pr_number}/head"], cwd=repo).split()
    if len(remote_head) != 2:
        raise DiagnosticError(f"could not resolve the current remote PR #{pr_number} head")
    run(["git", "fetch", "--no-tags", "origin",
         f"refs/pull/{pr_number}/head:refs/diagnostic/pr-{pr_number}-head"], cwd=repo)
    run(["git", "fetch", "--no-tags", "origin",
         f"refs/pull/{pr_number}/merge:refs/diagnostic/pr-{pr_number}-merge"], cwd=repo)
    fetched_head = run(["git", "rev-parse", f"refs/diagnostic/pr-{pr_number}-head"], cwd=repo).strip()
    if fetched_head != remote_head[0]:
        raise DiagnosticError(f"fetched PR #{pr_number} head differs from ls-remote")
    merge_fields = run(["git", "rev-list", "--parents", "-n", "1",
                        f"refs/diagnostic/pr-{pr_number}-merge"], cwd=repo).split()
    merge_parents = merge_fields[1:]
    allowed_pr_numbers = {pr_number}
    validate_current_candidate_request(repository, pr_number, base_sha,
                                       candidate_sha, reference_sha,
                                       workflow_ref, remote_main,
                                       remote_head[0], merge_parents,
                                       pull_request, allowed_pr_numbers)
    for name, sha in (("base", base_sha), ("reference", reference_sha),
                      ("candidate", candidate_sha)):
        resolved = run(["git", "rev-parse", "--verify", f"{sha}^{{commit}}"], cwd=repo).strip()
        if resolved != sha:
            raise DiagnosticError(f"{name} SHA did not resolve to the requested commit")
    run(["git", "merge-base", "--is-ancestor", base_sha, reference_sha], cwd=repo)
    run(["git", "merge-base", "--is-ancestor", reference_sha, candidate_sha], cwd=repo)
    return {"origin": origin, "remote_main": remote_main,
            "remote_pr_head": remote_head[0],
            "remote_pr_merge": merge_fields[0],
            "pull_request": pull_request,
            "reference_sha": reference_sha}


def verify_current_baseline_repository(repo: Path, repository: str, pr_number: int,
                                       base_sha: str, candidate_sha: str,
                                       workflow_ref: str) -> dict[str, Any]:
    if repository != REPOSITORY or pr_number not in CURRENT_TWO_TREE_PR_NUMBERS:
        raise DiagnosticError("current two-tree baseline attribution does not allow this PR")
    validate_sha(base_sha, "base SHA")
    validate_sha(candidate_sha, "candidate SHA")
    origin = run(["git", "remote", "get-url", "origin"], cwd=repo).strip()
    if not re.fullmatch(r"(?:https://github\.com/semantic-reasoning/wirelog(?:\.git)?|git@github\.com:semantic-reasoning/wirelog(?:\.git)?)", origin):
        raise DiagnosticError(f"unexpected origin URL {origin!r}")
    run(["git", "fetch", "--no-tags", "origin", "refs/heads/main:refs/remotes/origin/main"], cwd=repo)
    remote_main = run(["git", "rev-parse", "refs/remotes/origin/main"], cwd=repo).strip()
    pull_request = fetch_pull_request_metadata(repository, pr_number)
    remote_head = run(["git", "ls-remote", "origin", f"refs/pull/{pr_number}/head"], cwd=repo).split()
    if len(remote_head) != 2:
        raise DiagnosticError(f"could not resolve the current remote PR #{pr_number} head")
    run(["git", "fetch", "--no-tags", "origin",
         f"refs/pull/{pr_number}/head:refs/diagnostic/pr-{pr_number}-head"], cwd=repo)
    run(["git", "fetch", "--no-tags", "origin",
         f"refs/pull/{pr_number}/merge:refs/diagnostic/pr-{pr_number}-merge"], cwd=repo)
    fetched_head = run(["git", "rev-parse", f"refs/diagnostic/pr-{pr_number}-head"], cwd=repo).strip()
    if fetched_head != remote_head[0]:
        raise DiagnosticError(f"fetched PR #{pr_number} head differs from ls-remote")
    merge_fields = run(["git", "rev-list", "--parents", "-n", "1",
                        f"refs/diagnostic/pr-{pr_number}-merge"], cwd=repo).split()
    validate_current_baseline_request(
        repository, pr_number, base_sha, candidate_sha, workflow_ref,
        remote_main, remote_head[0], merge_fields[1:], pull_request,
        allowed_pr_numbers={pr_number})
    for name, sha in (("base", base_sha), ("candidate", candidate_sha)):
        resolved = run(["git", "rev-parse", "--verify", f"{sha}^{{commit}}"], cwd=repo).strip()
        if resolved != sha:
            raise DiagnosticError(f"{name} SHA did not resolve to the requested commit")
    run(["git", "merge-base", "--is-ancestor", base_sha, candidate_sha], cwd=repo)
    return {"origin": origin, "remote_main": remote_main,
            "remote_pr_head": remote_head[0],
            "remote_pr_merge": merge_fields[0],
            "pull_request": pull_request}


def extract_tree(repo: Path, sha: str, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=False)
    archive_path = destination.parent / f"{destination.name}.tar"
    with archive_path.open("wb") as archive:
        subprocess.run(["git", "archive", "--format=tar", sha], cwd=repo,
                       stdout=archive, stderr=subprocess.PIPE, check=True)
    with tarfile.open(archive_path, "r:") as archive:
        archive.extractall(destination, filter="data")
    archive_path.unlink()
    if run(["git", "rev-parse", "--verify", f"{sha}^{{commit}}"], cwd=repo).strip() != sha:
        raise DiagnosticError("commit identity changed while extracting source")


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def measure_library(repo: Path, library: Path, source_sha: str,
                    profile: Path, output: Path, env: dict[str, str]) -> dict[str, Any]:
    json_path = output.with_suffix(".json")
    run(["sh", str(repo / "scripts/ci/check-text-size.sh"), str(library),
         "--measure-only", "--json", str(json_path), "--source-sha", source_sha,
         "--profile", str(profile)], cwd=repo, env=env)
    return json.loads(json_path.read_text(encoding="utf-8"))


def build_one(repo: Path, source: Path, sha: str, target: Path,
              env: dict[str, str], map_path: Path | None = None,
              build_root: Path | None = None) -> dict[str, Any]:
    build = build_root or target / "build"
    profile = target / "profile.json"
    target.mkdir(parents=True, exist_ok=True)
    build.parent.mkdir(parents=True, exist_ok=True)
    build_env = dict(env)
    command_env = None
    if map_path is not None:
        map_path.parent.mkdir(parents=True, exist_ok=True)
        command_env = dict(build_env)
        command_env["LDFLAGS"] = f"-Wl,-Map={map_path}"
    setup = ["meson", "setup", str(build), str(source),
             "-Dtests=true", "-DmbedTLS=disabled"]
    run(setup, cwd=repo, env=command_env or build_env)
    run(["meson", "compile", "-C", str(build), "wirelog"], cwd=repo, env=command_env or build_env)
    run([sys.executable, str(repo / "scripts/ci/size-profile.py"), "capture",
         "--build-dir", str(build), "--source-dir", str(source), "--source-sha", sha,
         "--output", str(profile)], cwd=repo, env=command_env or build_env)
    targets = json.loads(run(["meson", "introspect", "--targets", str(build)], cwd=repo, env=command_env or build_env))
    target_info = next((item for item in targets if item.get("name") == "wirelog" and
                        item.get("type") in ("shared library", "static library")), None)
    if target_info is None:
        raise DiagnosticError(f"Meson has no production wirelog target for {sha}")
    library = Path(target_info["filename"][0])
    if not library.is_absolute():
        library = build / library
    if not library.is_file():
        raise DiagnosticError(f"production library is missing: {library}")
    return {"build": build, "profile": profile, "library": library,
            "map": map_path, "setup_command": setup,
            "compile_command": ["meson", "compile", "-C", str(build), "wirelog"],
            "binary_sha256": sha256_file(library)}


def text_size(library: Path) -> int:
    result = run(["size", "--format=sysv", str(library)])
    matches = re.findall(r"(?m)^\.text\s+(\d+)\s+", result)
    if len(matches) != 1:
        raise DiagnosticError(f"expected one .text entry in {library}")
    return int(matches[0])


def dwarf_mapping_available(section_listing: str) -> bool:
    return bool(re.search(r"\.(?:z?debug_info)\b", section_listing) and
                re.search(r"\.(?:z?debug_line)\b", section_listing))


def addr2line_address(nm_decimal_address: str) -> str:
    return f"0x{int(nm_decimal_address, 10):x}"


def symbol_artifacts(library: Path, output: Path, env: dict[str, str]) -> dict[str, Any]:
    nm = run(["nm", "-S", "--size-sort", "--radix=d", "--defined-only", str(library)], env=env)
    objdump = run(["objdump", "-t", str(library)], env=env)
    nm_path = output.with_suffix(".nm.txt")
    objdump_path = output.with_suffix(".objdump.txt")
    nm_path.write_text(nm, encoding="utf-8")
    objdump_path.write_text(objdump, encoding="utf-8")
    parsed: list[dict[str, Any]] = []
    for line in nm.splitlines():
        parts = line.split(maxsplit=3)
        if len(parts) == 4 and parts[1].isdigit():
            parsed.append({"address": parts[0], "size": int(parts[1]),
                           "kind": parts[2], "symbol": parts[3]})
    largest = sorted(parsed, key=lambda item: item["size"], reverse=True)[:100]
    text_symbols = [item for item in parsed if item["kind"] in ("T", "t", "W", "w")]
    largest_text_symbols = sorted(text_symbols, key=lambda item: item["size"], reverse=True)[:25]
    section_listing = run(["readelf", "--sections", "--wide", str(library)], env=env)
    has_dwarf = dwarf_mapping_available(section_listing)
    mapping_path = output.with_suffix(".source-map.json")
    if not has_dwarf:
        mapping = {"status": "unavailable", "reason": "binary has no DWARF info/line sections",
                   "records": []}
    else:
        records = []
        for symbol in largest_text_symbols:
            address = addr2line_address(symbol["address"])
            mapped = run(["addr2line", "-e", str(library), "-f", "-C", address], env=env)
            lines = mapped.splitlines()
            records.append({"symbol": symbol["symbol"], "address": address,
                            "kind": symbol["kind"],
                            "function": lines[0] if lines else "??",
                            "source": lines[1] if len(lines) > 1 else "??:?"})
        mapped_count = sum(record["source"] not in ("??:?", "??:0") for record in records)
        mapping = {"status": "available" if mapped_count else "unavailable",
                   "reason": None if mapped_count else "DWARF exists but addr2line found no text-symbol source locations",
                   "mapped_symbols": mapped_count, "records": records}
    mapping_path.write_text(json.dumps(mapping, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return {"nm_file": nm_path.name, "objdump_file": objdump_path.name,
            "source_map_file": mapping_path.name, "source_mapping": mapping,
            "largest_symbols": largest, "text_symbols": text_symbols}


def map_text_sections(map_path: Path) -> list[dict[str, Any]]:
    if not map_path.is_file():
        return []
    pattern = re.compile(
        r"^\s+(\.text(?:\.[^\s]+)?)\s+(0x[0-9a-fA-F]+)\s+"
        r"(0x[0-9a-fA-F]+)\s+(\S+\.o)\s*$")
    sections: list[dict[str, Any]] = []
    for line in map_path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.match(line)
        if match:
            section, address, size, name = match.groups()
            start, length = int(address, 16), int(size, 16)
            if length:
                sections.append({"section": section, "start": start, "end": start + length,
                                 "text_bytes": length, "object": name})
    return sections


def object_key(name: str) -> str:
    normalized = name.replace("\\", "/")
    parts = normalized.split("/")
    for index, part in enumerate(parts):
        if part.endswith(".p"):
            return "/".join(parts[index + 1:])
    return "/".join(parts[-3:])


def map_text_objects(map_path: Path) -> list[dict[str, Any]]:
    totals: dict[str, int] = {}
    for section in map_text_sections(map_path):
        name = section["object"]
        totals[name] = totals.get(name, 0) + section["text_bytes"]
    return [{"object": name, "text_bytes": amount}
            for name, amount in sorted(totals.items(), key=lambda pair: pair[1], reverse=True)]


def is_ltrans_object(name: str) -> bool:
    return bool(re.search(r"(?:^|/)ltrans[^/]*\.ltrans\.o$", name.replace("\\", "/")))


def object_text_deltas(base: list[dict[str, Any]], candidate: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def aggregate(rows: list[dict[str, Any]]) -> dict[str, int]:
        result: dict[str, int] = {}
        for row in rows:
            # LTO partition names describe compiler-generated chunks, not stable
            # source objects. Compare them per build only through lto_evidence().
            if is_ltrans_object(row["object"]):
                continue
            normalized = object_key(row["object"])
            result[normalized] = result.get(normalized, 0) + row["text_bytes"]
        return result

    before, after = aggregate(base), aggregate(candidate)
    names = sorted(set(before) | set(after))
    deltas = [{"object": name, "base_text_bytes": before.get(name, 0),
               "candidate_text_bytes": after.get(name, 0),
               "delta_bytes": after.get(name, 0) - before.get(name, 0)}
              for name in names]
    return sorted(deltas, key=lambda item: abs(item["delta_bytes"]), reverse=True)


def join_symbols_to_objects(object_deltas: list[dict[str, Any]],
                            sections: list[dict[str, Any]],
                            symbols: list[dict[str, Any]], *,
                            object_limit: int = 10,
                            symbol_limit: int = 20) -> list[dict[str, Any]]:
    """Join final-binary symbols to changed input objects using linker-map ranges.

    A symbol is attributed only when its full address range falls in sections
    belonging to exactly one normalized input object. Ranges are half-open to
    avoid assigning a symbol at one object's end to that object.
    """
    by_key: dict[str, list[dict[str, Any]]] = {}
    for section in sections:
        by_key.setdefault(object_key(section["object"]), []).append(section)

    result: list[dict[str, Any]] = []
    for delta in object_deltas[:object_limit]:
        key = delta["object"]
        matched: list[dict[str, Any]] = []
        ambiguous = 0
        boundary = 0
        for symbol in symbols:
            address = int(symbol["address"], 10)
            start_owners = {object_key(section["object"]) for section in sections
                            if section["start"] <= address < section["end"]}
            symbol_end = address + symbol["size"]
            owners = {object_key(section["object"]) for section in sections
                      if section["start"] <= address < section["end"]
                      and symbol_end <= section["end"]}
            if len(owners) > 1 and key in owners:
                ambiguous += 1
            elif not owners and key in start_owners:
                boundary += 1
            elif owners == {key}:
                matched.append({**symbol, "address_hex": f"0x{address:x}"})
        matched.sort(key=lambda item: item["size"], reverse=True)
        selected = matched[:symbol_limit]
        own_sections = by_key.get(key, [])
        if selected:
            status, reason = "available", None
        elif not own_sections:
            status, reason = "unavailable", "no linker-map .text range for this object in this build"
        elif ambiguous:
            status, reason = "ambiguous", "symbol addresses overlap multiple normalized input objects"
        elif boundary:
            status, reason = "ambiguous", "symbol ranges cross their linker-map input-section boundary"
        else:
            status, reason = "unavailable", "no final-binary text symbols uniquely joined to this object"
        result.append({
            "object": key,
            "delta_bytes": delta["delta_bytes"],
            "status": status,
            "reason": reason,
            "map_sections": len(own_sections),
            "matched_symbol_count": len(matched),
            "ambiguous_symbol_count": ambiguous,
            "boundary_symbol_count": boundary,
            "omitted_symbol_count": max(0, len(matched) - len(selected)),
            "symbols": selected,
        })
    return result


def attach_source_locations(library: Path, object_attribution: list[dict[str, Any]],
                            env: dict[str, str], *, dwarf_available: bool) -> None:
    for item in object_attribution:
        symbols = item["symbols"]
        if not symbols:
            item["source_mapping"] = {"status": "unavailable", "reason": item["reason"],
                                      "records": []}
            continue
        if not dwarf_available:
            item["source_mapping"] = {
                "status": "unavailable", "reason": "final binary has no DWARF info/line sections",
                "records": [],
            }
            continue
        mapped = run(["addr2line", "-e", str(library), "-f", "-C",
                      *(symbol["address_hex"] for symbol in symbols)], env=env).splitlines()
        records = []
        mapped_count = 0
        for index, symbol in enumerate(symbols):
            function = mapped[index * 2] if index * 2 < len(mapped) else "??"
            source = mapped[index * 2 + 1] if index * 2 + 1 < len(mapped) else "??:?"
            mapped_count += source not in ("??:?", "??:0")
            records.append({"symbol": symbol["symbol"], "address": symbol["address_hex"],
                            "function": function, "source": source})
        item["source_mapping"] = {
            "status": "available" if mapped_count else "unavailable",
            "reason": None if mapped_count else "addr2line found no source locations for joined symbols",
            "mapped_symbols": mapped_count,
            "records": records,
        }


def lto_evidence(profile_path: Path, sections: list[dict[str, Any]]) -> dict[str, Any]:
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    options = profile.get("options", {})
    lto_option = options.get("b_lto")
    link_flags = [parameter for entry in profile.get("effective_link_arguments", [])
                  for parameter in entry.get("parameters", [])]
    lto_flags = [flag for flag in link_flags if flag == "-flto" or flag.startswith("-flto=")]
    enabled = bool(lto_option) or bool(lto_flags)
    ltrans = sorted({section["object"] for section in sections
                     if re.search(r"(?:^|/)ltrans[^/]*\.ltrans\.o$", section["object"])})
    ltrans_sections = [section for section in sections if section["object"] in ltrans]
    if not enabled:
        ltrans_status, ltrans_reason = "not-applicable", "LTO is disabled in the effective profile"
    elif ltrans:
        ltrans_status, ltrans_reason = "available", None
    else:
        ltrans_status, ltrans_reason = "unavailable", "linker map contains no recognizable LTRANS input objects"
    return {
        "enabled": enabled,
        "profile_b_lto": lto_option,
        "effective_flto_flags": lto_flags,
        "ltrans_objects": {
            "status": ltrans_status,
            "reason": ltrans_reason,
            "count": len(ltrans),
            "text_bytes": sum(section["text_bytes"] for section in ltrans_sections),
            "objects": ltrans,
            "cross_build_delta_status": "unavailable",
            "cross_build_delta_reason": (
                "LTRANS partition names and membership are build-specific and are excluded from "
                "like-for-like object delta ranking"
            ),
        },
        "inlining_evidence": {
            "status": "unavailable",
            "reason": "the authoritative CI profile does not emit an optimizer inline-decision report",
        },
    }


def attribution_matches(forensic_hash: str, authoritative_hash: str,
                        forensic_text: int, authoritative_text: int,
                        map_digest: str | None) -> bool:
    return (bool(map_digest) and forensic_hash == authoritative_hash and
            forensic_text == authoritative_text)


def forensic_profile_matches(authoritative_path: Path, forensic_path: Path,
                             map_path: Path) -> tuple[bool, list[str]]:
    authoritative = json.loads(authoritative_path.read_text(encoding="utf-8"))
    forensic = json.loads(forensic_path.read_text(encoding="utf-8"))
    if not isinstance(authoritative, dict) or not isinstance(forensic, dict):
        return False, ["malformed profile root"]
    authoritative.pop("source_sha", None)
    forensic.pop("source_sha", None)
    expected_map_flag = f"-Wl,-Map={map_path}"
    found_map_flags = 0
    differences: list[str] = []

    def strip_map_flags(parameters: Any, field: str, *, required: bool) -> tuple[list[str] | None, int]:
        if not isinstance(parameters, list) or any(not isinstance(item, str) for item in parameters):
            differences.append(f"malformed linker argument list: {field}")
            return None, 0
        kept = []
        found = 0
        for parameter in parameters:
            if parameter == expected_map_flag:
                found += 1
            elif (parameter == "-Map" or
                  parameter.startswith(("-Wl,-Map", "-Map="))):
                differences.append("unexpected or split linker map argument")
                return None, found
            else:
                kept.append(parameter)
        if found > 1:
            differences.append(f"duplicate linker map flag in {field}")
        if required and found != 1:
            differences.append(f"expected exactly one linker map flag, found {found}")
        return kept, found

    effective = forensic.get("effective_link_arguments")
    if not isinstance(effective, list):
        differences.append("malformed effective_link_arguments")
    else:
        for index, entry in enumerate(effective):
            if not isinstance(entry, dict):
                differences.append(f"malformed effective linker argument entry: {index}")
                continue
            kept, found = strip_map_flags(entry.get("parameters"),
                                          f"effective_link_arguments[{index}]", required=False)
            found_map_flags += found
            if kept is not None:
                entry["parameters"] = kept

    options = forensic.get("options")
    if "options" not in forensic:
        options = None
    elif not isinstance(options, dict):
        differences.append("malformed options")
    else:
        for key in ("c_link_args", "cpp_link_args"):
            if key not in options:
                continue
            kept, _ = strip_map_flags(options[key], f"options.{key}", required=False)
            if kept is not None:
                options[key] = kept

    if found_map_flags != 1:
        differences.append(f"expected exactly one linker map flag, found {found_map_flags}")
    differences.extend(sorted(key for key in set(authoritative) | set(forensic)
                              if authoritative.get(key) != forensic.get(key)))
    differences = sorted(set(differences))
    return not differences, differences


def collect_two_tree_candidate(args: argparse.Namespace) -> int:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "schema_version": 1,
        "repository": args.repository,
        "pr_number": args.pr_number,
        "base_sha": args.base_sha,
        "candidate_sha": args.candidate_sha,
        "workflow_ref": args.workflow_ref,
        "measurement_mode": ("historical-pr1903" if args.base_sha == BASE_SHA
                             else "current-base-head"),
        "status": "running",
        "attribution_status": "not-run",
    }
    report_path = output / "report.json"
    write_json(report_path, report)
    try:
        report["runner"] = require_os_ubuntu_2404()
        repo = Path(args.repository_root).resolve()
        report["source_identity"] = verify_repository(repo, args.repository, args.pr_number,
                                                       args.base_sha, args.candidate_sha,
                                                       args.workflow_ref)
        report["workflow_code_files_sha256"] = read_workflow_code_identity(repo)
        report["workflow_sha"] = run(["git", "rev-parse", "HEAD"], cwd=repo).strip()
        report["toolchain"] = {
            "gcc_version": run(["gcc", "-dumpfullversion", "-dumpversion"]).strip(),
            "linker_version": run(["ld", "--version"]).splitlines()[0],
            "meson_version": run(["meson", "--version"]).strip(),
            "ninja_version": run(["ninja", "--version"]).strip(),
        }
        if not report["toolchain"]["gcc_version"].startswith("13.3.0"):
            raise DiagnosticError("expected CI GCC 13.3.0")
        if report["toolchain"]["meson_version"] != "1.12.0":
            raise DiagnosticError("expected CI Meson 1.12.0")

        env = clean_build_env()
        report["build_environment"] = {
            "meson_setup": "meson setup <build> <source> -Dtests=true -DmbedTLS=disabled",
            "meson_compile": "meson compile -C <build> wirelog",
            "project_defaults": {"buildtype": "release", "optimization": "s",
                                 "b_lto": True, "wirelog_log_max_level": "trace"},
            "credentials_passed_to_build": False,
        }
        with tempfile.TemporaryDirectory(prefix="wirelog-size-attribution-") as temp:
            temp_root = Path(temp)
            isolated_home = temp_root / "home"
            isolated_home.mkdir()
            env["HOME"] = str(isolated_home)
            env["TMPDIR"] = str(temp_root)
            source_base = temp_root / "source-base"
            source_candidate = temp_root / "source-candidate"
            extract_tree(repo, args.base_sha, source_base)
            extract_tree(repo, args.candidate_sha, source_candidate)
            current_policy = read_policy_identity(repo)
            base_policy = read_policy_identity(source_base)
            candidate_policy = read_policy_identity(source_candidate)
            report["measured_policy_files_sha256"] = {
                "workflow_checkout": current_policy,
                "base_tree": base_policy,
                "candidate_tree": candidate_policy,
            }
            policy_differences = compare_policy_identities(current_policy, base_policy, candidate_policy)
            if policy_differences:
                report["measured_policy_matches"] = False
                raise DiagnosticError("measured CI/size policy differs across workflow, base, and candidate trees: "
                                      + ", ".join(policy_differences))
            report["measured_policy_matches"] = True
            auth_base = build_one(repo, source_base, args.base_sha, output / "authoritative-base", env,
                                  build_root=temp_root / "authoritative-base-build")
            auth_candidate = build_one(repo, source_candidate, args.candidate_sha,
                                       output / "authoritative-candidate", env,
                                       build_root=temp_root / "authoritative-candidate-build")
            run([sys.executable, str(repo / "scripts/ci/size-profile.py"), "compare",
                 str(auth_base["profile"]), str(auth_candidate["profile"])], cwd=repo, env=env)
            auth_base_size = measure_library(repo, auth_base["library"], args.base_sha,
                                             auth_base["profile"], output / "authoritative-base-size", env)
            auth_candidate_size = measure_library(repo, auth_candidate["library"], args.candidate_sha,
                                                  auth_candidate["profile"], output / "authoritative-candidate-size", env)
            report["authoritative"] = {
                "base": {"profile": str(auth_base["profile"]),
                         "binary_sha256": auth_base["binary_sha256"],
                         "text_bytes": auth_base_size["measured_bytes"],
                         "size_record": auth_base_size},
                "candidate": {"profile": str(auth_candidate["profile"]),
                              "binary_sha256": auth_candidate["binary_sha256"],
                              "text_bytes": auth_candidate_size["measured_bytes"],
                              "size_record": auth_candidate_size},
                "delta_bytes": auth_candidate_size["measured_bytes"] - auth_base_size["measured_bytes"],
                "profiles_match": True,
                "commands": {
                    "base_setup": auth_base["setup_command"],
                    "base_compile": auth_base["compile_command"],
                    "candidate_setup": auth_candidate["setup_command"],
                    "candidate_compile": auth_candidate["compile_command"],
                },
            }
            write_json(report_path, report)

            attribution: dict[str, Any] = {}
            attribution_sections: dict[str, list[dict[str, Any]]] = {}
            attribution_text_symbols: dict[str, list[dict[str, Any]]] = {}
            attribution_libraries: dict[str, Path] = {}
            attribution_dwarf: dict[str, bool] = {}
            for label, source, sha, auth in (
                ("base", source_base, args.base_sha, auth_base),
                ("candidate", source_candidate, args.candidate_sha, auth_candidate),
            ):
                map_path = output / f"attribution-{label}.map"
                forensic = build_one(repo, source, sha, output / f"attribution-{label}", env, map_path,
                                     build_root=temp_root / f"attribution-{label}-build")
                profile_matches, profile_differences = forensic_profile_matches(
                    auth["profile"], forensic["profile"], map_path)
                forensic_text = text_size(forensic["library"])
                authoritative_text = text_size(auth["library"])
                same_hash = forensic["binary_sha256"] == auth["binary_sha256"]
                same_text = forensic_text == authoritative_text
                map_digest = sha256_file(map_path) if map_path.is_file() else None
                symbols = symbol_artifacts(forensic["library"], output / f"attribution-{label}", env)
                sections = map_text_sections(map_path)
                text_symbols = symbols.pop("text_symbols")
                section_listing = run(["readelf", "--sections", "--wide", str(forensic["library"])], env=env)
                attribution[label] = {
                    "binary_sha256": forensic["binary_sha256"],
                    "text_bytes": forensic_text,
                    "authoritative_binary_sha256": auth["binary_sha256"],
                    "authoritative_text_bytes": authoritative_text,
                    "binary_hash_matches": same_hash,
                    "text_matches": same_text,
                    "profile_matches_except_map_flag": profile_matches,
                    "profile_differences": profile_differences,
                    "forensic_profile": str(forensic["profile"]),
                    "map_file": map_path.name,
                    "map_sha256": map_digest,
                    "objects": map_text_objects(map_path),
                    "symbols": symbols,
                    "extra_linker_flag": f"-Wl,-Map={map_path}",
                    "lto_evidence": lto_evidence(forensic["profile"], sections),
                }
                attribution_sections[label] = sections
                attribution_text_symbols[label] = text_symbols
                attribution_libraries[label] = forensic["library"]
                attribution_dwarf[label] = dwarf_mapping_available(section_listing)
            report["attribution"] = attribution
            report["per_object_text_deltas"] = object_text_deltas(
                attribution["base"]["objects"], attribution["candidate"]["objects"])
            report["per_object_text_delta_note"] = (
                "LTRANS objects are excluded from cross-build object deltas because partition "
                "names and membership are not stable; inspect per-build lto_evidence instead."
            )
            for label in ("base", "candidate"):
                object_evidence = join_symbols_to_objects(
                    report["per_object_text_deltas"], attribution_sections[label],
                    attribution_text_symbols[label])
                attach_source_locations(attribution_libraries[label], object_evidence, env,
                                        dwarf_available=attribution_dwarf[label])
                attribution[label]["largest_changed_object_attribution"] = object_evidence
            usable = all(item["profile_matches_except_map_flag"] and
                         attribution_matches(item["binary_sha256"],
                                             item["authoritative_binary_sha256"],
                                             item["text_bytes"],
                                             item["authoritative_text_bytes"],
                                             item["map_sha256"])
                         for item in attribution.values())
            report["attribution_status"] = "usable" if usable else "unsafe-or-incomplete"
            if usable:
                positive_objects = [item for item in report["per_object_text_deltas"]
                                    if item["delta_bytes"] > 0]
                report["attribution_note"] = (
                    "Map and symbol attribution is forensic evidence only. GCC LTO may merge, split, "
                    "rename, or inline source functions; unavailable mappings are not inferred."
                )
                report["source_reduction_assessment"] = {
                    "status": "ambiguous-under-lto",
                    "largest_positive_object_deltas": positive_objects[:10],
                    "next_evidence": (
                        "Map/object totals do not prove a source-level saving under LTO. Inspect "
                        "optimized LTRANS symbols and source/DWARF mappings; if unavailable, report "
                        "the ambiguity instead of selecting speculative savings."
                    ),
                }
            else:
                report["attribution_note"] = (
                    "Forensic build profile/binary does not match authoritative evidence except for the "
                    "expected map flag, or its map artifact is missing; do not use it to select savings "
                    "or make gate claims."
                )
                report["source_reduction_assessment"] = {
                    "status": "attribution-invalid",
                    "next_evidence": "Resolve the forensic-versus-authoritative binary mismatch before source analysis.",
                }
        final_identity = verify_repository(repo, args.repository, args.pr_number,
                                           args.base_sha, args.candidate_sha,
                                           args.workflow_ref)
        report["source_identity"]["final_verification"] = final_identity
        report["status"] = "complete" if report["attribution_status"] == "usable" else "incomplete"
    except Exception as exc:  # noqa: BLE001 - preserve partial evidence for the always-upload step.
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        write_json(report_path, report)
        write_artifact_checksums(output)
        print(report["error"], file=sys.stderr)
        return 1
    write_json(report_path, report)
    write_artifact_checksums(output)
    return 0 if report["status"] == "complete" else 1


def profile_fingerprint(path: Path) -> str:
    profile = json.loads(path.read_text(encoding="utf-8"))
    profile.pop("source_sha", None)
    canonical = json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def write_artifact_checksums(root: Path) -> None:
    manifest = root / "SHA256SUMS"
    entries = []
    for path in sorted(item for item in root.rglob("*")
                       if item.is_file() and item != manifest):
        entries.append(f"{sha256_file(path)}  {path.relative_to(root).as_posix()}")
    manifest.write_text("\n".join(entries) + "\n", encoding="utf-8")


def collect_current_candidate(args: argparse.Namespace) -> int:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "schema_version": 2,
        "repository": args.repository,
        "pr_number": args.pr_number,
        "base_sha": args.base_sha,
        "reference_sha": args.reference_sha,
        "candidate_sha": args.candidate_sha,
        "workflow_ref": args.workflow_ref,
        "status": "running",
        "attribution_status": "not-run",
    }
    report_path = output / "report.json"
    write_json(report_path, report)
    try:
        if not args.reference_sha:
            raise DiagnosticError("current-candidate attribution requires an immutable reference SHA")
        report["runner"] = require_os_ubuntu_2404()
        repo = Path(args.repository_root).resolve()
        report["source_identity"] = verify_repository(
            repo, args.repository, args.pr_number, args.base_sha,
            args.candidate_sha, args.workflow_ref, args.reference_sha)
        report["workflow_code_files_sha256"] = read_workflow_code_identity(repo)
        report["workflow_sha"] = run(["git", "rev-parse", "HEAD"], cwd=repo).strip()
        report["toolchain"] = {
            "gcc_version": run(["gcc", "-dumpfullversion", "-dumpversion"]).strip(),
            "linker_version": run(["ld", "--version"]).splitlines()[0],
            "meson_version": run(["meson", "--version"]).strip(),
            "ninja_version": run(["ninja", "--version"]).strip(),
        }
        if not report["toolchain"]["gcc_version"].startswith("13.3.0"):
            raise DiagnosticError("expected CI GCC 13.3.0")
        if report["toolchain"]["meson_version"] != "1.12.0":
            raise DiagnosticError("expected CI Meson 1.12.0")

        env = clean_build_env()
        report["build_environment"] = {
            "meson_setup": "meson setup <build> <source> -Dtests=true -DmbedTLS=disabled",
            "meson_compile": "meson compile -C <build> wirelog",
            "project_defaults": {"buildtype": "release", "optimization": "s",
                                 "b_lto": True, "wirelog_log_max_level": "trace"},
            "credentials_passed_to_build": False,
        }
        labels = ("base", "reference", "candidate")
        shas = {"base": args.base_sha, "reference": args.reference_sha,
                "candidate": args.candidate_sha}
        with tempfile.TemporaryDirectory(prefix=f"wirelog-size-attribution-pr{args.pr_number}-") as temp:
            temp_root = Path(temp)
            isolated_home = temp_root / "home"
            isolated_home.mkdir()
            env["HOME"] = str(isolated_home)
            env["TMPDIR"] = str(temp_root)

            sources_by_sha: dict[str, Path] = {}
            sources: dict[str, Path] = {}
            for label in labels:
                sha = shas[label]
                if sha not in sources_by_sha:
                    source = temp_root / f"source-{label}"
                    extract_tree(repo, sha, source)
                    sources_by_sha[sha] = source
                sources[label] = sources_by_sha[sha]

            current_policy = read_policy_identity(repo)
            policies = {label: read_policy_identity(sources[label]) for label in labels}
            policy_differences = {
                label: sorted(path for path in set(current_policy) | set(policies[label])
                              if current_policy.get(path) != policies[label].get(path))
                for label in labels
            }
            report["measured_policy_files_sha256"] = {
                "workflow_checkout": current_policy,
                **{f"{label}_tree": policies[label] for label in labels},
            }
            report["measured_policy_matches"] = not any(policy_differences.values())
            if not report["measured_policy_matches"]:
                changed = sorted({path for paths in policy_differences.values() for path in paths})
                raise DiagnosticError("measured CI/size policy differs across workflow and PR trees: "
                                      + ", ".join(changed))

            authoritative_by_sha: dict[str, dict[str, Any]] = {}
            authoritative: dict[str, dict[str, Any]] = {}
            for label in labels:
                sha = shas[label]
                if sha not in authoritative_by_sha:
                    authoritative_by_sha[sha] = build_one(
                        repo, sources[label], sha, output / f"authoritative-{label}", env,
                        build_root=temp_root / f"authoritative-{label}-build")
                authoritative[label] = authoritative_by_sha[sha]
            for label in ("reference", "candidate"):
                run([sys.executable, str(repo / "scripts/ci/size-profile.py"), "compare",
                     str(authoritative["base"]["profile"]),
                     str(authoritative[label]["profile"])], cwd=repo, env=env)

            size_records: dict[str, dict[str, Any]] = {}
            report["authoritative"] = {}
            for label in labels:
                auth = authoritative[label]
                size_records[label] = measure_library(
                    repo, auth["library"], shas[label], auth["profile"],
                    output / f"authoritative-{label}-size", env)
                report["authoritative"][label] = {
                    "source_sha": shas[label],
                    "profile": str(auth["profile"]),
                    "profile_fingerprint": profile_fingerprint(auth["profile"]),
                    "binary_sha256": auth["binary_sha256"],
                    "text_bytes": size_records[label]["measured_bytes"],
                    "size_record": size_records[label],
                }

            comparison_pairs = (("base", "reference"),
                                ("reference", "candidate"),
                                ("base", "candidate"))
            report["total_text_comparisons"] = {}
            for left, right in comparison_pairs:
                name = f"{left}_to_{right}"
                report["total_text_comparisons"][name] = {
                    "from_sha": shas[left],
                    "to_sha": shas[right],
                    "from_text_bytes": size_records[left]["measured_bytes"],
                    "to_text_bytes": size_records[right]["measured_bytes"],
                    "delta_bytes": size_records[right]["measured_bytes"] - size_records[left]["measured_bytes"],
                    "profiles_match": (profile_fingerprint(authoritative[left]["profile"])
                                       == profile_fingerprint(authoritative[right]["profile"])),
                }

            attribution_by_sha: dict[str, dict[str, Any]] = {}
            attribution: dict[str, dict[str, Any]] = {}
            attribution_sections: dict[str, list[dict[str, Any]]] = {}
            attribution_text_symbols: dict[str, list[dict[str, Any]]] = {}
            attribution_libraries: dict[str, Path] = {}
            attribution_dwarf: dict[str, bool] = {}
            for label in labels:
                sha = shas[label]
                if sha not in attribution_by_sha:
                    map_label = label
                    map_path = output / f"attribution-{map_label}.map"
                    forensic = build_one(
                        repo, sources[label], sha, output / f"attribution-{map_label}",
                        env, map_path, build_root=temp_root / f"attribution-{map_label}-build")
                    auth = authoritative[label]
                    profile_matches, profile_differences = forensic_profile_matches(
                        auth["profile"], forensic["profile"], map_path)
                    forensic_text = text_size(forensic["library"])
                    authoritative_text = text_size(auth["library"])
                    same_hash = forensic["binary_sha256"] == auth["binary_sha256"]
                    same_text = forensic_text == authoritative_text
                    map_digest = sha256_file(map_path) if map_path.is_file() else None
                    symbols = symbol_artifacts(forensic["library"],
                                               output / f"attribution-{map_label}", env)
                    sections = map_text_sections(map_path)
                    text_symbols = symbols.pop("text_symbols")
                    section_listing = run(["readelf", "--sections", "--wide",
                                           str(forensic["library"])], env=env)
                    attribution_by_sha[sha] = {
                        "evidence": {
                            "source_sha": sha,
                            "binary_sha256": forensic["binary_sha256"],
                            "text_bytes": forensic_text,
                            "authoritative_binary_sha256": auth["binary_sha256"],
                            "authoritative_text_bytes": authoritative_text,
                            "binary_hash_matches": same_hash,
                            "text_matches": same_text,
                            "profile_matches_except_map_flag": profile_matches,
                            "profile_differences": profile_differences,
                            "forensic_profile": str(forensic["profile"]),
                            "map_file": map_path.name,
                            "map_sha256": map_digest,
                            "objects": map_text_objects(map_path),
                            "symbols": symbols,
                            "extra_linker_flag": f"-Wl,-Map={map_path}",
                            "lto_evidence": lto_evidence(forensic["profile"], sections),
                        },
                        "sections": sections,
                        "text_symbols": text_symbols,
                        "library": forensic["library"],
                        "dwarf_available": dwarf_mapping_available(section_listing),
                    }
                cached = attribution_by_sha[sha]
                attribution[label] = dict(cached["evidence"])
                attribution_sections[label] = cached["sections"]
                attribution_text_symbols[label] = cached["text_symbols"]
                attribution_libraries[label] = cached["library"]
                attribution_dwarf[label] = cached["dwarf_available"]

            report["attribution"] = attribution
            per_object: dict[str, list[dict[str, Any]]] = {}
            for left, right in comparison_pairs:
                name = f"{left}_to_{right}"
                deltas = object_text_deltas(attribution[left]["objects"],
                                            attribution[right]["objects"])
                per_object[name] = deltas
                mapped: dict[str, list[dict[str, Any]]] = {}
                for label in (left, right):
                    evidence = join_symbols_to_objects(
                        deltas, attribution_sections[label], attribution_text_symbols[label])
                    attach_source_locations(attribution_libraries[label], evidence, env,
                                            dwarf_available=attribution_dwarf[label])
                    mapped[label] = evidence
                report["total_text_comparisons"][name]["per_object_text_deltas"] = deltas
                report["total_text_comparisons"][name]["source_attribution"] = mapped
            report["per_object_text_deltas"] = per_object["base_to_candidate"]
            report["per_object_text_delta_note"] = (
                "LTRANS objects are excluded from cross-build object deltas because partition "
                "names and membership are not stable; inspect per-build lto_evidence instead.")
            report["attribution_note"] = (
                "Map and symbol attribution is forensic evidence only. GCC LTO may merge, split, "
                "rename, or inline source functions; unavailable mappings are not inferred.")
            usable = all(item["profile_matches_except_map_flag"] and
                         attribution_matches(item["binary_sha256"],
                                             item["authoritative_binary_sha256"],
                                             item["text_bytes"],
                                             item["authoritative_text_bytes"],
                                             item["map_sha256"])
                         for item in attribution.values())
            usable = usable and all(item["profiles_match"]
                                    for item in report["total_text_comparisons"].values())
            final_identity = verify_repository(
                repo, args.repository, args.pr_number, args.base_sha,
                args.candidate_sha, args.workflow_ref, args.reference_sha)
            report["source_identity"]["final_verification"] = dict(final_identity)
            report["attribution_status"] = "usable" if usable else "unsafe-or-incomplete"
            if usable:
                report["source_reduction_assessment"] = {
                    "status": "ambiguous-under-lto",
                    "largest_positive_object_deltas": {
                        name: [item for item in deltas if item["delta_bytes"] > 0][:10]
                        for name, deltas in per_object.items()
                    },
                    "next_evidence": (
                        "Map/object totals do not prove a source-level saving under LTO. Inspect "
                        "optimized LTRANS symbols and source/DWARF mappings; if unavailable, report "
                        "the ambiguity instead of selecting speculative savings."),
                }
            else:
                report["attribution_note"] = (
                    "Forensic build profile/binary does not match authoritative evidence except for "
                    "the expected map flag, or its map artifact is missing; do not use it to select "
                    "savings or make gate claims.")
                report["source_reduction_assessment"] = {
                    "status": "attribution-invalid",
                    "next_evidence": "Resolve the forensic-versus-authoritative binary mismatch before source analysis.",
                }
        report["status"] = "complete" if report["attribution_status"] == "usable" else "incomplete"
    except Exception as exc:  # noqa: BLE001 - preserve partial evidence for the always-upload step.
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        write_json(report_path, report)
        write_artifact_checksums(output)
        print(report["error"], file=sys.stderr)
        return 1
    write_json(report_path, report)
    write_artifact_checksums(output)
    return 0 if report["status"] == "complete" else 1


def collect(args: argparse.Namespace) -> int:
    if args.reference_sha:
        if args.pr_number in CURRENT_CANDIDATE_PR_NUMBERS:
            return collect_current_candidate(args)
        output = Path(args.output).resolve()
        output.mkdir(parents=True, exist_ok=True)
        report = {"schema_version": 1, "repository": args.repository,
                  "pr_number": args.pr_number, "base_sha": args.base_sha,
                  "candidate_sha": args.candidate_sha,
                  "reference_sha": args.reference_sha,
                  "workflow_ref": args.workflow_ref, "status": "failed",
                  "error": "reference SHA is restricted to allowlisted current candidates"}
        write_json(output / "report.json", report)
        write_artifact_checksums(output)
        print(report["error"], file=sys.stderr)
        return 1
    if args.pr_number not in CURRENT_TWO_TREE_PR_NUMBERS:
        raise DiagnosticError("two-tree attribution is restricted to allowlisted current candidates")
    return collect_two_tree_candidate(args)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pr-number", required=True, type=int)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--reference-sha", default="")
    parser.add_argument("--workflow-ref", required=True)
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--output", default="size-attribution")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(collect(parse_args()))
