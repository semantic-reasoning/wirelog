"""Collect reproducible, read-only size and linker-map evidence for PR #1903."""

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
from pathlib import Path
from typing import Any

REPOSITORY = "semantic-reasoning/wirelog"
PR_NUMBER = 1903
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


def verify_repository(repo: Path, repository: str, pr_number: int,
                      base_sha: str, candidate_sha: str, workflow_ref: str) -> dict[str, str]:
    if repository != REPOSITORY:
        raise DiagnosticError(f"unexpected repository {repository!r}")
    if pr_number != PR_NUMBER:
        raise DiagnosticError(f"only PR #{PR_NUMBER} is accepted")
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
    fetched_head = run(["git", "rev-parse", "refs/diagnostic/pr-1903-head"], cwd=repo).strip()
    if fetched_head != candidate_sha:
        raise DiagnosticError("fetched PR head differs from candidate SHA")
    for name, sha in (("base", base_sha), ("candidate", candidate_sha)):
        resolved = run(["git", "rev-parse", "--verify", f"{sha}^{{commit}}"], cwd=repo).strip()
        if resolved != sha:
            raise DiagnosticError(f"{name} SHA did not resolve to the requested commit")
    run(["git", "merge-base", "--is-ancestor", base_sha, candidate_sha], cwd=repo)
    run(["git", "merge-base", "--is-ancestor", base_sha, "refs/remotes/origin/main"], cwd=repo)
    return {"origin": origin, "remote_pr_head": remote_head[0]}


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
            "largest_symbols": largest}


def map_text_objects(map_path: Path) -> list[dict[str, Any]]:
    if not map_path.is_file():
        return []
    pattern = re.compile(r"^\s+\.text(?:\.[^\s]+)?\s+0x[0-9a-fA-F]+\s+0x([0-9a-fA-F]+)\s+(\S+\.o)\s*$")
    totals: dict[str, int] = {}
    for line in map_path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.match(line)
        if match:
            size, name = match.groups()
            totals[name] = totals.get(name, 0) + int(size, 16)
    return [{"object": name, "text_bytes": amount}
            for name, amount in sorted(totals.items(), key=lambda pair: pair[1], reverse=True)]


def object_text_deltas(base: list[dict[str, Any]], candidate: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(name: str) -> str:
        normalized = name.replace("\\", "/")
        parts = normalized.split("/")
        for index, part in enumerate(parts):
            if part.endswith(".p"):
                return "/".join(parts[index + 1:])
        return "/".join(parts[-3:])

    def aggregate(rows: list[dict[str, Any]]) -> dict[str, int]:
        result: dict[str, int] = {}
        for row in rows:
            normalized = key(row["object"])
            result[normalized] = result.get(normalized, 0) + row["text_bytes"]
        return result

    before, after = aggregate(base), aggregate(candidate)
    names = sorted(set(before) | set(after))
    deltas = [{"object": name, "base_text_bytes": before.get(name, 0),
               "candidate_text_bytes": after.get(name, 0),
               "delta_bytes": after.get(name, 0) - before.get(name, 0)}
              for name in names]
    return sorted(deltas, key=lambda item: abs(item["delta_bytes"]), reverse=True)


def attribution_matches(forensic_hash: str, authoritative_hash: str,
                        forensic_text: int, authoritative_text: int,
                        map_digest: str | None) -> bool:
    return (bool(map_digest) and forensic_hash == authoritative_hash and
            forensic_text == authoritative_text)


def forensic_profile_matches(authoritative_path: Path, forensic_path: Path,
                             map_path: Path) -> tuple[bool, list[str]]:
    authoritative = json.loads(authoritative_path.read_text(encoding="utf-8"))
    forensic = json.loads(forensic_path.read_text(encoding="utf-8"))
    authoritative.pop("source_sha", None)
    forensic.pop("source_sha", None)
    expected_map_flag = f"-Wl,-Map={map_path}"
    found_map_flags = 0
    for entry in forensic.get("effective_link_arguments", []):
        parameters = entry.get("parameters", [])
        kept = []
        for parameter in parameters:
            if parameter == expected_map_flag:
                found_map_flags += 1
            elif "-Wl,-Map=" in parameter or parameter == "-Map" or parameter.startswith("-Map="):
                return False, ["unexpected or split linker map argument"]
            else:
                kept.append(parameter)
        entry["parameters"] = kept
    if found_map_flags != 1:
        return False, [f"expected exactly one linker map flag, found {found_map_flags}"]
    differences = sorted(key for key in set(authoritative) | set(forensic)
                         if authoritative.get(key) != forensic.get(key))
    return not differences, differences


def collect(args: argparse.Namespace) -> int:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "schema_version": 1,
        "repository": args.repository,
        "pr_number": args.pr_number,
        "base_sha": args.base_sha,
        "candidate_sha": args.candidate_sha,
        "workflow_ref": args.workflow_ref,
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
                }
            report["attribution"] = attribution
            report["per_object_text_deltas"] = object_text_deltas(
                attribution["base"]["objects"], attribution["candidate"]["objects"])
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
        report["status"] = "complete" if report["attribution_status"] == "usable" else "incomplete"
    except Exception as exc:  # noqa: BLE001 - preserve partial evidence for the always-upload step.
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        write_json(report_path, report)
        print(report["error"], file=sys.stderr)
        return 1
    write_json(report_path, report)
    return 0 if report["status"] == "complete" else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pr-number", required=True, type=int)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--workflow-ref", required=True)
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--output", default="size-attribution")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(collect(parse_args()))
