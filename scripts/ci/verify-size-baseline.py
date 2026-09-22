#!/usr/bin/env python3
"""Authorize numeric baseline changes only from trusted main CI evidence."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
import zipfile

API = "https://api.github.com"

def canonical_hash(value):
    profile = dict(value); profile.pop("source_sha", None)
    return hashlib.sha256(json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

def request(url, token, binary=False):
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json",
                                               "Authorization": f"Bearer {token}",
                                               "X-GitHub-Api-Version": "2022-11-28"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read() if binary else json.loads(response.read())

def fail(message):
    raise ValueError(message)

def authorize(repo, base_sha, candidate_sha, base_value, candidate_value, provenance_path, token):
    base_value = int(base_value); candidate_value = int(candidate_value)
    if base_value == candidate_value:
        return "unchanged"
    p = json.loads(Path(provenance_path).read_text(encoding="utf-8"))
    if p.get("schema_version") != 1 or p.get("status") != "trusted-ci-artifact":
        fail("numeric baseline update requires a trusted-ci-artifact provenance record")
    if not token:
        fail("trusted baseline artifact verification requires read-only Actions API access")
    if p.get("baseline_bytes") != candidate_value:
        fail("provenance baseline value does not match candidate baseline")
    source_sha = p.get("source_sha", "")
    if not source_sha or source_sha in (base_sha, candidate_sha):
        fail("measurement source must be an earlier eligible main revision")
    ancestor = subprocess.run(["git", "merge-base", "--is-ancestor", source_sha, base_sha],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if ancestor.returncode != 0:
        fail("measurement source is not an ancestor of the event base")
    if p.get("repository") != repo:
        fail("provenance repository does not match this workflow")
    run_id = str(p.get("run_id", "")); artifact_id = str(p.get("artifact_id", ""))
    if not run_id.isdigit() or not artifact_id.isdigit():
        fail("provenance run_id and artifact_id must be numeric")
    run = request(f"{API}/repos/{repo}/actions/runs/{run_id}", token)
    if (run.get("path") != ".github/workflows/ci-main.yml" or run.get("event") != "push" or
            run.get("head_branch") != "main" or run.get("conclusion") != "success" or
            run.get("head_sha") != source_sha):
        fail("artifact must come from a successful designated main workflow run for its source SHA")
    artifacts = request(f"{API}/repos/{repo}/actions/runs/{run_id}/artifacts", token).get("artifacts", [])
    artifact = next((a for a in artifacts if str(a.get("id")) == artifact_id), None)
    if not artifact or artifact.get("expired") or artifact.get("name") != "wirelog-size-monitor-ubuntu-latest":
        fail("trusted size measurement artifact is missing, expired, or is not the canonical x86 production artifact")
    digest = artifact.get("digest", "").removeprefix("sha256:")
    if digest != p.get("artifact_sha256"):
        fail("artifact digest does not match provenance")
    archive = request(artifact["archive_download_url"], token, binary=True)
    if hashlib.sha256(archive).hexdigest() != digest:
        fail("downloaded artifact digest mismatch")
    with zipfile.ZipFile(io.BytesIO(archive)) as zf:
        candidates = [name for name in zf.namelist() if name.endswith("size-report.json")]
        if len(candidates) != 1:
            fail("artifact must contain exactly one size-report.json")
        report = json.loads(zf.read(candidates[0]))
    if report.get("status") not in ("within-budget", "over-budget"):
        fail("main artifact does not contain a completed valid size measurement")
    if report.get("runner_os") != "ubuntu-latest" or report.get("compiler") != "gcc":
        fail("main artifact does not identify the canonical ubuntu-latest GCC profile")
    if report.get("commit_sha") != source_sha or report.get("measured_bytes") != candidate_value:
        fail("artifact measurement source or byte count does not match provenance")
    profile = report.get("profile")
    if not isinstance(profile, dict) or profile.get("source_sha") != source_sha or \
            canonical_hash(profile) != p.get("profile_sha256"):
        fail("artifact profile digest does not match provenance")
    if report.get("library_sha256") != p.get("library_sha256"):
        fail("artifact library digest does not match provenance")
    # Rebuild the already-existing source revision using this runner's production
    # toolchain and require byte-for-byte evidence identity before authorization.
    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="wirelog-baseline-reproduce-") as temp:
        source = Path(temp) / "source"; build = Path(temp) / "build"
        source.mkdir()
        tar_data = subprocess.check_output(["git", "archive", "--format=tar", source_sha], cwd=root)
        with tarfile.open(fileobj=io.BytesIO(tar_data), mode="r:") as archive_tar:
            archive_tar.extractall(source, filter="data")
        subprocess.run(["meson", "setup", str(build), str(source), "-Dtests=true", "-DmbedTLS=disabled"], check=True)
        subprocess.run(["meson", "compile", "-C", str(build), "wirelog"], check=True)
        profile_path = Path(temp) / "profile.json"
        subprocess.run([sys.executable, str(root / "scripts/ci/size-profile.py"), "capture",
                        "--build-dir", str(build), "--source-dir", str(source),
                        "--source-sha", source_sha, "--output", str(profile_path)], check=True)
        reproduced_profile = json.loads(profile_path.read_text(encoding="utf-8"))
        if canonical_hash(reproduced_profile) != p["profile_sha256"]:
            fail("recorded toolchain/profile is unavailable or differs from reproducible source")
        library = build / "libwirelog.so"
        size_output = subprocess.check_output(["size", "--format=sysv", str(library)], text=True)
        sizes = [int(line.split()[1]) for line in size_output.splitlines()
                 if line.split() and line.split()[0] == ".text"]
        if len(sizes) != 1 or sizes[0] != candidate_value:
            fail("reproduced baseline bytes differ from trusted CI measurement")
        library_digest = hashlib.sha256(library.read_bytes()).hexdigest()
        if library_digest != p["library_sha256"]:
            fail("reproduced library digest differs from trusted CI measurement")
    return "trusted reproducible measurement"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repository", required=True); ap.add_argument("--base-sha", required=True)
    ap.add_argument("--candidate-sha", required=True); ap.add_argument("--base-value", required=True)
    ap.add_argument("--candidate-value", required=True); ap.add_argument("--provenance-file", required=True)
    ap.add_argument("--token", default=os.environ.get("GH_TOKEN", ""))
    a = ap.parse_args()
    try:
        verdict = authorize(a.repository, a.base_sha, a.candidate_sha, a.base_value,
                            a.candidate_value, a.provenance_file, a.token)
        print(f"baseline provenance: {verdict}")
        return 0
    except (OSError, ValueError, KeyError, urllib.error.URLError, json.JSONDecodeError,
            subprocess.CalledProcessError, zipfile.BadZipFile) as exc:
        print(f"verify-size-baseline: {exc}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
