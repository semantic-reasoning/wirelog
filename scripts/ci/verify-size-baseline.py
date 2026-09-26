#!/usr/bin/env python3
"""Verify trusted main CI evidence and one pinned, reviewed PR baseline reset."""
import argparse
import hashlib
import io
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile

API = "https://api.github.com"

# One maintainer-authorized reset to the reviewed PR #1959 measurement.
# This record deliberately provides no general PR-baseline eligibility.
REVIEWED_PR_BASELINE = {
    "schema_version": 1, "status": "trusted-reviewed-pr-size-job",
    "authorization_id": "pr-1959-reviewed-head-34eeb23c",
    "repository": "semantic-reasoning/wirelog", "pr_number": 1959,
    "baseline_bytes": 395540,
    "source_sha": "34eeb23c201ba443a3fca5a6d38aee6365d7a25b",
    "base_sha": "8e01e3cf7f406caa6e09ef100b480adbec8d35e9",
    "tested_merge_sha": "0b3866eef0cb8eb577041eafa6bddef3278f36ba",
    "run_id": 36214645977, "job_id": 108328340847,
    "profile_sha256": "cd6cc2f2c54520ba56c8efc0241b17d722d00c8b569cb6c6bb38f5e10d5e1508",
    "job_log_sha256": "92a8693156963c73b8ddc1e296527c0e12ec1a7cf2973247a1e9189d521759c4",
}

def canonical_hash(value):
    profile = dict(value); profile.pop("source_sha", None)
    return hashlib.sha256(json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

def measurement_is_reproducible(report):
    """Accept normal monitor results, or a measured library from a later full-build failure.

    The latter is only evidence when the canonical library, size, and profile
    were captured and the verifier can independently reproduce the profile and
    the measured `.text` section size.
    """
    if report.get("status") in ("within-budget", "over-budget"):
        return True
    steps = report.get("workflow_steps", {})
    return (report.get("status") == "monitoring-error" and
            report.get("phase") == "build" and
            report.get("error") == "production build step failed: build" and
            steps.get("configure") == "success" and steps.get("build") == "failure" and
            isinstance(report.get("measured_bytes"), int) and report["measured_bytes"] > 0 and
            isinstance(report.get("profile"), dict) and
            isinstance(report.get("profile_sha256"), str) and
            len(report["profile_sha256"]) == 64 and
            isinstance(report.get("library_sha256"), str) and
            len(report["library_sha256"]) == 64)

def checked_https_url(url, api=False):
    parts = urllib.parse.urlsplit(url)
    if (parts.scheme != "https" or not parts.hostname or parts.username is not None or
            parts.password is not None or (api and parts.netloc != "api.github.com")):
        fail("artifact request requires an absolute HTTPS URL without userinfo")


class SafeArtifactRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        checked_https_url(headers.get("location") or headers.get("uri") or newurl)
        checked_https_url(newurl)
        # A fresh request prevents the GitHub API token and headers from
        # reaching the short-lived artifact storage URL.
        return urllib.request.Request(newurl)


class RejectRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request(url, token, binary=False, authenticated=True):
    checked_https_url(url, api=True)
    headers = {"Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28"}
    if authenticated:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    opener = urllib.request.build_opener(SafeArtifactRedirect() if binary else RejectRedirect())
    with opener.open(req, timeout=30) as response:
        checked_https_url(response.geturl())
        body = response.read()
    return body if binary else json.loads(body)

def fail(message):
    raise ValueError(message)

def reviewed_pr_reference_tree(base_sha, pinned_base, source):
    """Apply the immutable reviewed source to the current, descendant base."""
    for sha in (pinned_base, source):
        if subprocess.run(["git", "cat-file", "-e", f"{sha}^{{commit}}"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
            subprocess.run(["git", "fetch", "--no-tags", "origin", sha], check=True,
                           stdout=subprocess.DEVNULL)
        resolved = subprocess.check_output(["git", "rev-parse", f"{sha}^{{commit}}"],
                                           text=True, encoding="utf-8").strip()
        if resolved != sha:
            fail("reviewed PR pinned revision is not an exact commit")
    for older, newer in ((pinned_base, base_sha), (pinned_base, source)):
        if subprocess.run(["git", "merge-base", "--is-ancestor", older, newer],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
            fail("reviewed PR baseline revision ancestry differs")
    merged = subprocess.run(["git", "merge-tree", "--write-tree", base_sha, source],
                            capture_output=True, text=True, encoding="utf-8")
    tree = merged.stdout.strip()
    if merged.returncode or not re.fullmatch(r"[0-9a-f]{40}", tree):
        fail("reviewed PR source cannot be cleanly applied to the current base")
    if subprocess.check_output(["git", "cat-file", "-t", tree],
                               text=True, encoding="utf-8").strip() != "tree":
        fail("reviewed PR rebased reference is not a tree")
    return tree

def authorize_reviewed_pr(repo, base_sha, candidate_sha, candidate_value, p, token):
    if p != REVIEWED_PR_BASELINE or repo != p["repository"] or not token:
        fail("reviewed PR baseline requires the exact approved record and Actions API access")
    source = p["source_sha"]
    if (candidate_value != p["baseline_bytes"] or
            candidate_sha in (source, p["tested_merge_sha"])):
        fail("reviewed PR baseline source, event base, or byte count differs")
    reference = reviewed_pr_reference_tree(base_sha, p["base_sha"], source)
    changed = subprocess.check_output(["git", "diff", "--name-only", reference,
                                      candidate_sha], text=True, encoding="utf-8").splitlines()
    allowed = {"tests/baseline_size.txt", "tests/baseline_size.provenance.json",
               "scripts/ci/verify-size-baseline.py",
               "scripts/ci/test-size-baseline-provenance.py",
               "tests/test_consolidate_kway_merge.c",
               "tests/test_memory_admission_relation.c",
               "tests/test_wirelog_easy.c", "tests/test_wirelog_advanced.c",
               "tests/test_memory_admission_join.c", "tests/test_join_batch_resume.c",
               "tests/test_diff_join.c", "tests/test_join_arrangement.c",
               "wirelog/columnar/relation.c",
               "wirelog/columnar/join.c",
               "wirelog/columnar/join_batch.c", "wirelog/columnar/diff_join_batch.c"}
    if not changed or not set(changed).issubset(allowed):
        fail("reviewed PR rebaseline has changes outside its reviewed repair paths")
    api = f"{API}/repos/{repo}"
    merge = request(f"{api}/git/commits/{p['tested_merge_sha']}", token)
    tree = subprocess.check_output(["git", "rev-parse", f"{source}^{{tree}}"],
                                   text=True, encoding="utf-8").strip()
    if (merge.get("tree", {}).get("sha") != tree or
            [v.get("sha") for v in merge.get("parents", [])] != [p["base_sha"], source]):
        fail("measured PR merge is not the reviewed source tree on its pinned base")
    parents = subprocess.check_output(["git", "rev-list", "--parents", "-n", "1",
                                       candidate_sha], text=True, encoding="utf-8").split()
    if len(parents) != 3 or parents[1] != base_sha:
        fail("reviewed PR rebaseline requires an exact event merge")
    # Public metadata needs no token; the CI token has Actions/contents scopes.
    live = request(f"{api}/pulls/{p['pr_number']}", token, authenticated=False)
    if (live.get("state") != "open" or live.get("number") != p["pr_number"] or
            live.get("base", {}).get("ref") != "main" or
            live.get("base", {}).get("sha") != base_sha or
            live.get("head", {}).get("sha") != parents[2] or
            any(live.get(side, {}).get("repo", {}).get("full_name") != repo
                for side in ("base", "head"))):
        fail("reviewed PR rebaseline does not match the live internal PR")
    run = request(f"{api}/actions/runs/{p['run_id']}", token)
    if (run.get("path") != ".github/workflows/ci-pr.yml" or
            run.get("event") != "pull_request" or run.get("head_sha") != source or
            not any(pr.get("number") == p["pr_number"] and
                    pr.get("base", {}).get("sha") in (p["base_sha"], base_sha) and
                    pr.get("head", {}).get("sha") == parents[2]
                    for pr in run.get("pull_requests", []))):
        fail("reviewed PR measurement run identity differs")
    job = request(f"{api}/actions/jobs/{p['job_id']}", token)
    steps = {v.get("name"): v.get("conclusion") for v in job.get("steps", [])}
    if (job.get("run_id") != p["run_id"] or job.get("head_sha") != source or
            job.get("name") != "Build / ubuntu-latest / gcc" or
            job.get("conclusion") != "failure" or steps.get("Configure") != "success" or
            steps.get("Build production library for early size gate") != "success" or
            steps.get("Check binary size") != "failure" or
            any(v == "failure" and k != "Check binary size" for k, v in steps.items())):
        fail("reviewed PR job did not build production with a size-only failure")
    log = request(f"{api}/actions/jobs/{p['job_id']}/logs", token, binary=True)
    if hashlib.sha256(log).hexdigest() != p["job_log_sha256"]:
        fail("reviewed PR measurement log digest differs")
    clean = re.sub(r"(?m)^\d{4}-\d{2}-\d{2}T[^ ]+Z ", "", log.decode("utf-8"))
    for key, value in (("BASE_SHA", p["base_sha"]), ("TESTED_SHA", p["tested_merge_sha"]),
                       ("PR_HEAD_SHA", source)):
        if f"  {key}: {value}\n" not in clean:
            fail("reviewed PR measurement environment differs")
    match = re.search(r'(?ms)^\{\n  "allowed_head_bytes":.*?^\}', clean)
    if not match:
        fail("reviewed PR measurement policy report is missing")
    report = json.loads(match.group())
    expected = {"base_sha": p["base_sha"], "head_sha": p["tested_merge_sha"],
                "base_bytes": 389203, "head_bytes": 395540, "baseline_bytes": 387391,
                "budget_bytes": 5120, "status": "over-budget",
                "base_profile": p["profile_sha256"], "head_profile": p["profile_sha256"]}
    if any(report.get(k) != v for k, v in expected.items()):
        fail("reviewed PR measurement profile or policy values differ")
    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="wirelog-reviewed-baseline-") as temp:
        source_dir = Path(temp) / "source"; build = Path(temp) / "build"
        source_dir.mkdir()
        data = subprocess.check_output(["git", "archive", "--format=tar", source], cwd=root)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            archive.extractall(source_dir, filter="data")
        env = os.environ.copy(); env.update(CC="cc", CXX="c++")
        subprocess.run(["meson", "setup", str(build), str(source_dir),
                        "-Dtests=true", "-DmbedTLS=disabled"], check=True, env=env)
        subprocess.run(["meson", "compile", "-C", str(build), "wirelog"], check=True)
        profile_path = Path(temp) / "profile.json"
        subprocess.run([sys.executable, str(root / "scripts/ci/size-profile.py"), "capture",
                        "--build-dir", str(build), "--source-dir", str(source_dir),
                        "--source-sha", source, "--output", str(profile_path)], check=True)
        if canonical_hash(json.loads(profile_path.read_text(encoding="utf-8"))) != p["profile_sha256"]:
            fail("reviewed PR canonical measurement profile cannot be reproduced")
        output = subprocess.check_output(["size", "--format=sysv", str(build / "libwirelog.so")],
                                         text=True, encoding="utf-8")
        sizes = [int(line.split()[1]) for line in output.splitlines()
                 if line.split() and line.split()[0] == ".text"]
        if sizes != [candidate_value]:
            fail("reviewed PR baseline bytes cannot be reproduced")
    return "one-time reviewed PR #1959 measurement reproduced"

def authorize(repo, base_sha, candidate_sha, base_value, candidate_value, provenance_path, token):
    base_value = int(base_value); candidate_value = int(candidate_value)
    if base_value == candidate_value:
        return "unchanged"
    p = json.loads(Path(provenance_path).read_text(encoding="utf-8"))
    if p.get("status") == "trusted-reviewed-pr-size-job":
        return authorize_reviewed_pr(repo, base_sha, candidate_sha, candidate_value, p, token)
    if p.get("schema_version") != 1 or p.get("status") != "trusted-ci-artifact":
        fail("numeric baseline update requires a trusted-ci-artifact provenance record")
    if not token:
        fail("trusted baseline artifact verification requires read-only Actions API access")
    if p.get("baseline_bytes") != candidate_value:
        fail("provenance baseline value does not match candidate baseline")
    source_sha = p.get("source_sha", "")
    if not source_sha or source_sha == candidate_sha:
        fail("measurement source must be an eligible main revision distinct from the candidate")
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
    if not measurement_is_reproducible(report):
        fail("main artifact does not contain an eligible, reproducible size measurement")
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
        build_tools = profile.get("tools", {}).get("build", {})
        compiler_env = os.environ.copy()
        for variable, language in (("CC", "c"), ("CXX", "cpp")):
            exelist = build_tools.get(language, {}).get("exelist")
            if not isinstance(exelist, list) or not exelist or not all(
                    isinstance(item, str) and item for item in exelist):
                fail(f"artifact profile has no usable {variable} compiler command")
            compiler_env[variable] = " ".join(exelist)
        subprocess.run(["meson", "setup", str(build), str(source), "-Dtests=true", "-DmbedTLS=disabled"],
                       check=True, env=compiler_env)
        subprocess.run(["meson", "compile", "-C", str(build), "wirelog"], check=True)
        profile_path = Path(temp) / "profile.json"
        subprocess.run([sys.executable, str(root / "scripts/ci/size-profile.py"), "capture",
                        "--build-dir", str(build), "--source-dir", str(source),
                        "--source-sha", source_sha, "--output", str(profile_path)], check=True)
        reproduced_profile = json.loads(profile_path.read_text(encoding="utf-8"))
        if canonical_hash(reproduced_profile) != p["profile_sha256"]:
            recorded = dict(profile); recorded.pop("source_sha", None)
            reproduced = dict(reproduced_profile); reproduced.pop("source_sha", None)
            changed = sorted(key for key in set(recorded) | set(reproduced)
                             if recorded.get(key) != reproduced.get(key))
            fail("recorded toolchain/profile is unavailable or differs from reproducible source "
                 f"(different profile sections: {', '.join(changed) or 'unknown'})")
        library = build / "libwirelog.so"
        size_output = subprocess.check_output(["size", "--format=sysv", str(library)], text=True, encoding="utf-8")
        sizes = [int(line.split()[1]) for line in size_output.splitlines()
                 if line.split() and line.split()[0] == ".text"]
        if len(sizes) != 1 or sizes[0] != candidate_value:
            fail("reproduced baseline bytes differ from trusted CI measurement")
    return ("trusted reproducible measurement" if report.get("status") != "monitoring-error"
            else "trusted reproducible production-library size measurement (full build failed)")

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
