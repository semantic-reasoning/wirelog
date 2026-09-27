#!/usr/bin/env python3
"""Offline fixtures for trusted main-artifact baseline authorization."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import sys
import tempfile
import zipfile
from unittest import mock
from types import SimpleNamespace

path = Path(__file__).with_name("verify-size-baseline.py")
spec = importlib.util.spec_from_file_location("verify_size_baseline", path)
mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)

def digest_profile(profile):
    return mod.canonical_hash(profile)

def check(name, fn, expected_error=None):
    try:
        fn()
    except Exception as exc:
        if expected_error and expected_error in str(exc):
            print(f"ok: {name}"); return
        raise AssertionError(f"{name}: unexpected error {exc}") from exc
    if expected_error:
        raise AssertionError(f"{name}: expected rejection containing {expected_error!r}")
    print(f"ok: {name}")

def _write_tmp(value):
    temp=tempfile.NamedTemporaryFile(mode="w",encoding="utf-8",delete=False)
    json.dump(value,temp); temp.close(); return temp.name

profile={"schema_version":1,"source_sha":"source123","platform":{"system":"Linux"},
         "options":{"buildtype":"release"},"tools":{"build":{"c":{"id":"gcc","exelist":["gcc"]},
                                                                         "cpp":{"id":"gcc","exelist":["c++"]}}},
         "effective_build_commands":["cc -O3"]}
library=b"fixture library bytes"
library_digest=hashlib.sha256(library).hexdigest()
report={"schema_version":1,"status":"within-budget","commit_sha":"source123",
        "measured_bytes":200,"runner_os":"ubuntu-latest","compiler":"gcc",
        "profile":profile,"profile_sha256":digest_profile(profile),
        "library_sha256":library_digest}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w") as z:
    z.writestr("size-monitor/size-report.json",json.dumps(report))
artifact_zip=buf.getvalue(); artifact_digest=hashlib.sha256(artifact_zip).hexdigest()

def fixture_provenance(**changes):
    p={"schema_version":1,"status":"trusted-ci-artifact","repository":"owner/repo",
       "baseline_bytes":200,"source_sha":"source123","run_id":11,"artifact_id":22,
       "artifact_sha256":artifact_digest,"profile_sha256":digest_profile(profile),
       "library_sha256":library_digest}
    p.update(changes); return p

def exercise(provenance, corrupt=None, base_sha="base456", candidate_sha="candidate789"):
    with tempfile.TemporaryDirectory() as temp:
        p=Path(temp)/"provenance.json"; p.write_text(json.dumps(provenance), encoding="utf-8")
        run={"path":".github/workflows/ci-main.yml","event":"push","head_branch":"main",
             "conclusion":"success","head_sha":"source123"}
        artifact={"id":22,"expired":False,"name":"wirelog-size-monitor-ubuntu-latest",
                  "digest":"sha256:"+artifact_digest,"archive_download_url":"fixture://zip"}
        if corrupt=="wrong-workflow": run["path"]=".github/workflows/other.yml"
        if corrupt=="arm-artifact": artifact["name"]="wirelog-size-monitor-ubuntu-24.04-arm"
        if corrupt=="expired": artifact["expired"]=True
        if corrupt=="digest": artifact["digest"]="sha256:"+"0"*64
        response_report=dict(report)
        if corrupt == "build-failed":
            response_report.update({"status":"monitoring-error", "phase":"build",
                                    "error":"production build step failed: build",
                                    "workflow_steps":{"configure":"success","build":"failure","test":"skipped"}})
        if corrupt == "bad-build-failure":
            response_report.update({"status":"monitoring-error", "phase":"build",
                                    "error":"production build step failed: configure",
                                    "workflow_steps":{"configure":"failure","build":"failure","test":"skipped"}})
        if corrupt == "missing-library-size":
            response_report.update({"status":"monitoring-error", "phase":"build",
                                    "error":"production build step failed: build", "measured_bytes":None,
                                    "workflow_steps":{"configure":"success","build":"failure","test":"skipped"}})
        if corrupt=="source": response_report["commit_sha"]="other123"
        if corrupt=="bytes": response_report["measured_bytes"]=201
        if corrupt=="profile": response_report["profile"]={"schema_version":1,"changed":True}
        if corrupt=="library": response_report["library_sha256"]="0"*64
        body=artifact_zip
        if corrupt in ("source","bytes","profile","library","build-failed","bad-build-failure","missing-library-size"):
            out=io.BytesIO()
            with zipfile.ZipFile(out,"w") as z: z.writestr("size-report.json",json.dumps(response_report))
            body=out.getvalue()
            # For a correct artifact checksum path, align API digest with the rebuilt fixture.
            artifact["digest"]="sha256:"+hashlib.sha256(body).hexdigest()
            provenance["artifact_sha256"]=hashlib.sha256(body).hexdigest()
            p.write_text(json.dumps(provenance), encoding="utf-8")
        def request(url, token, binary=False):
            if url.endswith("/actions/runs/11"): return run
            if url.endswith("/actions/runs/11/artifacts"): return {"artifacts":[] if corrupt=="missing" else [artifact]}
            return body
        def subprocess_run(cmd,*args,**kwargs):
            if isinstance(cmd,list) and cmd[:3]==["git","merge-base","--is-ancestor"]:
                return SimpleNamespace(returncode=1 if corrupt=="unrelated" else 0)
            if isinstance(cmd,list) and cmd[:2]==["meson","compile"]:
                build=Path(cmd[cmd.index("-C")+1]); build.mkdir(parents=True,exist_ok=True)
                rebuilt=b"same .text, different non-text build bytes" if corrupt=="rebuild-library-hash" else library
                (build/"libwirelog.so").write_bytes(rebuilt)
            if isinstance(cmd,list) and "size-profile.py" in " ".join(map(str,cmd)):
                output=Path(cmd[cmd.index("--output")+1]); output.write_text(json.dumps(profile), encoding="utf-8")
            return SimpleNamespace(returncode=0)
        def subprocess_output(cmd,*args,**kwargs):
            if cmd[:3]==["git","archive","--format=tar"]:
                archive=io.BytesIO()
                with __import__("tarfile").open(fileobj=archive,mode="w"):
                    pass
                return archive.getvalue()
            if cmd and cmd[0]=="size": return ".text 200 200\n"
            raise AssertionError(f"unexpected command {cmd}")
        with mock.patch.object(mod,"request",request), \
             mock.patch.object(mod.subprocess,"run",subprocess_run), \
             mock.patch.object(mod.subprocess,"check_output",subprocess_output):
            return mod.authorize("owner/repo",base_sha,candidate_sha,100,200,p,"fixture-token")

def exercise_binary_download(location="https://storage.example/artifact.zip", final_url=None):
    api_url = "https://api.github.com/repos/owner/repo/actions/artifacts/22/zip"
    final_url = final_url or location

    class Response(io.BytesIO):
        def geturl(self):
            return final_url

    class Opener:
        def __init__(self, handler):
            self.handler = handler

        def open(self, req, timeout):
            assert timeout == 30
            assert req.full_url == api_url
            assert req.get_header("Authorization") == "Bearer fixture-token"
            assert req.get_header("X-github-api-version") == "2022-11-28"
            resolved = ("https://storage.example/artifact.zip" if location.startswith("/")
                        else location)
            redirected = self.handler.redirect_request(
                req, None, 302, "Found", {"location": location}, resolved)
            assert redirected.headers == {}
            return Response(b"fixture artifact bytes")

    with mock.patch.object(mod.urllib.request, "build_opener",
                           side_effect=lambda handler: Opener(handler)), \
         mock.patch.object(mod.subprocess, "check_output",
                           side_effect=AssertionError("gh must not run")):
        assert mod.request(api_url, "fixture-token", binary=True) == b"fixture artifact bytes"

check("artifact download authenticates API and strips credentials on redirect",
      exercise_binary_download)
for bad_url in ("http://storage.example/artifact.zip", "/relative.zip", "https:///artifact.zip",
                "https://user@storage.example/artifact.zip"):
    check(f"artifact redirect rejects {bad_url}",
          lambda url=bad_url: exercise_binary_download(url), "absolute HTTPS URL")
for bad_url in ("http://api.github.com/artifact.zip", "/relative.zip", "https:///artifact.zip",
                "https://user@api.github.com/artifact.zip", "https://example.com/artifact.zip"):
    check(f"artifact API URL rejects {bad_url}",
          lambda url=bad_url: mod.request(url, "fixture-token", binary=True), "absolute HTTPS URL")
check("artifact response rejects a downgraded final URL",
      lambda: exercise_binary_download(final_url="http://storage.example/artifact.zip"),
      "absolute HTTPS URL")

def assert_json_redirect_rejected():
    assert mod.RejectRedirect().redirect_request(
        None, None, 302, "Found", {}, "https://storage.example/data") is None

check("JSON API rejects redirects", assert_json_redirect_rejected)

reviewed1961_paths = {
    "docs/BINARY_SIZE.md", "scripts/ci/verify-size-baseline.py",
    "scripts/ci/test-size-baseline-provenance.py",
    "tests/baseline_size.txt", "tests/baseline_size.provenance.json",
}
check("PR 1961 exception has only its five reviewed repair paths",
      lambda: (_ for _ in ()).throw(AssertionError("path scope changed"))
      if mod.reviewed_pr_repair_paths(mod.REVIEWED_PR1961_BASELINE) != reviewed1961_paths else None)
check("PR 1961 keeps base and measured-source baseline values distinct",
      lambda: (_ for _ in ()).throw(AssertionError("pinned values changed"))
      if (mod.REVIEWED_PR1961_BASELINE["base_repository_baseline_bytes"],
          mod.REVIEWED_PR1961_BASELINE["source_baseline_bytes"],
          mod.REVIEWED_PR1961_BASELINE["budget_bytes"]) != (395540, 397579, 5120) else None)
check("legacy PR 1959 measurement remains a separate compatible record",
      lambda: (_ for _ in ()).throw(AssertionError("legacy record changed"))
      if (mod.REVIEWED_PR_BASELINE["pr_number"] != 1959 or
          mod.reviewed_pr_measurement(mod.REVIEWED_PR_BASELINE) != {
              "base_repository_baseline_bytes": 387391, "source_baseline_bytes": 387391,
              "measured_base_bytes": 389203, "reported_baseline_bytes": 387391,
              "budget_bytes": 5120, "measurement_status": "over-budget"}) else None)
altered_1961_record = dict(mod.REVIEWED_PR1961_BASELINE, baseline_bytes=404283)
check("altered PR 1961 measurement record is rejected",
      lambda: mod.authorize_reviewed_pr("semantic-reasoning/wirelog", "base", "candidate",
          395540, 404283, altered_1961_record, "token"),
      "exact approved record")

check("unchanged legacy baseline remains allowed",
      lambda: mod.authorize("owner/repo","base456","candidate789",354887,354887,"missing",""))
check("legacy provenance cannot authorize numeric inflation",
      lambda: mod.authorize("owner/repo","base456","candidate789",354887,500000,
                            _write_tmp({"schema_version":1,"status":"legacy-unverified",
                                        "baseline_bytes":354887}),"token"),
      "trusted-ci-artifact")
check("candidate-authored URL without token is not trust evidence",
      lambda: mod.authorize("owner/repo","base456","candidate789",100,200,
                            _write_tmp(fixture_provenance(url="https://example.invalid/fake")),""),
      "read-only Actions API")
check("valid main artifact and reproducible source authorize the measurement",
      lambda: exercise(fixture_provenance()))
check("exact event-base main artifact authorizes the measurement",
      lambda: exercise(fixture_provenance(), base_sha="source123"))
check("candidate source cannot authorize its own baseline",
      lambda: exercise(fixture_provenance(), candidate_sha="source123"),
      "distinct from the candidate")
check("failed full build authorizes only its fully measured reproducible library",
      lambda: exercise(fixture_provenance(), "build-failed"))
check("rebuild may differ outside .text while reproducing the authorized size",
      lambda: exercise(fixture_provenance(), "rebuild-library-hash"))
for case, message in [("wrong-workflow","designated main workflow"),
                      ("arm-artifact","canonical x86 production artifact"),("expired","missing, expired"),
                      ("missing","missing, expired"),("digest","artifact digest"),
                      ("source","source or byte count"),("bytes","source or byte count"),
                      ("profile","profile digest"),("library","library digest"),
                      ("bad-build-failure","eligible, reproducible size measurement"),
                      ("missing-library-size","eligible, reproducible size measurement"),
                      ("unrelated","not an ancestor")]:
    check(f"{case} artifact/source/provenance mismatch rejected",
          lambda c=case: exercise(fixture_provenance(),c),message)

# Real Git histories exercise the pinned-source reference after rebase.
with tempfile.TemporaryDirectory(prefix="reviewed-rebase-") as history:
    previous = os.getcwd()
    try:
        os.chdir(history)
        def git(*args):
            return subprocess.check_output(["git", *args], text=True,
                                           encoding="utf-8", stderr=subprocess.DEVNULL).strip()
        git("init", "-q")
        git("config", "user.name", "Baseline fixture")
        git("config", "user.email", "baseline@example.invalid")
        Path("tests").mkdir()
        Path("tests/baseline_size.txt").write_text("395540\n", encoding="ascii")
        Path("reviewed.c").write_text("base\n", encoding="utf-8")
        git("add", "."); git("commit", "-qm", "base")
        pinned_base = git("rev-parse", "HEAD")
        Path("reviewed.c").write_text("reviewed\n", encoding="utf-8")
        Path("tests/baseline_size.txt").write_text("397579\n", encoding="ascii")
        git("commit", "-qam", "reviewed source")
        source = git("rev-parse", "HEAD")
        git("checkout", "-q", "--detach", pinned_base)
        Path("main.c").write_text("advanced main\n", encoding="utf-8")
        git("add", "."); git("commit", "-qm", "advance main")
        current_base = git("rev-parse", "HEAD")
        git("cherry-pick", source)
        rebased_source = git("rev-parse", "HEAD")
        reference = mod.reviewed_pr_reference_tree(current_base, pinned_base, source)
        check("clean rebase preserves immutable reviewed reference",
              lambda: (_ for _ in ()).throw(AssertionError("tree mismatch"))
              if reference != git("rev-parse", "HEAD^{tree}") else None)
        record = dict(mod.REVIEWED_PR1961_BASELINE, source_sha=source,
                      base_sha=pinned_base, tested_merge_sha="0" * 40)
        Path("scripts/ci").mkdir(parents=True)
        Path("scripts/ci/verify-size-baseline.py").write_text("repair\n", encoding="utf-8")
        Path("tests/baseline_size.txt").write_text("404282\n", encoding="ascii")
        Path("docs").mkdir()
        Path("docs/BINARY_SIZE.md").write_text("reviewed reset\n", encoding="utf-8")
        Path("tests/baseline_size.provenance.json").write_text("{}\n", encoding="utf-8")
        git("add", "."); git("commit", "-qm", "authorized repair")
        repair = git("rev-parse", "HEAD")
        tree = git("rev-parse", "HEAD^{tree}")
        event_merge = git("commit-tree", tree, "-p", current_base, "-p", repair,
                          "-m", "event merge")
        historical_report = {
            "allowed_head_bytes": 402699,
            "base_sha": pinned_base, "head_sha": record["tested_merge_sha"],
            "base_bytes": 397579, "head_bytes": 404282, "baseline_bytes": 397579,
            "delta_from_baseline_bytes": 6703,
            "budget_bytes": 5120, "status": "over-budget",
            "base_profile": record["profile_sha256"], "head_profile": record["profile_sha256"],
        }
        historical_log = (f"  BASE_SHA: {pinned_base}\n"
            f"  TESTED_SHA: {record['tested_merge_sha']}\n"
            f"  PR_HEAD_SHA: {source}\n"
            + json.dumps(historical_report, indent=2) + "\n").encode("utf-8")
        record["job_log_sha256"] = hashlib.sha256(historical_log).hexdigest()
        run_overrides = {}
        def reviewed_request(url, token, binary=False, authenticated=True):
            if binary:
                return historical_log
            if "/git/commits/" in url:
                return {"tree": {"sha": git("rev-parse", f"{source}^{{tree}}")},
                        "parents": [{"sha": pinned_base}, {"sha": source}]}
            if "/pulls/" in url:
                return {"state": "open", "number": record["pr_number"],
                        "base": {"ref": "main", "sha": current_base,
                                 "repo": {"full_name": record["repository"]}},
                        "head": {"sha": repair, "repo": {"full_name": record["repository"]}}}
            if "/actions/runs/" in url:
                run_response = {"path": ".github/workflows/ci-pr.yml", "event": "pull_request",
                        "repository": {"full_name": record["repository"]},
                        "head_sha": source, "pull_requests": [{"number": record["pr_number"],
                            "base": {"sha": pinned_base,
                                     "repo": {"url": f"{mod.API}/repos/{record['repository']}"}},
                            "head": {"sha": source,
                                     "repo": {"url": f"{mod.API}/repos/{record['repository']}"}}}]}
                run_response.update(run_overrides)
                return run_response
            return {"run_id": record["run_id"], "head_sha": source,
                    "name": "Build / ubuntu-latest / gcc", "conclusion": "failure",
                    "steps": [{"name": name, "conclusion": result} for name, result in
                              (("Configure", "success"),
                               ("Build production library for early size gate", "success"),
                               ("Check binary size", "failure"))]}
        original_check_output = subprocess.check_output
        def reviewed_check_output(cmd, *args, **kwargs):
            if cmd[:2] == ["git", "show"] and ":tests/baseline_size.txt" in cmd[2]:
                sha = cmd[2].split(":", 1)[0]
                return ("397579\n" if sha == source else "395540\n")
            return original_check_output(cmd, *args, **kwargs)
        build_variant = {"name": "valid"}
        profile_fixture = {"fixture": "reviewed-1961-canonical-profile"}
        empty_archive = io.BytesIO()
        with tarfile.open(fileobj=empty_archive, mode="w"):
            pass
        archive_bytes = empty_archive.getvalue()
        original_run = subprocess.run
        def reviewed_run(cmd, *args, **kwargs):
            if isinstance(cmd, list) and cmd[:2] == ["meson", "setup"]:
                return SimpleNamespace(returncode=0)
            if isinstance(cmd, list) and cmd[:3] == ["meson", "compile", "-C"]:
                build = Path(cmd[cmd.index("-C") + 1]); build.mkdir(parents=True, exist_ok=True)
                (build / "libwirelog.so").write_bytes(b"fixture")
                return SimpleNamespace(returncode=0)
            if isinstance(cmd, list) and "size-profile.py" in " ".join(map(str, cmd)):
                output = Path(cmd[cmd.index("--output") + 1])
                output.write_text(json.dumps(profile_fixture), encoding="utf-8")
                return SimpleNamespace(returncode=0)
            return original_run(cmd, *args, **kwargs)
        original_canonical_hash = mod.canonical_hash
        def reviewed_profile_hash(value):
            if value == profile_fixture:
                return ("0" * 64 if build_variant["name"] == "profile-mismatch"
                        else record["profile_sha256"])
            return original_canonical_hash(value)
        def reviewed_rebuild_output(cmd, *args, **kwargs):
            if cmd[:2] == ["git", "show"] and ":tests/baseline_size.txt" in cmd[2]:
                sha = cmd[2].split(":", 1)[0]
                return ("397579\n" if sha == source else "395540\n")
            if cmd[:3] == ["git", "archive", "--format=tar"]:
                return archive_bytes
            if cmd and cmd[0] == "size":
                measured = 404283 if build_variant["name"] == "size-mismatch" else 404282
                return f".text {measured} {measured}\n"
            return original_check_output(cmd, *args, **kwargs)
        def exercise_reviewed_rebuild(variant):
            build_variant["name"] = variant
            with mock.patch.object(mod, "REVIEWED_PR1961_BASELINE", record), \
                 mock.patch.object(mod, "request", side_effect=reviewed_request), \
                 mock.patch.object(mod.subprocess, "run", side_effect=reviewed_run), \
                 mock.patch.object(mod.subprocess, "check_output", side_effect=reviewed_rebuild_output), \
                 mock.patch.object(mod, "canonical_hash", side_effect=reviewed_profile_hash):
                return mod.authorize_reviewed_pr(record["repository"], current_base,
                    event_merge, 395540, record["baseline_bytes"], record, "token")
        check("PR 1961 pinned measurement rebuild and size reproduce",
              lambda: (_ for _ in ()).throw(AssertionError("wrong verdict"))
              if exercise_reviewed_rebuild("valid") !=
                 "one-time reviewed PR #1961 measurement reproduced" else None)
        check("PR 1961 profile mismatch rejects authorization",
              lambda: exercise_reviewed_rebuild("profile-mismatch"),
              "canonical measurement profile cannot be reproduced")
        check("PR 1961 .text mismatch rejects authorization",
              lambda: exercise_reviewed_rebuild("size-mismatch"),
              "baseline bytes cannot be reproduced")
        with mock.patch.object(mod, "REVIEWED_PR1961_BASELINE", record), \
             mock.patch.object(mod, "request", side_effect=ValueError("trusted proof reached")), \
             mock.patch.object(mod.subprocess, "check_output", side_effect=reviewed_check_output):
            check("rebased event reaches historical evidence verification",
                  lambda: mod.authorize_reviewed_pr(record["repository"], current_base,
                      event_merge, 395540, record["baseline_bytes"], record, "token"),
                  "trusted proof reached")
            check("changed event-base baseline is rejected",
                  lambda: mod.authorize_reviewed_pr(record["repository"], current_base,
                      event_merge, 395541, record["baseline_bytes"], record, "token"),
                  "current event base baseline differs")
        with mock.patch.object(mod, "REVIEWED_PR1961_BASELINE", record), \
             mock.patch.object(mod, "request", side_effect=reviewed_request), \
             mock.patch.object(mod.subprocess, "check_output", side_effect=reviewed_check_output):
            run_overrides["repository"] = {"full_name": "attacker/other"}
            check("wrong historical run repository is rejected",
                  lambda: mod.authorize_reviewed_pr(record["repository"], current_base,
                      event_merge, 395540, record["baseline_bytes"], record, "token"),
                  "measurement run identity differs")
            run_overrides.clear()
            run_overrides["repository"] = {}
            check("missing historical run repository is rejected",
                  lambda: mod.authorize_reviewed_pr(record["repository"], current_base,
                      event_merge, 395540, record["baseline_bytes"], record, "token"),
                  "measurement run identity differs")
            run_overrides.clear()
            run_overrides["pull_requests"] = []
            check("missing historical PR association is rejected",
                  lambda: mod.authorize_reviewed_pr(record["repository"], current_base,
                      event_merge, 395540, record["baseline_bytes"], record, "token"),
                  "measurement run identity differs")
            run_overrides.clear()
            Path("unauthorized.c").write_text("unexpected\n", encoding="utf-8")
            git("add", "."); git("commit", "-qm", "unauthorized change")
            check("rebased unauthorized candidate path rejected",
                  lambda: mod.authorize_reviewed_pr(record["repository"], current_base,
                      git("rev-parse", "HEAD"), 395540, record["baseline_bytes"], record, "token"),
                  "outside its reviewed repair paths")
        git("checkout", "-q", "--orphan", "unrelated")
        git("rm", "-qrf", ".")
        Path("other.c").write_text("other\n", encoding="utf-8")
        git("add", "."); git("commit", "-qm", "unrelated base")
        unrelated = git("rev-parse", "HEAD")
        check("unrelated current base rejected",
              lambda: mod.reviewed_pr_reference_tree(unrelated, pinned_base, source),
              "ancestry differs")
        git("checkout", "-q", "--detach", pinned_base)
        Path("reviewed.c").write_text("conflicting main\n", encoding="utf-8")
        git("commit", "-qam", "conflicting main")
        conflict = git("rev-parse", "HEAD")
        check("conflicting reviewed-source rebase rejected",
              lambda: mod.reviewed_pr_reference_tree(conflict, pinned_base, source),
              "cannot be cleanly applied")
    finally:
        os.chdir(previous)

print("test-size-baseline-provenance: all cases passed")
