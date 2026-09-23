#!/usr/bin/env python3
"""Offline fixtures for trusted main-artifact baseline authorization."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
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

def exercise(provenance, corrupt=None):
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
            return mod.authorize("owner/repo","base456","candidate789",100,200,p,"fixture-token")

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

print("test-size-baseline-provenance: all cases passed")
