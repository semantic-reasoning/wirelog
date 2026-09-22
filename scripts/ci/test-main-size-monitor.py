#!/usr/bin/env python3
"""Exercise always-run main size reporting, including missing inputs."""
import json
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from unittest import mock

root=Path(sys.argv[1]).resolve(); build=Path(sys.argv[2]).resolve()
script=root/"scripts/ci/report-main-size.py"
if sys.platform != "linux":
    print("test-main-size-monitor: SKIP requires Linux ELF size tooling")
    raise SystemExit(77)
with tempfile.TemporaryDirectory(prefix="wirelog-main-size-test-") as d:
    temp=Path(d); summary=temp/"summary.md"; report=temp/"new report directory"/"nested"/"report.json"
    env=os.environ.copy(); env.update({"GITHUB_STEP_SUMMARY":str(summary),"GITHUB_RUN_ID":"12345","GITHUB_JOB":"build-matrix"})
    # Use two controlled builds so caller build location/options cannot affect
    # this test. Capture the same tiny project from nested and external builds.
    profile_source=temp/"profile source with spaces"
    profile_source.mkdir()
    (profile_source/"meson.build").write_text("project('profile-fixture', 'c')\nshared_library('wirelog', 'wirelog.c', install: false)\n")
    (profile_source/"wirelog.c").write_text("int wirelog_fixture(void) { return 7; }\n")
    (profile_source/"tests").mkdir()
    (profile_source/"tests/baseline_size.txt").write_text("1000000\n")
    nested_build=profile_source/"nested build"
    external_build=temp/"external-meson-build"
    subprocess.run(["meson","setup",str(nested_build),str(profile_source)],check=True,
                   stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
    subprocess.run(["meson","setup",str(external_build),str(profile_source)],check=True,
                   stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
    profile_script=root/"scripts/ci/size-profile.py"
    nested_profile=temp/"nested-profile.json"; external_profile=temp/"external-profile.json"
    for builddir,destination in ((nested_build,nested_profile),(external_build,external_profile)):
        subprocess.run([sys.executable,str(profile_script),"capture","--build-dir",str(builddir),
                        "--source-dir",str(profile_source),"--source-sha","profile-fixture","--output",str(destination)],
                       check=True,capture_output=True,text=True)
    subprocess.run([sys.executable,str(profile_script),"compare",str(nested_profile),str(external_profile)],
                   check=True,capture_output=True,text=True)

    # Linker identity is queried through the configured compiler driver, not
    # an unrelated linker found on PATH. Unknown output fails closed.
    spec=importlib.util.spec_from_file_location("size_profile",profile_script)
    size_profile=importlib.util.module_from_spec(spec); spec.loader.exec_module(size_profile)
    sibling=str(profile_source)+"-neighbor/include"
    assert size_profile.replace_root(sibling,profile_source,"<SOURCE>")==sibling
    assert size_profile.replace_root(str(profile_source)+"/include",profile_source,"<SOURCE>")=="<SOURCE>/include"
    assert size_profile.replace_root("-I"+str(profile_source)+"/include",profile_source,"<SOURCE>")=="-I<SOURCE>/include"
    with mock.patch.object(size_profile.subprocess,"run",return_value=SimpleNamespace(
            returncode=0,stdout="LLD 19.1.0\n",stderr="")) as probe:
        selected=size_profile.selected_linker_fingerprint(["compiler"],["-B","/custom/toolchain","--ld-path=/custom/ld"])
        assert selected["version_line"]=="LLD 19.1.0"
        assert probe.call_args.args[0][1:4]==["-B","/custom/toolchain","--ld-path=/custom/ld"]
    with mock.patch.object(size_profile.subprocess,"run",return_value=SimpleNamespace(
            returncode=0,stdout="unknown linker\n",stderr="")):
        try: size_profile.selected_linker_fingerprint(["compiler"],[])
        except ValueError as exc: assert "unknown profile" in str(exc)
        else: raise AssertionError("unknown selected linker must fail closed")
    def invoke(library, builddir, baseline=None, extra_path=None):
        args=[sys.executable,str(script),"--library",str(library),"--build-dir",str(builddir),
              "--source-dir",str(profile_source),"--report",str(report),"--sha","deadbeef",
              "--run-url","https://example.invalid/actions/runs/12345",
              "--runner-os","ubuntu-latest","--compiler","gcc",
              "--configure-status","success","--build-status","success","--test-status","success"]
        if baseline: args.extend(["--baseline-file",str(baseline)])
        use_env=env.copy()
        if extra_path: use_env["PATH"]=str(extra_path)+os.pathsep+use_env["PATH"]
        result=subprocess.run(args,env=use_env,text=True,capture_output=True,check=True)
        data=json.loads(report.read_text())
        if data["status"] in ("monitoring-error","over-budget"):
            assert "::warning title=Binary size monitor" in result.stdout, result.stdout
        else:
            assert "::warning title=Binary size monitor" not in result.stdout, result.stdout
        assert data["commit_sha"]=="deadbeef" and data["run_id"]=="12345"
        assert data["runner_os"]=="ubuntu-latest" and data["compiler"]=="gcc"
        assert data["run_url"].endswith("/12345")
        assert summary.exists() and "deadbeef" in summary.read_text() and "12345" in summary.read_text()
        assert "`ubuntu-latest` / `gcc`" in summary.read_text()
        assert report.with_suffix(".profile.json").exists() or data["status"]=="monitoring-error"
        return data
    absent=invoke(temp/"missing.so",nested_build)
    assert absent["status"]=="monitoring-error" and absent["measured_bytes"] is None
    lib=temp/"fixture.so"; lib.write_bytes(b"fixture")
    unavailable=invoke(lib,temp/"missing-build")
    assert unavailable["status"]=="monitoring-error" and "profile unavailable" in unavailable["error"]
    badbin=temp/"bin"; badbin.mkdir()
    size=badbin/"size"; size.write_text("#!/bin/sh\nprintf 'not a size report\\n'\n"); size.chmod(0o755)
    malformed=invoke(lib,nested_build,extra_path=badbin)
    assert malformed["status"]=="monitoring-error" and "exactly one .text" in malformed["error"], malformed
    # Small and over-budget real ELF fixtures drive both measurement verdicts.
    small=temp/"small.c"; small.write_text("int size_fixture(void) { return 1; }\n")
    smalllib=temp/"small.so"
    subprocess.run(["cc","-shared","-fPIC","-O0","-o",str(smalllib),str(small)],check=True)
    baseline=temp/"baseline.txt"; baseline.write_text("1000000\n")
    small_report=invoke(smalllib,nested_build,baseline)
    assert small_report["status"]=="within-budget" and small_report["profile_sha256"]
    assert report.with_suffix(".profile.json").is_file(), "profile output in fresh report directory was not created"
    asm=temp/"large.s"
    asm.write_text(".text\n.globl large_fixture\n.type large_fixture, @function\nlarge_fixture:\n"+
                   "\tnop\n"*7000+"\tret\n")
    largelib=temp/"large.so"
    subprocess.run(["cc","-shared","-o",str(largelib),str(asm)],check=True)
    baseline.write_text("0\n")
    large_report=invoke(largelib,nested_build,baseline)
    assert large_report["status"]=="over-budget" and large_report["measured_bytes"]>5120
    # Prove a failed build cannot be reported as a clean measurement.
    failed_args=[sys.executable,str(script),"--library",str(smalllib),"--build-dir",str(nested_build),
                 "--source-dir",str(profile_source),"--report",str(report),"--sha","deadbeef",
                 "--run-url","https://example.invalid/actions/runs/12345",
                 "--runner-os","ubuntu-latest","--compiler","gcc",
                 "--configure-status","success","--build-status","failure","--test-status","skipped"]
    result=subprocess.run(failed_args,env=env,text=True,capture_output=True,check=True)
    failed_report=json.loads(report.read_text())
    assert failed_report["status"]=="monitoring-error" and failed_report["phase"]=="build", failed_report
print("test-main-size-monitor: all cases passed")
