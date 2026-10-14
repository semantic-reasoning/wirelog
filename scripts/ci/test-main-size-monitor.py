#!/usr/bin/env python3
"""Exercise always-run main size reporting, including missing inputs."""
import json
import copy
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from unittest import mock

def check_root_operands(profile):
    cases = {
        "/src": "<SOURCE>",
        "/src/include": "<SOURCE>/include",
        "-I/src/subprojects/nanoarrow/src": "-I<SOURCE>/subprojects/nanoarrow/src",
        "/elsewhere/src/header": "/elsewhere/src/header",
        "-I/elsewhere/src/header": "-I/elsewhere/src/header",
        "-I/src-old/include": "-I/src-old/include",
        "-L/src/lib": "-L<SOURCE>/lib",
        "-B/src/tools": "-B<SOURCE>/tools",
        "-isystem/src/include": "-isystem<SOURCE>/include",
        "-iquote/src/include": "-iquote<SOURCE>/include",
        "--sysroot=/src": "--sysroot=<SOURCE>",
        "--ld-path=/src/bin/ld": "--ld-path=<SOURCE>/bin/ld",
        "-Wl,--version-script=/src/export.map": "-Wl,--version-script=<SOURCE>/export.map",
        "-Wl,-rpath,/src/lib:/elsewhere/src/lib:/src/other":
            "-Wl,-rpath,<SOURCE>/lib:/elsewhere/src/lib:<SOURCE>/other",
        "-Wl,-L/src/lib,--version-script,/src/export.map":
            "-Wl,-L<SOURCE>/lib,--version-script,<SOURCE>/export.map",
        "-Wl,-rpath=/src/lib,-rpath-link,/src/other":
            "-Wl,-rpath=<SOURCE>/lib,-rpath-link,<SOURCE>/other",
        "-I/src/with spaces/src": "-I<SOURCE>/with spaces/src",
        "-I/src/suffix=literal/src": "-I<SOURCE>/suffix=literal/src",
        "-I/src/suffix,with:punctuation/src": "-I<SOURCE>/suffix,with:punctuation/src",
        "-I/src:literal": "-I/src:literal",
        "-DROOT=/src": "-DROOT=/src",
        "-DVALUES=/src,/src/header": "-DVALUES=/src,/src/header",
        "--unknown=/src": "--unknown=/src",
        "-Wl,-soname,/src": "-Wl,-soname,/src",
        "-Wl,-soname,-L/src": "-Wl,-soname,-L/src",
        "prefix /src/include": "prefix /src/include",
        "<SOURCE>/nested/src": "<SOURCE>/nested/src",
    }
    for value, expected in cases.items():
        actual = profile.replace_root(value, "/src", "<SOURCE>")
        assert actual == expected, (value, actual, expected)
        assert profile.replace_root(actual, "/src", "<SOURCE>") == actual
    # Separate options are separate argv elements, never whitespace-split.
    assert [profile.replace_root(v, "/src", "<SOURCE>") for v in
            ["-I", "/src/include", "-isystem", "/src/space dir"]] == [
                "-I", "<SOURCE>/include", "-isystem", "<SOURCE>/space dir"]
    assert profile.replace_root("-I/root with spaces/src/include",
                                "/root with spaces/src", "<SOURCE>") == "-I<SOURCE>/include"
    assert profile.replace_root(r"-IC:\src\include\src", r"C:\src",
                                "<SOURCE>") == r"-I<SOURCE>\include\src"
    for build_root in ("/src/build", "/external/build"):
        value = "-I" + build_root + "/nested/src"
        actual = profile.replace_root(value, build_root, "<BUILD>")
        assert profile.replace_root(actual, "/src", "<SOURCE>") == "-I<BUILD>/nested/src"

def check_relocated_profiles(profile_script, temp, build):
    evidence = build / "size-profile-regression"
    evidence.mkdir(parents=True, exist_ok=True)
    commands = []

    def run(args, expected=0):
        result = subprocess.run(list(map(str, args)), text=True, capture_output=True,
                                encoding="utf-8")
        commands.append({"argv": list(map(str, args)), "returncode": result.returncode,
                         "stdout": result.stdout, "stderr": result.stderr})
        (evidence / "commands.json").write_text(json.dumps(commands, indent=2) + "\n", encoding="utf-8")
        assert result.returncode == expected, commands[-1]
        return result.stdout.strip()

    short = temp / "src"
    unique = temp / "unique relocated checkout"
    changed = temp / "different include checkout"
    # The same relative suffix contains the *whole* short root. The old global
    # replacement corrupts it only in the short checkout, reproducing /src's
    # collision with nanoarrow/src without requiring permission to create /src.
    suffix = "subprojects/nanoarrow" + str(short)
    recipe = """project('profile-fixture', 'c', default_options: ['buildtype=release', 'b_lto=true'])
inc = meson.project_source_root() / '@SUFFIX@'
shared_library('wirelog', 'wirelog.c', install: false,
  c_args: ['-I' + inc, '-isystem', inc],
  link_args: ['-Wl,--version-script=' + meson.project_source_root() / 'exports.map'],
  build_rpath: meson.project_source_root() / 'lib one' + ':' + meson.project_source_root() / 'lib two')
"""
    for source in (short, unique, changed):
        include_suffix = suffix + ("/different" if source == changed else "")
        (source / include_suffix).mkdir(parents=True)
        (source / "meson.build").write_text(recipe.replace("@SUFFIX@", include_suffix), encoding="utf-8")
        (source / "wirelog.c").write_text("int wirelog_fixture(void) { return 7; }\n", encoding="utf-8")
        (source / "exports.map").write_text("{ global: wirelog_fixture; local: *; };\n", encoding="utf-8")
    (evidence / "fixture.json").write_text(json.dumps({"recipe": recipe, "suffix": suffix,
        "source": "int wirelog_fixture(void) { return 7; }\n",
        "exports": "{ global: wirelog_fixture; local: *; };\n"}, indent=2) + "\n", encoding="utf-8")
    profiles = {}
    fingerprints = {}
    for name, source, builddir in (("short", short, short / "build"),
                                   ("unique", unique, unique / "build"),
                                   ("external", unique, temp / "external relocated build"),
                                   ("different", changed, changed / "build")):
        run(["meson", "setup", builddir, source])
        run(["meson", "compile", "-C", builddir])
        profiles[name] = evidence / (name + ".profile.json")
        run([sys.executable, profile_script, "capture", "--build-dir", builddir,
             "--source-dir", source, "--source-sha", "profile-fixture", "--output", profiles[name]])
        fingerprints[name] = run([sys.executable, profile_script, "fingerprint", profiles[name]])
        raw = run(["meson", "introspect", "--targets", builddir])
        (evidence / (name + ".targets.json")).write_text(raw + "\n", encoding="utf-8")
        assert str(source / (suffix + ("/different" if name == "different" else ""))) in raw
    for name in ("unique", "external"):
        run([sys.executable, profile_script, "compare", profiles["short"], profiles[name]])
        assert fingerprints["short"] == fingerprints[name]
    run([sys.executable, profile_script, "compare", profiles["short"], profiles["different"]], expected=1)
    assert fingerprints["short"] != fingerprints["different"]

    # Actual captured evidence remains sensitive to every meaningful setting.
    baseline = json.loads(profiles["short"].read_text(encoding="utf-8"))
    mutations = [
        (("tools", "host", "c", "id"), "different-compiler"),
        (("tools", "host", "c", "version"), "different-version"),
        (("selected_linker", "version_line"), "different-linker"),
        (("selected_linker", "selected_flags"), ["-fuse-ld=lld"]),
        (("options", "optimization"), "0"),
        (("options", "b_lto"), False),
        (("options", "wirelog_log_max_level"), "error"),
        (("options", "mbedTLS"), "enabled"),
        (("options", "default_library"), "static"),
        (("effective_compile_arguments", 0, "parameters"), ["-DWL_LOG_COMPILE_MAX_LEVEL=0"]),
        (("effective_link_arguments", 0, "parameters"), ["-Wl,--different-flag"]),
    ]
    for index, (keys, value) in enumerate(mutations):
        variant = copy.deepcopy(baseline)
        target = variant
        for key in keys[:-1]:
            target = target[key]
        assert target.get(keys[-1]) != value
        target[keys[-1]] = value
        destination = evidence / ("negative-%d.profile.json" % index)
        destination.write_text(json.dumps(variant, indent=2) + "\n", encoding="utf-8")
        run([sys.executable, profile_script, "compare", profiles["short"], destination], expected=1)
        assert run([sys.executable, profile_script, "fingerprint", destination]) != fingerprints["short"]
    (evidence / "fingerprints.json").write_text(json.dumps(fingerprints, indent=2) + "\n", encoding="utf-8")

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
    (profile_source/"meson.build").write_text("project('profile-fixture', 'c')\nshared_library('wirelog', 'wirelog.c', install: false)\n", encoding="utf-8")
    (profile_source/"wirelog.c").write_text("int wirelog_fixture(void) { return 7; }\n", encoding="utf-8")
    (profile_source/"tests").mkdir()
    (profile_source/"tests/baseline_size.txt").write_text("1000000\n", encoding="utf-8")
    nested_build=profile_source/"nested build"
    external_build=temp/"external-meson-build"
    subprocess.run(["meson","setup",str(nested_build),str(profile_source)],check=True,
                   stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True, encoding="utf-8")
    subprocess.run(["meson","setup",str(external_build),str(profile_source)],check=True,
                   stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True, encoding="utf-8")
    profile_script=root/"scripts/ci/size-profile.py"
    nested_profile=temp/"nested-profile.json"; external_profile=temp/"external-profile.json"
    for builddir,destination in ((nested_build,nested_profile),(external_build,external_profile)):
        subprocess.run([sys.executable,str(profile_script),"capture","--build-dir",str(builddir),
                        "--source-dir",str(profile_source),"--source-sha","profile-fixture","--output",str(destination)],
                       check=True,capture_output=True,text=True, encoding="utf-8")
    subprocess.run([sys.executable,str(profile_script),"compare",str(nested_profile),str(external_profile)],
                   check=True,capture_output=True,text=True, encoding="utf-8")

    # Linker identity is queried through the configured compiler driver, not
    # an unrelated linker found on PATH. Unknown output fails closed.
    spec=importlib.util.spec_from_file_location("size_profile",profile_script)
    size_profile=importlib.util.module_from_spec(spec); spec.loader.exec_module(size_profile)
    check_root_operands(size_profile)
    check_relocated_profiles(profile_script, temp, build)
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
        result=subprocess.run(args,env=use_env,text=True,capture_output=True,check=True, encoding="utf-8")
        data=json.loads(report.read_text(encoding="utf-8"))
        if data["status"] in ("monitoring-error","over-budget"):
            assert "::warning title=Binary size monitor" in result.stdout, result.stdout
        else:
            assert "::warning title=Binary size monitor" not in result.stdout, result.stdout
        assert data["commit_sha"]=="deadbeef" and data["run_id"]=="12345"
        assert data["runner_os"]=="ubuntu-latest" and data["compiler"]=="gcc"
        assert data["run_url"].endswith("/12345")
        assert summary.exists() and "deadbeef" in summary.read_text(encoding="utf-8") and "12345" in summary.read_text(encoding="utf-8")
        assert "`ubuntu-latest` / `gcc`" in summary.read_text(encoding="utf-8")
        assert report.with_suffix(".profile.json").exists() or data["status"]=="monitoring-error"
        return data
    absent=invoke(temp/"missing.so",nested_build)
    assert absent["status"]=="monitoring-error" and absent["measured_bytes"] is None
    lib=temp/"fixture.so"; lib.write_bytes(b"fixture")
    unavailable=invoke(lib,temp/"missing-build")
    assert unavailable["status"]=="monitoring-error" and "profile unavailable" in unavailable["error"]
    badbin=temp/"bin"; badbin.mkdir()
    size=badbin/"size"; size.write_text("#!/bin/sh\nprintf 'not a size report\\n'\n", encoding="utf-8"); size.chmod(0o755)
    malformed=invoke(lib,nested_build,extra_path=badbin)
    assert malformed["status"]=="monitoring-error" and "exactly one .text" in malformed["error"], malformed
    # Small and over-budget real ELF fixtures drive both measurement verdicts.
    small=temp/"small.c"; small.write_text("int size_fixture(void) { return 1; }\n", encoding="utf-8")
    smalllib=temp/"small.so"
    subprocess.run(["cc","-shared","-fPIC","-O0","-o",str(smalllib),str(small)],check=True)
    baseline=temp/"baseline.txt"; baseline.write_text("1000000\n", encoding="utf-8")
    small_report=invoke(smalllib,nested_build,baseline)
    assert small_report["status"]=="within-budget" and small_report["profile_sha256"]
    assert report.with_suffix(".profile.json").is_file(), "profile output in fresh report directory was not created"
    asm=temp/"large.s"
    asm.write_text(".text\n.globl large_fixture\n.type large_fixture, @function\nlarge_fixture:\n"+
                   "\tnop\n"*7000+"\tret\n", encoding="utf-8")
    largelib=temp/"large.so"
    subprocess.run(["cc","-shared","-o",str(largelib),str(asm)],check=True)
    baseline.write_text("0\n", encoding="utf-8")
    large_report=invoke(largelib,nested_build,baseline)
    assert large_report["status"]=="over-budget" and large_report["measured_bytes"]>5120
    # Prove a failed build cannot be reported as a clean measurement.
    failed_args=[sys.executable,str(script),"--library",str(smalllib),"--build-dir",str(nested_build),
                 "--source-dir",str(profile_source),"--report",str(report),"--sha","deadbeef",
                 "--run-url","https://example.invalid/actions/runs/12345",
                 "--runner-os","ubuntu-latest","--compiler","gcc",
                 "--configure-status","success","--build-status","failure","--test-status","skipped"]
    result=subprocess.run(failed_args,env=env,text=True,capture_output=True,check=True, encoding="utf-8")
    failed_report=json.loads(report.read_text(encoding="utf-8"))
    assert failed_report["status"]=="monitoring-error" and failed_report["phase"]=="build", failed_report
print("test-main-size-monitor: all cases passed")
