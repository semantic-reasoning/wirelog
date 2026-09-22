#!/usr/bin/env python3
"""Capture and compare the resolved production Meson/toolchain size profile."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import re
import tempfile

EXCLUDED_OPTIONS = {"prefix", "bindir", "datadir", "includedir", "infodir", "libdir",
                    "libexecdir", "localedir", "localstatedir", "mandir", "meson_command",
                    "sharedstatedir", "sysconfdir"}

def run(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE)

def replace_root(value, root, placeholder):
    # Replace a complete path prefix only, so /repo does not alter /repo-old.
    # Roots have already been resolved; preserve all following option syntax.
    text = str(value)
    escaped = re.escape(str(root))
    return re.sub(escaped + r"(?=$|[/\\:=,;])", placeholder, text)

def selected_linker_fingerprint(compiler, parameters):
    if not compiler:
        raise ValueError("production C compiler is unavailable for selected linker discovery")
    selected = []
    include_next = False
    for value in map(str, parameters):
        if include_next:
            selected.append(value)
            include_next = False
            continue
        if value == "-B":
            selected.append(value)
            include_next = True
        elif (value.startswith(("-B", "-fuse-ld=", "-Wl,-fuse-ld=", "--ld-path=",
                                "-Wl,--ld-path="))):
            selected.append(value)
    with tempfile.TemporaryDirectory(prefix="wirelog-linker-profile-") as temp:
        output = Path(temp) / "linker-version-probe"
        command = list(compiler) + selected + ["-Wl,--version", "-x", "c", "/dev/null", "-o", str(output)]
        try:
            result = subprocess.run(command, text=True, capture_output=True, check=False)
        except OSError as exc:
            raise ValueError(f"could not query selected linker: {exc}") from exc
        transcript = result.stdout + "\n" + result.stderr
        recognizable = (r"GNU ld(?:\s|\()", r"GNU gold(?:\s|\()", r"LLD(?:\s|$)",
                        r"Apple ld(?:\s|$)", r"ld64(?:\s|$)")
        lines = [line.strip() for line in transcript.splitlines()
                 if any(re.search(pattern, line, re.IGNORECASE) for pattern in recognizable)]
        if result.returncode != 0 or not lines:
            raise ValueError("could not identify/version the selected linker; refusing unknown profile")
        return {"version_line": lines[-1], "selected_flags": selected}

def capture(build_dir, source_dir, source_sha):
    build = Path(build_dir).resolve()
    source = Path(source_dir).resolve()
    info = json.loads(run("meson", "introspect", "--buildoptions", str(build)))
    options = {item["name"]: item.get("value") for item in info
               if item.get("name") not in EXCLUDED_OPTIONS}
    compilers = json.loads(run("meson", "introspect", "--compilers", str(build)))
    tools = {}
    for machine, languages in sorted(compilers.items()):
        tools[machine] = {}
        for language, data in sorted(languages.items()):
            exe = list(data.get("exelist", []))
            linker = list(data.get("linker_exelist", []))
            target = None
            if exe:
                try:
                    target = run(*(exe + ["-dumpmachine"])).strip()
                except (OSError, subprocess.CalledProcessError):
                    target = None
            tools[machine][language] = {
                "id": data.get("id"), "version": data.get("version"),
                "full_version": data.get("full_version"),
                "exelist": exe, "linker_exelist": linker,
                "linker_id": data.get("linker_id"),
                "compiler_target": target,
            }
    targets = json.loads(run("meson", "introspect", "--targets", str(build)))
    target = next((item for item in targets if item.get("name") == "wirelog" and
                   item.get("type") in ("shared library", "static library")), None)
    if target is None:
        raise ValueError("Meson introspection has no production wirelog library target")
    def normalized(values):
        # Build paths can be nested under the source path; replace them first
        # so nested and external build layouts normalize identically.
        normalized_values = []
        for value in values:
            text = replace_root(value, build, "<BUILD>")
            text = replace_root(text, source, "<SOURCE>")
            normalized_values.append(text)
        return normalized_values
    compile_profile = []
    link_profile = []
    for item in target.get("target_sources", []):
        if "compiler" in item:
            compile_profile.append({"language": item.get("language"), "machine": item.get("machine"),
                                   "compiler": normalized(item.get("compiler", [])),
                                   "parameters": normalized(item.get("parameters", []))})
        if "linker" in item:
            link_profile.append({"linker": normalized(item.get("linker", [])),
                                 "parameters": normalized(item.get("parameters", []))})
    c_compiler = tools.get("host", {}).get("c", {}).get("exelist", [])
    all_link_parameters = [parameter for item in link_profile for parameter in item["parameters"]]
    selected_linker = selected_linker_fingerprint(c_compiler, all_link_parameters)
    profile = {
        "schema_version": 1,
        "source_sha": source_sha,
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "options": options,
        "tools": tools,
        "selected_linker": selected_linker,
        "production_target_type": target.get("type"),
        "effective_compile_arguments": sorted(compile_profile, key=lambda x: json.dumps(x, sort_keys=True)),
        "effective_link_arguments": link_profile,
    }
    return profile

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    cap = sub.add_parser("capture")
    cap.add_argument("--build-dir", required=True)
    cap.add_argument("--source-dir", required=True)
    cap.add_argument("--source-sha", required=True)
    cap.add_argument("--output", required=True)
    cmp = sub.add_parser("compare")
    cmp.add_argument("base")
    cmp.add_argument("head")
    fp = sub.add_parser("fingerprint")
    fp.add_argument("profile")
    args = parser.parse_args()
    try:
        if args.command == "capture":
            profile = capture(args.build_dir, args.source_dir, args.source_sha)
            Path(args.output).write_text(json.dumps(profile, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            print(f"profile captured for {args.source_sha}")
            return 0
        if args.command == "fingerprint":
            profile = json.loads(Path(args.profile).read_text(encoding="utf-8"))
            profile.pop("source_sha", None)
            print(hashlib.sha256(json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()).hexdigest())
            return 0
        base = json.loads(Path(args.base).read_text(encoding="utf-8"))
        head = json.loads(Path(args.head).read_text(encoding="utf-8"))
        if base.get("schema_version") != 1 or head.get("schema_version") != 1:
            raise ValueError("unsupported profile schema")
        left, right = dict(base), dict(head)
        left.pop("source_sha", None); right.pop("source_sha", None)
        if left != right:
            keys = sorted(set(left) | set(right))
            changed = [key for key in keys if left.get(key) != right.get(key)]
            raise ValueError("production profile mismatch: " + ", ".join(changed))
        print("production profiles match")
        return 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"size-profile: {exc}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
