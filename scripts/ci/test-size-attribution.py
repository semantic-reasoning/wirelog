"""Contract tests for the PR #1903 exact-profile diagnostic collector."""

import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "collect_size_attribution", ROOT / "scripts/ci/collect-size-attribution.py")
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)


class SizeAttributionContractTests(unittest.TestCase):
    def test_workflow_is_manual_read_only_and_profile_guarded(self):
        workflow = (ROOT / ".github/workflows/size-attribution.yml").read_text(encoding="utf-8")
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertIn("contents: read", workflow)
        self.assertIn("persist-credentials: false", workflow)
        self.assertIn("retention-days: 14", workflow)
        self.assertIn("type: choice", workflow)
        self.assertIn("Open admission pull request", workflow)
        self.assertIn("Fresh origin/main SHA", workflow)
        self.assertIn("- \"1945\"", workflow)
        self.assertIn("reference_sha", workflow)
        self.assertIn("keep it fixed across runs", workflow)
        self.assertIn("GH_REFERENCE_SHA", workflow)
        self.assertIn("timeout-minutes: 60", workflow)
        self.assertIn("ImageVersion", workflow)
        self.assertIn("scripts/ci/require-ubuntu-2404.sh", workflow)
        self.assertNotIn("grep -q '^ID=", workflow)
        self.assertNotIn("grep -q '^VERSION_ID=", workflow)
        self.assertIn('cat /etc/os-release > size-attribution/os-release.txt', workflow)
        self.assertIn("        if: always()\n        uses: actions/upload-artifact@v7", workflow)
        self.assertIn("collect-size-attribution.py", workflow)
        self.assertIn("test-size-attribution.py", workflow)
        self.assertNotIn("pull-requests: write", workflow)

    def test_rejects_noncanonical_repository_pr_or_shas(self):
        for value in ("a4c7", "A" * 40, "../" + "a" * 37):
            with self.subTest(value=value), self.assertRaises(collector.DiagnosticError):
                collector.validate_sha(value, "candidate SHA")
        collector.validate_sha("0" * 40, "candidate SHA")

    def test_rejects_workflow_ref_and_moved_remote_pr_head(self):
        with self.assertRaises(collector.DiagnosticError):
            collector.verify_repository(Path("."), collector.REPOSITORY, 1903,
                                        collector.BASE_SHA, "a4c7c622daa3b7247378acf71a74b7f40f4ff556",
                                        "refs/heads/feature")
        moved = "b" * 40
        with mock.patch.object(collector, "run", side_effect=[
            "https://github.com/semantic-reasoning/wirelog.git\n", "", f"{moved}\trefs/pull/1903/head\n"
        ]), self.assertRaises(collector.DiagnosticError):
            collector.verify_repository(Path("."), collector.REPOSITORY, 1903,
                                        collector.BASE_SHA, "a4c7c622daa3b7247378acf71a74b7f40f4ff556",
                                        "refs/heads/main")

    def test_pr1945_mode_accepts_only_fresh_main_head_and_exact_merge_parents(self):
        base, head, reference = "1" * 40, "3" * 40, "2" * 40
        metadata = {"state": "open", "base_ref": "main", "base_sha": base,
                    "head_sha": head, "head_repository": collector.REPOSITORY}
        collector.validate_pr1945_request(
            collector.REPOSITORY, 1945, base, head, reference,
            "refs/heads/main", base, head, [base, head], metadata)
        rejected = (
            {"repository": "other/repo"},
            {"pr_number": 1903},
            {"base_sha": "4" * 40},
            {"reference_sha": base},
            {"reference_sha": head},
            {"candidate_sha": "4" * 40},
            {"workflow_ref": "refs/heads/feature"},
            {"remote_main_sha": "5" * 40},
            {"remote_head_sha": "6" * 40},
            {"merge_parents": [base, "4" * 40]},
            {"pull_request": {**metadata, "state": "closed"}},
            {"pull_request": {**metadata, "base_ref": "release"}},
            {"pull_request": {**metadata, "base_sha": "7" * 40}},
            {"pull_request": {**metadata, "head_sha": "8" * 40}},
            {"pull_request": {**metadata, "head_repository": "fork/wirelog"}},
        )
        values = {"repository": collector.REPOSITORY, "pr_number": 1945,
                  "base_sha": base, "candidate_sha": head,
                  "reference_sha": reference, "workflow_ref": "refs/heads/main",
                  "remote_main_sha": base, "remote_head_sha": head,
                  "merge_parents": [base, head], "pull_request": metadata}
        for overrides in rejected:
            with self.subTest(overrides=overrides), self.assertRaises(collector.DiagnosticError):
                collector.validate_pr1945_request(**(values | overrides))

    def test_current_pr1903_mode_accepts_only_fresh_main_head_and_exact_merge_parents(self):
        base, head, reference = "1" * 40, "3" * 40, "2" * 40
        metadata = {"state": "open", "base_ref": "main", "base_sha": base,
                    "head_sha": head, "head_repository": collector.REPOSITORY}
        collector.validate_current_candidate_request(
            collector.REPOSITORY, 1903, base, head, reference,
            "refs/heads/main", base, head, [base, head], metadata)
        rejected = (
            {"repository": "other/repo"},
            {"pr_number": 1945},
            {"base_sha": "4" * 40},
            {"reference_sha": base},
            {"candidate_sha": "4" * 40},
            {"workflow_ref": "refs/heads/feature"},
            {"remote_main_sha": "5" * 40},
            {"remote_head_sha": "6" * 40},
            {"merge_parents": [base, "4" * 40]},
            {"pull_request": {**metadata, "state": "closed"}},
            {"pull_request": {**metadata, "base_ref": "release"}},
            {"pull_request": {**metadata, "base_sha": "7" * 40}},
            {"pull_request": {**metadata, "head_sha": "8" * 40}},
            {"pull_request": {**metadata, "head_repository": "fork/wirelog"}},
        )
        values = {"repository": collector.REPOSITORY, "pr_number": 1903,
                  "base_sha": base, "candidate_sha": head,
                  "reference_sha": reference, "workflow_ref": "refs/heads/main",
                  "remote_main_sha": base, "remote_head_sha": head,
                  "merge_parents": [base, head], "pull_request": metadata}
        for overrides in rejected:
            with self.subTest(overrides=overrides), self.assertRaises(collector.DiagnosticError):
                collector.validate_current_candidate_request(**(values | overrides))

    def test_current_pr1903_verify_requires_fixed_reference_and_fresh_refs(self):
        base, reference, candidate, merge = "1" * 40, "2" * 40, "3" * 40, "4" * 40
        outputs = [
            "https://github.com/semantic-reasoning/wirelog.git\n", "", base + "\n",
            candidate + "\trefs/pull/1903/head\n", "", "", candidate + "\n",
            f"{merge} {base} {candidate}\n", base + "\n", reference + "\n",
            candidate + "\n", "", "",
        ]
        metadata = {"state": "open", "base_ref": "main", "base_sha": base,
                    "head_sha": candidate, "head_repository": collector.REPOSITORY}
        with mock.patch.object(collector, "fetch_pull_request_metadata", return_value=metadata), \
             mock.patch.object(collector, "run", side_effect=outputs):
            identity = collector.verify_repository(
                Path("."), collector.REPOSITORY, 1903, base, candidate,
                "refs/heads/main", reference)
        self.assertEqual(identity["remote_main"], base)
        self.assertEqual(identity["remote_pr_head"], candidate)
        self.assertEqual(identity["reference_sha"], reference)

        with self.assertRaises(collector.DiagnosticError):
            collector.verify_repository(
                Path("."), collector.REPOSITORY, 1903, base, candidate,
                "refs/heads/main")

    def test_current_pr1903_baseline_requires_fresh_two_tree_identity(self):
        base, candidate, merge = "1" * 40, "3" * 40, "4" * 40
        metadata = {"state": "open", "base_ref": "main", "base_sha": base,
                    "head_sha": candidate, "head_repository": collector.REPOSITORY}
        collector.validate_current_baseline_request(
            collector.REPOSITORY, 1903, base, candidate,
            "refs/heads/main", base, candidate, [base, candidate], metadata)
        with self.assertRaises(collector.DiagnosticError):
            collector.validate_current_baseline_request(
                collector.REPOSITORY, 1937, base, candidate,
                "refs/heads/main", base, candidate, [base, candidate], metadata)

        outputs = [
            "https://github.com/semantic-reasoning/wirelog.git\n", "", base + "\n",
            candidate + "\trefs/pull/1903/head\n", "", "", candidate + "\n",
            f"{merge} {base} {candidate}\n", base + "\n", candidate + "\n", "",
        ]
        with mock.patch.object(collector, "fetch_pull_request_metadata", return_value=metadata), \
             mock.patch.object(collector, "run", side_effect=outputs):
            identity = collector.verify_repository(
                Path("."), collector.REPOSITORY, 1903, base, candidate,
                "refs/heads/main")
        self.assertEqual(identity["remote_main"], base)
        self.assertEqual(identity["remote_pr_head"], candidate)
        self.assertEqual(identity["remote_pr_merge"], merge)

    def test_current_pr1903_baseline_routes_to_two_tree_collection(self):
        args = collector.parse_args([
            "--repository", collector.REPOSITORY, "--pr-number", "1903",
            "--base-sha", "1" * 40, "--candidate-sha", "3" * 40,
            "--workflow-ref", "refs/heads/main",
        ])
        with mock.patch.object(collector, "collect_pr1903", return_value=0) as collect:
            self.assertEqual(collector.collect(args), 0)
        collect.assert_called_once_with(args)

    def test_current_pr1903_collection_uses_three_tree_attribution(self):
        args = collector.parse_args([
            "--repository", collector.REPOSITORY, "--pr-number", "1903",
            "--base-sha", "1" * 40, "--reference-sha", "2" * 40,
            "--candidate-sha", "3" * 40, "--workflow-ref", "refs/heads/main",
        ])
        with mock.patch.object(collector, "collect_current_candidate", return_value=0) as collect:
            self.assertEqual(collector.collect(args), 0)
        collect.assert_called_once_with(args)

    def test_pr1945_verify_requires_reference_ancestor_and_fresh_refs(self):
        base, reference, candidate, merge = "1" * 40, "2" * 40, "3" * 40, "4" * 40
        outputs = [
            "https://github.com/semantic-reasoning/wirelog.git\n", "", base + "\n",
            candidate + "\trefs/pull/1945/head\n", "", "", candidate + "\n",
            f"{merge} {base} {candidate}\n", base + "\n", reference + "\n",
            candidate + "\n", "", "",
        ]
        metadata = {"state": "open", "base_ref": "main", "base_sha": base,
                    "head_sha": candidate, "head_repository": collector.REPOSITORY}
        with mock.patch.object(collector, "fetch_pull_request_metadata", return_value=metadata), \
             mock.patch.object(collector, "run", side_effect=outputs):
            identity = collector.verify_repository(
                Path("."), collector.REPOSITORY, 1945, base, candidate,
                "refs/heads/main", reference)
        self.assertEqual(identity["remote_main"], base)
        self.assertEqual(identity["remote_pr_head"], candidate)
        self.assertEqual(identity["reference_sha"], reference)

        stale_outputs = list(outputs)
        stale_outputs[2] = "5" * 40 + "\n"
        with mock.patch.object(collector, "fetch_pull_request_metadata", return_value=metadata), \
             mock.patch.object(collector, "run", side_effect=stale_outputs), \
             self.assertRaises(collector.DiagnosticError):
            collector.verify_repository(
                Path("."), collector.REPOSITORY, 1945, base, candidate,
                "refs/heads/main", reference)

        remaining_outputs = iter(outputs)
        def reject_candidate_ancestry(command, **kwargs):
            if command[:3] == ["git", "merge-base", "--is-ancestor"] and command[3] == reference:
                raise collector.DiagnosticError("reference is not an ancestor")
            return next(remaining_outputs)

        with mock.patch.object(collector, "fetch_pull_request_metadata", return_value=metadata), \
             mock.patch.object(collector, "run", side_effect=reject_candidate_ancestry), \
             self.assertRaises(collector.DiagnosticError):
            collector.verify_repository(
                Path("."), collector.REPOSITORY, 1945, base, candidate,
                "refs/heads/main", reference)

    def test_pr1945_rejects_missing_or_non_ancestor_reference(self):
        with self.assertRaises(collector.DiagnosticError):
            collector.verify_repository(Path("."), collector.REPOSITORY, 1945,
                                        "1" * 40, "3" * 40, "refs/heads/main")
        base, reference, candidate, merge = "1" * 40, "2" * 40, "3" * 40, "4" * 40
        outputs = iter([
            "https://github.com/semantic-reasoning/wirelog.git\n", "", base + "\n",
            candidate + "\trefs/pull/1945/head\n", "", "", candidate + "\n",
            f"{merge} {base} {candidate}\n", base + "\n", reference + "\n",
            candidate + "\n", "",
        ])
        def reject_candidate_ancestry(command, **kwargs):
            if command[:3] == ["git", "merge-base", "--is-ancestor"] and command[3] == reference:
                raise collector.DiagnosticError("reference is not an ancestor")
            return next(outputs)

        metadata = {"state": "open", "base_ref": "main", "base_sha": base,
                    "head_sha": candidate, "head_repository": collector.REPOSITORY}
        with mock.patch.object(collector, "fetch_pull_request_metadata", return_value=metadata), \
             mock.patch.object(collector, "run", side_effect=reject_candidate_ancestry), \
             self.assertRaises(collector.DiagnosticError):
            collector.verify_repository(
                Path("."), collector.REPOSITORY, 1945, base, candidate,
                "refs/heads/main", reference)

    def test_pr1945_metadata_rejects_invalid_json_and_malformed_shapes(self):
        for payload in (b"not-json", b"[]", b'{"base": [], "head": {}}',
                        b'{"base": {}, "head": []}',
                        b'{"base": {}, "head": {"repo": "wrong-shape"}}'):
            with self.subTest(payload=payload), \
                 mock.patch.object(collector.urllib.request, "urlopen",
                                   return_value=io.BytesIO(payload)), \
                 self.assertRaises(collector.DiagnosticError):
                collector.fetch_pull_request_metadata(collector.REPOSITORY, 1945)

    def test_pr1945_collection_reports_three_tree_totals_and_checksums(self):
        base, reference, candidate = "1" * 40, "2" * 40, "3" * 40
        args = collector.parse_args([
            "--repository", collector.REPOSITORY, "--pr-number", "1945",
            "--base-sha", base, "--reference-sha", reference,
            "--candidate-sha", candidate, "--workflow-ref", "refs/heads/main",
        ])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args.repository_root = str(ROOT)
            args.output = str(root / "artifacts")

            def fake_build(repo, source, sha, target, env, map_path=None, build_root=None):
                target.mkdir(parents=True, exist_ok=True)
                profile = target / "profile.json"
                profile.write_text(json.dumps({
                    "schema_version": 1, "source_sha": sha,
                    "options": {"optimization": "s", "b_lto": True},
                    "effective_link_arguments": [{"parameters": ["-shared", "-flto"]}],
                }), encoding="utf-8")
                library = target / "libwirelog.so"
                library.write_bytes(sha.encode())
                if map_path is not None:
                    map_path.write_text(" .text 0x1000 0x20 build/lib.p/a.o\n", encoding="utf-8")
                digest = collector.sha256_file(library)
                return {"build": build_root, "profile": profile, "library": library,
                        "map": map_path, "setup_command": ["meson", "setup"],
                        "compile_command": ["meson", "compile"], "binary_sha256": digest}

            def fake_run(command, **kwargs):
                if command[0] == "git" and command[1:3] == ["rev-parse", "HEAD"]:
                    return "9" * 40 + "\n"
                if command[0] == "gcc":
                    return "13.3.0\n"
                if command[0] == "ld":
                    return "GNU ld 2.42\n"
                if command[0] == "meson":
                    return "1.12.0\n"
                if command[0] == "ninja":
                    return "1.11.1\n"
                if command[0] == "readelf":
                    return "[ 1] .debug_info [ 2] .debug_line"
                return ""

            def fake_measure(repo, library, sha, profile, output, env):
                value = int(sha[0], 16) * 100 + 360000
                return {"measured_bytes": value, "source_sha": sha}

            with mock.patch.object(collector, "require_os_ubuntu_2404", return_value={"version_id": "24.04"}), \
                 mock.patch.object(collector, "verify_repository", return_value={"remote_main": base}), \
                 mock.patch.object(collector, "read_workflow_code_identity", return_value={"tool": "hash"}), \
                 mock.patch.object(collector, "read_policy_identity", return_value={"policy": "hash"}), \
                 mock.patch.object(collector, "run", side_effect=fake_run), \
                 mock.patch.object(collector, "extract_tree"), \
                 mock.patch.object(collector, "build_one", side_effect=fake_build), \
                 mock.patch.object(collector, "measure_library", side_effect=fake_measure), \
                 mock.patch.object(collector, "forensic_profile_matches", return_value=(True, [])), \
                 mock.patch.object(collector, "text_size", return_value=360000), \
                 mock.patch.object(collector, "map_text_objects", return_value=[
                     {"object": "lib.p/a.o", "text_bytes": 32}]), \
                 mock.patch.object(collector, "symbol_artifacts", side_effect=lambda *args, **kwargs: {"text_symbols": []}), \
                 mock.patch.object(collector, "map_text_sections", return_value=[]), \
                 mock.patch.object(collector, "lto_evidence", return_value={"enabled": True}), \
                 mock.patch.object(collector, "join_symbols_to_objects", return_value=[]), \
                 mock.patch.object(collector, "attach_source_locations"):
                self.assertEqual(collector.collect(args), 0)

            report = json.loads((Path(args.output) / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["authoritative"]["base"]["source_sha"], base)
            self.assertEqual(report["authoritative"]["reference"]["source_sha"], reference)
            self.assertEqual(report["authoritative"]["candidate"]["source_sha"], candidate)
            self.assertEqual(set(report["total_text_comparisons"]),
                             {"base_to_reference", "reference_to_candidate", "base_to_candidate"})
            self.assertTrue((Path(args.output) / "SHA256SUMS").is_file())

    def test_artifact_checksums_cover_report_and_map_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "report.json").write_text("report\n", encoding="utf-8")
            (root / "attribution-base.map").write_text("map\n", encoding="utf-8")
            collector.write_artifact_checksums(root)
            lines = (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
            self.assertEqual([line.split("  ", 1)[1] for line in lines],
                             ["attribution-base.map", "report.json"])
            for line in lines:
                digest, relative = line.split("  ", 1)
                self.assertEqual(digest, collector.sha256_file(root / relative))

    def test_build_environment_drops_credentials_and_inherited_flags(self):
        with mock.patch.dict(os.environ, {"GH_TOKEN": "secret", "GITHUB_TOKEN": "secret",
                                          "GITHUB_ENV": "/tmp/github-env", "GITHUB_OUTPUT": "/tmp/github-output",
                                          "LDFLAGS": "-Wl,unexpected", "CFLAGS": "-O0"}, clear=False):
            env = collector.clean_build_env()
        for key in collector.TOKEN_KEYS + ("GITHUB_ENV", "GITHUB_OUTPUT", "LDFLAGS", "CFLAGS", "CPPFLAGS"):
            self.assertNotIn(key, env)

    def test_profile_policy_guard_tracks_authoritative_configuration(self):
        identities = collector.read_policy_identity(ROOT)
        self.assertEqual(set(identities), set(collector.POLICY_FILES))
        self.assertTrue(all(len(digest) == 64 for digest in identities.values()))
        workflow_identities = collector.read_workflow_code_identity(ROOT)
        self.assertEqual(set(workflow_identities), set(collector.WORKFLOW_CODE_FILES))
        self.assertFalse(set(collector.POLICY_FILES) & set(collector.WORKFLOW_CODE_FILES))

    def test_measured_base_policy_does_not_require_diagnostic_code(self):
        with tempfile.TemporaryDirectory() as temp:
            base_tree = Path(temp)
            for relative in collector.POLICY_FILES:
                target = base_tree / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, target)
            self.assertEqual(set(collector.read_policy_identity(base_tree)), set(collector.POLICY_FILES))

    def test_measured_tree_policy_drift_is_reported(self):
        same = {"meson.build": "a", "scripts/ci/size-profile.py": "b"}
        self.assertEqual(collector.compare_policy_identities(same, dict(same), dict(same)), [])
        changed = dict(same)
        changed["meson.build"] = "candidate-change"
        self.assertEqual(collector.compare_policy_identities(same, dict(same), changed), ["meson.build"])

    def test_source_mapping_requires_dwarf_info_and_line_tables(self):
        self.assertTrue(collector.dwarf_mapping_available("[ 1] .debug_info [ 2] .debug_line"))
        self.assertTrue(collector.dwarf_mapping_available("[ 1] .zdebug_info [ 2] .zdebug_line"))
        self.assertFalse(collector.dwarf_mapping_available("[ 1] .debug_line"))
        self.assertFalse(collector.dwarf_mapping_available("no debug sections"))

    def test_decimal_nm_address_is_converted_to_correct_hex_addr2line_address(self):
        # GNU nm with --radix=d prints 4176 for the hexadecimal address 0x1050.
        self.assertEqual(collector.addr2line_address("4176"), "0x1050")

    def test_map_parser_aggregates_object_text_sections(self):
        with tempfile.TemporaryDirectory() as temp:
            map_file = Path(temp) / "sample.map"
            map_file.write_text(
                " .text 0x0000000000001000 0x20 build/a.o\n"
                " .text.foo 0x0000000000001020 0x10 build/a.o\n"
                " .text 0x0000000000001030 0x08 build/b.o\n", encoding="utf-8")
            self.assertEqual(collector.map_text_objects(map_file), [
                {"object": "build/a.o", "text_bytes": 48},
                {"object": "build/b.o", "text_bytes": 8},
            ])

    def test_map_parser_retains_half_open_ranges_and_ltrans_objects(self):
        with tempfile.TemporaryDirectory() as temp:
            map_file = Path(temp) / "sample.map"
            map_file.write_text(
                " .text.alpha 0x0000000000001000 0x10 build/lib.p/a.o\n"
                " .text.beta 0x0000000000002000 0x08 build/lib.p/ltrans0.ltrans.o\n",
                encoding="utf-8")
            self.assertEqual(collector.map_text_sections(map_file), [
                {"section": ".text.alpha", "start": 0x1000, "end": 0x1010,
                 "text_bytes": 0x10, "object": "build/lib.p/a.o"},
                {"section": ".text.beta", "start": 0x2000, "end": 0x2008,
                 "text_bytes": 0x08, "object": "build/lib.p/ltrans0.ltrans.o"},
            ])

    def test_symbols_join_only_to_unique_half_open_object_ranges(self):
        deltas = [{"object": "a.o", "delta_bytes": 12},
                  {"object": "b.o", "delta_bytes": -4}]
        sections = [
            {"start": 0x1000, "end": 0x1010, "text_bytes": 16, "object": "base/lib.p/a.o"},
            {"start": 0x1010, "end": 0x1020, "text_bytes": 16, "object": "base/lib.p/b.o"},
        ]
        symbols = [
            {"address": "4096", "size": 5, "kind": "T", "symbol": "first"},
            {"address": "4111", "size": 1, "kind": "T", "symbol": "last_in_a"},
            {"address": "4111", "size": 2, "kind": "T", "symbol": "crosses_a_boundary"},
            {"address": "4112", "size": 2, "kind": "T", "symbol": "first_in_b"},
            {"address": "8192", "size": 2, "kind": "T", "symbol": "unmapped"},
        ]
        result = collector.join_symbols_to_objects(deltas, sections, symbols)
        self.assertEqual(result[0]["status"], "available")
        self.assertEqual([row["symbol"] for row in result[0]["symbols"]], ["first", "last_in_a"])
        self.assertEqual(result[0]["boundary_symbol_count"], 1)
        self.assertEqual([row["symbol"] for row in result[1]["symbols"]], ["first_in_b"])
        self.assertEqual(result[1]["symbols"][0]["address_hex"], "0x1010")

    def test_zero_sized_symbol_at_section_end_is_not_attributed(self):
        result = collector.join_symbols_to_objects(
            [{"object": "a.o", "delta_bytes": 1}],
            [{"start": 0x1000, "end": 0x1010, "text_bytes": 16, "object": "lib.p/a.o"}],
            [{"address": str(0x1010), "size": 0, "kind": "T", "symbol": "at_end"}])
        self.assertEqual(result[0]["status"], "unavailable")
        self.assertEqual(result[0]["symbols"], [])

    def test_only_largest_changed_objects_receive_symbol_rows(self):
        deltas = [{"object": f"obj{index}.o", "delta_bytes": 20 - index}
                  for index in range(12)]
        sections = [{"start": 0x1000 + index * 0x10, "end": 0x1010 + index * 0x10,
                     "text_bytes": 16, "object": f"build/lib.p/obj{index}.o"}
                    for index in range(12)]
        symbols = [{"address": str(0x1000 + index * 0x10), "size": 4,
                    "kind": "T", "symbol": f"symbol{index}"} for index in range(12)]
        result = collector.join_symbols_to_objects(deltas, sections, symbols)
        self.assertEqual(len(result), 10)
        self.assertEqual(result[-1]["object"], "obj9.o")

    def test_symbol_join_marks_overlapping_object_ranges_ambiguous(self):
        result = collector.join_symbols_to_objects(
            [{"object": "a.o", "delta_bytes": 1}],
            [{"start": 0x1000, "end": 0x1020, "text_bytes": 32, "object": "x.p/a.o"},
             {"start": 0x1000, "end": 0x1020, "text_bytes": 32, "object": "y.p/b.o"}],
            [{"address": "4096", "size": 4, "kind": "T", "symbol": "shared"}])
        self.assertEqual(result[0]["status"], "ambiguous")
        self.assertEqual(result[0]["ambiguous_symbol_count"], 1)
        self.assertEqual(result[0]["symbols"], [])

    def test_object_source_rows_are_nested_and_explicitly_unavailable(self):
        item = {"object": "a.o", "status": "available", "reason": None,
                "symbols": [{"address_hex": "0x1000", "address": "4096", "size": 4,
                             "kind": "T", "symbol": "foo"}]}
        with mock.patch.object(collector, "run", return_value="foo()\nsrc/foo.c:12") as run_mock:
            collector.attach_source_locations(Path("libwirelog.so"), [item], {}, dwarf_available=True)
            self.assertEqual(item["source_mapping"]["status"], "available")
            self.assertEqual(item["source_mapping"]["records"][0]["source"], "src/foo.c:12")
            run_mock.assert_called_once()
        unavailable = {"object": "new.o", "reason": "no linker-map range", "symbols": []}
        collector.attach_source_locations(Path("libwirelog.so"), [unavailable], {}, dwarf_available=False)
        self.assertEqual(unavailable["source_mapping"]["status"], "unavailable")
        self.assertEqual(unavailable["source_mapping"]["reason"], "no linker-map range")

    def test_lto_and_inlining_evidence_are_explicit(self):
        with tempfile.TemporaryDirectory() as temp:
            profile = Path(temp) / "profile.json"
            profile.write_text(json.dumps({
                "options": {"b_lto": True},
                "effective_link_arguments": [{"parameters": ["-shared", "-flto=auto"]}],
            }), encoding="utf-8")
            sections = [{"object": "build/lib.p/ltrans0.ltrans.o", "text_bytes": 20}]
            evidence = collector.lto_evidence(profile, sections)
            self.assertTrue(evidence["enabled"])
            self.assertEqual(evidence["ltrans_objects"]["status"], "available")
            self.assertEqual(evidence["ltrans_objects"]["count"], 1)
            self.assertEqual(evidence["ltrans_objects"]["text_bytes"], 20)
            self.assertEqual(evidence["inlining_evidence"]["status"], "unavailable")
            self.assertTrue(evidence["inlining_evidence"]["reason"])

            profile.write_text(json.dumps({"options": {"b_lto": False},
                                           "effective_link_arguments": [{"parameters": []}]}),
                               encoding="utf-8")
            disabled = collector.lto_evidence(profile, [])
            self.assertFalse(disabled["enabled"])
            self.assertEqual(disabled["ltrans_objects"]["status"], "not-applicable")

            profile.write_text(json.dumps({"options": {"b_lto": True},
                                           "effective_link_arguments": [{"parameters": ["-flto"]}]}),
                               encoding="utf-8")
            unavailable = collector.lto_evidence(profile, [])
            self.assertEqual(unavailable["ltrans_objects"]["status"], "unavailable")
            self.assertTrue(unavailable["ltrans_objects"]["reason"])

    def test_per_object_delta_is_ranked_by_absolute_change(self):
        deltas = collector.object_text_deltas(
            [{"object": "/base/build/libwirelog.so.p/a.o", "text_bytes": 20},
             {"object": "/base/build/libwirelog.so.p/b.o", "text_bytes": 40}],
            [{"object": "/candidate/build/libwirelog.so.p/a.o", "text_bytes": 50},
             {"object": "/candidate/build/libwirelog.so.p/b.o", "text_bytes": 20}])
        self.assertEqual(deltas[0]["object"], "a.o")
        self.assertEqual(deltas[0]["delta_bytes"], 30)
        self.assertEqual({item["object"]: item["delta_bytes"] for item in deltas},
                         {"a.o": 30, "b.o": -20})

    def test_ltrans_partition_names_are_not_paired_as_object_deltas(self):
        deltas = collector.object_text_deltas(
            [{"object": "/base/lib.p/a.o", "text_bytes": 12},
             {"object": "/base/lib.p/ltrans0.ltrans.o", "text_bytes": 100}],
            [{"object": "/candidate/lib.p/a.o", "text_bytes": 15},
             {"object": "/candidate/lib.p/ltrans1.ltrans.o", "text_bytes": 120},
             {"object": "/candidate/lib.p/ltrans2.ltrans.o", "text_bytes": 40}])
        self.assertEqual(deltas, [{"object": "a.o", "base_text_bytes": 12,
                                   "candidate_text_bytes": 15, "delta_bytes": 3}])

    def test_size_profile_comparison_rejects_profile_mismatch(self):
        with tempfile.TemporaryDirectory() as temp:
            base, candidate = Path(temp) / "base.json", Path(temp) / "candidate.json"
            base.write_text(json.dumps({"schema_version": 1, "options": {"optimization": "s"}}), encoding="utf-8")
            candidate.write_text(json.dumps({"schema_version": 1, "options": {"optimization": "0"}}), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/ci/size-profile.py"), "compare", str(base), str(candidate)],
                text=True, encoding="utf-8", capture_output=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("production profile mismatch", result.stderr)

    def test_instrumented_binary_requires_both_hash_and_text_match(self):
        self.assertTrue(collector.attribution_matches("a", "a", 10, 10, "abc"))
        self.assertFalse(collector.attribution_matches("b", "a", 10, 10, "abc"))
        self.assertFalse(collector.attribution_matches("a", "a", 11, 10, "abc"))
        self.assertFalse(collector.attribution_matches("a", "a", 10, 10, None))

    def test_forensic_profile_allows_only_expected_map_argument(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            auth, forensic = root / "auth.json", root / "forensic.json"
            map_path = root / "candidate.map"
            base = {"schema_version": 1, "source_sha": "a" * 40,
                    "options": {"optimization": "s"},
                    "effective_link_arguments": [{"parameters": ["-shared", "-flto"]}]}
            changed = json.loads(json.dumps(base))
            changed["source_sha"] = "b" * 40
            changed["effective_link_arguments"][0]["parameters"].append(f"-Wl,-Map={map_path}")
            auth.write_text(json.dumps(base), encoding="utf-8")
            forensic.write_text(json.dumps(changed), encoding="utf-8")
            self.assertEqual(collector.forensic_profile_matches(auth, forensic, map_path), (True, []))

            changed["options"]["optimization"] = "0"
            forensic.write_text(json.dumps(changed), encoding="utf-8")
            matches, differences = collector.forensic_profile_matches(auth, forensic, map_path)
            self.assertFalse(matches)
            self.assertEqual(differences, ["options"])

            changed["options"]["optimization"] = "s"
            changed["effective_link_arguments"][0]["parameters"].append(f"-Wl,-Map={map_path}")
            forensic.write_text(json.dumps(changed), encoding="utf-8")
            matches, differences = collector.forensic_profile_matches(auth, forensic, map_path)
            self.assertFalse(matches)
            self.assertEqual(differences, ["expected exactly one linker map flag, found 2"])

    def test_forensic_profile_rejects_missing_or_unexpected_map_argument(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            auth, forensic = root / "auth.json", root / "forensic.json"
            map_path = root / "expected.map"
            base = {"schema_version": 1, "options": {"optimization": "s"},
                    "effective_link_arguments": [{"parameters": ["-shared"]}]}
            auth.write_text(json.dumps(base), encoding="utf-8")
            forensic.write_text(json.dumps(base), encoding="utf-8")
            self.assertFalse(collector.forensic_profile_matches(auth, forensic, map_path)[0])
            bad = json.loads(json.dumps(base))
            bad["effective_link_arguments"][0]["parameters"].append("-Wl,-Map=/wrong/path")
            forensic.write_text(json.dumps(bad), encoding="utf-8")
            self.assertFalse(collector.forensic_profile_matches(auth, forensic, map_path)[0])

    def test_os_guard_rejects_profile_drift(self):
        with mock.patch.object(collector.Path, "read_text", return_value='ID="ubuntu"\nVERSION_ID="22.04"\n'), \
             self.assertRaises(collector.DiagnosticError):
            collector.require_os_ubuntu_2404()

    def test_workflow_os_guard_accepts_quoted_and_unquoted_ubuntu_2404(self):
        helper = ROOT / "scripts/ci/require-ubuntu-2404.sh"
        valid_files = (
            'ID=ubuntu\nVERSION_ID=24.04\n',
            'ID="ubuntu"\nVERSION_ID="24.04"\n',
            'ID=ubuntu\nVERSION_ID="24.04"\n',
            'ID="ubuntu"\nVERSION_ID=24.04\n',
        )
        with tempfile.TemporaryDirectory() as temp:
            os_release = Path(temp) / "os-release"
            for contents in valid_files:
                with self.subTest(contents=contents):
                    os_release.write_text(contents, encoding="utf-8")
                    result = subprocess.run(
                        [str(helper), str(os_release)], text=True, encoding="utf-8",
                        capture_output=True, check=False)
                    self.assertEqual(result.returncode, 0, result.stderr)

    def test_workflow_os_guard_rejects_invalid_or_missing_identity_fields(self):
        helper = ROOT / "scripts/ci/require-ubuntu-2404.sh"
        invalid_files = (
            'ID=debian\nVERSION_ID=24.04\n',
            'ID=ubuntu\nVERSION_ID=22.04\n',
            'ID=ubuntu\nVERSION_ID=24.10\n',
            'VERSION_ID=24.04\n',
            'ID=ubuntu\n',
            'ID="ubuntu\nVERSION_ID=24.04\n',
            'ID=ubuntu; :\nVERSION_ID=24.04\n',
            'ID\nID=ubuntu\nVERSION_ID=24.04\n',
            'ID =ubuntu\nID=ubuntu\nVERSION_ID=24.04\n',
            'ID=ubuntu\nID=ubuntu\nVERSION_ID=24.04\n',
        )
        with tempfile.TemporaryDirectory() as temp:
            os_release = Path(temp) / "os-release"
            inherited_env = os.environ.copy()
            inherited_env.update(ID="ubuntu", VERSION_ID="24.04")
            for contents in invalid_files:
                with self.subTest(contents=contents):
                    os_release.write_text(contents, encoding="utf-8")
                    result = subprocess.run(
                        [str(helper), str(os_release)], env=inherited_env,
                        text=True, encoding="utf-8", capture_output=True, check=False)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("error:", result.stderr)

            missing_file = subprocess.run(
                [str(helper), str(os_release / "missing")], text=True,
                encoding="utf-8", capture_output=True, check=False)
            self.assertNotEqual(missing_file.returncode, 0)
            self.assertIn("cannot read OS release file", missing_file.stderr)

            marker = Path(temp) / "should-not-exist"
            os_release.write_text(
                f'ID=ubuntu\nVERSION_ID=24.04\nID_LIKE=debian\n: > {marker}\n',
                encoding="utf-8")
            result = subprocess.run(
                [str(helper), str(os_release)], text=True, encoding="utf-8",
                capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(marker.exists())

    def test_failure_report_survives_build_failure(self):
        args = collector.parse_args([
            "--repository", collector.REPOSITORY, "--pr-number", "1903",
            "--base-sha", collector.BASE_SHA, "--candidate-sha", "a4c7c622daa3b7247378acf71a74b7f40f4ff556",
            "--workflow-ref", "refs/heads/main",
        ])
        args.repository_root = str(ROOT)
        with tempfile.TemporaryDirectory() as temp:
            args.output = str(Path(temp) / "artifacts")
            tool_versions = {
                "gcc": "13.3.0\n", "ld": "GNU ld 2.42\n",
                "meson": "1.12.0\n", "ninja": "1.11.1\n",
            }

            def fake_run(command, **kwargs):
                if command[:3] == ["git", "rev-parse", "HEAD"]:
                    return "1" * 40 + "\n"
                for key, value in tool_versions.items():
                    if command[0] == key:
                        return value
                raise AssertionError(command)

            with mock.patch.object(collector, "require_os_ubuntu_2404", return_value={"version_id": "24.04"}), \
                 mock.patch.object(collector, "verify_repository", return_value={"origin": "test"}), \
                 mock.patch.object(collector, "read_policy_identity", return_value={"policy": "hash"}), \
                 mock.patch.object(collector, "run", side_effect=fake_run), \
                 mock.patch.object(collector, "extract_tree"), \
                 mock.patch.object(collector, "build_one", side_effect=collector.DiagnosticError("mock build failure")):
                self.assertEqual(collector.collect(args), 1)
            report = __import__("json").loads((Path(args.output) / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "failed")
            self.assertIn("mock build failure", report["error"])


if __name__ == "__main__":
    unittest.main()
