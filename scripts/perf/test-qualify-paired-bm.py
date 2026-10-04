#!/usr/bin/env python3
"""Contract tests for frozen two-campaign B/M eligibility interpretation."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

PATH = Path(__file__).with_name('qualify-paired-bm.py')
SPEC = importlib.util.spec_from_file_location('qualify_bm', PATH)
Q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(Q)
C = Q.COLLECTOR
TEST_TMP = Path.home() / '.cache' / 'wirelog-test-tmp'
TEST_TMP.mkdir(parents=True, exist_ok=True)


def entry(value):
    return dict(value=value, unavailable_reason=None)


def host_snapshot(throttle=4, pressure=0.2, load=1.0):
    return dict(machine_id=entry('test-host'), logical_cpu_count=entry(4), load_one=entry(load),
                cpu_psi_some_avg10_percent=entry(pressure),
                cgroup=entry(dict(version='v2', membership_path='/job',
                    mount_root='/', mountpoint='/sys/fs/cgroup',
                    levels=[dict(path='job', nr_throttled=throttle,
                                 throttle_time=0, throttle_time_unit='us',
                                 cpu_limit=dict(quota='max', period_us=100000))])))


def legacy_host_snapshot():
    return dict(time_utc='2026-10-05T00:00:00+00:00', loadavg='1 1 1',
                cpu_pressure='some avg10=0.00 avg60=0.00 avg300=0.00 total=0',
                cgroup_cpu_stat='usage_usec 0\nthrottled_usec 0', governor='performance',
                frequency_khz='1000000', frequency_min_khz='800000',
                frequency_max_khz='2000000')


class PairedBMTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=TEST_TMP)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.plan_path = self.root / 'plan.json'
        self.plan = dict(schema_version=1, plan_id='tagged-runner-bm-repeat-v1',
                         base_sha='a' * 40, candidate_sha='b' * 40,
                         base_tree_sha='c' * 40, candidate_tree_sha='d' * 40,
                         policy_id=Q.POLICY['policy_id'], policy_sha256=Q.POLICY_SHA256,
                         campaign_mode='comparison', workloads=list(Q.WORKLOADS),
                         attempts=['attempt-1', 'attempt-2'],
                         note='committed before collection')
        self.write(self.plan_path, self.plan)
        self.first = self.make_campaign('attempt-1')
        self.second = self.make_campaign('attempt-2')

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n', encoding='utf-8')

    def rewrite_raw_event(self, event_index, mutate):
        raw_path = self.first / 'raw-attempts.jsonl'
        raw_lines = raw_path.read_bytes().splitlines(keepends=True)
        raw = json.loads(raw_lines[event_index])
        mutate(raw)
        raw_lines[event_index] = (json.dumps(raw, sort_keys=True) + '\n').encode()
        raw_path.write_bytes(b''.join(raw_lines))
        link_path = self.first / 'bm-attempt-links.jsonl'
        links = [json.loads(line) for line in link_path.read_text(encoding='utf-8').splitlines()]
        links[event_index]['raw_line_sha256'] = hashlib.sha256(raw_lines[event_index]).hexdigest()
        link_path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in links),
                             encoding='utf-8')

    def replace_raw_line(self, event_index, value):
        raw_path = self.first / 'raw-attempts.jsonl'
        raw_lines = raw_path.read_bytes().splitlines(keepends=True)
        raw_lines[event_index] = (json.dumps(value, sort_keys=True) + '\n').encode()
        raw_path.write_bytes(b''.join(raw_lines))
        link_path = self.first / 'bm-attempt-links.jsonl'
        links = [json.loads(line) for line in link_path.read_text(encoding='utf-8').splitlines()]
        links[event_index]['raw_line_sha256'] = hashlib.sha256(raw_lines[event_index]).hexdigest()
        link_path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in links),
                             encoding='utf-8')

    def rewrite_preflight(self, mutate, root=None):
        path = (root or self.first) / 'preflight.json'
        preflight = json.loads(path.read_text(encoding='utf-8'))
        mutate(preflight)
        self.write(path, preflight)

    def rewrite_workload_result(self, workload, key, value, *, expected_value=None):
        campaign_path = self.first / 'campaign-v1.json'
        campaign = json.loads(campaign_path.read_text(encoding='utf-8'))
        if expected_value is not None:
            campaign['manifest']['workloads'][workload]['expected'][key] = expected_value
        manifest_sha = C['EVALUATOR']['digest'](campaign['manifest'])
        for event in campaign['attempts']:
            event['manifest_sha256'] = manifest_sha
            if event['workload'] == workload:
                event['correctness']['observed'][key] = value

        raw_path = self.first / 'raw-attempts.jsonl'
        raw_lines = []
        for line in raw_path.read_text(encoding='utf-8').splitlines():
            raw = json.loads(line)
            raw['manifest_sha256'] = manifest_sha
            if raw['workload'] == workload:
                if workload == 'crdt':
                    stdout = json.loads(raw['stdout'])
                    stdout[key] = value
                    raw['stdout'] = json.dumps(stdout) + '\n'
                else:
                    lines = raw['stdout'].splitlines()
                    fields = lines[1].split('\t')
                    fields[9 if key == 'tuples' else 10] = str(value)
                    lines[1] = '\t'.join(fields)
                    raw['stdout'] = '\n'.join(lines) + '\n'
            raw_lines.append((json.dumps(raw, sort_keys=True) + '\n').encode())
        raw_path.write_bytes(b''.join(raw_lines))

        links_path = self.first / 'bm-attempt-links.jsonl'
        links = [json.loads(line) for line in links_path.read_text(encoding='utf-8').splitlines()]
        for link, raw_line in zip(links, raw_lines):
            link['manifest_sha256'] = manifest_sha
            link['raw_line_sha256'] = hashlib.sha256(raw_line).hexdigest()
        links_path.write_text(''.join(json.dumps(link, sort_keys=True) + '\n'
                                      for link in links), encoding='utf-8')

        self.write(campaign_path, campaign)
        if expected_value is not None:
            preflight_path = self.first / 'preflight.json'
            preflight = json.loads(preflight_path.read_text(encoding='utf-8'))
            preflight['manifest'] = campaign['manifest']
            self.write(preflight_path, preflight)
        campaign_sha = hashlib.sha256(campaign_path.read_bytes()).hexdigest()
        evaluation = C['EVALUATOR']['evaluate'](campaign, campaign_sha)
        self.write(self.first / 'evaluation-report.json', evaluation)
        self.write(self.first / 'collection-status.json', dict(status=evaluation['status']))

    def make_campaign(self, label, *, pressure=0.2, correctness='OK'):
        root = self.root / label
        root.mkdir()
        policy_plan_sha = hashlib.sha256(self.plan_path.read_bytes()).hexdigest()
        raw_profile = {key: 'value' for key in C['VERIFY']['PROFILE_OPTIONS']}
        raw_profile['prefix'] = '/usr/local'
        profile = {key: C['profile_value'](value) for key, value in raw_profile.items()}
        sources = dict(base=self.plan['base_sha'], candidate=self.plan['candidate_sha'])
        records, paths, artifacts, provenance, binary_paths = {}, {}, {}, {}, {}
        fixture_all = {
            'bench/data/crdt/Insert_input.csv': '1' * 64,
            'bench/data/crdt/Remove_input.csv': '2' * 64,
            'bench/data/cspa/assign.csv': '3' * 64,
            'bench/data/cspa/dereference.csv': '4' * 64,
        }
        source_hashes = {name: '5' * 64 for name in C['VERIFY']['SAME_SOURCES'] +
                         C['VERIFY']['GATE_SOURCES']}
        timer_hashes = {name: source_hashes.get(name, '6' * 64)
                        for name in C['CONTRACT_SOURCES']}
        binaries_by_side = {}
        for side in C['SIDES']:
            build = root / f'{side}-build'
            build.mkdir()
            meson_info = build / 'meson-info'
            meson_info.mkdir()
            options_document = [dict(name=key, value=value)
                                for key, value in raw_profile.items()]
            compiler = dict(id='gcc', version='13.3.0', full_version='gcc 13.3.0',
                            linker_id='ld.bfd', exelist=['cc'])
            options_file = meson_info / 'intro-buildoptions.json'
            compilers_file = meson_info / 'intro-compilers.json'
            options_file.write_text(json.dumps(options_document), encoding='utf-8')
            compilers_file.write_text(json.dumps({'host': {'c': compiler}}), encoding='utf-8')
            artifacts[str(options_file)] = hashlib.sha256(options_file.read_bytes()).hexdigest()
            artifacts[str(compilers_file)] = hashlib.sha256(compilers_file.read_bytes()).hexdigest()
            build_log = root / f'{side}-build.log'
            build_log.write_text(f'{label} {side} build log\n', encoding='utf-8')
            log_sha = hashlib.sha256(build_log.read_bytes()).hexdigest()
            bins, binary_hashes = {}, {}
            for workload, name in (('crdt', 'tests/test_crdt_perf_gate'),
                                   ('cspa-fast', 'bench/bench_flowlog')):
                path = build / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(f'{label}:{side}:{workload}'.encode())
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                bins[workload] = str(path)
                binary_hashes[name] = digest
                artifacts[str(path)] = digest
            extra_binary = 'tests/test_cspa_perf_gate'
            extra_path = build / extra_binary
            extra_path.parent.mkdir(parents=True, exist_ok=True)
            extra_path.write_bytes(f'{label}:{side}:cspa-gate'.encode())
            binary_hashes[extra_binary] = hashlib.sha256(extra_path.read_bytes()).hexdigest()
            artifacts[str(extra_path)] = binary_hashes[extra_binary]
            artifacts[str(build_log)] = log_sha
            binary_paths[side] = bins
            binaries_by_side[side] = binary_hashes
            source_root = f'/sources/{side}'
            side_fixtures = {key: value for key, value in fixture_all.items()}
            for relative, digest in source_hashes.items():
                artifacts[str(Path(source_root) / relative)] = digest
            for relative, digest in timer_hashes.items():
                artifacts[str(Path(source_root) / relative)] = digest
            for relative, digest in fixture_all.items():
                artifacts[str(Path(source_root) / relative)] = digest
            records[side] = dict(source_sha=sources[side],
                source_tree=self.plan[f'{side}_tree_sha'], profile=raw_profile.copy(),
                compiler=compiler.copy(),
                source_sha256=source_hashes.copy(), fixture_sha256=side_fixtures,
                binary_sha256=binary_hashes.copy(), timer_contract_sha256=timer_hashes.copy())
            paths[side] = dict(source=source_root, build=str(build), log=str(build_log),
                               executables=bins)
            provenance[side] = dict(build_instance_id=f'{label}-{side}', source_sha=sources[side],
                                    profile=profile, build_log_sha256=log_sha)
        workload_specs = {}
        for workload in Q.WORKLOADS:
            prefix = 'bench/data/crdt/' if workload == 'crdt' else 'bench/data/cspa/'
            workload_specs[workload] = dict(
                binary_sha256={side: binaries_by_side[side]['tests/test_crdt_perf_gate'
                    if workload == 'crdt' else 'bench/bench_flowlog'] for side in C['SIDES']},
                fixture_sha256={side: {key: value for key, value in fixture_all.items()
                                       if key.startswith(prefix)} for side in C['SIDES']},
                timer=C['TIMERS'][workload], expected=C['EXPECTED'][workload])
        campaign_id = f'{label}-campaign'
        manifest = dict(sources=sources, profiles={side: profile.copy() for side in C['SIDES']},
                        build_provenance=provenance, host_id='test-host', cpu=1,
                        workloads=workload_specs)
        campaign = dict(schema_version=1, campaign_id=campaign_id, campaign_mode='comparison',
                        manifest=manifest,
                        blocks=[dict(block_id='AB', order='AB'), dict(block_id='BA', order='BA')],
                        attempts=[])
        manifest_sha = C['EVALUATOR']['digest'](manifest)
        raw_lines, link_lines = [], []
        for ordinal, item in enumerate(C['schedule']()):
            elapsed_value = 100.0 if item['side'] == 'base' else 103.0
            if item['workload'] == 'crdt':
                record = dict(schema_version=1, measurement='crdt_perf_gate_single_run',
                    workload='crdt', fixture='full', workers=1, expected=104851,
                    elapsed_ms=elapsed_value, result=104851, aggregate=2152328, iterations=14148,
                    status=correctness)
                stdout = json.dumps(record) + '\n'
            else:
                status = 'OK' if correctness == 'OK' else 'FAIL'
                stdout = (C['LEGACY']['HEADER'] +
                    f'\ncspa\t-\t-\t1\t1\t{elapsed_value}\t{elapsed_value}\t{elapsed_value}\t100\t20381\t6\t{status}\n')
            elapsed, result = C['parse_result'](stdout, item['workload'])
            raw = dict(item, ordinal=ordinal, campaign_id=campaign_id,
                manifest_sha256=manifest_sha, stdout=stdout, stderr='', exit_code=0,
                command=['fixture'], timed_out=False,
                host_before=legacy_host_snapshot(), host_after=legacy_host_snapshot(),
                bm_host_before=host_snapshot(pressure=pressure),
                bm_host_after=host_snapshot(pressure=pressure))
            raw_line = (json.dumps(raw, sort_keys=True) + '\n').encode()
            raw_lines.append(raw_line)
            link_lines.append(json.dumps(dict(ordinal=ordinal, campaign_id=campaign_id,
                manifest_sha256=manifest_sha, block_id=item['block_id'],
                workload=item['workload'], sequence=item['sequence'], side=item['side'],
                raw_line_sha256=hashlib.sha256(raw_line).hexdigest()), sort_keys=True) + '\n')
            campaign['attempts'].append(dict(item, campaign_id=campaign_id,
                manifest_sha256=manifest_sha, elapsed_ms=elapsed, correctness=result,
                exit_code=0, timed_out=False, host_before=C['telemetry'](raw['host_before']),
                host_after=C['telemetry'](raw['host_after'])))
        campaign_path = root / 'campaign-v1.json'
        self.write(campaign_path, campaign)
        raw_path = root / 'raw-attempts.jsonl'
        raw_path.write_bytes(b''.join(raw_lines))
        (root / 'bm-attempt-links.jsonl').write_text(''.join(link_lines), encoding='utf-8')
        campaign_bytes = campaign_path.read_bytes()
        evaluation = C['EVALUATOR']['evaluate'](campaign, hashlib.sha256(campaign_bytes).hexdigest())
        self.write(root / 'evaluation-report.json', evaluation)
        self.write(root / 'collection-status.json', dict(status=evaluation['status']))
        preflight = dict(schema_version=1, mode='comparison', qualify_aa=False,
            qualify_bm=True, bm_plan_sha256=policy_plan_sha, campaign_id=campaign_id,
            manifest=manifest, records=records, paths=paths, artifacts=artifacts,
            schedule=C['schedule']())
        self.write(root / 'preflight.json', preflight)
        for side in C['SIDES']:
            (root / f'{side}-build.log').write_bytes((root / f'{side}-build.log').read_bytes())
            build = Path(paths[side]['build'])
            (root / f'{side}-intro-buildoptions.json').write_bytes(
                (build / 'meson-info/intro-buildoptions.json').read_bytes())
            (root / f'{side}-intro-compilers.json').write_bytes(
                (build / 'meson-info/intro-compilers.json').read_bytes())
        return root

    def test_two_complete_campaigns_are_interpretation_eligible(self):
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'ELIGIBLE')
        self.assertEqual(report['eligibility'], 'interpretation only; never a performance PASS')
        self.assertEqual(report['cspa_absolute_gate'], 'not evaluated by this diagnostic qualifier')

    def test_missing_second_campaign_is_inconclusive(self):
        report = Q.qualify(self.plan_path, self.first, self.root / 'missing')
        self.assertEqual(report['status'], 'INCONCLUSIVE')

    def test_missing_collection_status_is_inconclusive(self):
        (self.first / 'collection-status.json').unlink()
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INCONCLUSIVE')

    def test_non_object_collection_status_is_invalid(self):
        for value in (None, [], 'status', 7, True):
            with self.subTest(value=value):
                self.write(self.first / 'collection-status.json', value)
                report = Q.qualify(self.plan_path, self.first, self.second)
                self.assertEqual(report['status'], 'INVALID_EVIDENCE', report)

    def test_non_object_raw_jsonl_records_are_invalid_even_with_matching_link(self):
        raw_path = self.first / 'raw-attempts.jsonl'
        link_path = self.first / 'bm-attempt-links.jsonl'
        valid_raw = raw_path.read_bytes()
        valid_links = link_path.read_bytes()
        for value in (None, [], 'raw', 7, True):
            with self.subTest(value=value):
                self.replace_raw_line(0, value)
                report = Q.qualify(self.plan_path, self.first, self.second)
                self.assertEqual(report['status'], 'INVALID_EVIDENCE', report)
                raw_path.write_bytes(valid_raw)
                link_path.write_bytes(valid_links)

    def test_malformed_raw_record_fields_are_invalid(self):
        raw_path = self.first / 'raw-attempts.jsonl'
        link_path = self.first / 'bm-attempt-links.jsonl'
        valid_raw = raw_path.read_bytes()
        valid_links = link_path.read_bytes()
        cases = (
            ('stdout', lambda raw: raw.update(stdout=[])),
            ('legacy_host_scalar', lambda raw: raw['host_before'].update(loadavg=[])),
            ('boolean_exit_code', lambda raw: raw.update(exit_code=False)),
            ('boolean_ordinal_and_sequence',
             lambda raw: raw.update(ordinal=False, sequence=False)),
            ('boolean_pair_index', lambda raw: raw.update(pair_index=True)),
            ('string_prelaunch_flag', lambda raw: raw.update(prelaunch_rejected='yes')),
            ('non_string_command', lambda raw: raw.update(command=['fixture', 1])),
            ('integer_timed_out', lambda raw: raw.update(timed_out=0)),
            ('non_object_bm_snapshot', lambda raw: raw.update(bm_host_before=[])),
        )
        for name, mutate in cases:
            with self.subTest(name=name):
                raw_path.write_bytes(valid_raw)
                link_path.write_bytes(valid_links)
                self.rewrite_raw_event(0, mutate)
                report = Q.qualify(self.plan_path, self.first, self.second)
                self.assertEqual(report['status'], 'INVALID_EVIDENCE', report)
        raw_path.write_bytes(valid_raw)
        link_path.write_bytes(valid_links)

    def test_missing_raw_required_field_is_inconclusive(self):
        self.rewrite_raw_event(0, lambda raw: raw.pop('stdout'))
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual((report['status'], report['reason']),
                         ('INCONCLUSIVE', 'missing_evidence'))

    def test_host_pressure_is_inconclusive(self):
        pressured = self.make_campaign('pressured', pressure=10.1)
        report = Q.qualify(self.plan_path, pressured, self.second)
        self.assertEqual((report['status'], report['reason']),
                         ('INCONCLUSIVE', 'eligibility_criteria_not_met'))

    def test_tampered_raw_record_is_invalid(self):
        path = self.first / 'raw-attempts.jsonl'
        raw = json.loads(path.read_text(encoding='utf-8').splitlines()[0])
        raw['stdout'] = raw['stdout'].replace('104851', '104850')
        lines = path.read_text(encoding='utf-8').splitlines()
        lines[0] = json.dumps(raw, sort_keys=True)
        path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INVALID_EVIDENCE')

    def test_correctness_failure_is_distinct(self):
        broken = self.make_campaign('broken', correctness='FAIL')
        report = Q.qualify(self.plan_path, broken, self.second)
        self.assertEqual(report['status'], 'CORRECTNESS_FAILURE')

    def test_pinned_expected_contract_rejects_recomputed_crdt_and_cspa_tampering(self):
        for workload, key, value in (('crdt', 'result', 104850),
                                     ('cspa-fast', 'tuples', 20380)):
            with self.subTest(workload=workload):
                self.rewrite_workload_result(workload, key, value, expected_value=value)
                report = Q.qualify(self.plan_path, self.first, self.second)
                self.assertEqual(report['status'], 'INVALID_EVIDENCE', report)
                self.first = self.make_campaign(f'reset-{workload}')

    def test_wrong_observation_under_pinned_expected_contract_is_correctness_failure(self):
        self.rewrite_workload_result('crdt', 'result', 104850)
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'CORRECTNESS_FAILURE', report)

    def test_missing_required_host_telemetry_is_inconclusive(self):
        self.rewrite_raw_event(0, lambda raw: raw['bm_host_before'].pop(
            'cpu_psi_some_avg10_percent'))
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual((report['status'], report['reason']),
                         ('INCONCLUSIVE', 'missing_evidence'))

    def test_present_malformed_host_snapshot_is_invalid(self):
        self.rewrite_raw_event(0, lambda raw: raw.update(bm_host_before=[]))
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INVALID_EVIDENCE')

    def test_absent_host_snapshot_is_inconclusive(self):
        self.rewrite_raw_event(0, lambda raw: raw.pop('bm_host_after'))
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual((report['status'], report['reason']),
                         ('INCONCLUSIVE', 'missing_evidence'))

    def test_frozen_plan_requires_exact_policy_digest_and_source_pair(self):
        C['require_bm_plan'](self.plan, self.plan['base_sha'], self.plan['candidate_sha'])
        changed = dict(self.plan, policy_sha256='0' * 64)
        with self.assertRaises(ValueError):
            C['require_bm_plan'](changed, self.plan['base_sha'], self.plan['candidate_sha'])

    def test_nonzero_process_with_missing_typed_record_is_correctness_failure(self):
        raw_path = self.first / 'raw-attempts.jsonl'
        raw_lines = raw_path.read_bytes().splitlines(keepends=True)
        raw = json.loads(raw_lines[0])
        raw['exit_code'] = 1
        raw_lines[0] = (json.dumps(raw, sort_keys=True) + '\n').encode()
        raw_path.write_bytes(b''.join(raw_lines))
        link_path = self.first / 'bm-attempt-links.jsonl'
        links = [json.loads(line) for line in link_path.read_text(encoding='utf-8').splitlines()]
        links[0]['raw_line_sha256'] = hashlib.sha256(raw_lines[0]).hexdigest()
        link_path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in links),
                             encoding='utf-8')
        campaign_path = self.first / 'campaign-v1.json'
        campaign = json.loads(campaign_path.read_text(encoding='utf-8'))
        campaign['attempts'].pop(0)
        self.write(campaign_path, campaign)
        evaluation = C['EVALUATOR']['evaluate'](
            campaign, hashlib.sha256(campaign_path.read_bytes()).hexdigest())
        self.write(self.first / 'evaluation-report.json', evaluation)
        self.write(self.first / 'collection-status.json', dict(status=evaluation['status']))
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'CORRECTNESS_FAILURE')

    def test_malformed_nested_preflight_record_is_invalid(self):
        self.rewrite_preflight(lambda data: data['records'].update(base=[]))
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INVALID_EVIDENCE')

    def test_source_tree_hash_tampering_is_invalid(self):
        self.rewrite_preflight(lambda data: data['records']['base'].update(source_tree='0' * 40))
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INVALID_EVIDENCE')

    def test_source_inventory_digest_mismatch_is_invalid(self):
        def tamper(data):
            data['records']['base']['source_sha256']['bench/bench_flowlog.c'] = '0' * 64
        self.rewrite_preflight(tamper)
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INVALID_EVIDENCE')

    def test_timer_contract_inventory_binds_bench_util_source(self):
        def remove_bench_util(data):
            source = Path(data['paths']['base']['source'])
            data['artifacts'].pop(str((source / 'bench/bench_util.h').resolve()))
        self.rewrite_preflight(remove_bench_util)
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INVALID_EVIDENCE')

    def test_cross_campaign_timer_contract_must_match(self):
        def change_both_sides(data):
            for side in C['SIDES']:
                source = Path(data['paths'][side]['source'])
                path = str((source / 'bench/bench_util.h').resolve())
                data['records'][side]['timer_contract_sha256']['bench/bench_util.h'] = '0' * 64
                data['artifacts'][path] = '0' * 64
        self.rewrite_preflight(change_both_sides, root=self.second)
        report = Q.qualify(self.plan_path, self.first, self.second)
        self.assertEqual(report['status'], 'INVALID_EVIDENCE')

    def test_collector_output_with_full_profile_and_all_binaries_is_eligible(self):
        root = self.root / 'collector-round-trip'
        root.mkdir()
        sources = {}
        source_files = set(C['VERIFY']['SAME_SOURCES'] + C['VERIFY']['GATE_SOURCES'] +
                           C['VERIFY']['FIXTURES'] + C['CONTRACT_SOURCES'])
        base_source = root / 'source-base'
        base_source.mkdir()
        for relative in source_files:
            path = base_source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            contents = b'"WIRELOG_CRDT_PROBE"\n' if relative == 'tests/test_crdt_perf_gate.c' else b'fixture\n'
            path.write_bytes(contents)
        (base_source / 'README.md').write_text('base\n', encoding='utf-8')

        def git(*args, cwd=None):
            subprocess.run(['git', *args], cwd=cwd, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        git('init', '--quiet', '--initial-branch=main', cwd=base_source)
        git('config', 'user.name', 'Fixture', cwd=base_source)
        git('config', 'user.email', 'fixture@example.test', cwd=base_source)
        git('add', '-A', cwd=base_source)
        git('commit', '--quiet', '-m', 'base fixture', cwd=base_source)
        base_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=base_source,
                                           encoding='utf-8').strip()
        candidate_source = root / 'source-candidate'
        subprocess.run(['git', 'clone', '--quiet', str(base_source), str(candidate_source)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        git('config', 'user.name', 'Fixture', cwd=candidate_source)
        git('config', 'user.email', 'fixture@example.test', cwd=candidate_source)
        (candidate_source / 'README.md').write_text('candidate\n', encoding='utf-8')
        git('add', 'README.md', cwd=candidate_source)
        git('commit', '--quiet', '-m', 'candidate fixture', cwd=candidate_source)
        candidate_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'],
                                                cwd=candidate_source,
                                                encoding='utf-8').strip()
        source_paths = dict(base=base_source, candidate=candidate_source)
        plan = dict(schema_version=1, plan_id='tagged-runner-bm-repeat-v1',
            base_sha=base_sha, candidate_sha=candidate_sha,
            policy_id=Q.POLICY['policy_id'], policy_sha256=Q.POLICY_SHA256,
            campaign_mode='comparison', workloads=list(Q.WORKLOADS),
            attempts=['attempt-1', 'attempt-2'], note='test plan',
            base_tree_sha=subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'],
                cwd=base_source, encoding='utf-8').strip(),
            candidate_tree_sha=subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'],
                cwd=candidate_source, encoding='utf-8').strip())
        plan_path = root / 'plan.json'
        self.write(plan_path, plan)
        options = dict(buildtype='release', optimization='2', b_lto=True,
            b_sanitize='none', c_args=[], c_link_args=[],
            wirelog_log_max_level='trace', tests=True, mbedTLS='disabled',
            threads='native', prefix='/usr/local')
        compiler = dict(id='gcc', version='13.3.0', full_version='gcc 13.3.0',
                        linker_id='ld.bfd', exelist=['cc'])
        builds, logs = {}, {}
        for attempt in ('attempt-1', 'attempt-2'):
            builds[attempt], logs[attempt] = {}, {}
            for side in C['SIDES']:
                build = root / f'{attempt}-{side}-build'
                meson_info = build / 'meson-info'
                meson_info.mkdir(parents=True)
                (meson_info / 'intro-buildoptions.json').write_text(
                    json.dumps([dict(name=name, value=value) for name, value in options.items()]),
                    encoding='utf-8')
                (meson_info / 'intro-compilers.json').write_text(
                    json.dumps({'host': {'c': compiler}}), encoding='utf-8')
                for relative in C['VERIFY']['BINARIES']:
                    binary = build / relative
                    binary.parent.mkdir(parents=True, exist_ok=True)
                    binary.write_bytes(f'{relative}:{side}'.encode())
                    binary.chmod(0o755)
                build_log = root / f'{attempt}-{side}.log'
                build_log.write_text(f'{attempt} {side} build log\n', encoding='utf-8')
                builds[attempt][side] = build
                logs[attempt][side] = build_log

        machine_id = Path('/etc/machine-id').read_text(encoding='utf-8').strip()
        host = host_snapshot()
        host['machine_id'] = entry(machine_id)
        collector_globals = C['collect_campaign'].__globals__
        original_plan_guard = collector_globals['require_committed_bm_plan']
        original_execute = collector_globals['execute']
        collector_globals['require_committed_bm_plan'] = lambda _path: None

        def execute(binary, workload, data_root, cpu, timeout, *, qualify_bm=False, **kwargs):
            side = 'candidate' if 'candidate-build' in str(binary) else 'base'
            elapsed = 100.0 if side == 'base' else 102.0
            if workload == 'crdt':
                stdout = json.dumps(dict(schema_version=1,
                    measurement='crdt_perf_gate_single_run', workload='crdt', fixture='full',
                    workers=1, expected=104851, elapsed_ms=elapsed, result=104851,
                    aggregate=2152328, iterations=14148, status='OK')) + '\n'
            else:
                stdout = (C['LEGACY']['HEADER'] +
                    f'\ncspa\t-\t-\t1\t1\t{elapsed}\t{elapsed}\t{elapsed}\t100\t20381\t6\tOK\n')
            return dict(command=['fixture'], host_before=legacy_host_snapshot(),
                host_after=legacy_host_snapshot(), timed_out=False,
                stdout=stdout, stderr='', exit_code=0,
                bm_host_before=json.loads(json.dumps(host)),
                bm_host_after=json.loads(json.dumps(host)))

        collector_globals['execute'] = execute
        try:
            outputs = []
            for attempt in ('attempt-1', 'attempt-2'):
                cpu = min(os.sched_getaffinity(0))
                args = type('CollectorArgs', (), {})()
                args.mode = 'comparison'
                args.base_source = source_paths['base']
                args.candidate_source = source_paths['candidate']
                args.base_build = builds[attempt]['base']
                args.candidate_build = builds[attempt]['candidate']
                args.base_build_log = logs[attempt]['base']
                args.candidate_build_log = logs[attempt]['candidate']
                args.base_sha = base_sha
                args.candidate_sha = candidate_sha
                args.out_dir = root / f'{attempt}-evidence'
                args.cpu = cpu
                args.timeout = 10
                args.qualify_aa = False
                args.qualify_bm = True
                args.bm_plan = plan_path
                self.assertEqual(C['collect_campaign'](args), 0)
                outputs.append(args.out_dir)
        finally:
            collector_globals['require_committed_bm_plan'] = original_plan_guard
            collector_globals['execute'] = original_execute
        report = Q.qualify(plan_path, outputs[0], outputs[1])
        self.assertEqual(report['status'], 'ELIGIBLE', report)
        preflight = json.loads((outputs[0] / 'preflight.json').read_text(encoding='utf-8'))
        self.assertIn('prefix', preflight['records']['base']['profile'])
        for side in C['SIDES']:
            for relative in C['VERIFY']['BINARIES']:
                evidence_path = str((builds['attempt-1'][side] / relative).resolve())
                self.assertIn(evidence_path, preflight['artifacts'])


if __name__ == '__main__':
    unittest.main()
