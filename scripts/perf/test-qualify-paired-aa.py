#!/usr/bin/env python3
"""Tests for protected-main paired A/A qualification evidence."""
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import tempfile
import unittest

PATH = Path(__file__).with_name('qualify-paired-aa.py')
SPEC = importlib.util.spec_from_file_location('qualify_aa', PATH)
Q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(Q)
COLLECTOR = Q.COLLECTOR
TEST_TMP = Path.home() / '.cache' / 'wirelog-test-tmp'
TEST_TMP.mkdir(parents=True, exist_ok=True)


def snapshot(cpu, tick, *, governor='performance', some=0.2, throttle=0):
    entry = lambda value, unit=None: dict(value=value, unavailable_reason=None, unit=unit)
    return dict(timestamp_utc='2026-10-04T00:00:00+00:00', monotonic_ns=tick,
                selected_cpu=cpu,
                machine_id=entry('test-machine'),
                collector_affinity=entry([cpu, cpu + 1], 'cpu_ids'),
                child_affinity=entry([cpu], 'cpu_ids'),
                governor=entry(governor),
                cpu_psi_some_avg10_percent=entry(some, 'percent'),
                cpu_psi_full_avg10_percent=entry(0.01, 'percent'),
                memory_psi_full_avg10_percent=entry(0.01, 'percent'),
                swap_pages=entry(dict(pswpin=0, pswpout=0), 'pages'),
                cgroup=entry(dict(version='v2', membership_path='/job', mount_root='/',
                    mountpoint='/sys/fs/cgroup',
                    levels=[dict(path='job', cpu_limit=dict(quota='max', period_us=100000),
                                 nr_throttled=throttle, throttle_time=0,
                                 throttle_time_unit='us')])) )


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=TEST_TMP)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.evidence = self.root / 'evidence'
        self.evidence.mkdir()
        self.preflight_path = self.root / 'preflight.json'
        self.cpu = 1
        build_path = self.root / 'build'
        build_path.mkdir()
        log_path = self.root / 'build.log'
        log_path.write_text('fixture build log\n', encoding='utf-8')
        binary_hashes = {}
        for workload in Q.WORKLOADS:
            binary = build_path / workload
            binary.write_text(f'{workload} binary fixture\n', encoding='utf-8')
            binary_hashes[workload] = hashlib.sha256(binary.read_bytes()).hexdigest()
        log_hash = hashlib.sha256(log_path.read_bytes()).hexdigest()
        raw_profile = dict(buildtype='release', compiler='gcc')
        profile = {name: COLLECTOR['profile_value'](value)
                   for name, value in raw_profile.items()}
        sources = dict(base='a' * 40, candidate='a' * 40)
        provenance = {side: dict(build_instance_id='shared-build', source_sha=sources[side],
                                 profile=profile, build_log_sha256=log_hash)
                      for side in Q.SIDES}
        workloads = {}
        for name in Q.WORKLOADS:
            workloads[name] = dict(binary_sha256={side: binary_hashes[name] for side in Q.SIDES},
                                   fixture_sha256={side: {'fixture.csv': 'd' * 64}
                                                   for side in Q.SIDES},
                                   timer=COLLECTOR['TIMERS'][name],
                                   expected=COLLECTOR['EXPECTED'][name])
        manifest = dict(sources=sources, profiles={side: profile.copy() for side in Q.SIDES},
                        workloads=workloads, host_id='test-machine', cpu=self.cpu,
                        build_provenance=provenance)
        campaign_id = 'test-campaign'
        campaign = dict(schema_version=1, campaign_id=campaign_id, campaign_mode='aa_control',
                        manifest=manifest,
                        blocks=[dict(block_id='AB', order='AB'), dict(block_id='BA', order='BA')],
                        attempts=[])
        manifest_sha = COLLECTOR['EVALUATOR']['digest'](manifest)
        planned = COLLECTOR['schedule']()
        raw_lines = []
        links = []
        for ordinal, item in enumerate(planned):
            if item['workload'] == 'crdt':
                correctness = COLLECTOR['EXPECTED']['crdt']
                record = dict(schema_version=1, measurement='crdt_perf_gate_single_run', workload='crdt',
                              fixture='full', workers=1, expected=104851, result=104851,
                              aggregate=2152328, iterations=14148, status='OK', elapsed_ms=10)
                stdout = json.dumps(record) + '\n'
            else:
                correctness = COLLECTOR['EXPECTED']['cspa-fast']
                stdout = (COLLECTOR['LEGACY']['HEADER'] +
                          '\ncspa\t-\t-\t1\t1\t2\t2\t2\t100\t20381\t6\tOK\n')
            elapsed, parsed = COLLECTOR['parse_result'](stdout, item['workload'])
            tick = ordinal * 10 + 1
            raw = dict(item, ordinal=ordinal, campaign_id=campaign_id,
                       manifest_sha256=manifest_sha, stdout=stdout, stderr='', exit_code=0,
                       timed_out=False, host_before=dict(), host_after=dict(),
                       process_group_id=ordinal + 100,
                       launcher_ready=dict(pid=ordinal + 100, affinity=[self.cpu]),
                       aa_host_before=snapshot(self.cpu, tick),
                       aa_host_after=snapshot(self.cpu, tick + 1))
            raw['aa_host_after']['child_affinity']['observation'] = (
                'carried_forward_from_verified_preexec_affinity')
            line = (json.dumps(raw, sort_keys=True) + '\n').encode()
            raw_lines.append(line)
            links.append(dict(ordinal=ordinal, campaign_id=campaign_id,
                              manifest_sha256=manifest_sha, block_id=item['block_id'],
                              workload=item['workload'], sequence=item['sequence'], side=item['side'],
                              raw_line_sha256=hashlib.sha256(line).hexdigest()))
            campaign['attempts'].append(dict(item, campaign_id=campaign_id,
                manifest_sha256=manifest_sha, elapsed_ms=elapsed, correctness=parsed,
                exit_code=0, timed_out=False,
                host_before=COLLECTOR['telemetry']({}), host_after=COLLECTOR['telemetry']({})))
        (self.evidence / 'campaign-v1.json').write_text(json.dumps(campaign), encoding='utf-8')
        (self.evidence / 'raw-attempts.jsonl').write_bytes(b''.join(raw_lines))
        (self.evidence / 'aa-attempt-links.jsonl').write_text(
            ''.join(json.dumps(link, sort_keys=True) + '\n' for link in links), encoding='utf-8')
        paths = {side: dict(source='/same/source', build=str(build_path), log=str(log_path),
                            executables={name: str(build_path / name) for name in Q.WORKLOADS})
                 for side in Q.SIDES}
        artifacts = {str(build_path / name): binary_hashes[name] for name in Q.WORKLOADS}
        artifacts[str(log_path)] = log_hash
        records = {side: dict(source_sha=sources[side], profile=raw_profile.copy())
                   for side in Q.SIDES}
        preflight = dict(schema_version=1, mode='aa_control', qualify_aa=True,
                         campaign_id=campaign_id, manifest=manifest, paths=paths, records=records,
                         artifacts=artifacts, schedule=planned,
                         launcher_sha256=COLLECTOR['LEGACY']['sha256'](Q.HERE / 'paired-aa-affinity-launcher.py'))
        self.preflight_path.write_text(json.dumps(preflight), encoding='utf-8')

    def test_stable_same_binary_campaign_passes_deterministically(self):
        one = Q.qualify(self.evidence, self.preflight_path)
        two = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual(one, two)
        self.assertEqual(one['status'], 'PASS')
        self.assertEqual(one['reason'], 'both_workloads_within_noise_floor')
        self.assertEqual(one['host']['checked_attempts'], 80)
        for result in one['workloads'].values():
            self.assertEqual(result['median_log_ratio'], 0)
            self.assertLessEqual(result['ci_half_width'], 0.05)

    def test_raw_sidecar_tampering_is_inconclusive(self):
        path = self.evidence / 'raw-attempts.jsonl'
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]['stdout'] = rows[0]['stdout'].replace('"elapsed_ms": 10', '"elapsed_ms": 11')
        path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']), ('INCONCLUSIVE', 'invalid_evidence'))

    def test_boolean_cpu_and_affinity_values_are_invalid_evidence(self):
        raw_path = self.evidence / 'raw-attempts.jsonl'
        links_path = self.evidence / 'aa-attempt-links.jsonl'
        original_raw = raw_path.read_bytes()
        original_links = links_path.read_bytes()
        corruptions = (
            ('selected_cpu', lambda row: row['aa_host_before'].__setitem__('selected_cpu', True)),
            ('child_affinity_before', lambda row: row['aa_host_before']['child_affinity'].__setitem__(
                'value', [True])),
            ('child_affinity_after', lambda row: row['aa_host_after']['child_affinity'].__setitem__(
                'value', [True])),
            ('launcher_affinity', lambda row: row['launcher_ready'].__setitem__('affinity', [True])),
        )
        for name, corrupt in corruptions:
            with self.subTest(corruption=name):
                rows = [json.loads(line) for line in original_raw.splitlines()]
                links = [json.loads(line) for line in original_links.splitlines()]
                corrupt(rows[0])
                encoded = (json.dumps(rows[0], sort_keys=True) + '\n').encode()
                links[0]['raw_line_sha256'] = hashlib.sha256(encoded).hexdigest()
                raw_path.write_bytes(encoded + b''.join(
                    (json.dumps(row, sort_keys=True) + '\n').encode() for row in rows[1:]))
                links_path.write_text(
                    ''.join(json.dumps(link, sort_keys=True) + '\n' for link in links),
                    encoding='utf-8')
                result = Q.qualify(self.evidence, self.preflight_path)
                self.assertEqual((result['status'], result['reason']),
                                 ('INCONCLUSIVE', 'invalid_evidence'))
        raw_path.write_bytes(original_raw)
        links_path.write_bytes(original_links)

    def test_raw_schedule_fields_are_bound_to_the_planned_schedule(self):
        raw_path = self.evidence / 'raw-attempts.jsonl'
        links_path = self.evidence / 'aa-attempt-links.jsonl'
        original_raw = raw_path.read_bytes()
        original_links = links_path.read_bytes()
        corruptions = (
            ('sequence_boolean', 0, lambda row: row.__setitem__('sequence', False)),
            ('pair_index_boolean', 4, lambda row: row.__setitem__('pair_index', False)),
            ('wrong_phase', 0, lambda row: row.__setitem__('phase', 'sample')),
        )
        for name, ordinal, corrupt in corruptions:
            with self.subTest(corruption=name):
                rows = [json.loads(line) for line in original_raw.splitlines()]
                links = [json.loads(line) for line in original_links.splitlines()]
                corrupt(rows[ordinal])
                encoded = (json.dumps(rows[ordinal], sort_keys=True) + '\n').encode()
                links[ordinal]['raw_line_sha256'] = hashlib.sha256(encoded).hexdigest()
                raw_path.write_bytes(b''.join(
                    (json.dumps(row, sort_keys=True) + '\n').encode() for row in rows))
                links_path.write_text(
                    ''.join(json.dumps(link, sort_keys=True) + '\n' for link in links),
                    encoding='utf-8')
                result = Q.qualify(self.evidence, self.preflight_path)
                self.assertEqual((result['status'], result['reason']),
                                 ('INCONCLUSIVE', 'invalid_evidence'))
        raw_path.write_bytes(original_raw)
        links_path.write_bytes(original_links)

    def test_host_pressure_or_swap_makes_valid_campaign_inconclusive(self):
        path = self.evidence / 'raw-attempts.jsonl'
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]['aa_host_before']['cpu_psi_some_avg10_percent']['value'] = 1.1
        rows[0]['aa_host_after']['swap_pages']['value']['pswpout'] = 1
        path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
        links_path = self.evidence / 'aa-attempt-links.jsonl'
        links = [json.loads(line) for line in links_path.read_text().splitlines()]
        for index, (row, link) in enumerate(zip(rows, links)):
            encoded = (json.dumps(row, sort_keys=True) + '\n').encode()
            link['raw_line_sha256'] = hashlib.sha256(encoded).hexdigest()
            link['ordinal'] = index
        links_path.write_text(''.join(json.dumps(link, sort_keys=True) + '\n' for link in links))
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']), ('INCONCLUSIVE', 'host_ineligible'))

    def test_missing_required_host_telemetry_is_inconclusive(self):
        path = self.evidence / 'raw-attempts.jsonl'
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]['aa_host_before']['governor'] = dict(value=None,
                                                      unavailable_reason='governor file missing')
        path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
        links_path = self.evidence / 'aa-attempt-links.jsonl'
        links = [json.loads(line) for line in links_path.read_text().splitlines()]
        links[0]['raw_line_sha256'] = hashlib.sha256(
            (json.dumps(rows[0], sort_keys=True) + '\n').encode()).hexdigest()
        links_path.write_text(''.join(json.dumps(link, sort_keys=True) + '\n' for link in links))
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']), ('INCONCLUSIVE', 'host_ineligible'))

    def test_cgroup_counter_and_path_or_limit_drift_are_inconclusive(self):
        raw_path = self.evidence / 'raw-attempts.jsonl'
        links_path = self.evidence / 'aa-attempt-links.jsonl'
        original_raw = raw_path.read_bytes()
        original_links = links_path.read_bytes()
        for violation in ('throttle_delta', 'counter_regression', 'path_drift', 'limit_drift'):
            with self.subTest(violation=violation):
                rows = [json.loads(line) for line in original_raw.splitlines()]
                after_level = rows[0]['aa_host_after']['cgroup']['value']['levels'][0]
                before_level = rows[0]['aa_host_before']['cgroup']['value']['levels'][0]
                if violation == 'throttle_delta':
                    after_level['throttle_time'] = 1
                elif violation == 'counter_regression':
                    before_level['nr_throttled'] = 2
                    after_level['nr_throttled'] = 1
                elif violation == 'path_drift':
                    after_level['path'] = 'job/changed'
                else:
                    after_level['cpu_limit']['quota'] = '50000'
                raw_path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
                links = [json.loads(line) for line in original_links.splitlines()]
                links[0]['raw_line_sha256'] = hashlib.sha256(
                    (json.dumps(rows[0], sort_keys=True) + '\n').encode()).hexdigest()
                links_path.write_text(''.join(json.dumps(link, sort_keys=True) + '\n' for link in links))
                result = Q.qualify(self.evidence, self.preflight_path)
                self.assertEqual((result['status'], result['reason']),
                                 ('INCONCLUSIVE', 'host_ineligible'))
        raw_path.write_bytes(original_raw)
        links_path.write_bytes(original_links)

    def test_binary_mismatch_fails_before_runs(self):
        preflight = json.loads(self.preflight_path.read_text())
        base_path = preflight['paths']['base']['executables']['crdt']
        candidate = self.root / 'different-crdt-binary'
        candidate.write_text('different binary bytes\n', encoding='utf-8')
        candidate_hash = hashlib.sha256(candidate.read_bytes()).hexdigest()
        preflight['paths']['candidate']['executables']['crdt'] = str(candidate)
        preflight['artifacts'][str(candidate)] = candidate_hash
        self.preflight_path.write_text(json.dumps(preflight), encoding='utf-8')
        document = dict(schema_version=1, status='FAIL', reason='binary_sha_mismatch',
                        benchmark_launches=0,
                        workloads=[dict(workload='crdt',
                                        base_sha256=preflight['artifacts'][base_path],
                                        candidate_sha256=candidate_hash)])
        (self.evidence / 'aa-qualification-preflight.json').write_text(json.dumps(document))
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']), ('FAIL', 'binary_sha_mismatch'))
        self.assertEqual(result['benchmark_launches'], 0)
        document['benchmark_launches'] = False
        (self.evidence / 'aa-qualification-preflight.json').write_text(json.dumps(document))
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']),
                         ('INCONCLUSIVE', 'invalid_evidence'))

    def test_preflight_schema_version_boolean_is_invalid_evidence(self):
        preflight = json.loads(self.preflight_path.read_text())
        preflight['schema_version'] = True
        self.preflight_path.write_text(json.dumps(preflight), encoding='utf-8')
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']),
                         ('INCONCLUSIVE', 'invalid_evidence'))

    def test_shared_path_early_result_rejects_boolean_launch_count(self):
        preflight = json.loads(self.preflight_path.read_text())
        preflight['paths']['candidate']['build'] = str(self.root / 'different-build')
        self.preflight_path.write_text(json.dumps(preflight), encoding='utf-8')
        document = dict(schema_version=1, status='INCONCLUSIVE',
                        reason='shared_build_path_identity_missing', workloads=[],
                        benchmark_launches=False)
        (self.evidence / 'aa-qualification-preflight.json').write_text(json.dumps(document))
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']),
                         ('INCONCLUSIVE', 'invalid_evidence'))

    def test_valid_correctness_failure_fails_campaign(self):
        raw_path = self.evidence / 'raw-attempts.jsonl'
        rows = [json.loads(line) for line in raw_path.read_text().splitlines()]
        record = json.loads(rows[0]['stdout'])
        record['result'] -= 1
        record['status'] = 'FAIL'
        rows[0]['stdout'] = json.dumps(record) + '\n'
        rows[0]['aa_host_before']['governor'] = dict(
            value=None, unavailable_reason='governor file missing')
        raw_path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
        link_path = self.evidence / 'aa-attempt-links.jsonl'
        links = [json.loads(line) for line in link_path.read_text().splitlines()]
        links[0]['raw_line_sha256'] = hashlib.sha256(
            (json.dumps(rows[0], sort_keys=True) + '\n').encode()).hexdigest()
        link_path.write_text(''.join(json.dumps(link, sort_keys=True) + '\n' for link in links))
        campaign_path = self.evidence / 'campaign-v1.json'
        campaign = json.loads(campaign_path.read_text())
        elapsed, correctness = COLLECTOR['parse_result'](rows[0]['stdout'], 'crdt')
        campaign['attempts'][0]['elapsed_ms'] = elapsed
        campaign['attempts'][0]['correctness'] = correctness
        campaign_path.write_text(json.dumps(campaign), encoding='utf-8')
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']),
                         ('FAIL', 'process_or_correctness_failure'))

    def test_wide_interval_is_inconclusive_before_noise_floor_verdict(self):
        path = self.evidence / 'raw-attempts.jsonl'
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        # One order stratum has a large spread while retaining valid output and host evidence.
        for row in rows:
            if row['workload'] == 'crdt' and row['block_id'] == 'AB' and row['sequence'] in range(3, 19, 2):
                record = json.loads(row['stdout'])
                record['elapsed_ms'] = 10 * math.exp(0.3)
                row['stdout'] = json.dumps(record) + '\n'
        path.write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
        links_path = self.evidence / 'aa-attempt-links.jsonl'
        links = [json.loads(line) for line in links_path.read_text().splitlines()]
        for index, (row, link) in enumerate(zip(rows, links)):
            encoded = (json.dumps(row, sort_keys=True) + '\n').encode()
            link['raw_line_sha256'] = hashlib.sha256(encoded).hexdigest()
        links_path.write_text(''.join(json.dumps(link, sort_keys=True) + '\n' for link in links))
        # Campaign attempts must tie to updated raw timings.
        camp_path = self.evidence / 'campaign-v1.json'
        campaign = json.loads(camp_path.read_text())
        for row, attempt in zip(rows, campaign['attempts']):
            elapsed, correctness = COLLECTOR['parse_result'](row['stdout'], row['workload'])
            attempt['elapsed_ms'] = elapsed
            attempt['correctness'] = correctness
        camp_path.write_text(json.dumps(campaign), encoding='utf-8')
        result = Q.qualify(self.evidence, self.preflight_path)
        self.assertEqual((result['status'], result['reason']),
                         ('INCONCLUSIVE', 'wide_confidence_interval'))


if __name__ == '__main__':
    unittest.main()
