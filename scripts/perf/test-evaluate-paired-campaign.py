#!/usr/bin/env python3
"""Offline campaign schema, pairing, correctness and deterministic reporting tests."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

PATH = Path(__file__).with_name('evaluate-paired-campaign.py')
SPEC = importlib.util.spec_from_file_location('evaluator', PATH)
evaluator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evaluator)

def fixture(aa=False):
    profile = {'compiler': 'clang-18', 'buildtype': 'release', 'lto': 'false'}
    sources = {'base': 'a'*40, 'candidate': ('a' if aa else 'b')*40}
    manifest = dict(sources=sources, profiles={s: profile.copy() for s in evaluator.SIDES},
        host_id='host-1', cpu=2, build_provenance={s: dict(build_instance_id=s+'-build',
            source_sha=sources[s], profile=profile.copy(), build_log_sha256='f'*64)
            for s in evaluator.SIDES}, workloads={'crdt': dict(binary_sha256={s:'c'*64 for s in evaluator.SIDES},
                fixture_sha256={s:{'fixture.csv':'d'*64} for s in evaluator.SIDES},
                timer=dict(id='CLOCK_MONOTONIC', unit='ms', scope='solve'), expected={'tuples': 104851})})
    result = dict(schema_version=1, campaign_id='campaign-1', campaign_mode='aa_control' if aa else 'comparison',
                  manifest=manifest, blocks=[dict(block_id='one',order='AB'),dict(block_id='two',order='BA')], attempts=[])
    host = {k:dict(value=None, unavailable_reason='not exposed') for k in evaluator.TELEMETRY}
    for block in result['blocks']:
        sides = evaluator.SIDES if block['order']=='AB' else evaluator.SIDES[::-1]
        for seq in range(20):
            side = sides[seq%2]
            result['attempts'].append(dict(campaign_id=result['campaign_id'], manifest_sha256=evaluator.digest(manifest),
                block_id=block['block_id'], workload='crdt', sequence=seq, phase='warmup' if seq<2 else 'sample',
                pair_index=None if seq<2 else (seq-2)//2, side=side, elapsed_ms=10 if aa or side=='base' else 20,
                exit_code=0, timed_out=False, correctness=dict(status='OK', observed={'tuples':104851}),
                host_before=copy.deepcopy(host), host_after=copy.deepcopy(host)))
    return result

def refresh(c):
    for e in c['attempts']:
        e['manifest_sha256']=evaluator.digest(c['manifest'])

class Tests(unittest.TestCase):
    def evaluate(self,c):
        return evaluator.evaluate(c, 'e'*64)
    def test_valid_math_and_telemetry(self):
        r=self.evaluate(fixture())
        self.assertEqual(r['status'],'COMPLETE_VALID')
        w=r['workloads']['crdt']
        self.assertAlmostEqual(w['blocks']['one']['mean_log_ratio'], evaluator.math.log(2))
        self.assertEqual(w['order_effect_log_ratio'],0)
        self.assertEqual(w['blocks']['one']['sides']['base']['cov_percent'],0)
        self.assertIsNone(r['telemetry'][0]['host_before']['governor']['value'])
    def test_shuffled_and_reversed_blocks(self):
        c=fixture(); original=self.evaluate(c)
        c['attempts'].reverse()
        self.assertEqual(self.evaluate(c),original)
        c['blocks'].reverse()
        self.assertEqual(self.evaluate(c)['workloads'],original['workloads'])
    def test_aa_zero(self):
        r=self.evaluate(fixture(True))
        self.assertEqual(r['status'],'COMPLETE_VALID')
        for b in r['workloads']['crdt']['blocks'].values():
            self.assertEqual(b['paired_log_ratios'],[0]*9)
        self.assertEqual(r['workloads']['crdt']['order_effect_log_ratio'],0)
    def test_aa_shared_build_identity_is_valid_only_for_matching_inputs(self):
        c=fixture(True)
        c['manifest']['build_provenance']['candidate']['build_instance_id']='base-build'
        refresh(c)
        self.assertEqual(self.evaluate(c)['status'],'COMPLETE_VALID')
        c['manifest']['workloads']['crdt']['binary_sha256']['candidate']='e'*64
        refresh(c)
        r=self.evaluate(c)
        self.assertEqual(r['status'],'INVALID_EVIDENCE')
        self.assertEqual(r['reason'],'shared A/A build provenance differs')
    def test_malformed_attempts(self):
        changes={'sequence':[True,-1,20,'2'], 'elapsed_ms':[True,0,-1,float('nan'),float('inf'),'10'],
                 'pair_index':[9,True,'0'], 'campaign_id':['other'], 'manifest_sha256':['f'*64],
                 'side':['candidate'], 'phase':['sample'], 'exit_code':[False,'0'],
                 'timed_out':[0], 'correctness':[{}, {'status':'OK','observed':{'tuples':True}}]}
        for key,values in changes.items():
            for value in values:
                with self.subTest(key=key,value=value):
                    c=fixture(); c['attempts'][0][key]=value
                    self.assertEqual(self.evaluate(c)['status'],'INVALID_EVIDENCE')
    def test_indices_and_incomplete(self):
        c=fixture(); c['attempts'].append(c['attempts'][0].copy())
        self.assertEqual(self.evaluate(c)['status'],'INVALID_EVIDENCE')
        for i in (0,3,39):
            c=fixture(); c['attempts'].pop(i)
            self.assertEqual(self.evaluate(c)['status'],'INCOMPLETE')
    def test_sample_pair_and_telemetry(self):
        for field,value in [('pair_index',True),('pair_index',9),('side','candidate'),('phase','warmup')]:
            c=fixture(); c['attempts'][2][field]=value
            self.assertEqual(self.evaluate(c)['status'],'INVALID_EVIDENCE')
        c=fixture()
        for e in c['attempts']:
            for h in ('host_before','host_after'):
                e[h]={k:dict(value='performance' if k=='governor' else 100,unavailable_reason=None)
                      for k in evaluator.TELEMETRY}
        self.assertEqual(self.evaluate(c)['status'],'COMPLETE_VALID')
        c['attempts'][0]['host_before']['load_one']['value']=float('inf')
        self.assertEqual(self.evaluate(c)['status'],'INVALID_EVIDENCE')
    def test_failures(self):
        for key,value in [('exit_code',1),('timed_out',True),('correctness',dict(status='FAIL',observed={'tuples':104851})),
                          ('correctness',dict(status='OK',observed={'tuples':1}))]:
            c=fixture(); c['attempts'][0][key]=value
            self.assertEqual(self.evaluate(c)['status'],'CORRECTNESS_FAILURE')
    def test_empty_variable_map_keys(self):
        c=fixture()
        for side in evaluator.SIDES:
            c['manifest']['profiles'][side]={'': 'release'}
            c['manifest']['build_provenance'][side]['profile']={'': 'release'}
        refresh(c)
        r=self.evaluate(c)
        self.assertEqual(r['status'],'INVALID_EVIDENCE')
        self.assertEqual(r['reason'],'profile must contain resolved string settings')
        c=fixture()
        c['manifest']['workloads']['crdt']['expected']={'':104851}
        for event in c['attempts']:
            event['correctness']['observed']={'':104851}
        refresh(c)
        r=self.evaluate(c)
        self.assertEqual(r['status'],'INVALID_EVIDENCE')
        self.assertEqual(r['reason'],'invalid expected correctness')
    def test_manifest_constraints(self):
        mutations=[lambda c:c.update(campaign_mode='other'),
            lambda c:c['manifest']['sources'].update(base='A'*40),
            lambda c:c['manifest']['sources'].update(candidate='a'*40),
            lambda c:c['manifest']['profiles']['candidate'].update(lto='true'),
            lambda c:c['manifest']['workloads']['crdt']['fixture_sha256']['candidate'].update(other='e'*64),
            lambda c:c['manifest']['workloads']['crdt']['timer'].update(unit='seconds'),
            lambda c:c['manifest']['build_provenance']['candidate'].update(build_instance_id='base-build'),
            lambda c:c['blocks'][1].update(order='AB'),
            lambda c:c['attempts'][0]['host_before']['governor'].update(unavailable_reason=None),
            lambda c:c['attempts'][0]['host_before']['frequency_khz'].update(value='1000',unavailable_reason=None)]
        for mutation in mutations:
            c=fixture(); mutation(c); refresh(c)
            self.assertEqual(self.evaluate(c)['status'],'INVALID_EVIDENCE')
        c=fixture(True); c['manifest']['sources']['candidate']='b'*40; refresh(c)
        self.assertEqual(self.evaluate(c)['status'],'INVALID_EVIDENCE')
    def test_cli_exit_and_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'campaign.json'
            for mode,expected in [('valid',0),('failure',1),('invalid',2),('incomplete',3)]:
                c=fixture()
                if mode=='failure': c['attempts'][0]['exit_code']=1
                if mode=='invalid': c['schema_version']=2
                if mode=='incomplete': c['attempts'].pop()
                path.write_text(json.dumps(c), encoding='utf-8')
                run=subprocess.run([sys.executable,str(PATH),str(path)],capture_output=True,text=True, encoding='utf-8')
                self.assertEqual(run.returncode,expected,run.stderr)
                r=json.loads(run.stdout)
                self.assertEqual(r['input_sha256'],evaluator.hashlib.sha256(path.read_bytes()).hexdigest())
                self.assertEqual(run.stdout,subprocess.run([sys.executable,str(PATH),str(path)],capture_output=True,text=True, encoding='utf-8').stdout)
            for raw in ['{"schema_version":1,"schema_version":1}', 'NaN', '{']:
                path.write_text(raw, encoding='utf-8')
                self.assertEqual(subprocess.run([sys.executable,str(PATH),str(path)],capture_output=True).returncode,2)

if __name__=='__main__': unittest.main()
