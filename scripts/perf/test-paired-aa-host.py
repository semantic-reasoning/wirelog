#!/usr/bin/env python3
"""Host evidence parsing contracts for paired A/A qualification."""
import runpy
from pathlib import Path
import tempfile
import unittest

M = runpy.run_path(str(Path(__file__).with_name('paired-aa-host.py')))
TEST_TMP = Path.home() / '.cache' / 'wirelog-test-tmp'
TEST_TMP.mkdir(parents=True, exist_ok=True)


class HostEvidenceTests(unittest.TestCase):
    def test_cgroup_v2_resolves_mount_root_and_parent_throttles(self):
        with tempfile.TemporaryDirectory(dir=TEST_TMP) as temporary:
            mount = Path(temporary) / 'cg'
            leaf = mount / 'job' / 'leaf'
            leaf.mkdir(parents=True)
            for path, count in ((mount, 1), (mount / 'job', 2), (leaf, 3)):
                (path / 'cpu.stat').write_text(f'nr_throttled {count}\nthrottled_usec {count * 11}\n', encoding='utf-8')
                (path / 'cpu.max').write_text('max 100000', encoding='utf-8')
            membership = '0::/job/leaf\n'
            mountinfo = f'31 20 0:29 / {mount} rw - cgroup2 cgroup rw\n'
            result = M['resolve_cgroup'](membership, mountinfo)
            self.assertEqual(result['version'], 'v2')
            self.assertEqual([level['path'] for level in result['levels']], ['job/leaf', 'job', '.'])
            self.assertEqual(result['levels'][0]['throttle_time_unit'], 'us')
            self.assertEqual(result['levels'][0]['throttle_time'], 33)

    def test_v1_uses_native_nanosecond_throttle_counter(self):
        with tempfile.TemporaryDirectory(dir=TEST_TMP) as temporary:
            mount = Path(temporary) / 'cpu'
            mount.mkdir()
            (mount / 'cpu.stat').write_text('nr_throttled 4\nthrottled_time 987654321\n', encoding='utf-8')
            (mount / 'cpu.cfs_quota_us').write_text('50000', encoding='utf-8')
            (mount / 'cpu.cfs_period_us').write_text('100000', encoding='utf-8')
            membership = '2:cpu,cpuacct:/\n'
            mountinfo = f'32 20 0:30 / {mount} rw - cgroup cgroup rw,cpu,cpuacct\n'
            result = M['resolve_cgroup'](membership, mountinfo)
            self.assertEqual(result['levels'][0]['throttle_time_unit'], 'ns')
            self.assertEqual(result['levels'][0]['throttle_time'], 987654321)

    def test_rejects_membership_escape_and_duplicate_json(self):
        with self.assertRaisesRegex(ValueError, 'unsafe'):
            M['parse_cgroup_membership']('0::/../../host\n')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            M['strict_json']('{"x":1,"x":2}')

    def test_rejects_nonfinite_json(self):
        with self.assertRaisesRegex(ValueError, 'nonfinite'):
            M['strict_json']('{"x":NaN}')


if __name__ == '__main__':
    unittest.main()
