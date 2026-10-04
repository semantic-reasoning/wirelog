#!/usr/bin/env python3
"""Exec handshake ensures measurement starts after affinity verification."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

TEST_TMP = Path.home() / '.cache' / 'wirelog-test-tmp'
TEST_TMP.mkdir(parents=True, exist_ok=True)


class AffinityLauncherTests(unittest.TestCase):
    @unittest.skipUnless(hasattr(os, 'sched_getaffinity'), 'Linux affinity required')
    def test_waits_for_release_before_exec_and_eof_aborts(self):
        launcher = Path(__file__).with_name('paired-aa-affinity-launcher.py')
        with tempfile.TemporaryDirectory(dir=TEST_TMP) as temporary:
            root = Path(temporary)
            marker = root / 'started'
            probe = root / 'probe.py'
            probe.write_text('from pathlib import Path; Path(%r).write_text("started")\n' % str(marker), encoding='utf-8')
            ready_r, ready_w = os.pipe()
            release_r, release_w = os.pipe()
            command = [sys.executable, str(launcher), '--cpu', str(min(os.sched_getaffinity(0))),
                       '--ready-fd', str(ready_w), '--release-fd', str(release_r),
                       sys.executable, str(probe)]
            process = subprocess.Popen(command, pass_fds=(ready_w, release_r), close_fds=True)
            os.close(ready_w)
            os.close(release_r)
            try:
                ready = os.read(ready_r, 4096)
                self.assertEqual(json.loads(ready)['affinity'], [min(os.sched_getaffinity(0))])
                self.assertFalse(marker.exists())
                os.write(release_w, b'1')
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertTrue(marker.exists())
            finally:
                os.close(ready_r)
                os.close(release_w)

            marker.unlink()
            ready_r, ready_w = os.pipe()
            release_r, release_w = os.pipe()
            command = [sys.executable, str(launcher), '--cpu', str(min(os.sched_getaffinity(0))),
                       '--ready-fd', str(ready_w), '--release-fd', str(release_r),
                       sys.executable, str(probe)]
            process = subprocess.Popen(command,
                pass_fds=(ready_w, release_r), close_fds=True)
            os.close(ready_w)
            os.close(release_r)
            try:
                self.assertTrue(os.read(ready_r, 4096))
                os.close(release_w)
                release_w = -1
                self.assertEqual(process.wait(timeout=5), 125)
                self.assertFalse(marker.exists())
            finally:
                os.close(ready_r)
                if release_w >= 0:
                    os.close(release_w)


if __name__ == '__main__':
    unittest.main()
