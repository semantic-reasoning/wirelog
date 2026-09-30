#!/usr/bin/env python3
"""Fail closed on the shadow runner trust boundary and orchestration contract.

Dependency-free checks intentionally pin security-critical shell fragments.
Executable fixtures also exercise the actual checked-in resolution shell.
"""
from pathlib import Path
import os
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github/workflows/perf-paired-diagnostic.yml'
MINIMUM = 'c6e263d492828d1208fa11005c18d19b05342233'


def steps(text):
    return re.findall(r'^      - (.*?)(?=^      - |\Z)', text, re.M | re.S)


def named(text, name):
    found = [item for item in steps(text) if item.startswith('name: ' + name + '\n')]
    assert len(found) == 1, name
    return found[0]


def shell(item):
    match = re.search(r'^        run: \|\n(.*)', item, re.M | re.S)
    assert match, 'missing shell body'
    return '\n'.join(line[10:] for line in match[1].splitlines()) + '\n'


def require(text, fragments):
    for fragment in fragments:
        assert fragment in text, 'missing contract: ' + fragment


def validate(text):
    triggers = text.split('\non:\n', 1)[1].split('\npermissions:', 1)[0]
    assert re.findall(r'^  ([a-z_]+):', triggers, re.M) == ['workflow_dispatch', 'schedule']
    assert re.findall(r'^      ([a-z_]+):', triggers, re.M) == ['campaign_mode', 'base_sha']
    require(triggers, ['required: false', '- comparison', '- aa_control'])
    assert text.split('\npermissions:\n', 1)[1].split('\nconcurrency:', 1)[0].strip() == 'contents: read'
    assert not re.search(r'secrets\.|pull_request|pull_request_target|workflow_run|workflow_call|continue-on-error|\benvironment:|^\s+push:', text, re.M)
    require(text, ['name: Paired Campaign V1 Shadow', 'name: Campaign v1 shadow evidence',
                   "if: github.ref == 'refs/heads/main' && github.repository == 'semantic-reasoning/wirelog'",
                   'group: wirelog-paired-perf-diagnostic', 'cancel-in-progress: false',
                   'runs-on: [self-hosted, Linux, X64, wirelog-perf]', 'MINIMUM_SHA: ' + MINIMUM])
    assert int(re.search(r'timeout-minutes: (\d+)', text)[1]) >= 300
    assert re.findall(r'^    if: (.*)$', text, re.M) == ["github.ref == 'refs/heads/main' && github.repository == 'semantic-reasoning/wirelog'"]
    external = re.findall(r'uses: (?!\./)(\S+)', text)
    assert external == ['actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09',
                        'actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a']
    assert re.findall(r'\$\{\{\s*(.*?)\s*\}\}', text) == [
        'inputs.campaign_mode', 'inputs.base_sha', 'github.sha', 'github.run_id', 'github.run_attempt']
    all_steps = steps(text)
    require(all_steps[0], ['fetch-depth: 0', 'persist-credentials: false'])
    assert not re.search(r'^          (repository|ref|token):', all_steps[0], re.M)
    validate_step = named(text, 'Validate trusted revisions and resolve mode')
    # Require the complete ordered boundary, not mere occurrence anywhere.
    ordered = [
        'set -euo pipefail',
        'git fetch --no-tags origin +refs/heads/main:refs/remotes/origin/main',
        'main_sha="$(git rev-parse refs/remotes/origin/main^{commit})"',
        'test "$(git rev-parse HEAD)" = "$main_sha"',
        'test "$DISPATCH_SHA" = "$main_sha"',
        'git merge-base --is-ancestor "$MINIMUM_SHA" "$main_sha"',
        'candidate_sha="$main_sha"',
        'for revision in "$base_sha" "$candidate_sha"; do',
        '[[ "$revision" =~ ^[0-9a-f]{40}$ ]]',
        'git cat-file -e "$revision^{commit}"',
        'git merge-base --is-ancestor "$MINIMUM_SHA" "$revision"',
        'git merge-base --is-ancestor "$revision" "$main_sha"',
        'git show "$revision:tests/test_crdt_perf_gate.c" | grep -F \'WIRELOG_CRDT_PROBE\' >/dev/null',
        'done',
        '"$mode" "$base_sha" "$candidate_sha" >> "$GITHUB_ENV"',
    ]
    positions = [validate_step.index(fragment) for fragment in ordered]
    assert positions == sorted(positions)
    require(validate_step, ['if [[ "$GITHUB_EVENT_NAME" == schedule ]]; then\n            mode=aa_control\n            base_sha="$main_sha"',
                           'elif [[ "$GITHUB_EVENT_NAME" == workflow_dispatch ]]; then',
                           'mode="$REQUESTED_MODE"', 'case "$mode" in',
                           'base_sha="$REQUESTED_BASE"', 'test -n "$base_sha"',
                           'test "$base_sha" != "$candidate_sha"', 'test -z "$REQUESTED_BASE"',
                           '*) exit 1 ;;'])
    assert 'if:' not in validate_step and 'continue-on-error' not in validate_step
    # Only pinned checkout and plain evidence initialization precede validation.
    assert all_steps[1].startswith('name: Initialize durable evidence\n')
    assert all_steps[2] == validate_step
    assert not re.search(r'uses:|meson|python|worktree|sudo|uv ', all_steps[1] + validate_step)
    trees = named(text, 'Create independent worktrees')
    assert all_steps[3] == trees
    require(trees, ['test ! -e "$pair_root"',
                    'git worktree add --detach "$pair_root/base" "$BASE_SHA"',
                    'git worktree add --detach "$pair_root/candidate" "$CANDIDATE_SHA"',
                    'BASE_BUILD=%s/base-build', 'CANDIDATE_BUILD=%s/candidate-build'])
    cpu = named(text, 'Select available CPU')
    require(cpu, ['min(os.sched_getaffinity(0))', 'CAMPAIGN_CPU=%s', 'affinity.txt'])
    build = named(text, 'Build both production profiles')
    require(build, ['set -euo pipefail', 'CC: gcc', 'for side in base candidate; do',
                    'source="$BASE_SRC"', 'build="$BASE_BUILD"',
                    'source="$CANDIDATE_SRC"', 'build="$CANDIDATE_BUILD"',
                    'meson setup "$build" "$source" --buildtype=release',
                    '-Dwirelog_log_max_level=trace -Dtests=true -DmbedTLS=disabled',
                    'meson compile -C "$build" bench_flowlog test_crdt_perf_gate test_cspa_perf_gate',
                    'side=%s source=%s build=%s phase=configure', 'side=%s source=%s build=%s phase=compile',
                    '$side-configure.log', '$side-build.log', '$side-complete-build.log'])
    collect = named(text, 'Collect campaign v1')
    require(collect, ['set -euo pipefail', 'python scripts/perf/paired-benchmark.py v1',
                      '--mode "$CAMPAIGN_MODE" --base-sha "$BASE_SHA" --candidate-sha "$CANDIDATE_SHA"',
                      '--base-source "$BASE_SRC" --candidate-source "$CANDIDATE_SRC"',
                      '--base-build "$BASE_BUILD" --candidate-build "$CANDIDATE_BUILD"',
                      '--base-build-log perf-artifacts/paired/base-complete-build.log',
                      '--candidate-build-log perf-artifacts/paired/candidate-complete-build.log',
                      '--cpu "$CAMPAIGN_CPU" --timeout 180 --out-dir perf-artifacts/paired/campaign'])
    assert '--first' not in collect and '--base-binary' not in collect
    command = shell(collect).replace('\\\n', '').split()
    expected = '''set -euo pipefail
python scripts/perf/paired-benchmark.py v1
--mode "$CAMPAIGN_MODE" --base-sha "$BASE_SHA" --candidate-sha "$CANDIDATE_SHA"
--base-source "$BASE_SRC" --candidate-source "$CANDIDATE_SRC"
--base-build "$BASE_BUILD" --candidate-build "$CANDIDATE_BUILD"
--base-build-log perf-artifacts/paired/base-complete-build.log
--candidate-build-log perf-artifacts/paired/candidate-complete-build.log
--cpu "$CAMPAIGN_CPU" --timeout 180 --out-dir perf-artifacts/paired/campaign'''.split()
    assert command == expected, 'collector must own all scheduling and evaluation'
    # Workflow delegates all execution and timing to the collector, including
    # bench_flowlog and test_crdt_perf_gate; test_cspa_perf_gate is built only.
    assert text.count('test_cspa_perf_gate') == 1
    assert not re.search(r'taskset -c|--cpu 0|CoV|median|ratio|speedup|regression', text)
    upload = named(text, 'Upload all paired diagnostic evidence')
    require(upload, ['if: always()', 'path: perf-artifacts/paired', 'if-no-files-found: error',
                     'retention-days: 35', 'perf-paired-v1-${{ github.run_id }}-${{ github.run_attempt }}'])
    summary = named(text, 'Summarize diagnostic status')
    require(summary, ['if: always()', 'CAMPAIGN_MODE', 'BASE_SHA', 'CANDIDATE_SHA',
                      'collection-status.json', 'descriptive', 'required perf gate'])
    cleanup = named(text, 'Clean independent worktrees and builds')
    assert all_steps[-3:] == [upload, summary, cleanup]
    require(cleanup, ['if: always()', 'set -euo pipefail',
                      '[[ "$GITHUB_RUN_ID" =~ ^[0-9]+$ ]]', '[[ "$GITHUB_RUN_ATTEMPT" =~ ^[0-9]+$ ]]',
                      'test -n "$RUNNER_TEMP"', 'test "$RUNNER_TEMP" != /',
                      'pair_root="$RUNNER_TEMP/wirelog-paired-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"',
                      'test "$(realpath -m "$pair_root")" = "$(realpath -m "$RUNNER_TEMP")/wirelog-paired-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"',
                      'for side in base candidate; do', 'git worktree remove --force "$pair_root/$side"',
                      'git worktree prune', 'rm -rf -- "$pair_root"'])
    assert cleanup.index('rm -rf') < cleanup.index('git worktree prune')
    for item in all_steps:
        if 'run: |' in item and 'shell: bash' in item:
            subprocess.run(['bash', '-n'], input=shell(item), text=True, encoding='utf-8', check=True)


class ShadowContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding='utf-8')

    def test_checked_in_contract(self):
        validate(self.text)
        collector = (ROOT / 'scripts/perf/paired-collector-v1.py').read_text(encoding='utf-8')
        require(collector, ["binaries[side] = {'crdt': build / 'tests/test_crdt_perf_gate', 'cspa-fast': build / 'bench/bench_flowlog'}"])
        self.assertNotIn('test_cspa_perf_gate', collector)

    def test_mutations_fail_closed(self):
        for fragment in ['test "$DISPATCH_SHA" = "$main_sha"',
                         'git merge-base --is-ancestor "$MINIMUM_SHA" "$revision"',
                         'git merge-base --is-ancestor "$revision" "$main_sha"',
                         '[[ "$revision" =~ ^[0-9a-f]{40}$ ]]',
                         'git cat-file -e "$revision^{commit}"',
                         'git show "$revision:tests/test_crdt_perf_gate.c"',
                         'persist-credentials: false', 'if: always()',
                         'min(os.sched_getaffinity(0))', 'git worktree remove --force',
                         'git worktree prune', 'test -z "$REQUESTED_BASE"']:
            with self.subTest(fragment=fragment), self.assertRaises((AssertionError, ValueError)):
                validate(self.text.replace(fragment, '# removed', 1))
        for before, after in [('timeout-minutes: 360', 'timeout-minutes: 150'),
                              ('actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09', 'actions/checkout@v5')]:
            with self.subTest(after=after), self.assertRaises(AssertionError):
                validate(self.text.replace(before, after))
        boundary = '      - ' + named(self.text, 'Validate trusted revisions and resolve mode')
        moved = self.text.replace(boundary, '').replace('      - name: Collect campaign v1', boundary + '      - name: Collect campaign v1')
        with self.assertRaises(AssertionError):
            validate(moved)

    def test_actual_cleanup_shell(self):
        script = shell(named(self.text, 'Clean independent worktrees and builds'))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git = root / 'git'
            git.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$CLEANUP_CALLS"\n[[ "$*" != *"/base" ]]\n', encoding='utf-8')
            git.chmod(0o755)
            run_root = root / 'wirelog-paired-123-2'
            for path in ['base', 'candidate', 'base-build', 'candidate-build']:
                (run_root / path).mkdir(parents=True)
            unrelated = root / 'keep'
            unrelated.mkdir()
            env = dict(os.environ, PATH=str(root) + ':' + os.environ['PATH'],
                       RUNNER_TEMP=str(root), GITHUB_RUN_ID='123', GITHUB_RUN_ATTEMPT='2',
                       CLEANUP_CALLS=str(root / 'calls'))
            result = subprocess.run(['bash'], input=script, text=True, encoding='utf-8', cwd=root, env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)  # First removal failed.
            self.assertFalse(run_root.exists())
            self.assertTrue(unrelated.exists())
            calls = (root / 'calls').read_text(encoding='utf-8')
            self.assertIn('worktree remove --force ' + str(run_root / 'base'), calls)
            self.assertIn('worktree remove --force ' + str(run_root / 'candidate'), calls)
            self.assertIn('worktree prune', calls)
            # Reject a redirected root before running git or deleting anything.
            run_root.symlink_to(unrelated, target_is_directory=True)
            (root / 'calls').unlink()
            result = subprocess.run(['bash'], input=script, text=True, encoding='utf-8', cwd=root, env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(unrelated.exists())
            self.assertFalse((root / 'calls').exists())

    def test_actual_resolution_shell(self):
        script = shell(named(self.text, 'Validate trusted revisions and resolve mode'))
        # Fake git owns only repository facts; run the exact shell with its
        # real mode, full-SHA and ancestry checks and output propagation.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git = root / 'git'
            git.write_text('''#!/bin/bash
case "$1" in
 fetch) exit 0 ;;
 rev-parse) echo "$FAKE_MAIN" ;;
 merge-base) [[ "$3" != "$REJECT_SHA" && "$4" != "$REJECT_SHA" ]] ;;
 cat-file) [[ "$3" != "$MISSING_SHA^{commit}" ]] ;;
 show) echo WIRELOG_CRDT_PROBE ;;
 *) exit 1 ;;
esac
''', encoding='utf-8')
            git.chmod(0o755)
            main = 'a' * 40
            base = 'b' * 40
            cases = [('schedule', '', '', main, True),
                     ('workflow_dispatch', 'aa_control', '', main, True),
                     ('workflow_dispatch', 'comparison', base, main, True),
                     ('workflow_dispatch', 'comparison', '', main, False),
                     ('workflow_dispatch', 'comparison', main, main, False),
                     ('workflow_dispatch', 'aa_control', base, main, False),
                     ('workflow_dispatch', 'other', '', main, False),
                     ('workflow_dispatch', 'comparison', 'b' * 39, main, False),
                     ('workflow_dispatch', 'comparison', 'B' * 40, main, False),
                     ('schedule', '', '', base, False)]
            for event, mode, selected, dispatch, success in cases:
                with self.subTest(event=event, mode=mode, base=selected):
                    env = dict(os.environ, PATH=str(root) + ':' + os.environ['PATH'],
                               GITHUB_EVENT_NAME=event, REQUESTED_MODE=mode, REQUESTED_BASE=selected,
                               DISPATCH_SHA=dispatch, MINIMUM_SHA=MINIMUM, FAKE_MAIN=main,
                               GITHUB_ENV=str(root / 'env'), REJECT_SHA='', MISSING_SHA='')
                    (root / 'perf-artifacts/paired').mkdir(parents=True, exist_ok=True)
                    result = subprocess.run(['bash'], input=script, text=True, encoding='utf-8', env=env, cwd=root,
                                            capture_output=True)
                    self.assertEqual(result.returncode == 0, success, result.stderr)
                    if success:
                        output = (root / 'env').read_text(encoding='utf-8')
                        self.assertIn('CANDIDATE_SHA=' + main, output)
                        self.assertIn('BASE_SHA=' + (selected if mode == 'comparison' else main), output)
                    (root / 'env').unlink(missing_ok=True)
            for reject in [base, main, MINIMUM]:
                env.update(GITHUB_EVENT_NAME='workflow_dispatch', REQUESTED_MODE='comparison',
                           REQUESTED_BASE=base, DISPATCH_SHA=main, REJECT_SHA=reject)
                result = subprocess.run(['bash'], input=script, text=True, encoding='utf-8', env=env, cwd=root, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((root / 'env').exists())


if __name__ == '__main__':
    unittest.main()
