#!/usr/bin/env python3
"""Clean-clone integration contract for pinned batch-append source materialization."""
from pathlib import Path
import runpy
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
CAMPAIGN = runpy.run_path(str(HERE / 'prepare-batch-append-campaign.py'))
FALLBACKS = runpy.run_path(str(HERE / 'batch_append_fallbacks.py'))
MATERIALIZER = runpy.run_path(str(HERE / 'materialize-batch-append-sources.py'))


class MaterializerIntegrationTests(unittest.TestCase):
    def setUp(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=home_tmp,
                                                prefix='batch-append-materializer-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    @staticmethod
    def git(root, *args):
        return subprocess.check_output(['git', '-C', str(root), *args],
                                       text=True, encoding='utf-8').strip()

    def inventory(self, root):
        git_dir = Path(self.git(root, 'rev-parse', '--git-dir'))
        if not git_dir.is_absolute():
            git_dir = root / git_dir
        objects = git_dir / 'objects'
        return dict(refs=self.git(root, 'show-ref'),
                    status=self.git(root, 'status', '--porcelain=v1', '-z',
                                     '--untracked-files=all', '--ignored=matching'),
                    index=(git_dir / 'index').read_bytes(),
                    objects=sorted((str(path.relative_to(objects)), path.stat().st_size,
                                    path.stat().st_mtime_ns)
                                   for path in objects.rglob('*') if path.is_file()))

    def make_overlay(self, seed):
        contents = {
            'bench/bench_batch_append.c': '/* materializer integration */\n',
            'bench/meson.build': 'benchmark integration fixture\n',
            'tests/meson.build': 'test integration fixture\n',
            'tests/test_bench_batch_append.c': '/* integration fixture */\n',
        }
        for path, content in contents.items():
            target = seed / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding='utf-8')
        self.git(seed, 'add', *CAMPAIGN['OVERLAY_PATHS'])
        patch = self.root / 'overlay.patch'
        raw = subprocess.check_output(['git', '-C', str(seed), 'diff', '--cached',
                                       '--binary', 'HEAD', '--',
                                       *CAMPAIGN['OVERLAY_PATHS']])
        patch.write_bytes(raw)
        return patch

    def test_clean_clone_materializes_valid_comparison_sources(self):
        repository = Path(CAMPAIGN['REPOSITORY_ROOT'])
        # A source checkout without Meson-hydrated fallback inputs cannot act as
        # a seed. The materializer itself remains fail-closed in that case.
        for relative in FALLBACKS['IGNORED_ROOTS']:
            if not (repository / relative.rstrip('/')).exists():
                self.skipTest('pinned Meson fallback inputs are not hydrated in this test checkout')

        seed = self.root / 'seed'
        seed.mkdir()
        self.git(seed, 'init', '-q')
        subprocess.run(['git', '-C', str(seed), 'fetch', '--no-tags',
                        str(CAMPAIGN['ANCHOR_BUNDLE_PATH']), CAMPAIGN['ANCHOR_REF']],
                       check=True, capture_output=True, text=True, encoding='utf-8')
        self.git(seed, 'update-ref', 'refs/heads/anchor', 'FETCH_HEAD')
        self.git(seed, 'checkout', '-q', '--detach', 'FETCH_HEAD')
        self.git(seed, 'config', 'user.name', 'Materializer Integration')
        self.git(seed, 'config', 'user.email', 'materializer@example.invalid')
        for relative in FALLBACKS['IGNORED_ROOTS']:
            source = repository / relative.rstrip('/')
            shutil.copytree(source, seed / relative.rstrip('/'), symlinks=True)
        clean_identity = FALLBACKS['validate_fallback_seed'](seed, 'clean')
        overlay = self.make_overlay(seed)
        seed_before = self.inventory(seed)
        caller_before = self.inventory(repository)
        fallback_seed = FALLBACKS['validate_fallback_seed'](seed, 'overlay_staged')

        caller_output = repository / '.materializer-output-guard-test'
        self.assertFalse(caller_output.exists())
        with self.assertRaisesRegex(ValueError, 'must not overlap the caller checkout'):
            MATERIALIZER['materialize'](caller_output, CAMPAIGN['PRODUCT_PATCH_PATH'], overlay,
                                        seed, 'overlay_staged')
        self.assertFalse(caller_output.exists())
        self.assertEqual(caller_before, self.inventory(repository))

        output = self.root / 'materialized'
        MATERIALIZER['materialize'](output, CAMPAIGN['PRODUCT_PATCH_PATH'], overlay,
                                    seed, 'overlay_staged')
        base, candidate = output / 'base-source', output / 'candidate-source'
        base_provenance = CAMPAIGN['source_provenance'](
            'base', base, 'pre', CAMPAIGN['load_revision_manifest']()[0], overlay.read_bytes())
        candidate_provenance = CAMPAIGN['source_provenance'](
            'candidate', candidate, 'post', CAMPAIGN['load_revision_manifest']()[0],
            overlay.read_bytes())
        args = type('Args', (), dict(mode='comparison', aa_product=None, seed='materializer-test',
                                     base_source=base, candidate_source=candidate,
                                     overlay_patch=overlay))()
        plan = CAMPAIGN['build_plan'](args)
        self.assertEqual(plan['sources']['base']['fallback_dependencies'], fallback_seed)
        self.assertEqual(plan['sources']['candidate']['fallback_dependencies'], fallback_seed)
        self.assertEqual(base_provenance['checkout_kind'], 'direct_anchor')
        self.assertEqual(candidate_provenance['checkout_kind'], 'synthetic_product')
        for source in (base, candidate):
            self.assertFalse((source / '.git/objects/info/alternates').exists())
            self.assertFalse((source / 'subprojects/nanoarrow/.git/objects/info/alternates').exists())
            self.assertEqual(self.git(source, 'remote'), '')
        self.assertEqual(seed_before, self.inventory(seed))
        self.assertEqual(caller_before, self.inventory(repository))
        self.assertEqual(clean_identity['nanoarrow']['manifest_sha256'],
                         fallback_seed['nanoarrow']['manifest_sha256'])

        aa_output = self.root / 'aa-post'
        MATERIALIZER['materialize'](aa_output, CAMPAIGN['PRODUCT_PATCH_PATH'], overlay,
                                    seed, 'overlay_staged', mode='aa_control',
                                    aa_product='post')
        aa_args = type('Args', (), dict(mode='aa_control', aa_product='post',
                                        seed='materializer-aa-test',
                                        base_source=aa_output / 'base-source',
                                        candidate_source=aa_output / 'candidate-source',
                                        overlay_patch=overlay))()
        aa_plan = CAMPAIGN['build_plan'](aa_args)
        self.assertEqual(aa_plan['sources']['base']['checkout_commit'],
                         aa_plan['sources']['candidate']['checkout_commit'])
        self.assertEqual(aa_plan['sources']['base']['product_tree'],
                         aa_plan['sources']['candidate']['product_tree'])

        failed_output = self.root / 'partial-output'
        def fail_copy(*_args):
            raise OSError('injected fallback copy failure')
        with patch.dict(MATERIALIZER['materialize'].__globals__, copy_fallbacks=fail_copy):
            with self.assertRaisesRegex(OSError, 'injected fallback copy failure'):
                MATERIALIZER['materialize'](failed_output, CAMPAIGN['PRODUCT_PATCH_PATH'],
                                           overlay, seed, 'overlay_staged')
        self.assertFalse(failed_output.exists())

        changed_seed_output = self.root / 'seed-mutated-output'
        real_copy = MATERIALIZER['copy_fallbacks']
        unexpected = seed / 'unexpected-seed-entry'
        def mutate_seed(seed_root, target):
            real_copy(seed_root, target)
            unexpected.write_text('concurrent seed mutation\n', encoding='utf-8')
        try:
            with patch.dict(MATERIALIZER['materialize'].__globals__,
                            copy_fallbacks=mutate_seed):
                with self.assertRaisesRegex(ValueError,
                                            'unapproved tracked/untracked/ignored path'):
                    MATERIALIZER['materialize'](
                        changed_seed_output, CAMPAIGN['PRODUCT_PATCH_PATH'], overlay,
                        seed, 'overlay_staged')
        finally:
            unexpected.unlink(missing_ok=True)
        self.assertFalse(changed_seed_output.exists())

    def test_materializer_rejects_missing_seed_before_creating_output(self):
        output = self.root / 'must-not-exist'
        with self.assertRaises((ValueError, FALLBACKS['FallbackError'])):
            MATERIALIZER['materialize'](output, CAMPAIGN['PRODUCT_PATCH_PATH'],
                                        self.root / 'missing.patch',
                                        self.root / 'missing-seed', 'clean')
        self.assertFalse(output.exists())

    def test_output_overlapping_external_caller_is_rejected_before_home_check(self):
        caller = Path('/opt/wirelog-external-caller-fixture')
        output = caller / 'materialized'
        materializer_campaign = MATERIALIZER['materialize'].__globals__['campaign']
        with patch.dict(materializer_campaign, REPOSITORY_ROOT=caller):
            with self.assertRaisesRegex(ValueError, 'must not overlap the caller checkout'):
                MATERIALIZER['materialize'](
                    output, CAMPAIGN['PRODUCT_PATCH_PATH'], self.root / 'overlay.patch',
                    self.root / 'seed', 'clean')
        self.assertFalse(output.exists())

    def test_output_outside_home_is_rejected_without_creating_output(self):
        output = Path('/opt/wirelog-materializer-outside-home-fixture')
        self.assertFalse(output.exists())
        with self.assertRaisesRegex(ValueError, 'must be a child of HOME'):
            MATERIALIZER['materialize'](
                output, CAMPAIGN['PRODUCT_PATCH_PATH'], self.root / 'overlay.patch',
                self.root / 'seed', 'clean')
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
