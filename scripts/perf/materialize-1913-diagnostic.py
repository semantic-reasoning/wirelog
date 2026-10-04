#!/usr/bin/env python3
"""Create a provenance-checked CRDT diagnostic source wrapper."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


TEST_PATH = 'tests/test_crdt_perf_gate.c'
PROBE_BLOB = '06aa75dd22cae319a0ca17acfdfc63f6501abd95'
EXPECTED_OLD_TEST_BLOB = '2a2df51607a99f27b279f19f207595a4be5dfd79'
SOURCES = {
    'base': {
        'sha': '8d91c2da2b188b94af5b9f1da21c569d80ccb387',
        'tree': '078a36a45de616b9094ae0ebaac0635c80ce9e36',
        'parents': ('5a40987a41d7abaf33684893d2758318a35fcd83',),
    },
    'candidate': {
        'sha': '10d5ba8d9daba473476fa7a2aa75072da42cef39',
        'tree': 'abf7b0e18b5e3fc7eaa9270732cea2ae42c32e76',
        'parents': (
            '8d91c2da2b188b94af5b9f1da21c569d80ccb387',
            'b712e443457bca0a9d573b29af5885453728f504',
        ),
    },
}


def git(repo, *args, env=None):
    return subprocess.check_output(['git', '-C', str(repo), *args], env=env).decode().strip()


def ensure_clean(repo):
    status = git(repo, 'status', '--porcelain=v1', '--untracked-files=all')
    if status:
        raise ValueError('repository has tracked or untracked changes')


def parse_commit(repo, sha):
    output = git(repo, 'cat-file', '-p', sha)
    headers = output.split('\n\n', 1)[0].splitlines()
    tree = next((line[5:] for line in headers if line.startswith('tree ')), None)
    parents = tuple(line[7:] for line in headers if line.startswith('parent '))
    if tree is None:
        raise ValueError('pinned object is not a commit')
    return tree, parents


def materialize(repo, side):
    """Write one synthetic commit without changing refs or the worktree."""
    if side not in SOURCES:
        raise ValueError('side must be base or candidate')
    repo = Path(repo).resolve()
    ensure_clean(repo)
    source = SOURCES[side]
    sha = source['sha']
    tree, parents = parse_commit(repo, sha)
    if tree != source['tree'] or parents != source['parents']:
        raise ValueError('pinned source tree or parent provenance mismatch')
    old_blob = git(repo, 'rev-parse', f'{sha}:{TEST_PATH}')
    if old_blob != EXPECTED_OLD_TEST_BLOB:
        raise ValueError('pinned source probe path blob mismatch')
    probe = git(repo, 'cat-file', '-t', PROBE_BLOB)
    if probe != 'blob':
        raise ValueError('pinned diagnostic probe is unavailable')
    probe_text = git(repo, 'cat-file', 'blob', PROBE_BLOB)
    if 'WIRELOG_CRDT_PROBE' not in probe_text or 'crdt_perf_gate_single_run' not in probe_text:
        raise ValueError('pinned diagnostic probe contents are incompatible')

    temp_root = Path.home() / '.cache' / 'wirelog-tmp'
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=temp_root, prefix='wirelog-1913-index-', delete=False) as index:
        index_path = Path(index.name)
    index_path.unlink()
    env = dict(os.environ, GIT_INDEX_FILE=str(index_path))
    try:
        subprocess.run(['git', '-C', str(repo), 'read-tree', sha], env=env, check=True)
        subprocess.run([
            'git', '-C', str(repo), 'update-index', '--add', '--cacheinfo',
            f'100644,{PROBE_BLOB},{TEST_PATH}',
        ], env=env, check=True)
        wrapper_tree = git(repo, 'write-tree', env=env)
    finally:
        index_path.unlink(missing_ok=True)

    # Prove the only tree difference is the pinned test driver blob.
    changes = git(repo, 'diff-tree', '--no-commit-id', '--name-status', '-r', sha, wrapper_tree)
    if changes != f'M\t{TEST_PATH}':
        raise ValueError('synthetic wrapper changes paths beyond the diagnostic probe')
    if git(repo, 'ls-tree', wrapper_tree, TEST_PATH) != f'100644 blob {PROBE_BLOB}\t{TEST_PATH}':
        raise ValueError('synthetic wrapper probe path or mode mismatch')
    commit = subprocess.check_output(
        ['git', '-C', str(repo), 'commit-tree', wrapper_tree, '-p', sha],
        input=b'Add pinned CRDT diagnostic probe\n',
        env=dict(os.environ,
                 GIT_AUTHOR_NAME='Wirelog diagnostic materializer',
                 GIT_AUTHOR_EMAIL='wirelog-diagnostic@invalid',
                 GIT_COMMITTER_NAME='Wirelog diagnostic materializer',
                 GIT_COMMITTER_EMAIL='wirelog-diagnostic@invalid'),
    ).decode().strip()
    wrapper_tree_actual, wrapper_parents = parse_commit(repo, commit)
    if wrapper_tree_actual != wrapper_tree or wrapper_parents != (sha,):
        raise ValueError('synthetic wrapper commit provenance mismatch')
    return {
        'side': side,
        'source_sha': sha,
        'source_tree': tree,
        'source_parents': list(parents),
        'source_probe_blob': old_blob,
        'probe_blob': PROBE_BLOB,
        'wrapper_sha': commit,
        'wrapper_tree': wrapper_tree,
        'wrapper_parent': sha,
        'changed_path': TEST_PATH,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--side', choices=tuple(SOURCES), required=True)
    args = parser.parse_args()
    root = git(Path.cwd(), 'rev-parse', '--show-toplevel')
    try:
        print(json.dumps(materialize(root, args.side), sort_keys=True))
    except (ValueError, subprocess.CalledProcessError) as error:
        print(f'materialize-1913-diagnostic: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
