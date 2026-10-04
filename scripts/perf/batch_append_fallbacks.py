#!/usr/bin/env python3
"""Canonical provenance and exact filesystem validation for benchmark fallbacks."""
import configparser
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess
import tarfile

NANO_COMMIT = 'ec8a58cae18beaa241c7fea7cb26816ac27c280f'
NANO_TREE = '1d477a02d0f027659c19afd95528bace072107a3'
NANO_LINK_PATH = 'python/subprojects/arrow-nanoarrow'
NANO_LINK_OID = 'c25bddb6dd4666c6eb8cc92e33f1d60f64c3162b'
NANO_WRAP_OID = 'dca955a9ad67f8bc5196323e9a806604903ad5f5'
NANO_WRAP_SHA256 = '89ff0f4d5b51a88c5b4192a93c6c365d252df9a2df72bcf5a74ee6252b329e5f'
XX_WRAP_OID = '0eb9a8765df4c26ba87c6b8827dbccc83f8f2b78'
XX_WRAP_SHA256 = '6eddd768bdf22b24c3fbbbd212615cafbde916e6dceceb88db10353141b2e252'
XX_ARCHIVE_SHA256 = '5738270935e7c3d38a79b3adf7c9692566ce7895a25f67de43ad52ab504acd32'
XX_WRAP_MARKER = '.meson-subproject-wrap-hash.txt'
NANO_MARKER = '.meson-subproject-wrap-hash.txt'
WRAP_HASH_MARKER = '.meson-subproject-wrap-hash.txt'
PACKAGE_FILES = {
    'LICENSE.build': ('b59833dedbe2e4a84304431f4acca5dc4599cec7', '100644'),
    'meson.build': ('c64f0d86e961c0a6d361cdc56d344f0dff67a7fc', '100644'),
    'meson_options.txt': ('96a0c84d1006a9ebb24a3968b15dd8362ed93ed0', '100644'),
}
OVERLAY_PATHS = ('bench/bench_batch_append.c', 'bench/meson.build',
                 'tests/meson.build', 'tests/test_bench_batch_append.c')
IGNORED_ROOTS = ('subprojects/nanoarrow/', 'subprojects/xxHash-0.8.4/',
                 'subprojects/packagecache/')
SCHEMA = 'wirelog.batch-append-fallback-dependencies.v1'


class FallbackError(ValueError):
    pass


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def git(root, *args, binary=False):
    result = subprocess.run(['git', '-C', str(root), *args], capture_output=True)
    if result.returncode:
        raise FallbackError(f'git {args[0]} failed in {root}: {result.stderr.decode(errors="replace").strip()}')
    return result.stdout if binary else result.stdout.decode('utf-8').strip()


def outer_status(root):
    raw = git(root, 'status', '--porcelain=v1', '-z', '--untracked-files=all',
              '--ignored=matching', binary=True)
    records = raw.split(b'\0')
    if records and records[-1] == b'':
        records.pop()
    observed = {}
    for record in records:
        if len(record) < 4 or record[2:3] != b' ':
            raise FallbackError('outer Git status record is malformed')
        code, path = record[:2].decode('ascii'), record[3:].decode('utf-8', errors='strict')
        if code in ('??', '!!') and path in IGNORED_ROOTS:
            observed[path] = code
        elif path in OVERLAY_PATHS and code in ('A ', 'M '):
            observed[path] = code
        else:
            raise FallbackError(f'outer source has unapproved tracked/untracked/ignored path: {code} {path}')
    for path in IGNORED_ROOTS:
        if observed.get(path) != '!!':
            raise FallbackError(f'outer status must report exact ignored dependency path {path}')
    for path in OVERLAY_PATHS:
        if observed.get(path) not in ('M ', 'A '):
            raise FallbackError(f'outer status must contain staged overlay path {path}')
    if set(observed) != set(IGNORED_ROOTS) | set(OVERLAY_PATHS):
        raise FallbackError('outer source status differs from exact fallback/overlay allowlist')
    return observed


def tracked_blob(root, revision, path, expected_oid, expected_mode='100644'):
    oid = git(root, 'rev-parse', f'{revision}:{path}')
    if oid != expected_oid:
        raise FallbackError(f'tracked fallback input changed: {path}')
    raw = git(root, 'show', f'{revision}:{path}', binary=True)
    physical = Path(root) / path
    try:
        info = physical.lstat()
    except OSError as error:
        raise FallbackError(f'missing tracked fallback input {physical}: {error}') from error
    if not stat.S_ISREG(info.st_mode) or physical.read_bytes() != raw \
            or stat.S_IMODE(info.st_mode) != (int(expected_mode, 8) & 0o777):
        raise FallbackError(f'tracked fallback input bytes/type changed: {path}')
    return raw, oid


def check_wraps(root, revision):
    nano, nano_oid = tracked_blob(root, revision, 'subprojects/nanoarrow.wrap', NANO_WRAP_OID)
    xx, xx_oid = tracked_blob(root, revision, 'subprojects/xxhash.wrap', XX_WRAP_OID)
    if sha256(nano) != NANO_WRAP_SHA256 or sha256(xx) != XX_WRAP_SHA256:
        raise FallbackError('tracked fallback wrap hash changed')
    parser = configparser.ConfigParser(interpolation=None, strict=True)
    try:
        parser.read_string(nano.decode('utf-8'))
        nano_url, nano_revision = parser['wrap-git']['url'], parser['wrap-git']['revision']
        parser.read_string(xx.decode('utf-8'))
        xx_hash = parser['wrap-file']['source_hash']
        xx_dir = parser['wrap-file']['directory']
        xx_archive = parser['wrap-file']['source_filename']
    except (UnicodeError, configparser.Error, KeyError) as error:
        raise FallbackError(f'fallback wrap metadata is invalid: {error}') from error
    if nano_url != 'https://github.com/apache/arrow-nanoarrow.git' \
            or nano_revision != NANO_COMMIT:
        raise FallbackError('nanoarrow wrap URL/revision differs from pinned identity')
    if xx_hash != XX_ARCHIVE_SHA256 or xx_dir != 'xxHash-0.8.4' \
            or xx_archive != 'xxHash-0.8.4.tar.gz':
        raise FallbackError('xxHash wrap archive identity differs from pinned identity')
    return dict(nanoarrow=dict(wrap_oid=nano_oid, wrap_sha256=sha256(nano),
                               url=nano_url, revision=nano_revision),
                xxhash=dict(wrap_oid=xx_oid, wrap_sha256=sha256(xx),
                            archive_sha256=xx_hash, directory=xx_dir,
                            archive=xx_archive))


def nested_status(root, allowed):
    raw = git(root, 'status', '--porcelain=v1', '-z', '--untracked-files=all',
              '--ignored=matching', binary=True)
    records = [record for record in raw.split(b'\0') if record]
    parsed = []
    for record in records:
        if len(record) < 4 or record[2:3] != b' ':
            raise FallbackError(f'nested Git status is malformed in {root}')
        code, path = record[:2].decode('ascii'), record[3:].decode('utf-8', errors='strict')
        parsed.append((code, path))
    if sorted(parsed) != sorted(allowed):
        raise FallbackError(f'nested fallback checkout is dirty or has extra files: {root}')


def walk_tree(root, *, ignored=()):
    entries = {}
    root = Path(root)
    for parent, dirs, files in os.walk(root, topdown=True, followlinks=False):
        current = Path(parent)
        rel_parent = current.relative_to(root).as_posix()
        if rel_parent == '.':
            dirs[:] = [name for name in dirs if name != '.git']
        for name in list(dirs):
            path = current / name
            rel = path.relative_to(root).as_posix()
            if rel in ignored:
                dirs.remove(name)
                continue
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                entries[rel] = ('symlink', stat.S_IMODE(info.st_mode), os.readlink(path))
                dirs.remove(name)
            elif stat.S_ISDIR(info.st_mode):
                entries[rel] = ('directory', stat.S_IMODE(info.st_mode), None)
            else:
                raise FallbackError(f'special directory entry in fallback: {path}')
        for name in files:
            path = current / name
            rel = path.relative_to(root).as_posix()
            if rel in ignored:
                continue
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                entries[rel] = ('symlink', stat.S_IMODE(info.st_mode), os.readlink(path))
            elif stat.S_ISREG(info.st_mode):
                entries[rel] = ('file', stat.S_IMODE(info.st_mode), sha256(path.read_bytes()))
            else:
                raise FallbackError(f'special file in fallback: {path}')
    return entries


def nanoarrow_manifest(source_root, wrap):
    nested = source_root / 'subprojects/nanoarrow'
    try:
        top = Path(git(nested, 'rev-parse', '--show-toplevel')).resolve()
        shallow = git(nested, 'rev-parse', '--is-shallow-repository')
        head = git(nested, 'rev-parse', 'HEAD')
        tree = git(nested, 'rev-parse', 'HEAD^{tree}')
    except (OSError, FallbackError) as error:
        raise FallbackError(f'nanoarrow checkout identity unavailable: {error}') from error
    if top != nested.resolve() or shallow != 'false' or head != NANO_COMMIT or tree != NANO_TREE:
        raise FallbackError('nanoarrow nested Git root/revision/tree/shallow identity mismatch '
                            f'(root={top}, shallow={shallow}, head={head}, tree={tree})')
    nested_info = nested.lstat()
    if not stat.S_ISDIR(nested_info.st_mode):
        raise FallbackError('nanoarrow checkout root must be a regular directory')
    tracked = git(nested, 'ls-files', '-s', '-z', binary=True).split(b'\0')
    manifest, expected_paths = {'.': dict(type='directory', mode=stat.S_IMODE(nested_info.st_mode))}, set()
    for record in tracked:
        if not record:
            continue
        metadata, path_raw = record.split(b'\t', 1)
        mode, oid, stage = metadata.decode('ascii').split()
        path = path_raw.decode('utf-8')
        if stage != '0' or mode not in ('100644', '100755', '120000'):
            raise FallbackError(f'nanoarrow has unsupported tracked entry: {path}')
        blob = git(nested, 'cat-file', 'blob', oid, binary=True)
        absolute = nested / path
        info = absolute.lstat()
        if mode == '120000':
            if path != NANO_LINK_PATH or oid != NANO_LINK_OID \
                    or not stat.S_ISLNK(info.st_mode) or os.readlink(absolute) != '../..' \
                    or stat.S_IMODE(info.st_mode) != 0o777 \
                    or os.readlink(absolute).encode('utf-8') != blob:
                raise FallbackError(f'nanoarrow tracked symlink differs from Git blob: {path}')
            resolved = (absolute.parent / os.readlink(absolute)).resolve()
            if resolved != nested.resolve():
                raise FallbackError(f'nanoarrow tracked symlink does not resolve to checkout root: {path}')
            kind = 'symlink'
        else:
            expected_mode = int(mode, 8) & 0o777
            if not stat.S_ISREG(info.st_mode) or absolute.read_bytes() != blob \
                    or stat.S_IMODE(info.st_mode) != expected_mode:
                raise FallbackError(f'nanoarrow tracked file differs from Git blob: {path}')
            kind = 'file'
        expected_paths.add(path)
        entry = dict(type=kind, git_mode=mode,
                     mode=stat.S_IMODE(info.st_mode), sha256=sha256(blob))
        if kind == 'symlink':
            entry['target'] = os.readlink(absolute)
        manifest[path] = entry
    marker = nested / NANO_MARKER
    # Meson records the SHA-256 of the tracked wrap bytes, followed by a newline.
    marker_bytes = (wrap['nanoarrow']['wrap_sha256'] + '\n').encode('ascii')
    if not marker.is_file() or marker.is_symlink() or marker.read_bytes() != marker_bytes:
        raise FallbackError('nanoarrow wrap-hash marker is missing or invalid')
    expected_paths.add(NANO_MARKER)
    actual = walk_tree(nested)
    expected_dirs = {str(Path(path).parent).replace('\\', '/') for path in expected_paths
                     if str(Path(path).parent) not in ('', '.')}
    for path in tuple(expected_dirs):
        while path not in ('', '.'):
            expected_dirs.add(path)
            path = str(Path(path).parent).replace('\\', '/')
    expected = set(expected_paths) | expected_dirs
    if set(actual) != expected:
        raise FallbackError('nanoarrow filesystem has missing or additional files/directories '
                            f'(missing={sorted(expected - set(actual))[:5]}, '
                            f'extra={sorted(set(actual) - expected)[:5]})')
    marker_info = marker.lstat()
    if not stat.S_ISREG(marker_info.st_mode) or stat.S_IMODE(marker_info.st_mode) != 0o644:
        raise FallbackError('nanoarrow wrap-hash marker type/mode is invalid')
    manifest[NANO_MARKER] = dict(type='file', mode=stat.S_IMODE(marker_info.st_mode),
                                 sha256=sha256(marker_bytes))
    for path in expected_dirs:
        if path in actual and actual[path][0] != 'directory':
            raise FallbackError(f'nanoarrow directory entry changed type: {path}')
        if path in actual:
            manifest[path] = dict(type='directory', mode=actual[path][1])
    for path, observed in actual.items():
        expected_entry = manifest[path]
        if expected_entry['type'] == 'file':
            expected_value = expected_entry['sha256']
        elif expected_entry['type'] == 'symlink':
            expected_value = expected_entry['target']
        else:
            expected_value = None
        expected_tuple = (expected_entry['type'], expected_entry['mode'], expected_value)
        if observed != expected_tuple:
            raise FallbackError(f'nanoarrow filesystem entry differs from exact manifest: {path}')
    nested_status(nested, [('??', NANO_MARKER)])
    return dict(commit=head, tree=tree, shallow=False, tracked_wrap_oid=wrap['nanoarrow']['wrap_oid'],
                marker=dict(path=NANO_MARKER, sha256=sha256(marker_bytes)),
                entries=manifest, manifest_sha256=sha256(canonical(manifest)))


def canonical(value):
    import json
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def archive_expected(source_root, revision, archive_path, wrap):
    expected = {}
    try:
        raw = archive_path.read_bytes()
    except OSError as error:
        raise FallbackError(f'xxHash source archive is unavailable: {error}') from error
    if sha256(raw) != XX_ARCHIVE_SHA256 or sha256(raw) != wrap['xxhash']['archive_sha256']:
        raise FallbackError('xxHash source archive SHA-256 differs from tracked wrap')
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
            members = archive.getmembers()
            seen_members = set()
            for member in members:
                name = member.name
                if member.mode & ~0o777:
                    raise FallbackError(f'xxHash archive has special permission bits: {name}')
                pure = PurePosixPath(name)
                if pure.is_absolute() or '\\' in name or any(part in ('..', '.') for part in pure.parts):
                    raise FallbackError(f'unsafe xxHash archive path: {name}')
                if not pure.parts or pure.parts[0] != 'xxHash-0.8.4':
                    raise FallbackError(f'xxHash archive has unexpected root/path: {name}')
                relative = PurePosixPath(*pure.parts[1:]).as_posix()
                if not relative or relative in ('', '.'):
                    if name.rstrip('/') in seen_members:
                        raise FallbackError(f'duplicate xxHash archive path: {name}')
                    seen_members.add(name.rstrip('/'))
                    if member.isdir():
                        expected['.'] = dict(type='directory', mode=member.mode & 0o777)
                        continue
                    raise FallbackError('xxHash archive root must be a directory')
                if relative in seen_members or relative in expected:
                    raise FallbackError(f'duplicate xxHash archive path: {relative}')
                seen_members.add(relative)
                if member.isdir():
                    expected[relative] = dict(type='directory', mode=member.mode & 0o777)
                elif member.isreg():
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise FallbackError(f'cannot read xxHash archive member: {name}')
                    data = stream.read()
                    expected[relative] = dict(type='file', mode=member.mode & 0o777,
                                              sha256=sha256(data), _bytes=data)
                else:
                    raise FallbackError(f'xxHash archive contains link/special member: {name}')
    except (OSError, tarfile.TarError) as error:
        raise FallbackError(f'cannot safely parse xxHash archive: {error}') from error
    # Include implicit directories and overlay the exact tracked Meson package files.
    for path in list(expected):
        parent = PurePosixPath(path).parent
        while str(parent) not in ('', '.'):
            expected.setdefault(str(parent), dict(type='directory', mode=0o755))
            parent = parent.parent
    for relative, (oid, mode) in PACKAGE_FILES.items():
        tracked, _ = tracked_blob(source_root, revision,
                                  f'subprojects/packagefiles/xxhash-0.8.4/{relative}', oid,
                                  expected_mode=mode)
        expected[relative] = dict(type='file', mode=int(mode, 8) & 0o777,
                                  sha256=sha256(tracked), _bytes=tracked,
                                  git_blob_oid=oid)
    return raw, expected


def xxhash_manifest(source_root, revision, wrap):
    source_root = Path(source_root)
    cache = source_root / 'subprojects/packagecache'
    if not cache.is_dir() or cache.is_symlink():
        raise FallbackError('xxHash packagecache directory is missing or linked')
    cache_info = cache.lstat()
    if not stat.S_ISDIR(cache_info.st_mode) or stat.S_IMODE(cache_info.st_mode) != 0o755:
        raise FallbackError('xxHash packagecache directory type/mode is invalid')
    package_files = list(cache.iterdir())
    if len(package_files) != 1 or package_files[0].name != wrap['xxhash']['archive'] \
            or not package_files[0].is_file() or package_files[0].is_symlink():
        raise FallbackError('packagecache must contain exactly the pinned regular archive')
    archive_info = package_files[0].lstat()
    if not stat.S_ISREG(archive_info.st_mode) or stat.S_IMODE(archive_info.st_mode) != 0o600:
        raise FallbackError('xxHash packagecache archive type/mode is invalid')
    raw, expected = archive_expected(source_root, revision, package_files[0], wrap)
    extracted = source_root / 'subprojects/xxHash-0.8.4'
    root_expected = expected.pop('.', None)
    if root_expected is None:
        raise FallbackError('xxHash archive has no explicit top-level directory')
    root_info = extracted.lstat()
    if not stat.S_ISDIR(root_info.st_mode) \
            or stat.S_IMODE(root_info.st_mode) != root_expected['mode']:
        raise FallbackError('xxHash extracted root type/mode differs from archive')
    marker = extracted / XX_WRAP_MARKER
    expected[XX_WRAP_MARKER] = dict(type='file', mode=0o644,
                                    sha256=sha256((wrap['xxhash']['wrap_sha256'] + '\n').encode('ascii')))
    actual = walk_tree(extracted)
    marker_bytes = (wrap['xxhash']['wrap_sha256'] + '\n').encode('ascii')
    if not marker.is_file() or marker.is_symlink() or marker.read_bytes() != marker_bytes:
        raise FallbackError('xxHash wrap-hash marker is missing or invalid')
    if set(actual) != set(expected):
        raise FallbackError('xxHash extracted filesystem has missing or extra paths')
    manifest = {}
    manifest['.'] = root_expected
    for path, wanted in expected.items():
        observed = actual[path]
        if observed[0] != wanted['type']:
            raise FallbackError(f'xxHash extracted type differs from archive/patch: {path}')
        if observed[1] & 0o777 != wanted['mode'] & 0o777:
            raise FallbackError(f'xxHash extracted mode differs from archive/patch: {path}')
        if wanted['type'] == 'file' and observed[2] != wanted['sha256']:
            raise FallbackError(f'xxHash extracted bytes differ from archive/patch: {path}')
        manifest[path] = {key: value for key, value in wanted.items() if key != '_bytes'}
    packagecache = dict(directory=dict(type='directory', mode=stat.S_IMODE(cache_info.st_mode)),
                        archive=dict(path=wrap['xxhash']['archive'], type='file',
                                     mode=stat.S_IMODE(archive_info.st_mode),
                                     sha256=sha256(raw)))
    identity = dict(packagecache=packagecache, extracted=manifest)
    return dict(wrap_oid=wrap['xxhash']['wrap_oid'],
                package_overlay={path: dict(git_blob_oid=oid, mode=mode)
                                 for path, (oid, mode) in PACKAGE_FILES.items()},
                packagecache=packagecache, archive=wrap['xxhash']['archive'],
                archive_sha256=sha256(raw), marker=dict(path=XX_WRAP_MARKER,
                sha256=sha256(marker_bytes)), entries=manifest,
                manifest_sha256=sha256(canonical(identity)))


def validate_fallbacks(source_root):
    source_root = Path(source_root).resolve()
    outer = outer_status(source_root)
    try:
        revision = git(source_root, 'rev-parse', 'HEAD')
    except FallbackError as error:
        raise FallbackError(f'outer source checkout unavailable: {error}') from error
    wraps = check_wraps(source_root, revision)
    nano = nanoarrow_manifest(source_root, wraps)
    xxhash = xxhash_manifest(source_root, revision, wraps)
    # Check the allowlist again so concurrent unrelated dirt cannot pass.
    if outer_status(source_root) != outer:
        raise FallbackError('outer source status changed during fallback verification')
    return dict(schema=SCHEMA, outer_ignored_paths=list(IGNORED_ROOTS),
                nanoarrow=nano, xxhash=xxhash)


def validate_fallback_seed(seed_root, status_mode):
    """Validate a hydrated local donor without requiring benchmark overlay files."""
    seed_root = Path(seed_root).resolve()
    def status_snapshot():
        if status_mode == 'overlay_staged':
            return outer_status(seed_root)
        if status_mode != 'clean':
            raise FallbackError('fallback seed status mode must be clean or overlay_staged')
        raw = git(seed_root, 'status', '--porcelain=v1', '-z', '--untracked-files=all',
                  '--ignored=matching', binary=True)
        records = [record for record in raw.split(b'\0') if record]
        observed = {}
        for record in records:
            if len(record) < 4 or record[2:3] != b' ':
                raise FallbackError('fallback seed outer Git status record is malformed')
            code = record[:2].decode('ascii')
            path = record[3:].decode('utf-8', errors='strict')
            if code != '!!' or path not in IGNORED_ROOTS or path in observed:
                raise FallbackError(f'fallback seed has an unapproved outer status entry: {code} {path}')
            observed[path] = code
        if observed != {path: '!!' for path in IGNORED_ROOTS}:
            raise FallbackError('clean fallback seed must contain exactly the three ignored dependency roots')
        return observed

    outer = status_snapshot()
    revision = git(seed_root, 'rev-parse', 'HEAD')
    wraps = check_wraps(seed_root, revision)
    nano = nanoarrow_manifest(seed_root, wraps)
    xxhash = xxhash_manifest(seed_root, revision, wraps)
    if status_snapshot() != outer:
        raise FallbackError('fallback seed changed during provenance verification')
    return dict(schema=SCHEMA, outer_ignored_paths=list(IGNORED_ROOTS),
                nanoarrow=nano, xxhash=xxhash)
