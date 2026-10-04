#!/usr/bin/env python3
"""Capture host telemetry used only by the paired A/A qualification gate."""
from __future__ import annotations

from datetime import datetime, timezone
import math
import json
import os
from pathlib import Path, PurePosixPath
import re
import time


def _entry(value=None, reason=None, unit=None):
    return dict(value=value, unavailable_reason=reason, unit=unit)


def _read(path, encoding='utf-8'):
    return Path(path).read_text(encoding=encoding).strip()


def _unescape_mount_field(value):
    return re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), value)


def _within(path, root):
    path = PurePosixPath(path)
    root = PurePosixPath(root)
    return path == root or root == PurePosixPath('/') or root in path.parents


def parse_cgroup_membership(text):
    memberships = []
    for line in text.splitlines():
        fields = line.split(':', 2)
        if len(fields) != 3:
            raise ValueError('malformed /proc/self/cgroup line')
        hierarchy, controllers, path = fields
        if not path.startswith('/') or '..' in PurePosixPath(path).parts:
            raise ValueError('unsafe cgroup membership path')
        if hierarchy == '0' and not controllers:
            memberships.append(('v2', path, ''))
        elif 'cpu' in controllers.split(','):
            memberships.append(('v1', path, controllers))
    if len(memberships) != 1:
        raise ValueError('missing or ambiguous CPU cgroup membership')
    return memberships[0]


def _parse_mounts(text, version, controllers):
    mounts = []
    for line in text.splitlines():
        if ' - ' not in line:
            raise ValueError('malformed mountinfo line')
        left, right = line.split(' - ', 1)
        fields = left.split()
        tail = right.split()
        if len(fields) < 5 or len(tail) < 3:
            raise ValueError('malformed mountinfo fields')
        fs_type = tail[0]
        if version == 'v2' and fs_type == 'cgroup2':
            matches = True
        elif version == 'v1' and fs_type == 'cgroup':
            options = set(tail[2].split(','))
            matches = bool(options.intersection(controllers))
        else:
            matches = False
        if matches:
            root = _unescape_mount_field(fields[3])
            mountpoint = _unescape_mount_field(fields[4])
            mounts.append((root, mountpoint))
    return mounts


def resolve_cgroup(membership_text, mountinfo_text, *, proc_root='/proc'):
    version, member_path, controllers = parse_cgroup_membership(membership_text)
    mounts = _parse_mounts(mountinfo_text, version,
                           set(controllers.split(',')) if version == 'v1' else set())
    matching = [(root, point) for root, point in mounts if _within(member_path, root)]
    if not matching:
        raise ValueError('CPU cgroup mount does not contain membership path')
    mount_root, mountpoint = max(matching, key=lambda item: len(PurePosixPath(item[0]).parts))
    relative = PurePosixPath(member_path).relative_to(PurePosixPath(mount_root))
    mount = Path(mountpoint).resolve(strict=True)
    effective = (mount / str(relative)).resolve(strict=True)
    if effective != mount and mount not in effective.parents:
        raise ValueError('resolved CPU cgroup escaped mounted hierarchy')
    levels = []
    current = effective
    while True:
        levels.append(current)
        if current == mount:
            break
        current = current.parent
        if mount not in current.parents and current != mount:
            raise ValueError('CPU cgroup ancestry escaped mounted hierarchy')
    observations = []
    for path in levels:
        stats = {}
        stat_name = 'cpu.stat'
        stat_lines = (path / stat_name).read_text(encoding='utf-8').splitlines()
        for line in stat_lines:
            fields = line.split()
            if len(fields) == 2 and fields[0] in ('nr_throttled', 'throttled_usec', 'throttled_time'):
                stats[fields[0]] = int(fields[1], 10)
        if version == 'v2':
            required = ('nr_throttled', 'throttled_usec')
            quota_text = _read(path / 'cpu.max')
            quota_fields = quota_text.split()
            if len(quota_fields) != 2:
                raise ValueError('malformed cgroup v2 cpu.max')
            cpu_limit = dict(quota=quota_fields[0], period_us=int(quota_fields[1], 10))
            throttle_unit = 'us'
            throttle_key = 'throttled_usec'
        else:
            required = ('nr_throttled', 'throttled_time')
            quota = int(_read(path / 'cpu.cfs_quota_us'), 10)
            period = int(_read(path / 'cpu.cfs_period_us'), 10)
            cpu_limit = dict(quota_us=quota, period_us=period)
            throttle_unit = 'ns'
            throttle_key = 'throttled_time'
        if any(key not in stats for key in required):
            raise ValueError(f'missing CPU throttle counter at {path}')
        if any(value < 0 for value in stats.values()) or cpu_limit.get('period_us', 0) <= 0:
            raise ValueError(f'invalid CPU cgroup counters or limit at {path}')
        observations.append(dict(path=str(path.relative_to(mount)), cpu_limit=cpu_limit,
                                 nr_throttled=stats['nr_throttled'],
                                 throttle_time=stats[throttle_key], throttle_time_unit=throttle_unit))
    return dict(version=version, membership_path=member_path,
                mount_root=mount_root, mountpoint=str(mount), levels=observations)


def _psi(path, resource, lines):
    result = {}
    for line in lines.splitlines():
        fields = line.split()
        if not fields or fields[0] not in ('some', 'full'):
            continue
        values = dict(item.split('=', 1) for item in fields[1:] if '=' in item)
        if 'avg10' in values:
            value = float(values['avg10'])
            if value < 0 or not math.isfinite(value):
                raise ValueError(f'invalid {resource} PSI avg10')
            result[fields[0]] = value
    return result


def _snapshot(cpu, child_affinity, *, proc_root='/proc', sys_root='/sys'):
    proc = Path(proc_root)
    sys = Path(sys_root)
    snapshot = dict(timestamp_utc=datetime.now(timezone.utc).isoformat(),
                    monotonic_ns=time.monotonic_ns(), selected_cpu=cpu)
    try:
        snapshot['machine_id'] = _entry(_read('/etc/machine-id'))
    except (OSError, ValueError) as error:
        snapshot['machine_id'] = _entry(reason=f'{type(error).__name__}: {error}')
    try:
        snapshot['collector_affinity'] = _entry(sorted(os.sched_getaffinity(0)), unit='cpu_ids')
    except (AttributeError, OSError, ValueError) as error:
        snapshot['collector_affinity'] = _entry(reason=f'{type(error).__name__}: {error}')
    snapshot['child_affinity'] = (_entry(sorted(child_affinity), unit='cpu_ids')
                                  if child_affinity is not None else
                                  _entry(reason='child affinity was not observed', unit='cpu_ids'))
    try:
        gov = _read(sys / f'devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor')
        snapshot['governor'] = _entry(gov)
    except (OSError, ValueError) as error:
        snapshot['governor'] = _entry(reason=f'{type(error).__name__}: {error}')
    for field, file, line_kind in (
        ('cpu_psi_some_avg10_percent', proc / 'pressure/cpu', 'some'),
        ('cpu_psi_full_avg10_percent', proc / 'pressure/cpu', 'full'),
        ('memory_psi_full_avg10_percent', proc / 'pressure/memory', 'full'),
    ):
        try:
            values = _psi(file, file.name, _read(file))
            if line_kind not in values:
                raise ValueError(f'missing {line_kind} PSI avg10')
            snapshot[field] = _entry(values[line_kind], unit='percent')
        except (OSError, ValueError) as error:
            snapshot[field] = _entry(reason=f'{type(error).__name__}: {error}', unit='percent')
    try:
        vmstat = {}
        for line in _read(proc / 'vmstat').splitlines():
            fields = line.split()
            if len(fields) == 2 and fields[0] in ('pswpin', 'pswpout'):
                vmstat[fields[0]] = int(fields[1], 10)
        if set(vmstat) != {'pswpin', 'pswpout'} or any(value < 0 for value in vmstat.values()):
            raise ValueError('missing or invalid vmstat swap counters')
        snapshot['swap_pages'] = _entry(vmstat, unit='pages')
    except (OSError, ValueError) as error:
        snapshot['swap_pages'] = _entry(reason=f'{type(error).__name__}: {error}', unit='pages')
    try:
        identity = _read(proc / 'self/cgroup')
        mountinfo = _read(proc / 'self/mountinfo')
        cgroup = resolve_cgroup(identity, mountinfo, proc_root=proc_root)
        snapshot['cgroup'] = _entry(cgroup)
    except (OSError, ValueError, IndexError) as error:
        snapshot['cgroup'] = _entry(reason=f'{type(error).__name__}: {error}')
    return snapshot


def read_snapshot(cpu, child_affinity=None):
    """Return raw evidence; unavailable host metrics retain an explicit reason."""
    return _snapshot(cpu, child_affinity)


def strict_json(line):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'duplicate JSON key: {key}')
            result[key] = value
        return result
    return json.loads(line, object_pairs_hook=unique,
                      parse_constant=lambda value: (_ for _ in ()).throw(
                          ValueError(f'nonfinite JSON value: {value}')))
