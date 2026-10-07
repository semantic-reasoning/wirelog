#!/usr/bin/env python3
"""Meson Ninja executable override: cap each build at eight available CPUs."""

import os
from pathlib import Path
import shutil
import sys


MAX_JOBS = 8
REAL_ENV = 'WIRELOG_NINJA_REAL'


def available_jobs():
    """Prefer the process affinity, then Python's available CPU count."""
    affinity = getattr(os, 'sched_getaffinity', None)
    if affinity is not None:
        try:
            count = len(affinity(0))
            if count:
                return min(count, MAX_JOBS)
        except (OSError, NotImplementedError):
            pass
    count_cpu = getattr(os, 'process_cpu_count', os.cpu_count)
    return min(max(count_cpu() or 1, 1), MAX_JOBS)


def bounded_args(args, cap):
    """Normalize Ninja's -j forms, including short option clusters.

    Options with operands must retain those operands, and -t ends Ninja's
    option parsing (the remaining arguments belong to the subtool).
    """
    result = []
    jobs = cap
    i = 0
    while i < len(args):
        arg = args[i]
        i += 1
        if arg == '--':
            result.extend([arg, *args[i:]])
            break
        if not arg.startswith('-') or arg.startswith('--') or arg == '-':
            result.append(arg)
            continue
        cluster = arg[1:]
        flags = ''
        for pos, option in enumerate(cluster):
            if option in 'Cfjkldtw':
                value = cluster[pos + 1:]
                if not value:
                    if i == len(args):
                        raise ValueError(f'missing operand for -{option}')
                    value = args[i]
                    i += 1
                if option == 'j':
                    if not value.isascii() or not value.isdecimal():
                        raise ValueError(f'invalid Ninja job count: {value!r}')
                    requested = int(value)
                    if requested:
                        jobs = min(jobs, requested)
                    if flags:
                        result.append('-' + flags)
                else:
                    # Preserve this option and its operand verbatim.
                    result.append(arg)
                    if not cluster[pos + 1:]:
                        result.append(value)
                    if option == 't':
                        result.extend(args[i:])
                        return ['-j', str(jobs), *result]
                break
            if option not in 'vnh':
                # Unknown options fail in Ninja; never allow an unknown cluster
                # to conceal a jobs option that a future Ninja might recognize.
                if 'j' in cluster[pos:]:
                    raise ValueError(f'unsupported Ninja option cluster: {arg!r}')
                result.append(arg)
                break
            flags += option
        else:
            result.append(arg)
    return ['-j', str(jobs), *result]


def executable_path(selection):
    resolved = shutil.which(selection)
    if resolved is None:
        raise ValueError(f'Ninja executable not found: {selection!r}')
    backend = Path(resolved).resolve()
    if backend.samefile(Path(__file__)):
        raise ValueError('Ninja backend must differ from the cap launcher')
    return str(backend)


def resolve_backend():
    """Select the inherited Meson backend, or reuse an already active cap."""
    selection = os.environ.get('NINJA') or 'ninja'
    selected_path = Path(selection)
    if selected_path.is_file() and selected_path.samefile(Path(__file__)):
        selection = os.environ.get(REAL_ENV, '')
        if not selection:
            raise ValueError(f'{REAL_ENV} is required when the cap is active')
    return executable_path(selection)


def main(args):
    try:
        if args == ['--wirelog-resolve-backend']:
            print(resolve_backend())
            return 0
        selection = os.environ.get(REAL_ENV, '')
        if not selection or not Path(selection).is_absolute():
            raise ValueError(f'{REAL_ENV} must name an absolute Ninja executable')
        backend = executable_path(selection)
        os.execv(backend, [backend, *bounded_args(args, available_jobs())])
    except (OSError, ValueError) as exc:
        print(f'ninja-cap: {exc}', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
