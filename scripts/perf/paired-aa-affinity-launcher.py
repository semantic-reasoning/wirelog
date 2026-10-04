#!/usr/bin/env python3
"""Pin a child, wait for the collector's observation, then exec its workload."""
import argparse
import json
import os
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpu', type=int, required=True)
    parser.add_argument('--ready-fd', type=int, required=True)
    parser.add_argument('--release-fd', type=int, required=True)
    parser.add_argument('binary')
    parser.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.cpu < 0:
        raise SystemExit('invalid CPU')
    try:
        os.sched_setaffinity(0, {args.cpu})
        affinity = sorted(os.sched_getaffinity(0))
        if affinity != [args.cpu]:
            raise OSError(f'child affinity mismatch: {affinity}')
        message = json.dumps(dict(pid=os.getpid(), affinity=affinity),
                             sort_keys=True, separators=(',', ':')).encode() + b'\n'
        os.write(args.ready_fd, message)
        released = os.read(args.release_fd, 1)
        if released != b'1':
            return 125
        os.execv(args.binary, [args.binary, *args.arguments])
    except BaseException as error:
        try:
            os.write(2, f'paired-aa-affinity-launcher: {type(error).__name__}: {error}\n'.encode())
        except OSError:
            pass
        return 126


if __name__ == '__main__':
    sys.exit(main())
