#!/usr/bin/env python3
"""Derive a small, structurally valid CRDT fixture from the full one.

The full CRDT fixture takes about 23 seconds to evaluate, which is too slow
to run on every `meson test` invocation -- and roughly eight times that under
the sanitizer legs, which build at -O0. This produces a subset small enough
for the default suite while keeping the properties the result-count sentinel
depends on.

Method: breadth-first descendant closure of the single root, restricted to
insert counters <= THRESHOLD, with removes restricted to the kept keys.
Taking a closure (rather than, say, the first N rows) is what keeps the
subset a valid CRDT instance: every kept node's parent is also kept, so the
tree stays connected and single-rooted.

The expected result count follows structurally:

    result = kept_inserts - kept_removes - 1

`result(c1, c2, v) :- nextVisible(c1, _, c2, n2), currentValue(c2, n2, v)`
projects away prev_node, and `assign(ctr, n, ctr, n, n)` forces v == n2, so
each tuple is (c1, c2, n2). In a chain every visible node has exactly one
predecessor, so distinct edges carry distinct (c2, n2) and the projection
cannot collide. The minus one is the root, which has no predecessor.

Note this derivation is a property of the subset, not something inherited
from docs/CRDT_PERF_BASELINE.md -- that document asserts the equivalent fact
for the full fixture on the back of an independent traversal check, which
does not transfer to an arbitrary subset.

Deterministic: no randomness, no dependence on dict iteration order.

Usage:
    scripts/perf/make_crdt_subset.py [--threshold N] [--src DIR] [--dst DIR]
"""

import argparse
import hashlib
import os
import sys
from collections import defaultdict, deque

DEFAULT_THRESHOLD = 2000


def read_csv(path):
    with open(path, encoding="utf-8") as f:
        return [line.rstrip("\n").split(",") for line in f if line.strip()]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD)
    ap.add_argument("--src", default="bench/data/crdt")
    ap.add_argument("--dst", default="bench/data/crdt-small")
    args = ap.parse_args()

    ins = read_csv(os.path.join(args.src, "Insert_input.csv"))
    rem = read_csv(os.path.join(args.src, "Remove_input.csv"))

    # Insert_input columns: ctr, node, parent_ctr, parent_node
    keys = {(r[0], r[1]) for r in ins}
    if len(keys) != len(ins):
        sys.exit("source inserts are not unique on (ctr, node); aborting")

    children = defaultdict(list)
    roots = []
    for r in ins:
        key, parent = (r[0], r[1]), (r[2], r[3])
        if parent in keys:
            children[parent].append(key)
        else:
            roots.append(key)
    if len(roots) != 1:
        sys.exit(f"expected exactly one root, found {len(roots)}; aborting")

    # Breadth-first closure from the root, keeping only small counters.
    limit = args.threshold
    keep = set()
    q = deque(roots)
    while q:
        node = q.popleft()
        if node in keep or int(node[0]) > limit:
            continue
        keep.add(node)
        # sort so the output is independent of input ordering
        for c in sorted(children[node], key=lambda k: (int(k[0]), int(k[1]))):
            q.append(c)

    kept_ins = [r for r in ins if (r[0], r[1]) in keep]
    kept_rem = [r for r in rem if (r[0], r[1]) in keep]

    os.makedirs(args.dst, exist_ok=True)
    ip = os.path.join(args.dst, "Insert_input.csv")
    rp = os.path.join(args.dst, "Remove_input.csv")
    with open(ip, "w", encoding="utf-8") as f:
        for r in kept_ins:
            f.write(",".join(r) + "\n")
    with open(rp, "w", encoding="utf-8") as f:
        for r in kept_rem:
            f.write(",".join(r) + "\n")

    expected = len(kept_ins) - len(kept_rem) - 1
    print(f"threshold      = {limit}")
    print(f"inserts        = {len(kept_ins)}")
    print(f"removes        = {len(kept_rem)}")
    print(f"expected result= {expected}   (inserts - removes - 1)")
    print(f"{os.path.basename(ip)} sha256 = {sha256(ip)}")
    print(f"{os.path.basename(rp)} sha256 = {sha256(rp)}")


if __name__ == "__main__":
    main()
