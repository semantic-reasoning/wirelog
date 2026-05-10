# Example 12: Snapshot vs Delta

## Overview

Wirelog exposes two ways to read derived facts: **delta mode**
(`wirelog_easy_set_delta_cb` + `wirelog_easy_step`) streams sign-tagged changes
as they happen, while **snapshot mode** (`wirelog_easy_snapshot`) returns the
full IDB in one shot.  Two APIs, same answer, different costs.

This example runs the same access-control Datalog program through both
APIs and verifies that the results are identical.

## Datalog Program

```datalog
.decl can(user: symbol, perm: symbol)
.decl granted(user: symbol, perm: symbol)
granted(U, P) :- can(U, P).
```

Five `can` facts are inserted (alice/read, alice/write, bob/read,
bob/admin, carol/read).  The single rule derives a corresponding
`granted` tuple for each.

## Build & Run

```sh
meson compile -C build snapshot_demo
./build/examples/12-snapshot-vs-delta/snapshot_demo
```

## Expected Output

```
Example 12: Snapshot vs Delta
=============================

=== delta mode: wirelog_easy_step ===
=== snapshot mode: wirelog_easy_snapshot ===

delta: granted("alice", "read")        snapshot: granted("alice", "read")        MATCH
delta: granted("alice", "write")       snapshot: granted("alice", "write")       MATCH
delta: granted("bob", "admin")         snapshot: granted("bob", "admin")         MATCH
delta: granted("bob", "read")          snapshot: granted("bob", "read")          MATCH
delta: granted("carol", "read")        snapshot: granted("carol", "read")        MATCH

PASS
```

## When To Use Which

| API | Use when | Cost shape |
|---|---|---|
| `wirelog_easy_set_delta_cb` + `wirelog_easy_step` | Facts trickle in over time and you want to react to *changes* | Proportional to delta size |
| `wirelog_easy_snapshot` | One-shot batch query where you want the full IDB | Proportional to total IDB size |

Delta mode is ideal for long-running sessions where only incremental
updates matter (monitoring, streaming pipelines).  Snapshot mode is
simpler when you just need the current answer without tracking history.

## What This Demonstrates

- **Semantic equivalence** -- the incremental delta path produces the
  same derived facts as full re-evaluation via snapshot.
- **Two independent sessions** -- because `wirelog_easy_snapshot` is an
  evaluating call, the driver uses separate sessions to avoid
  double-counting.  See `wirelog-easy.h` for the full contract.
- **Deterministic comparison** -- both result sets are sorted before
  comparison so the test is stable regardless of backend evaluation
  order.

## What You Will NOT See Here

- **Retraction** (`-1` deltas) -- see Example 09 (#442).
- **Recursive relations under update** -- see Example 10 (#443).
- **Multi-step time evolution** -- see Example 11 (#444).

## See Also

- `examples/08-delta-queries/` -- single-step delta introduction
- `examples/09-retraction-basics/` -- retraction deltas
- `examples/10-recursive-under-update/` -- recursive transitive closure
- `examples/11-time-evolution/` -- per-epoch delta isolation
- `wirelog/wirelog-easy.h` -- convenience facade API
