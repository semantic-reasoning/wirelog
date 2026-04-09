# Example 09: Retraction Basics

## Overview

This example demonstrates incremental retraction in wirelog using
`wl_easy_remove_sym` (which wraps `wl_session_remove`).  When a base fact
is retracted, the engine propagates `-1` deltas to any derived relations
that depended on it.  The scope here is non-recursive: a single join rule
derives `mutual(A, B)` from `friend(A, B), friend(B, A)`, and removing one
side of the friendship pair produces exactly two `-1` deltas on `mutual`,
one for each derived tuple that is no longer supported.

## Datalog Program

```datalog
.decl friend(a: symbol, b: symbol)
.decl mutual(a: symbol, b: symbol)

mutual(A, B) :- friend(A, B), friend(B, A).
```

## Build & Run

```sh
meson compile -C build retract_demo
./build/examples/09-retraction-basics/retract_demo
meson test -C build example_09_retract_demo_golden
```

## Expected Output

```
Example 09: Retraction Basics (-1 deltas)
=========================================

=== step 1: alice <-> bob ===
+ mutual("alice", "bob")
+ mutual("bob", "alice")

=== step 2: one-way alice -> carol ===

=== step 3: bob unfriends alice ===
- mutual("alice", "bob")
- mutual("bob", "alice")

Done.
```

## What This Demonstrates

- **Step 1**: `wl_easy_insert_sym` inserts `friend(alice,bob)` and
  `friend(bob,alice)`.  After `wl_easy_step`, two `+1` deltas fire for
  `mutual(alice,bob)` and `mutual(bob,alice)`.
- **Step 2**: `wl_easy_insert_sym` inserts `friend(alice,carol)`.  Since
  `friend(carol,alice)` does not exist, the join produces no new `mutual`
  tuples and no deltas fire.
- **Step 3**: `wl_easy_remove_sym` retracts `friend(bob,alice)`.  After
  `wl_easy_step`, the engine emits exactly two `-1` deltas:
  `mutual(alice,bob)` and `mutual(bob,alice)`, reflecting that both
  derived tuples are no longer supported.
- **In-driver assertion**: The driver counts the `-1` deltas on `mutual` in
  step 3 and exits non-zero if the count is not exactly 2.

## What You Will NOT See Here

- **Recursive retraction under update** (e.g., transitive closure with
  deletion) -- covered in issue #443.
- **Multi-step time evolution** (insert/retract/insert across many steps with
  cumulative state tracking) -- covered in issue #444.
- **Snapshot vs delta comparison** (querying full materialized state after
  retraction) -- covered in issue #445.

## See Also

- `examples/08-delta-queries/` -- the preceding example that introduces
  delta callbacks with `wl_easy_print_delta`.
- `tests/test_delta_retraction.c` -- engine-level retraction tests (issue
  #158) that exercise the same `wl_session_remove` contract used here.
