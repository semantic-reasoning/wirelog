# Example 10: Recursive Under Update

## Overview

This example demonstrates incremental maintenance of a **recursive** Datalog
rule -- transitive closure (`reach`) -- under a sequence of insert, remove, and
re-insert operations on the base `edge` relation.  Unlike example 09, which
exercises retraction on a non-recursive single-join rule, this example shows
that the wirelog engine correctly propagates deletions (and subsequent
re-derivations) through a recursively defined relation.

## Datalog Program

```datalog
.decl edge(x: symbol, y: symbol)
.decl reach(x: symbol, y: symbol)

reach(X, Y) :- edge(X, Y).
reach(X, Z) :- reach(X, Y), edge(Y, Z).
```

## Build & Run

```sh
meson compile -C build tc_demo
./build/examples/10-recursive-under-update/tc_demo
meson test -C build example_10_tc_demo_golden
```

## Expected Output

```
Example 10: Recursive Under Update (Transitive Closure)
=======================================================

=== step 1: insert edges a->b->c->d ===
+ reach("a", "b")
+ reach("a", "c")
+ reach("a", "d")
+ reach("b", "c")
+ reach("b", "d")
+ reach("c", "d")

=== step 2: remove edge(b,c) ===
- reach("a", "c")
- reach("a", "d")
- reach("b", "c")
- reach("b", "d")

=== step 3: re-insert edge(b,c) ===
+ reach("a", "c")
+ reach("a", "d")
+ reach("b", "c")
+ reach("b", "d")

Done.
```

## What This Demonstrates

- **Recursive transitive closure**: `reach` is defined recursively over
  `edge`, computing all-pairs reachability in a directed graph.
- **Retraction of derived tuples**: Removing `edge(b,c)` causes the engine
  to retract exactly the `reach` tuples that depended on that edge --
  `reach(a,c)`, `reach(a,d)`, `reach(b,c)`, and `reach(b,d)` -- while
  leaving unaffected tuples (e.g., `reach(a,b)`, `reach(c,d)`) intact.
- **Re-derivation on re-insert**: Re-inserting the same edge re-derives
  exactly the same four `reach` tuples, demonstrating that the engine's
  internal state is fully consistent after retraction.
- **Per-step delta isolation**: Each `wl_easy_step` call produces only the
  deltas for that step's input changes; no cross-step leakage occurs.

## What You Will NOT See Here

- **Multi-step time evolution** (insert/retract/insert across many steps with
  cumulative state tracking) -- covered in issue #444.
- **Snapshot vs delta comparison** (querying full materialized state after
  retraction) -- covered in issue #445.
- **Non-recursive retraction basics** (single-join rule without recursion) --
  covered in example 09 (issue #442).

## See Also

- `examples/08-delta-queries/` -- introduces delta callbacks with
  `wl_easy_print_delta`.
- `examples/09-retraction-basics/` -- non-recursive retraction basics with
  `-1` deltas on a single-join rule.
- `tests/test_delta_retraction.c` -- engine-level retraction tests (issue
  #158) that exercise the same `wl_session_remove` contract used here.
