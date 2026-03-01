---
title: Tutorial
nav_order: 2
---

# Tutorial

This guide walks you through wirelog from your first Datalog program to recursion, negation, and aggregation.

## 1. Facts and Rules

A Datalog program consists of **facts** (ground data) and **rules** (logical derivations).

```
# Facts: edges in a graph
edge(1, 2).
edge(2, 3).

# Rule: copy facts into a result relation
result(x, y) :- edge(x, y).
```

Save this as `first.dl` and run:

```bash
wirelog-cli first.dl
```

Output:

```
result(1, 2)
result(2, 3)
```

Facts state what is true. Rules derive new facts from existing ones. The `:-` symbol reads as "if" -- `result(x, y)` holds **if** `edge(x, y)` holds.

## 2. Joins

When multiple predicates share a variable, wirelog joins them on that variable.

```
parent(1, 2).
parent(2, 3).
name(1, "Alice").
name(2, "Bob").
name(3, "Carol").

# Join parent and name on shared variable p
child_name(n) :- parent(p, c), name(c, n).
```

Output:

```
child_name("Bob")
child_name("Carol")
```

The shared variable `c` forces wirelog to match `parent(p, c)` with `name(c, n)` where the second column of `parent` equals the first column of `name`.

## 3. Filters

Comparison operators filter results.

```
edge(1, 2).
edge(2, 3).
edge(3, 1).

# Only edges where source > target
backward(x, y) :- edge(x, y), x > y.
```

Output:

```
backward(3, 1)
```

Available operators: `=`, `!=`, `<`, `>`, `<=`, `>=`.

## 4. Recursion

Datalog's power comes from recursive rules. This computes **transitive closure** -- all paths in a graph:

```
edge(1, 2).
edge(2, 3).
edge(3, 4).

# Base case: direct edges are paths
tc(x, y) :- edge(x, y).

# Recursive case: extend paths by one edge
tc(x, z) :- tc(x, y), edge(y, z).
```

Output:

```
tc(1, 2)
tc(1, 3)
tc(1, 4)
tc(2, 3)
tc(2, 4)
tc(3, 4)
```

wirelog automatically computes the fixed point -- it keeps applying rules until no new facts are derived.

## 5. Negation

Use `!` to negate a predicate. This finds nodes with no outgoing edges:

```
edge(1, 2).
edge(2, 3).

node(x) :- edge(x, _).
node(y) :- edge(_, y).

# Nodes that are NOT a source of any edge
sink(x) :- node(x), !edge(x, _).
```

Output:

```
sink(3)
```

**Important**: Variables in negated predicates must be bound by other (positive) predicates in the same rule. This program is **invalid**:

```
# INVALID: y is only bound in the negated predicate
bad(x) :- node(x), !edge(x, y).
```

Negation is **stratified** -- a relation cannot recursively depend on its own negation.

## 6. Aggregation

Aggregate functions compute summary values: `count`, `sum`, `min`, `max`.

### Count

```
data(1, 10).
data(1, 20).
data(1, 30).
data(2, 40).

# Count values per key
cnt(x, count(y)) :- data(x, y).
```

Output:

```
cnt(1, 3)
cnt(2, 1)
```

### Min (Shortest Path)

```
edge(1, 2, 5).
edge(2, 3, 3).
edge(1, 3, 10).

dist(1, 0).
dist(y, min(d + w)) :- dist(x, d), edge(x, y, w).
```

Output:

```
dist(1, 0)
dist(2, 5)
dist(3, 8)
```

Node 3 gets distance 8 (via 1→2→3: 5+3) rather than 10 (direct 1→3) because `min` selects the smallest value.

## 7. Declarations and CSV Input

For larger datasets, declare relation schemas and load data from CSV files.

```
.decl edge(x: int32, y: int32)
.input edge(filename="edges.csv", delimiter=",")

.decl tc(x: int32, y: int32)
.output tc

tc(x, y) :- edge(x, y).
tc(x, z) :- tc(x, y), edge(y, z).
```

Create `edges.csv`:

```
1,2
2,3
3,4
```

Run:

```bash
wirelog-cli program.dl
```

The `.output tc` directive means only `tc` results are printed. Without `.output`, all derived relations are printed.

### Supported Types

| Type | Description |
|------|-------------|
| `int32` | 32-bit signed integer |
| `int64` | 64-bit signed integer |
| `string` | Text (interned internally) |

## 8. String Values

wirelog supports string values via interning:

```
edge("Alice", "Bob").
edge("Bob", "Carol").

tc(x, y) :- edge(x, y).
tc(x, z) :- tc(x, y), edge(y, z).
```

Output:

```
tc("Alice", "Bob")
tc("Alice", "Carol")
tc("Bob", "Carol")
```

## 9. Wildcards

Use `_` to ignore a column:

```
edge(1, 2, 100).
edge(2, 3, 200).

# Ignore the weight column
has_edge(x, y) :- edge(x, y, _).
```

Each `_` is independent -- `edge(_, _)` matches any edge regardless of source or target.

## 10. Multi-Worker Execution

For large datasets, use multiple workers for parallel execution:

```bash
wirelog-cli --workers 4 program.dl
```

The `--workers` flag sets the number of Differential Dataflow worker threads. Results are identical regardless of worker count.

## Next Steps

- [Language Reference](reference) -- complete grammar and operator details
- [Examples](examples) -- more programs to learn from
- [CLI Usage](cli) -- all command-line options
