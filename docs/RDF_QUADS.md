# RDF Named Graphs and Quad Relations

**Last Updated:** 2026-04-24
**Introduced:** Issue #535

---

## Table of Contents

1. [Overview](#overview)
2. [Declaring a Quad Relation](#declaring-a-quad-relation)
3. [Semantics of `__graph_id`](#semantics-of-__graph_id)
4. [Graph Metadata](#graph-metadata)
5. [Filtering by Graph](#filtering-by-graph)
6. [Cross-Graph Joins](#cross-graph-joins)
7. [SPARQL Pattern Mapping](#sparql-pattern-mapping)
8. [Examples](#examples)
9. [Backward Compatibility](#backward-compatibility)

---

## Overview

Wirelog supports RDF-style **named graphs** through a special column name
convention. Any relation that includes a column named `__graph_id` of type
`int64` is treated as a *quad relation*: each row carries a graph identifier
in addition to its other attributes, equivalent to an RDF quad
`(subject, predicate, object, graph)`.

Named-graph support is an opt-in, additive feature:

- Relations without `__graph_id` behave exactly as before.
- Relations with `__graph_id` gain a `has_graph_column = true` flag on their
  internal `col_rel_t` and record the column index as `graph_col_idx`.
- The graph ID is **not an implicit filter**. All rows are visible to rules
  regardless of their graph ID value, unless an explicit join or filter is
  written.

Graph IDs are opaque `int64` values. The meaning of a specific value (tenant
identifier, named-graph URI hash, shard key, etc.) is defined by the
application.

---

## Declaring a Quad Relation

Add `__graph_id: int64` as a column in any `.decl` statement:

```
.decl triple(s: int64, p: int64, __graph_id: int64)
```

The column may appear at any position. Convention places it last (matching
RDF quad order `s p o g`), but the engine imposes no ordering constraint.

```
.decl triple(s: int64, p: int64, o: int64, __graph_id: int64)
```

More than one relation in a program may have a `__graph_id` column; each
relation's flag and column index are tracked independently.

The column name `__graph_id` is reserved. Declaring a column with this name
on a type other than `int64` produces a schema error.

---

## Semantics of `__graph_id`

The graph ID column behaves like any other `int64` column in Wirelog:

- **No implicit filtering.** A rule that does not reference `__graph_id`
  sees rows from all graphs.
- **Explicit binding.** Binding `__graph_id` to a variable or constant in a
  rule body restricts matching to rows with that graph ID value.
- **Variable vs constant.** Use a variable to project or join on the graph
  ID; use a constant to filter to a specific graph.

**All facts visible (no filter):**

```
.decl triple(s: int64, p: int64, __graph_id: int64)
.decl result(s: int64, p: int64)

result(s, p) :- triple(s, p, g).
```

`result` contains every `(s, p)` pair regardless of which graph the triple
belongs to. The variable `g` binds the graph ID but is not propagated to
the head.

**Filter to a specific graph:**

```
result(s, p) :- triple(s, p, 42).
```

Only rows with `__graph_id = 42` match.

---

## Graph Metadata

When any relation in a program declares `__graph_id`, the session layer
automatically creates `__graph_metadata` as an empty 6-column relation with a
fixed schema (`wirelog/columnar/session.c:712-745`):

| Column        | Type    | Role                                      |
|---------------|---------|-------------------------------------------|
| `graph_id`    | int64   | Primary key matching `__graph_id` values  |
| `tenant`      | int64   | Tenant or owner identifier (intern id)    |
| `timestamp`   | int64   | Creation or assertion timestamp           |
| `location`    | int64   | Provenance location (intern id)           |
| `risk`        | int64   | Risk or sensitivity level                 |
| `description` | int64   | Free-form description (intern id)         |

The relation is created empty; the application populates it via
`wl_session_insert` before calling `wl_session_step`. No `.decl` is needed in
your Datalog program — the engine registers the relation automatically.

If you do declare `__graph_metadata` explicitly, it must have **exactly 6
typed columns** matching the schema above. Any other arity is a parse-time
error (`wirelog/ir/program.c:341-355`):

```
/* Valid — matches engine schema exactly: */
.decl __graph_metadata(graph_id: int64, tenant: int64,
    timestamp: int64, location: int64, risk: int64, description: int64)

/* Invalid — rejected at parse time with a WL_LOG ERROR: */
.decl __graph_metadata(graph_id: int64, owner: int64)
```

The double-underscore prefix (`__graph_metadata`) signals that this is an
engine-convention relation, not application-domain data. Applications must
not use `__` prefixes for their own relations.

---

## Filtering by Graph

Join a quad relation against `__graph_metadata` to restrict derived tuples
to graphs matching a metadata predicate:

```
.decl triple(s: int64, p: int64, __graph_id: int64)
.decl __graph_metadata(graph_id: int64, tenant: int64,
    timestamp: int64, location: int64, risk: int64, description: int64)
.decl trusted(s: int64, p: int64)

trusted(s, p) :-
    triple(s, p, g),
    __graph_metadata(g, 99, _, _, _, _).
```

`trusted` derives only those `(s, p)` pairs whose graph has `tenant = 99` in
`__graph_metadata`. Triples in graphs with other tenants are silently excluded.

To filter on multiple metadata attributes at once:

```
.decl high_value(s: int64, p: int64)

high_value(s, p) :-
    triple(s, p, g),
    __graph_metadata(g, tenant, ts, loc, risk, _),
    tenant = 99,
    risk > 50.
```

---

## Cross-Graph Joins

When joining two quad relations, use **distinct variable names** for their
respective `__graph_id` columns. Using the same variable would add an
implicit equality constraint (only matching pairs from the same graph):

**Cross-graph join (independent graph IDs):**

```
.decl left_rel(key: int64, val: int64, __graph_id: int64)
.decl right_rel(key: int64, info: int64, __graph_id: int64)
.decl joined(key: int64, val: int64, info: int64)

joined(k, v, i) :- left_rel(k, v, g1), right_rel(k, i, g2).
```

`g1` and `g2` are independent. The join is on `key` only; graph IDs are not
constrained to be equal.

**Same-graph join (graph IDs must match):**

```
joined_same_graph(k, v, i) :- left_rel(k, v, g), right_rel(k, i, g).
```

Here a single variable `g` constrains both relations to the same graph.

---

## SPARQL Pattern Mapping

Wirelog quad relations map directly to SPARQL 1.1 dataset patterns. The
correspondence is:

| SPARQL concept          | Wirelog equivalent                          |
|-------------------------|---------------------------------------------|
| Triple `(s p o)` in named graph `G` | Row `(s, p, o, G)` in a quad relation |
| Default graph           | Rows with a sentinel graph ID (e.g., `0`)   |
| `GRAPH ?g { ?s ?p ?o }` | `triple(s, p, o, g)` with `g` as a variable |
| `GRAPH <uri> { ... }`   | `triple(s, p, o, 42)` with a constant graph ID |
| `UNION` across graphs   | No filter on `g` in the rule body            |

**SPARQL basic graph pattern** `?s ?p ?o`:

```
result(s, p, o) :- triple(s, p, o, _).
```

**SPARQL `GRAPH ?g { ?s ?p ?o }`** — project the graph variable:

```
result(s, p, o, g) :- triple(s, p, o, g).
```

**SPARQL `GRAPH <http://example.org/graph1> { ?s ?p ?o }`** — filter on a
known graph ID (assuming `http://example.org/graph1` hashes to `7654321`):

```
result(s, p, o) :- triple(s, p, o, 7654321).
```

**SPARQL `FROM NAMED` + `GRAPH ?g`** — enumerate all named graphs:

```
.decl named_graphs(g: int64)

named_graphs(g) :- triple(_, _, g).
```

---

## Examples

### End-to-end: multi-tenant triple store

```
.decl triple(s: int64, p: int64, o: int64, __graph_id: int64)
.decl __graph_metadata(graph_id: int64, tenant: int64,
    timestamp: int64, location: int64, risk: int64, description: int64)

/* All triples visible to tenant 42 */
.decl tenant42_triples(s: int64, p: int64, o: int64)
.output tenant42_triples

tenant42_triples(s, p, o) :-
    triple(s, p, o, g),
    __graph_metadata(g, 42, _, _, _, _).
```

### End-to-end: provenance tracking

```
.decl claim(subject: int64, predicate: int64, __graph_id: int64)
.decl __graph_metadata(graph_id: int64, source: int64,
    confidence: int64, timestamp: int64, location: int64, notes: int64)

/* Derive high-confidence claims */
.decl reliable(subject: int64, predicate: int64, source: int64)
.output reliable

reliable(s, p, src) :-
    claim(s, p, g),
    __graph_metadata(g, src, conf, _, _, _),
    conf > 80.
```

### End-to-end: cross-graph reachability

```
.decl edge(src: int64, dst: int64, __graph_id: int64)
.decl reachable(src: int64, dst: int64)
.output reachable

/* Reachability ignoring graph boundaries */
reachable(x, y) :- edge(x, y, _).
reachable(x, z) :- reachable(x, y), edge(y, z, _).
```

### End-to-end: same-graph path (intra-graph only)

```
.decl edge(src: int64, dst: int64, __graph_id: int64)
.decl graph_path(src: int64, dst: int64, g: int64)
.output graph_path

graph_path(x, y, g) :- edge(x, y, g).
graph_path(x, z, g) :- graph_path(x, y, g), edge(y, z, g).
```

---

## Backward Compatibility

Existing programs without `__graph_id` columns are unaffected:

- `has_graph_column` is `false` on all relations that do not declare
  `__graph_id`.
- No runtime overhead is added to relations without the named-graph column.
- All existing rules, facts, and C API calls work without modification.

Relations that do declare `__graph_id` remain fully compatible with all
standard Wirelog operators: joins, aggregations, negation, semi-naive
evaluation, K-Fusion, and Magic Sets all treat `__graph_id` as a regular
`int64` column.

---

## See Also

- `docs/SYNTAX.md` — base Datalog syntax reference
- `docs/COMPOUND_TERMS.md` — compound term storage tiers
- `docs/MIGRATION.md` — migration guide for programs upgrading to Issue #530/#535
- `tests/test_rdf_named_graph.c` — integration test suite (TC-1 through TC-6)
