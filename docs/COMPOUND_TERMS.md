# Compound Terms

**Last Updated:** 2026-04-24
**Introduced:** Issue #530 / #531 / #532 / #533

---

## Table of Contents

1. [Overview](#overview)
2. [Declaration Syntax](#declaration-syntax)
3. [Storage Tiers](#storage-tiers)
4. [Using Compound Terms in Rules](#using-compound-terms-in-rules)
5. [Nested Compounds](#nested-compounds)
6. [Inline vs Side Decision Guide](#inline-vs-side-decision-guide)
7. [Errors and Validation](#errors-and-validation)
8. [Examples](#examples)
9. [Observability](#observability)
10. [Backward Compatibility](#backward-compatibility)

---

## Overview

Compound terms extend Wirelog's type system with structured values that group
multiple scalar fields under a single named functor. They are useful for
representing nested records, tagged unions, and structured keys without
manually managing a separate join relation.

A compound term has a **functor** (name) and a fixed **arity** (number of
arguments). For example, `point(3, 7)` is a compound with functor `point` and
arity 2. In rule bodies, variables bind to individual arguments using the
standard functor-application pattern.

Compound terms are stored in one of two tiers depending on arity, nesting
depth, and an optional per-column hint:

- **Inline tier**: arguments are stored as additional physical columns in the
  main relation. No extra allocation.
- **Side-relation tier**: arguments are stored in an auto-created auxiliary
  relation; the main relation column holds a 64-bit opaque handle.

---

## Declaration Syntax

Declare a compound column using the type `functor/arity` in a `.decl` statement:

```
.decl RelationName(col: functor/arity)
.decl RelationName(col: functor/arity inline)
.decl RelationName(col: functor/arity side)
```

- `functor` is any valid identifier (`[A-Za-z_][A-Za-z0-9_]*`)
- `arity` is a positive integer (zero is rejected; `f()` is a parse error)
- `inline` forces the inline tier; errors if limits are exceeded
- `side` forces the side-relation tier (default when no hint is given)
- Omitting the hint selects the **side-relation tier** by default

### Examples

```
.decl event(id: int64, payload: metadata/4)
.decl point_cloud(idx: int64, pt: point/3 inline)
.decl record(id: int64, tag: scope/1 side)
```

### Type names at a glance

| Type string          | Tier    | Meaning                          |
|----------------------|---------|----------------------------------|
| `f/2`                | side    | functor `f`, arity 2, side tier  |
| `f/2 side`           | side    | same, explicit hint              |
| `f/2 inline`         | inline  | functor `f`, arity 2, inline     |
| `metadata/4`         | side    | functor `metadata`, arity 4      |
| `metadata/4 inline`  | inline  | same, forced inline              |

---

## Storage Tiers

### Inline tier

The inline tier stores compound arguments as additional physical columns
appended directly to the main relation.

**Limits** (enforced in IR lowering, `wirelog/columnar/internal.h`):

| Constant                     | Value | Meaning                                   |
|------------------------------|-------|-------------------------------------------|
| `WL_COMPOUND_INLINE_MAX_ARITY` | 4   | Maximum number of compound arguments      |
| `WL_COMPOUND_INLINE_MAX_DEPTH` | 1   | Maximum nesting depth for inline compounds|

A compound declared with `functor/N inline` where `N > 4` or where it is
nested inside another compound (depth > 1) is rejected at schema-apply time.

**Physical layout example** — relation `edge(src: int64, lbl: pair/2 inline, dst: int64)`:

```
Physical columns:  [ src ] [ lbl_arg0 ] [ lbl_arg1 ] [ dst ]
Logical columns:   [ src ] [  lbl/2   ]              [ dst ]
```

The compound occupies two consecutive physical slots starting at
`inline_physical_offset`. The `compound_arity_map` array records the arity of
each logical column (0 for scalars, N for inline compounds).

### Side-relation tier

The side-relation tier stores compound arguments in an automatically created
auxiliary relation named:

```
__compound_<functor>_<arity>
```

with schema:

```
(handle: int64, arg0: int64, arg1: int64, ..., arg{N-1}: int64)
```

All columns are `int64`. Functor arguments that are themselves compound terms
are stored as nested handles.

The relation is created idempotently on first use
(`wl_compound_side_ensure`). Calling with the same functor and arity a second
time returns the existing relation without allocating anything new.

The main relation column holds a **64-bit opaque handle** allocated from the
session-local compound arena.

#### Handle format

```
 63       44 43     32 31                0
+----------+---------+--------------------+
| seed(20) | epoch(12) |   offset(32)     |
+----------+---------+--------------------+
```

| Field    | Bits | Meaning                                                      |
|----------|------|--------------------------------------------------------------|
| `seed`   | 20   | Low 20 bits of session creation counter; prevents cross-session handle reuse |
| `epoch`  | 12   | Arena generation counter; advanced at each GC epoch boundary (capped at 4095) |
| `offset` | 32   | Byte offset into the current generation buffer               |

The null handle is `0` (`WL_COMPOUND_HANDLE_NULL`). A null handle in a column
means no compound is present for that row.

#### Arena lifecycle

- The arena is allocated once per session alongside the main `wl_arena_t`.
- At each epoch frontier, `wl_compound_arena_gc_epoch_boundary()` reclaims
  handles whose Z-set multiplicity has reached zero.
- During K-Fusion parallel execution, the arena is **frozen** (read-only).
  New allocations return `WL_COMPOUND_HANDLE_NULL` while frozen; lookups
  remain valid.
- When the epoch counter reaches 4095, the arena saturates and refuses new
  allocations. The caller is expected to rotate to a fresh session.

---

## Using Compound Terms in Rules

In rule bodies, compound terms appear with functor-application syntax:

```
head(...) :- rel(x, f(a, b), y), ...
```

The pattern `f(a, b)` in a body literal:
- Matches any row whose compound column has functor `f` and arity 2.
- Binds variables `a` and `b` to the compound's arguments.
- Fails to match rows where the compound column holds a different functor or
  the null handle.

### Example: projecting a compound argument

```
.decl event(id: int64, payload: point/2 inline)
.decl x_values(id: int64, x: int64)

x_values(id, x) :- event(id, point(x, _)).
```

### Example: filtering on a compound field

```
.decl item(key: int64, label: tag/2)
.decl hot_items(key: int64)

hot_items(key) :- item(key, tag(category, priority)), priority > 10.
```

### Parser limitation: compound terms in rule heads

Compound terms appear in **body** positions only. The parser's head-argument
path (`parse_head_arg`) dispatches to arithmetic and aggregate expressions but
has no compound-term production. Writing `f(x, y)` in a rule head is a parse
error with the current implementation.

To construct a compound value from body bindings, derive the arguments
individually into a scalar relation and let the compound schema join handle
the structural encoding:

```
/* Body compound: extract arguments from a compound column */
.decl item(key: int64, label: tag/2 inline)
.decl args(key: int64, category: int64, priority: int64)

args(key, cat, pri) :- item(key, tag(cat, pri)).
```

---

## Nested Compounds

The parser accepts nesting up to depth 64 (`WL_PARSER_COMPOUND_MAX_DEPTH`).
IR lowering then applies the tier constraints:

- Depth-1 compounds with arity ≤ 4: eligible for inline (if `inline` hint or
  auto-selection applies).
- Depth > 1 compounds: always lowered to the side-relation tier, regardless
  of hint.

For a nested compound like `scope(metadata(t, ts, loc, risk))`:

1. Inner: `__compound_metadata_4` stores `(handle_meta, t, ts, loc, risk)`.
   Returns `handle_meta`.
2. Outer: `__compound_scope_1` stores `(handle_scope, handle_meta)`.
   Returns `handle_scope`.
3. Main relation column: holds `handle_scope`.

Rule body matching follows the same nesting:

```
.decl record(id: int64, scope_col: scope/1)
.decl result(id: int64, tenant: int64, location: int64)

result(id, t, loc) :-
    record(id, scope(metadata(t, ts, loc, risk))).
```

---

## Inline vs Side Decision Guide

| Condition                          | Recommended tier |
|------------------------------------|------------------|
| Arity ≤ 4 and no nesting           | `inline`         |
| Arity > 4                          | `side` (default) |
| Nested inside another compound     | `side` (default) |
| High-frequency join on arguments   | `inline`         |
| Rare access or large arity         | `side`           |
| K-Fusion parallel execution paths  | `inline` preferred (no arena freeze concern) |

When in doubt, omit the hint. The default (`side`) is always correct.

---

## Errors and Validation

| Condition                                | Behavior                                              |
|------------------------------------------|-------------------------------------------------------|
| `f()` — arity zero                       | Parse error: "compound term requires at least one argument" |
| Nesting > 64 levels                      | Parse error: "compound term nesting too deep"         |
| `inline` hint with arity > 4            | Schema-apply error; `WL_LOG=COMPOUND:2` emits `error=arity_overflow` |
| `inline` hint with depth > 1            | Schema-apply error; `WL_LOG=COMPOUND:2` emits `error=depth_overflow` |
| Handle from session A used in session B  | `arena_lookup` returns NULL (session seed mismatch)   |
| Handle after arena saturation (epoch 4095)| `arena_alloc` returns `WL_COMPOUND_HANDLE_NULL`      |

---

## Examples

### Minimal inline compound

```
.decl pair_rel(id: int64, p: pair/2 inline)
.decl sum_rel(id: int64, total: int64)
.output sum_rel

sum_rel(id, x + y) :- pair_rel(id, pair(x, y)).
```

### Side-relation compound with filtering

```
.decl event(id: int64, meta: metadata/4)
.decl high_risk(id: int64, tenant: int64)
.output high_risk

high_risk(id, tenant) :-
    event(id, metadata(tenant, ts, loc, risk)),
    risk > 100.
```

The engine creates `__compound_metadata_4` automatically. No `.decl` for the
side relation is needed.

### Nested compound (side tier)

```
.decl message(msg_id: int64, envelope: envelope/2)
.decl trusted_messages(msg_id: int64, sender: int64)
.output trusted_messages

trusted_messages(msg_id, sender) :-
    message(msg_id, envelope(header(sender, ts), body_hash)).
```

Side relations created: `__compound_header_2`, `__compound_envelope_2`.

---

## Observability

Use `WL_LOG=COMPOUND:N` to trace compound-term operations:

| Level | Meaning                                                         |
|-------|-----------------------------------------------------------------|
| 2     | WARN — arity/depth overflow, arena saturation, invalid handles  |
| 3     | INFO — side relation creation, arena GC epoch boundaries        |
| 4     | DEBUG — inline store/retrieve, handle allocation                |
| 5     | TRACE — physical column layout expansion per row                |

Example:

```
WL_LOG=COMPOUND:4 ./your_app
```

The log header is `[DEBUG][COMPOUND]`. Inline-path entries include a
`path=inline` tag; side-relation paths include `functor=<name> arity=<N>`.

See `docs/ARCHITECTURE.md` and `wirelog/util/log.h` (internal) for the full
`WL_LOG` syntax.

---

## Backward Compatibility

Programs that do not use compound column types are unaffected. All compound
metadata fields on `col_rel_t` are zero-initialized by default
(`compound_kind == WIRELOG_COMPOUND_KIND_NONE`). Existing `.decl` statements,
rules, and C API calls continue to work without any changes.

The side-relation naming scheme (`__compound_<functor>_<arity>`) uses a
double-underscore prefix, which is reserved for engine-generated relations.
User programs must not declare relations with this prefix.

---

## See Also

- `docs/SYNTAX.md` — base Datalog syntax reference
- `docs/RDF_QUADS.md` — named-graph (`__graph_id`) column
- `docs/MIGRATION.md` — migration guide for programs upgrading to Issue #530
- `docs/design/compound-terms-design.md` — internal design document
- `wirelog/arena/compound_arena.h` — arena API (internal)
- `wirelog/columnar/compound_side.h` — side-relation API (internal)
