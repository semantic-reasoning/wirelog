# Migration Guide

**Last Updated:** 2026-04-24

This document describes breaking changes, opt-in features, and migration
steps for each significant Wirelog release. Entries are ordered newest first.

---

## 0.30 -> 1.0

This section accumulates the API renames and behaviour pins landing
during the v0.40 API audit and subsequent v1.0 freeze.  See epic #755
for the full scope.  Entries are filed atomically as the renames land;
#745 then consolidates the narrative for the GA migration guide.

### String-fn enum constants rename (#757)

The 12 `WL_STR_FN_*` enum constants in `wirelog/wirelog-types.h`
declare the supported string-operation kinds (#444).  Per
`AGENTS.md:17-20`, public macros / enums use the `WIRELOG_*`
prefix.  All 12 constants are renamed:

| Old | New |
|---|---|
| `WL_STR_FN_STRLEN` | `WIRELOG_STR_FN_STRLEN` |
| `WL_STR_FN_CAT` | `WIRELOG_STR_FN_CAT` |
| `WL_STR_FN_CONTAINS` | `WIRELOG_STR_FN_CONTAINS` |
| `WL_STR_FN_STR_ORD` | `WIRELOG_STR_FN_STR_ORD` |
| `WL_STR_FN_STR_PREFIX` | `WIRELOG_STR_FN_STR_PREFIX` |
| `WL_STR_FN_STR_REPLACE` | `WIRELOG_STR_FN_STR_REPLACE` |
| `WL_STR_FN_STR_SUFFIX` | `WIRELOG_STR_FN_STR_SUFFIX` |
| `WL_STR_FN_SUBSTR` | `WIRELOG_STR_FN_SUBSTR` |
| `WL_STR_FN_TO_LOWER` | `WIRELOG_STR_FN_TO_LOWER` |
| `WL_STR_FN_TO_NUMBER` | `WIRELOG_STR_FN_TO_NUMBER` |
| `WL_STR_FN_TO_STRING` | `WIRELOG_STR_FN_TO_STRING` |
| `WL_STR_FN_TO_UPPER` | `WIRELOG_STR_FN_TO_UPPER` |
| `WL_STR_FN_TRIM` | `WIRELOG_STR_FN_TRIM` |

Source-incompatible for downstream consumers passing the
constants to `wirelog_*` string-op APIs; migrate via a textual
rename.  Underlying enum values and behaviour are unchanged.

### Public-API typedef rename: wl_intern_t -> wirelog_intern_t (#760)

The `wirelog/wirelog.h` umbrella header previously exposed
`typedef struct wl_intern wl_intern_t;` -- an internal-style
`wl_*` typedef name on a public installed header.  The struct
tag (`struct wl_intern`) is internal and stays internal; only
the typedef name on the public surface changes:

| Old (internal-style on public surface) | New (public conforming) |
|---|---|
| `wl_intern_t` (in `wirelog/wirelog.h`) | `wirelog_intern_t` |

`wirelog_program_get_intern()` now returns
`const wirelog_intern_t *` instead of `const wl_intern_t *`.
The internal header `wirelog/intern.h` continues to declare
`typedef struct wl_intern wl_intern_t;` for in-tree callers
(both names alias the same struct), so internal code is
unchanged.

Two docstrings in `wirelog/wirelog.h` previously referenced the
internal helper `wl_dd_load_edb()` by name; they are rephrased
to refer to "the project's internal EDB-load helper" without
naming the internal symbol on the public surface.

### Callback typedef rename (#758)

Public-API callback typedefs lose their internal `wl_*` prefix to
match `AGENTS.md:17-20`:

| Old (internal-style) | New (public conforming) |
|---|---|
| `wl_on_delta_fn` | `wirelog_on_delta_fn` |
| `wl_on_tuple_fn` | `wirelog_on_tuple_fn` |

Both typedefs live in `wirelog/wirelog-types.h` and are consumed by
`wirelog/wirelog-advanced.h` (`wirelog_session_set_delta_cb`,
`wirelog_session_snapshot`) and `wirelog/wl_easy.h`
(`wl_easy_set_delta_cb`, `wl_easy_snapshot`).  Function-pointer
parameter declarations rename to the new typedef names; the function
signatures are otherwise unchanged.  Source-incompatible; existing
callers update via a textual rename.

---

## Compound Terms and RDF Named Graphs (Issue #530 / #535)

Wirelog gains two additive features in this release. Both are strictly
opt-in: existing programs compile and run without any changes.

### What changed

- **Compound terms** (`docs/COMPOUND_TERMS.md`): A new column type syntax
  `functor/arity` (and `functor/arity inline` / `functor/arity side`) lets
  a column hold a structured compound value instead of a scalar. The engine
  stores compounds in one of two tiers: inline (expanded into physical
  columns) or side-relation (stored in an auto-created
  `__compound_<functor>_<arity>` relation, with the main column holding a
  64-bit handle).

- **RDF named graphs** (`docs/RDF_QUADS.md`): Declaring a column named
  `__graph_id: int64` in any relation turns it into a quad relation. The
  graph ID is a plain `int64`; it is not an implicit filter. A companion
  `__graph_metadata` relation (user-declared) enables graph-level attribute
  joins.

### Backward compatibility

No existing program is affected. Specifically:

- Relations without `__graph_id` behave exactly as before. The
  `has_graph_column` field on `col_rel_t` is `false` for all legacy
  relations.
- Relations without compound column types have `compound_kind ==
  WIRELOG_COMPOUND_KIND_NONE` on all columns. All existing `.decl`,
  rule, fact, and C API call sites are unchanged.
- The reserved name prefixes `__compound_` and `__graph_` are new in this
  release. Programs that happen to use those prefixes for their own
  relations must rename them (see Reserved Names below).

### Reserved names (new in this release)

The following naming prefixes are now reserved for engine-generated relations.
User programs must not declare relations with these prefixes:

| Prefix             | Purpose                                    |
|--------------------|--------------------------------------------|
| `__compound_`      | Auto-created side-relation for compound terms |
| `__graph_metadata` | Convention relation for named-graph attributes |

If an existing program declares a relation whose name starts with
`__compound_` or equals `__graph_metadata`, rename it before upgrading.

### Opt-in: compound terms

To add a compound column to an existing relation:

1. Add the compound column to the `.decl` with `functor/arity` as the type.
2. Update any rules that reference that column to use compound pattern
   matching syntax (`f(x, y)` in rule bodies).
3. Update any C API call sites that insert rows into the relation to supply
   the compound arguments. For the side-relation tier, obtain a handle via
   `wl_compound_arena_alloc` and store it in the column. For the inline
   tier, pass the argument values directly as additional `int64` columns in
   the row array.

No changes are needed to relations or rules that do not use compound columns.

#### Migration checklist: compound terms

- [ ] Identify columns that hold structured data currently split across
      multiple scalar columns or encoded as an integer tag.
- [ ] Add `functor/arity` (or `functor/arity inline`) type to those columns
      in `.decl`.
- [ ] Update rule bodies: replace scalar variable bindings with compound
      pattern syntax where applicable.
- [ ] Update C insertion call sites to supply the compound arguments in the
      correct column positions.
- [ ] Verify that no existing relation name starts with `__compound_`.
- [ ] If using the inline tier, confirm arity ≤ 4 and nesting depth = 1.
      Relations that exceed these limits must use the side-relation tier.
- [ ] Run the full test suite: `meson test -C build`.

### Opt-in: RDF named graphs

To add named-graph support to an existing relation:

1. Add `__graph_id: int64` as a column to the `.decl`.
2. Update fact insertion call sites to supply the graph ID value in the
   corresponding column position.
3. Update any rules that should filter by graph to include an explicit join
   against `__graph_metadata` or a constant comparison on the graph ID
   variable.

Rules that do not reference `__graph_id` at all will continue to see rows
from all graphs — no filtering is implicit.

#### Migration checklist: RDF named graphs

- [ ] Identify relations that represent graph-partitioned data.
- [ ] Add `__graph_id: int64` to those relations' `.decl` statements.
- [ ] Update fact insertion call sites to supply the graph ID.
- [ ] If you need per-graph metadata, declare `__graph_metadata` and insert
      graph attribute rows before calling `wl_session_step`.
- [ ] Review existing rules: rules that previously saw all rows continue to
      see all rows (graph ID is not an implicit filter). Add explicit graph
      ID constraints only where isolation is required.
- [ ] Verify that no existing relation is named `__graph_metadata` (unless
      it already has the expected schema).
- [ ] Run the full test suite: `meson test -C build`.

### Performance notes

See `docs/PERFORMANCE.md` for measured latency and memory impact of compound
terms at the inline and side-relation tiers. The inline tier adds zero
allocation overhead (physical column expansion only). The side-relation tier
adds one arena allocation per compound instance plus a hash lookup for the
side-relation on first access.

---

## Earlier releases

No prior migration entries. This is the first migration guide entry for
wirelog.

---

## See Also

- `docs/COMPOUND_TERMS.md` — compound term syntax and storage tiers
- `docs/RDF_QUADS.md` — RDF named-graph column convention
- `docs/PERFORMANCE.md` — performance methodology and measurements
- `docs/SYNTAX.md` — base Datalog syntax reference
- `docs/ARCHITECTURE.md` — engine architecture overview
