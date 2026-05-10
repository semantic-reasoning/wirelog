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

### I/O adapter framework rename + ABI v2 bump (#762)

The `wirelog/io/io_adapter.h` public-installed header carried
internal-style `wl_io_*` / `WL_IO_*` prefixes across its entire
20-symbol surface, contradicting `AGENTS.md:17-20`.  All
identifiers are renamed and the I/O-adapter ABI version is
bumped from `1u` to `2u` so plugins compiled against v0.30.0
fail loud at registration time.

Symbol renames (full surface):

| Old | New |
|---|---|
| `wl_io_ctx_t` | `wirelog_io_ctx_t` |
| `wl_io_adapter_t` | `wirelog_io_adapter_t` |
| `wl_io_register_adapter` | `wirelog_io_register_adapter` |
| `wl_io_unregister_adapter` | `wirelog_io_unregister_adapter` |
| `wl_io_find_adapter` | `wirelog_io_find_adapter` |
| `wl_io_last_error` | `wirelog_io_last_error` |
| `wl_io_ctx_relation_name` | `wirelog_io_ctx_relation_name` |
| `wl_io_ctx_num_cols` | `wirelog_io_ctx_num_cols` |
| `wl_io_ctx_col_type` | `wirelog_io_ctx_col_type` |
| `wl_io_ctx_param` | `wirelog_io_ctx_param` |
| `wl_io_ctx_intern_string` | `wirelog_io_ctx_intern_string` |
| `wl_io_ctx_platform` | `wirelog_io_ctx_platform` |
| `wl_io_ctx_set_platform` | `wirelog_io_ctx_set_platform` |
| `wl_io_plugin_entry_fn` | `wirelog_io_plugin_entry_fn` |
| `wl_io_plugin_entry` (dlsym key) | `wirelog_io_plugin_entry` |
| `WL_IO_ABI_VERSION` (1u) | `WIRELOG_IO_ABI_VERSION` (2u) |
| `WL_IO_MAX_ADAPTERS` | `WIRELOG_IO_MAX_ADAPTERS` |
| `WL_IO_PLUGIN_ENTRY_SYMBOL` | `WIRELOG_IO_PLUGIN_ENTRY_SYMBOL` |
| `WL_IO_PLUGIN_EXPORT` | `WIRELOG_IO_PLUGIN_EXPORT` |
| `WL_IO_USED` | `WIRELOG_IO_USED` |

ABI bump 1u -> 2u: the registration-time check
(`adapter->abi_version != WIRELOG_IO_ABI_VERSION`) rejects
v0.30.0 plugins (abi_version == 1u) with a clear diagnostic
retrievable via `wirelog_io_last_error()`.  Combined with the
dlsym-symbol rename, v0.30.0 Path-B plugins fail loudly on
load: `dlsym(handle, "wirelog_io_plugin_entry")` returns NULL
because the plugin still exports `wl_io_plugin_entry`.

Migration for downstream consumers:

1. Rebuild Path-A consumers (link-time adapter registration)
   against `WIRELOG_IO_ABI_VERSION = 2u` headers.
2. Rebuild Path-B plugins (dynamic `dlopen` load) and rename
   the exported entry symbol from `wl_io_plugin_entry` to
   `wirelog_io_plugin_entry`.
3. Textual-rename all source-level `wl_io_*` / `WL_IO_*`
   references to `wirelog_io_*` / `WIRELOG_IO_*`.

### wl_easy facade rename + file move (#756)

The `wl_easy` convenience facade carried internal `wl_*`
prefixes on a public installed header (`wirelog/wl_easy.h`).
Per `AGENTS.md:17-20`, public symbols and headers must use
the `wirelog_*` / `WIRELOG_*` prefix and (by convention) the
`wirelog-` filename prefix.

File moves:

| Old path | New path |
|---|---|
| `wirelog/wl_easy.h` | `wirelog/wirelog-easy.h` |
| `wirelog/wl_easy.c` | `wirelog/wirelog-easy.c` |
| `tests/test_wl_easy.c` | `tests/test_wirelog_easy.c` |
| `tests/test_wl_easy_inline_facts.c` | `tests/test_wirelog_easy_inline_facts.c` |

Symbol renames (full surface):

| Old | New |
|---|---|
| `wl_easy_session_t` | `wirelog_easy_session_t` |
| `wl_easy_open_opts_t` | `wirelog_easy_open_opts_t` |
| `WL_EASY_OPEN_OPTS_INIT` | `WIRELOG_EASY_OPEN_OPTS_INIT` |
| `WL_EASY_MAX_COLS` | `WIRELOG_EASY_MAX_COLS` |
| `wl_easy_open` / `wl_easy_close` | `wirelog_easy_open` / `wirelog_easy_close` |
| `wl_easy_insert` / `wl_easy_remove` | `wirelog_easy_insert` / `wirelog_easy_remove` |
| `wl_easy_insert_sym` / `wl_easy_remove_sym` | `wirelog_easy_insert_sym` / `wirelog_easy_remove_sym` |
| `wl_easy_step` / `wl_easy_snapshot` | `wirelog_easy_step` / `wirelog_easy_snapshot` |
| `wl_easy_set_delta_cb` | `wirelog_easy_set_delta_cb` |
| `wl_easy_intern` | `wirelog_easy_intern` |
| `wl_easy_print_delta` / `wl_easy_banner` | `wirelog_easy_print_delta` / `wirelog_easy_banner` |
| `wl_easy_snapshot_filter_t` | `wirelog_easy_snapshot_filter_t` |

`AGENTS.md:23` public-headers list updates the path.
`meson.build` install_headers SSoT and source list reflect
both the file move and the new symbol names.  Source-
incompatible across the entire facade; migrate via:

1. Update `#include` paths from `wirelog/wl_easy.h` to
   `wirelog/wirelog-easy.h`.
2. Textual-rename all `wl_easy_*` symbol references to
   `wirelog_easy_*`.
3. Textual-rename all `WL_EASY_*` macro references to
   `WIRELOG_EASY_*`.

### Export-attribute macro rename: WL_PUBLIC -> WIRELOG_PUBLIC (#759)

`wirelog/wirelog-export.h` previously defined `WL_PUBLIC` as
the symbol-visibility / dllexport attribute macro that gates
every exported function on the public surface
(`WL_PUBLIC int wirelog_session_create(...)`, etc.).  The
`WL_*` prefix on a public macro contradicts `AGENTS.md:17-20`.

The macro is renamed in a clean break:

| Old | New |
|---|---|
| `WL_PUBLIC` (in `wirelog/wirelog-export.h`) | `WIRELOG_PUBLIC` |

`WIRELOG_API` continues as the existing alias (now defined as
`#define WIRELOG_API WIRELOG_PUBLIC`), and remains the
recommended attribute name for new public-API declarations.
Source-incompatible for downstream code that referenced
`WL_PUBLIC` directly; migrate via a textual rename to
`WIRELOG_PUBLIC` (or, preferred, switch to `WIRELOG_API`).

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
`wirelog_session_snapshot`) and `wirelog/wirelog-easy.h`
(`wirelog_easy_set_delta_cb`, `wirelog_easy_snapshot`).  Function-pointer
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
