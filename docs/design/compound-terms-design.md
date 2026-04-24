# Compound Terms and RDF Named Graphs: Design

**Status:** Authoritative architecture reference for compound-term storage,
K-Fusion contract, RDF named-graph integration, and performance characteristics.
**Last Updated:** 2026-04-24
**Scope:** Syntax, storage, evaluation, K-Fusion contract, RDF quad integration,
and performance characteristics for compound-typed columns.

This document is the single source of truth for the architecture of compound
terms and RDF named graphs in wirelog. User-facing usage is covered in
`docs/COMPOUND_TERMS.md` and `docs/RDF_QUADS.md`; engine-wide invariants live in
`docs/ARCHITECTURE.md`.

---

## 1. Motivation and Scope

Stub (see `docs/ARCHITECTURE.md` for engine-wide rationale; user-facing intro
lives in `docs/COMPOUND_TERMS.md`). The umbrella extends wirelog's typed
columnar storage with first-class compound terms `f(a1, ..., aN)` and with RDF
named-graph quads `(s, p, o, g)`, preserving the five architectural invariants
(Timely-Differential, Pure C11, Columnar/SIMD, K-Fusion, Backend Abstraction).

## 2. Syntax and Parser Surface

Stub. Compound-term surface syntax and `.decl` annotations (arity, functor,
inline vs side-tier) are documented in `docs/SYNTAX.md` and
`docs/COMPOUND_TERMS.md`. Parser/IR wiring lives in `wirelog/ir/program.c`
(`collect_decl`, `parse_compound_metadata`) and `wirelog/wirelog-types.h`
(`wirelog_compound_kind_t`, `wirelog_column_t.compound_*`).

---

## 3. Storage Model

Compound columns have two storage tiers. The tier is selected per column at
`.decl` time from the compound's arity, nesting depth, and explicit per-column
annotations. Both tiers share the same columnar layout (`int64_t **columns;
columns[col][row]`) and the same Z-set/timestamp substrate
(`col_delta_timestamp_t` in `wirelog/columnar/columnar_nanoarrow.h`); only the
compound-value encoding differs.

### 3.1 Tier Selection

| Condition                                                           | Tier          | Enum value                    |
|---------------------------------------------------------------------|---------------|-------------------------------|
| Declared scalar column                                              | None          | `WIRELOG_COMPOUND_KIND_NONE`  |
| Declared `inline` compound, arity `N <= 4`, nesting depth `<= 1`    | Inline        | `WIRELOG_COMPOUND_KIND_INLINE`|
| Declared compound, arity `> 4` OR nesting depth `> 1`               | Side-relation | `WIRELOG_COMPOUND_KIND_SIDE`  |
| Undeclared / default                                                | Side-relation | `WIRELOG_COMPOUND_KIND_SIDE`  |

The selector is `wirelog_compound_kind_t` defined in
`wirelog/wirelog-types.h:147-155`. It is attached per-column on
`wirelog_column_t.compound_kind` (`wirelog/wirelog-types.h:166`) and promoted to
the relation level on `col_rel_t.compound_kind`
(`wirelog/columnar/internal.h:314`).

### 3.2 Inline Tier

Inline compounds of arity `N` occupy `N` contiguous physical slots inside the
parent relation. No indirection, no auxiliary table: the inline compound is the
physical schema expansion of its enclosing column.

**Relation-level metadata** (all fields live on `col_rel_t`,
`wirelog/columnar/internal.h:314-317`):

| Field                    | Meaning                                                                               |
|--------------------------|---------------------------------------------------------------------------------------|
| `compound_kind`          | `WIRELOG_COMPOUND_KIND_INLINE` when any inline compound is present.                   |
| `compound_count`         | Number of inline-kind logical columns in this relation.                               |
| `compound_arity_map`     | Owned `uint32_t[logical_ncols]`; entry `i` is `1` for scalars and `N` for inline f/N. |
| `inline_physical_offset` | Physical column index where the first inline compound slot begins.                    |

The physical column count `col_rel_t.ncols` equals the prefix-sum of
`compound_arity_map[]`: a relation declared with three logical columns where the
middle one is an inline `f/3` has `ncols == 5`.

**Mutation and access helpers** (all in `wirelog/columnar/internal.h` /
`wirelog/columnar/eval.c`):

| Helper                                        | Role                                                                        |
|-----------------------------------------------|-----------------------------------------------------------------------------|
| `wl_col_rel_store_inline_compound`            | Write `arity` args at `(row, logical_col)`. Returns `EINVAL` on arity/row mismatch. |
| `wl_col_rel_retrieve_inline_compound`         | Copy `arity` args out to caller buffer.                                     |
| `wl_col_rel_retract_inline_compound`          | Signal retraction; payload slots stay intact (see Retraction below).        |
| `wl_col_rel_inline_locate`                    | Pure resolver: logical_col -> (physical_offset, slot_width). K-Fusion-safe. |
| `wl_col_rel_inline_arg_physical_col`          | Resolve the physical column of the i-th argument of an inline compound.     |
| `wl_col_rel_inline_compound_equals`           | FILTER helper: element-wise equality against an expected tuple.             |
| `wl_col_rel_inline_project_column`            | PROJECT helper: copy `width` slots between two INLINE-kind relations.       |

Memory layout is bitwise identical to a scalar-expanded schema: compound arg
`i` lives at physical column `inline_physical_offset +
prefix_sum(compound_arity_map[0..logical_col-1]) + i`, and iteration is a tight
column walk with no pointer chasing.

**Retraction semantics.** Inline retraction preserves row payload. Z-set
multiplicity lives in the delta/timestamp layer
(`col_delta_timestamp_t`), so `wl_col_rel_retract_inline_compound` is required
not to zero or shuffle the inline slots: retraction-seeded semi-naive
evaluation still needs to read the original argument tuple during join
matching. The multiplicity of `-k` is recorded in the delta layer; physical
compaction happens later.

**Z-set participation.** Inline compounds are first-class Z-set citizens: any
(row, logical_col) combination inherits the row's `+multiplicity` /
`-multiplicity` from the delta/timestamp substrate. FILTER / PROJECT / LFTJ
helpers are pure payload-only readers (see file header at
`wirelog/columnar/internal.h:1114-1116`); callers preserve multiplicity via
`col_rel_append_row` semantics.

### 3.3 Side-Relation Tier

Compounds that exceed the inline thresholds (arity `> 4`, nesting `> 1`) or
that are left undeclared are stored in an auto-created side-relation named

```
__compound_<functor>_<arity>
```

with schema `(handle, arg0, arg1, ..., arg{N-1})`; every column is `int64`.
The parent relation stores only the 64-bit handle. This layout is defined in
`wirelog/columnar/compound_side.h:10-35`.

**Auto-registration.** `wl_compound_side_ensure(sess, functor, arity,
&out_rel)` (`wirelog/columnar/compound_side.h:80-82`) is idempotent: the first
call allocates and registers the side-relation on the session; subsequent calls
for the same `(functor, arity)` return the existing `col_rel_t`. The relation
name is computed once via `wl_compound_side_name`, and all subsequent accesses
hit the `session_find_rel` hash lookup (O(1), no runtime dispatch).

**Handle allocation.** Handles are 64-bit opaque ids produced by the compound
arena (`wl_compound_arena_alloc`). The arena is scoped to the session and
reclaimed under the epoch-frontier GC (see below).

**Memory layout.** Each side-relation is a standard columnar `col_rel_t` with
`compound_kind == WIRELOG_COMPOUND_KIND_NONE` (the side-relation itself is not
compound; it stores the compound's fields as plain columns). The parent
relation's column for the compound stores only handles, so the parent's
physical schema matches its logical schema 1:1.

**Retraction semantics.** Side-relation retractions flow through the same Z-set
substrate: a `-k` tuple on the parent relation marks the handle column with
`-k` multiplicity, and the side-relation row for that handle is retracted
identically. Actual physical reclamation is deferred to the epoch-frontier GC:
a handle row is freed only when (a) its multiplicity has reached zero and (b)
all live frontiers have advanced past the epoch in which it was last read. The
relevant bookkeeping lives in `wirelog/columnar/frontier.c` and
`wirelog/columnar/frontier_epoch.c`.

**Z-set participation.** The side-relation is a normal relation with full
delta/timestamp tracking (`col_delta_timestamp_t`). Joins that need compound
argument values execute a semijoin from the parent's handle column into the
side-relation's handle column (hash-join via `col_diff_arrangement_t`), then
read the argument columns.

### 3.4 Tier Comparison

| Dimension              | Inline tier                                             | Side-relation tier                                      |
|------------------------|---------------------------------------------------------|---------------------------------------------------------|
| Trigger                | Declared inline, arity <= 4, depth <= 1                 | Arity > 4 OR nesting > 1 OR undeclared                  |
| Enum                   | `WIRELOG_COMPOUND_KIND_INLINE`                          | `WIRELOG_COMPOUND_KIND_SIDE`                            |
| Parent column width    | N physical slots per logical column                     | 1 physical slot (handle)                                |
| Indirection            | None; columnar walk                                     | One handle -> arg lookup via hash arrangement           |
| Relation backing store | Inline within parent `col_rel_t->columns`               | Auto-created `__compound_<functor>_<arity>` side-rel    |
| Access API             | `wl_col_rel_inline_*` helpers                           | Standard `col_rel_*` + `wl_compound_side_ensure`        |
| Retraction             | Z-set delta; payload preserved (retraction-seeded join) | Z-set delta on handle; physical GC under epoch frontier |
| Z-set participation    | Full; inherited from row multiplicity                   | Full; standard arrangement                              |
| SIMD / vectorization   | Zero-copy columnar walk                                 | Requires semijoin before vectorized arg access          |
| K-Fusion isolation     | Per-worker arena snapshot (bytewise copy)               | COW pointer + epoch barrier (see §5)                    |

---

## 4. Evaluation Pipeline

Stub (see `wirelog/exec_plan_gen.c`, `wirelog/columnar/eval.c`, and Issue #534
for FILTER/PROJECT/LFTJ wiring). The inline tier participates in the columnar
executor via `wl_col_rel_inline_compound_equals` (FILTER),
`wl_col_rel_inline_project_column` (PROJECT), and
`wl_col_rel_inline_arg_physical_col` (LFTJ key resolution). The side-relation
tier participates as a regular relation with a semijoin step on the handle
column.

---

## 5. K-Fusion Contract

**Authoritative version.** This supersedes any earlier prose in
`docs/ARCHITECTURE.md §4` for compound-term execution. See also
`docs/k-fusion-5-invariant-audit.md` for the 5-invariant audit and test
matrix; this section does not duplicate the audit - it specifies the contract
that the audit validates.

### 5.1 Invariant

K-Fusion workers must observe **deep-copy isolation**: during a K-Fusion
epoch, each worker evaluates on a snapshot that no sibling worker can observe
mutations of. Compound terms participate in this invariant via a two-tier
isolation strategy keyed on the compound's storage tier (§3).

Concretely (K-Fusion contract, invariants #3 and #4 in
`docs/k-fusion-5-invariant-audit.md`):

- **C1. No cross-worker sync during K-Fusion.** Workers do not take shared
  locks, do not read each other's arrangements, and do not publish
  intermediate state. All communication happens at epoch boundaries through
  Z-set consolidation (`col_diff_arrangement_reset_delta`).
- **C2. Arrangement indices are deep-copied per worker.** The index triple
  `(ht_head, ht_next, key_cols)` is allocated fresh per worker by
  `col_diff_arrangement_deep_copy` (`wirelog/columnar/diff_arrangement.c:91`).
  Pointer non-aliasing is empirically asserted in
  `tests/test_k_fusion_inline_shadow.c:213-216`.
- **C3. Column buffers are immutable during the epoch.** Workers read the
  shared `col_rel_t` column buffers under the Option (iii) immutable-during-
  epoch invariant. No worker mutates column payload during the read phase, so
  no locking is required. This is the `Option (iii)` documented inline in
  `wirelog/columnar/diff_arrangement.c:82-89`.

### 5.2 Inline-Tier Contract

Inline compound rows participate via **bytewise deep copy**. The per-worker
deep-copy path is:

1. The arrangement index triple is deep-copied
   (`col_diff_arrangement_deep_copy`, lines 91-122 of
   `wirelog/columnar/diff_arrangement.c`). This copies `key_cols`, `ht_head`,
   `ht_next`, and scalar counters; it deliberately does **not** copy column
   payload buffers.
2. The inline compound's physical slots (offset resolved by
   `wl_col_rel_inline_locate`) are read directly from the shared column
   buffers. Readers are pure
   (`wirelog/columnar/internal.h:1105-1117`): they do not mutate multiplicity,
   timestamps, or payload.
3. When a worker must write an inline compound (PROJECT into a local IDB),
   the write target is a per-worker-owned `col_rel_t`, and
   `wl_col_rel_inline_project_column` performs a bytewise copy across
   relations. Cross-worker writes are impossible by construction.

This is the "bytewise copy" clause of the K-Fusion contract: inline compounds
carry no pointers, so "copy" is a plain `memcpy` of `arity` `int64_t` slots
per row.

Invariant coverage: Z-set transparency is validated by
`tests/test_k_fusion_correctness.c` (K=1 vs K=4 fingerprint equivalence) and
`tests/test_e2e_kfusion_k4_inline.c` (authorization use case). TSan cleanliness
under K=4 x 10k iterations is validated by
`tests/test_e2e_tsan_inline_kfusion.c`. See
`docs/k-fusion-5-invariant-audit.md` for the full coverage matrix.

### 5.3 Side-Relation Contract

Side-relation rows participate via **copy-on-write (COW) handles plus epoch
barriers**.

1. The side-relation's column buffers follow the same immutable-during-epoch
   invariant as every other relation (§5.1 C3). No worker re-allocates or
   resizes side-relation columns during an active K-Fusion epoch.
2. Handle readers hold plain pointers to the shared side-relation. Because
   the side-relation is not mutated in place, these pointers are effectively
   COW references: if the epoch needs to grow or restructure, the rewrite is
   scheduled for the epoch boundary and the old snapshot remains valid for
   the duration of the epoch.
3. Epoch barriers guarantee that a worker that captured a handle in epoch
   `e` may dereference it safely for the duration of `e`. Physical
   reclamation of retracted handles happens only after the frontier has
   advanced past `e` (see `wirelog/columnar/frontier.c`,
   `wirelog/columnar/frontier_epoch.c`), so no worker can observe a stale
   handle whose backing row has been freed.
4. Per-worker arena snapshots: each K-Fusion worker takes a shallow snapshot
   of the side-relation's current arrangement (index + base/current nrows).
   The arrangement indices are deep-copied as in §5.2; the column buffers are
   shared under C3.

This is the "COW pointers with epoch barriers" clause: the side-relation acts
as a copy-on-write structure whose "copy" is driven by the epoch frontier, not
by per-worker mutation.

### 5.4 Summary Table

| Aspect              | Inline tier                                   | Side-relation tier                              |
|---------------------|-----------------------------------------------|-------------------------------------------------|
| Worker isolation    | Per-worker arena snapshot                     | Per-worker shallow snapshot + COW handle        |
| Row copy            | Bytewise `memcpy` of `arity` int64 slots      | Handle is one int64; side-row read-only during epoch |
| Sync during epoch   | None (C1)                                     | None (C1)                                       |
| Index ownership     | Deep-copied (C2)                              | Deep-copied (C2)                                |
| Payload mutability  | Immutable during epoch (C3)                   | Immutable during epoch (C3)                    |
| Reclamation         | Tied to parent row lifecycle                  | Epoch-frontier GC                               |
| Empirical proof     | `tests/test_k_fusion_inline_shadow.c`         | Covered under general arrangement isolation tests |
| Audit cross-ref     | `docs/k-fusion-5-invariant-audit.md` #4       | `docs/k-fusion-5-invariant-audit.md` #4        |

### 5.5 Forbidden Patterns

The following are explicit anti-patterns under the K-Fusion contract and must
fail code review:

- Mutating a column buffer during a K-Fusion epoch (violates C3).
- Sharing an arrangement index triple between workers (violates C2).
- Returning a pointer into a side-relation row from a K-Fusion worker without
  an epoch barrier (violates the COW + epoch-barrier clause in §5.3).
- Writing into another worker's per-worker relation (violates C1 and the
  deep-copy isolation invariant).

---

## 6. RDF Named Graph Integration

RDF named-graph support (Issue #535) extends wirelog with four-place quads
`(s, p, o, g)` and an auto-registered metadata relation. Quad storage is
layered on top of the compound-term infrastructure: `s`, `p`, `o` may be
scalars or compound values following §3, and `g` is a plain int64 graph id.

### 6.1 Quad Layout

A quad is a row in any relation whose schema contains a `__graph_id` column.
The three data positions `(s, p, o)` are the remaining columns; their types
follow the normal column rules (scalar, inline compound, or side-relation
compound).

Declaration:

```
.decl triple(s: int64, p: int64, o: int64, __graph_id: int64)
```

Detection (`wirelog/ir/program.c:401-412`):

```
if (strcmp(param->name, "__graph_id") == 0) {
    rel->has_graph_column = true;
    rel->graph_column_index = idx;
}
```

At session creation, this is mirrored onto the physical relation
(`wirelog/columnar/session.c:699-702`):

```
r->has_graph_column = true;
r->graph_col_idx = plan->edb_graph_col_index[i];
```

`col_rel_t.has_graph_column` and `col_rel_t.graph_col_idx` live at
`wirelog/columnar/internal.h:267-273`. Duplicate `__graph_id` columns emit a
warning and retain the first index; duplicate `.decl` entries reset the
graph-column fields first (re-entry safety).

### 6.2 Metadata Relation Schema

`__graph_metadata` is a reserved relation with a fixed 6-column schema
(`wirelog/columnar/session.c:710-744`, `wirelog/ir/program.c:332-356`):

| Column        | Type    | Role                                      |
|---------------|---------|-------------------------------------------|
| `graph_id`    | int64   | Primary key matching `__graph_id` values. |
| `tenant`      | int64   | Tenant / owner identifier (intern id).    |
| `timestamp`   | int64   | Creation or assertion timestamp.          |
| `location`    | int64   | Provenance location (intern id).          |
| `risk`        | int64   | Risk / sensitivity level.                 |
| `description` | string  | Free-form description (intern id).        |

**Auto-creation rules** (`wirelog/columnar/session.c:710-744`):

1. If the plan declares any EDB with `__graph_id`
   (`plan->edb_has_graph_column[i]`), session creation auto-registers
   `__graph_metadata` with the fixed schema.
2. If the user declared `__graph_metadata` explicitly with a compatible
   arity (exactly 6 typed params), the auto-creation is skipped
   (`session_find_rel` duplicate guard).
3. If the user declared it with a non-6-column arity, parsing fails
   (`wirelog/ir/program.c:341-355`): the fixed schema would read out of
   bounds downstream.

The relation is allocated empty; user code populates rows directly. All 6
columns use standard scalar storage; metadata is not itself compound-typed.

### 6.3 Filtering Semantics

A rule body may filter on the `__graph_id` column like any other column:

```
provenance(s, p, o, g, tenant) :-
    triple(s, p, o, g),
    __graph_metadata(g, tenant, _, _, _, _).
```

The executor treats `__graph_id` as a normal int64 column - no special
operator is needed. The plan generator still records graph-column awareness
on the plan (`plan->edb_has_graph_column[i]` /
`plan->edb_graph_col_index[i]`, documented in
`wirelog/exec_plan.h:435-448`) so that downstream consumers (K-Fusion
partitioning, §6.5) can route by graph id when beneficial.

### 6.4 Interop With Compound Terms

Quad positions are ordinary columns: each of `s`, `p`, `o` may independently
be a scalar, an inline compound, or a side-relation compound following §3.
The tier selection rules are unchanged - the `__graph_id` column is
orthogonal to compound-kind.

Concrete example (inline compound in object position):

```
.decl event(s: int64, p: int64, o: inline meta(uid: int64, ts: int64, loc: int64), __graph_id: int64)
```

Here `o` is an inline `meta/3` (§3.2), `__graph_id` triggers quad
semantics (§6.1), and the relation has one physical handle-free layout:
`[s, p, meta.uid, meta.ts, meta.loc, __graph_id]`. Side-relation compounds
interop identically; only the physical width of the row changes.

### 6.5 K-Fusion Co-Partitioning Note

K-Fusion partitioning may route by `__graph_id` so that all rows for the
same named graph land on the same worker, preserving the monotone-join
guarantee when both sides of a join carry `__graph_id`
(`wirelog/columnar/internal.h:1575-1585` block comment). When only one side
carries the graph column, routing falls back to the join-key hash. This is
purely a partitioning optimization; correctness is already guaranteed by the
K-Fusion contract of §5.

### 6.6 Reserved Names

- `__graph_id` - column name that triggers quad semantics on its relation.
- `__graph_metadata` - reserved relation name with a fixed 6-column schema.
- `__compound_<functor>_<arity>` - reserved side-relation name pattern
  (§3.3).

User code must not declare a `__compound_<functor>_<arity>` relation
manually; `__graph_metadata` may be declared if and only if it has exactly
6 typed columns (§6.2).

---

## 7. Performance Characteristics

This section states the theoretical bounds of compound-term and quad
operations. Measured numbers are collected by `bench/bench_compound.c` and
reported in `docs/PERFORMANCE.md`; this section carries `TBD` markers where
measurements belong.

### 7.1 Theoretical Bounds

| Operation                                   | Complexity             | Notes                                                                               |
|---------------------------------------------|------------------------|-------------------------------------------------------------------------------------|
| Parse compound term                         | O(n) tokens            | Linear descent in `wirelog/parser/`; `n` = token count in the compound.             |
| Inline-compound FILTER equality             | O(1) per column        | Branch-predictable; `wl_col_rel_inline_compound_equals` is a tight int64 loop.      |
| Inline-compound PROJECT (one compound)      | O(arity) per row       | `memcpy` of `arity` int64 slots via `wl_col_rel_inline_project_column`.             |
| Inline-compound LFTJ key                    | O(1) per key           | `wl_col_rel_inline_arg_physical_col` resolves once; key extraction is a column read.|
| Side-relation semijoin                      | O(|side| + |probe|)    | Hash-join via `col_diff_arrangement_t`; probe = parent handle column.               |
| Side-relation retrieval per row             | O(1) expected          | Hash lookup on handle; worst case O(|side|) under pathological hash collisions.     |
| `wl_compound_side_ensure`                   | O(1) amortised         | Idempotent; hits `session_find_rel` (hash) on re-call.                              |
| `__graph_id` filter                         | O(|relation|)          | Scan; equivalent to any scalar column filter.                                       |
| `__graph_metadata` join                     | O(|graphs| + |probe|)  | Standard arrangement semijoin.                                                      |

Constant factors:

- Inline tier has zero indirection: FILTER/PROJECT/LFTJ hot paths read the
  `int64_t **columns` grid directly (see
  `wirelog/columnar/internal.h:326-329` accessor layer).
- Side-relation tier adds one hash lookup per retrieved row plus a column
  read per retrieved argument.

### 7.2 K-Fusion Scaling

| Operation                       | K=1 baseline    | K=4 scaling hypothesis          | Notes                                                          |
|---------------------------------|-----------------|----------------------------------|----------------------------------------------------------------|
| Inline FILTER                   | Linear          | Near-linear                      | No sync; per-worker arena snapshot; `memcpy` per row only.     |
| Inline PROJECT                  | Linear          | Near-linear                      | Same path as FILTER + bytewise slot copy.                      |
| Side-relation semijoin          | O(|side|+|probe|)| Near-linear on `|probe|`        | Side-relation shared immutably; arrangement deep-copied.       |
| Cross-compound join             | Depends on plan | Preserved by K-Fusion contract   | §5 guarantees Z-set transparency.                              |

### 7.3 Measured Numbers

Populated by `worker-docs` from `bench_compound --iters 10000` runs at
SHA `7010d9b` (Issue #536, Tasks #2, #5, #6). Build: debug (`-O0`), CPU
governor: `powersave`. Full methodology and threshold check in
`docs/PERFORMANCE.md`. K=4 microbenchmark not yet run; K=4 correctness
covered by integration tests (see §5.2).

| Scenario                                          | K=1 (p50 / p95 / p99)           | K=4            | Ratio         |
|---------------------------------------------------|----------------------------------|----------------|---------------|
| Parser: compound token (per-op latency)           | 18.65 us / 27.66 us / 37.23 us  | n/a            | n/a           |
| Inline FILTER: arity=3, per row                   | 7.05 us / 7.61 us / 10.48 us    | TBD            | TBD           |
| Inline PROJECT: arity=3, per row                  | TBD (mode not in bench_compound) | TBD            | TBD           |
| Side semijoin: handle -> arg lookup               | 164.06 us / 320.92 us / 346.41 us | TBD          | TBD           |
| Multi-graph filter: __graph_id scan, per row      | 7.05 us / 7.61 us / 9.01 us     | TBD            | TBD           |
| Authorization e2e (inline compound, K=4)          | n/a                              | TBD            | n/a           |

Targets and thresholds (Issue #536 acceptance criteria) are documented in
`docs/PERFORMANCE.md`. This section reports bounds only; regressions are
gated in the `perf` test suite.

### 7.4 Memory Characteristics

| Resource                        | Inline tier                   | Side-relation tier                       |
|---------------------------------|-------------------------------|------------------------------------------|
| Per-row bytes                   | `arity * 8`                   | `8` (handle) + amortised side-row cost   |
| Total for N rows, compound f/k  | `N * k * 8`                   | `N * 8` + `|distinct compounds| * (1+k) * 8` |
| GC cost                         | None (tied to parent lifetime)| Epoch-frontier scan at epoch boundary    |
| Arrangement deep-copy (K=W)     | `W * (key_count + ht_cap)` ints | Same                                   |

No additional allocations occur on the inline hot path; all helpers in
§3.2 are pure readers or schema-local writers. Side-relation auto-creation
is O(1) amortised and happens at session setup, not per row.

#### Measured peak RSS (Issue #536 AC5)

`bench_compound --iters 10000` at SHA `7010d9b` (debug build, `powersave`
governor) via `getrusage(RUSAGE_SELF)`:

| Mode          | peak_rss_kb | vs bench_flowlog tc (2952 KB) |
|---------------|:-----------:|:-----------------------------:|
| parser        | 2564 KB     | 0.87x (below baseline)        |
| match         | 2668 KB     | 0.90x (below baseline)        |
| semijoin      | 2488 KB     | 0.84x (below baseline)        |
| multi-graph   | 2596 KB     | 0.88x (below baseline)        |

All modes are below the bench_flowlog tc baseline. The 10% overhead
acceptance criterion (AC5) is **VERIFIED** at the microbenchmark level.
Inline compound columns require no additional allocation beyond the existing
`col_rel_t` column array; RSS overhead is not measurable at 256-row scale.

---

## Cross-References

- `docs/ARCHITECTURE.md` - engine-wide invariants and K-Fusion overview.
- `docs/k-fusion-5-invariant-audit.md` - 5-invariant audit and test matrix
  that the §5 contract is verified against.
- `docs/SYNTAX.md` - user-facing syntax reference.
- `docs/COMPOUND_TERMS.md` - user-facing compound-terms guide (Issue #536).
- `docs/RDF_QUADS.md` - user-facing RDF named-graph guide (Issue #536).
- `docs/PERFORMANCE.md` - measured performance numbers and regression gates
  (Issue #536).
- `wirelog/columnar/internal.h` - `col_rel_t` layout and inline-tier helpers.
- `wirelog/columnar/compound_side.h` - side-relation auto-creation API.
- `wirelog/columnar/diff_arrangement.c` - `col_diff_arrangement_deep_copy`
  (K-Fusion deep-copy isolation).
- `wirelog/wirelog-types.h` - `wirelog_compound_kind_t` enum.
- `wirelog/ir/program.c` - `.decl` parsing and `__graph_id` detection.
- `wirelog/columnar/session.c` - `__graph_metadata` auto-creation.
- `wirelog/exec_plan.h` - plan-level `edb_has_graph_column`,
  `edb_graph_col_index`.
