# Ownership Flags Policy for Deep-Copied Relations (Issue #555)

**Status:** Decided
**Related:** #553 (col_rel_deep_copy implementation), #554 (mem_ledger=NULL policy)
**Rule:** R-3 — ownership flags reset in transient copies

---

## 1. Problem

`col_rel_t` carries two ownership flags that govern lifecycle:

```c
bool pool_owned;   // struct allocated from delta_pool
bool arena_owned;  // column buffers allocated from wl_arena_t
```

These flags determine who is responsible for reclaiming memory:

- `pool_owned == true` → struct memory is reclaimed on `delta_pool_reset()`;
  `col_rel_destroy()` must NOT call `free()` on the struct.
- `arena_owned == true` → column buffers were allocated from a `wl_arena_t`;
  `col_rel_free_contents()` must NOT call `free()` on each column.

When `col_rel_deep_copy()` (Issue #553) creates a transient copy of a relation
during epoch rotation, K-Fusion fork points, retraction backup, or debug
snapshots, **what should the ownership flags of the destination be?**

The choices are:

- **(a)** Inherit from `src`: copy `src->pool_owned` and `src->arena_owned`.
- **(b)** Reset both to `false`: declare the copy heap-owned regardless.
- **(c)** Mixed inheritance: e.g. clear `pool_owned` (struct is fresh
  `calloc()`) but propagate `arena_owned` if buffers were arena-cloned.

Option (a) is wrong outright: the destination struct comes from `calloc()`,
not from `src`'s pool, so propagating `pool_owned=true` would corrupt the
heap on `col_rel_destroy()`. Option (c) is plausible if a future
arena-aware deep_copy variant is introduced, but the current implementation
always heap-clones buffers (the `wl_arena_t *arena` parameter is reserved
and ignored — see `col_rel_deep_copy` contract block).

This document settles the policy for the current implementation.

---

## 2. Decision

```
pool_owned  = false   (not managed by allocator pool)
arena_owned = false   (not managed by arena allocator)
```

Both flags are unconditionally cleared on the destination. The deep copy is
fully heap-owned, and `col_rel_destroy()` will reclaim both the column
buffers and the struct itself via `free()`.

This pairs with the parallel **Issue #554 / R-1** rule that
`dst->mem_ledger = NULL` on the deep copy: the transient relation does not
participate in pool, arena, or ledger lifecycle accounting.

---

## 3. Why

### 3.1 Transient lifetime
A deep copy exists only for the duration of a workspace operation
(epoch rotation, K-Fusion fork, retraction backup, debug snapshot). It is
not a long-lived relation and does not belong in any pool's lifetime
budget.

### 3.2 Caller responsibility
The caller of `col_rel_deep_copy()` allocates the copy and must free it
explicitly via `col_rel_destroy()` (or `col_rel_free_contents()` followed
by `free()` on the struct). With both flags cleared, `col_rel_destroy()`
follows the standard heap path with no ambiguity.

### 3.3 No auto-reclaim
Setting `pool_owned=true` would cause `col_rel_destroy()` to skip
`free(struct)`, leaking the calloc'd struct (the source pool has no
record of the copy). Setting `arena_owned=true` would cause
`col_rel_free_contents()` to skip `free(column_buf)` for buffers that
were in fact heap-allocated by `col_columns_alloc()`, leaking the column
storage. **Both flags must be `false` to match the actual allocation
path used by the deep copy.**

### 3.4 Memory safety
Mismatched ownership flags are a double-free or leak waiting to happen:

- Wrong `pool_owned=true`: pool reset later frees the struct as part of
  pool teardown; if the caller also calls `col_rel_destroy()`, that path
  reads the (now-stale) `pool_owned` and skips `free()` — leak. If the
  caller calls `free()` directly assuming heap ownership, double-free.
- Wrong `arena_owned=true`: arena reset frees buffers; subsequent
  `col_rel_destroy()` skips `free(columns[c])` based on the flag — leak.
  Inverse mismatch causes double-free when both arena and `free()` reclaim
  the same buffer.

Forcing both flags to `false` makes the lifetime contract trivial: the
copy is heap-owned end-to-end; one `col_rel_destroy()` call reclaims
everything.

---

## 4. Comparison

| Flag          | Normal Relation       | Deep Copy   | Meaning when `true`                       |
| ------------- | --------------------- | ----------- | ----------------------------------------- |
| `pool_owned`  | `true` or `false`     | **`false`** | Struct managed by `delta_pool`; do NOT `free()` struct |
| `arena_owned` | `true` or `false`     | **`false`** | Column buffers managed by `wl_arena_t`; do NOT `free()` per-column |
| `mem_ledger`  | non-`NULL` or `NULL`  | **`NULL`**  | Buffer growth/free reported to subsystem ledger (R-1, #554) |

A "normal" relation here is one created by `col_rel_alloc()`,
`col_rel_pool_new_auto()`, or operator-output paths (`col_op_join`,
etc.). For those, the flags reflect the actual allocation source and
must be honored on destroy.

For a deep copy, the allocation source is fixed (`calloc()` for the
struct, `col_columns_alloc()` for buffers, both heap), so the flags are
fixed too.

---

## 5. Interaction with Issue #554 (mem_ledger=NULL)

Issue #554 establishes that `dst->mem_ledger = NULL` for deep copies.
The R-3 ownership-flag reset is the natural counterpart:

| Field         | R-1 (#554)        | R-3 (#555)               |
| ------------- | ----------------- | ------------------------ |
| `mem_ledger`  | `NULL` (reset)    | —                        |
| `pool_owned`  | —                 | `false` (reset)          |
| `arena_owned` | —                 | `false` (reset)          |

Together, R-1 and R-3 say: **the deep copy participates in no upstream
accounting or lifecycle infrastructure.** It is a private, heap-owned
workspace relation. The caller may opt back in by wiring up
`dst->mem_ledger` after the copy returns; there is no equivalent opt-in
for `pool_owned`/`arena_owned` because those flags reflect the allocation
source, which the deep copy fixes at creation time.

---

## 6. Memory Safety Argument

Claim: the R-3 policy guarantees no double-free and no leak from the
deep-copy lifecycle.

Proof sketch:
1. `col_rel_deep_copy()` allocates `dst` via `calloc(1, sizeof(*dst))` —
   heap-owned struct.
2. Column grid is allocated via `col_columns_alloc(ncols, capacity)` —
   heap-owned per-column buffers.
3. Other owned fields (`name`, `col_names[]`, `timestamps`,
   `merge_columns`, `retract_backup_columns`, `compound_arity_map`,
   ArrowSchema) are deep-cloned via heap allocators or
   `ArrowSchemaInit`-then-fill (no aliasing of `src`'s release callback).
4. `pool_owned=false` ⇒ `col_rel_destroy()` calls `free(dst)` ✓.
5. `arena_owned=false` ⇒ `col_rel_free_contents()` calls
   `col_columns_free(columns, ncols)` which `free()`s each column buffer ✓.
6. No path in `col_rel_destroy()` or `col_rel_free_contents()` reaches
   any pool or arena state for the copy. Therefore reclaim is independent
   of `src`'s lifetime and cannot double-free `src`'s buffers.

Conversely, if either flag were inherited from `src` and `src` were
pool- or arena-owned, the destroy path would skip a heap `free()` that is
required (because `dst`'s buffers are in fact on the heap, not in
`src`'s pool/arena) — a leak. R-3 is therefore not just a convention; it
is required for correctness given the current allocation strategy.

---

## 7. Testing Strategy

The #553 test suite must validate R-3 directly. Required assertions
after every successful `col_rel_deep_copy()`:

```c
assert(dst->pool_owned  == false);
assert(dst->arena_owned == false);
assert(dst->mem_ledger  == NULL);   // R-1 (#554), included for completeness
```

Coverage matrix — assertions must hold for all four ownership states of
`src`:

| `src->pool_owned` | `src->arena_owned` | Expected on `dst` |
| ----------------- | ------------------ | ----------------- |
| `false`           | `false`            | both `false`      |
| `false`           | `true`             | both `false`      |
| `true`            | `false`            | both `false`      |
| `true`            | `true`             | both `false`      |

Lifecycle test (memory safety):
1. Create `src` with `pool_owned=true` (via `col_rel_pool_new_auto`).
2. `col_rel_deep_copy(src, &dst, NULL)`.
3. `col_rel_destroy(dst)` — must succeed without touching the pool.
4. `delta_pool_reset(pool)` — must reclaim `src` cleanly.
5. ASan/TSan clean across the sequence.

---

## 8. Policy Statement (for #553 Implementer Reference)

> All deep copies must set `pool_owned=false` and `arena_owned=false`;
> caller is responsible for freeing via `col_rel_destroy()` (or
> `col_rel_free_contents()` + `free(struct)`). The transient copy
> participates in neither pool nor arena lifecycle.

This statement is incorporated into the `col_rel_deep_copy()` contract
in `internal.h` and into the Group I block of the implementation in
`relation.c`.
