# Session Thread-Safety Contract

## Overview

`wl_col_session_t` is not thread-safe by default. However, the K-fusion parallel
evaluation path (`col_op_k_fusion`) safely dispatches multiple worker threads by
following a strict isolation contract: each worker receives a **shallow copy** of
the session with only the fields that require write access independently allocated.
All other fields are read-only during worker execution.

This document specifies which session fields belong to which access category, the
synchronization points that enforce the contract, and rules for correct usage.

---

## Field Access Categories

### [SHARED_RO] — Read-only during worker execution

These fields are written by the main thread before workers are dispatched. Workers
read them freely without synchronization.

| Field | Notes |
|-------|-------|
| `base` | Backend vtable; immutable after session creation |
| `plan` | Borrowed pointer; immutable during eval |
| `rels[]` | Relation array; workers call `session_find_rel()` read-only |
| `nrels`, `rel_cap` | Relation count and capacity; immutable during eval |
| `delta_cb`, `delta_data` | Callback and context; not invoked during K-fusion |
| `outer_epoch` | Epoch counter; set before dispatch, not modified by workers |
| `frontiers[MAX_STRATA]` | Per-stratum convergence; read for skip condition |
| `rule_frontiers[MAX_RULES]` | Per-rule convergence; read for skip condition |
| `current_iteration` | Set at start of each iteration loop; read by FORCE_DELTA logic |
| `delta_seeded` | Pre-seeding flag; set before dispatch |
| `retraction_seeded` | Retraction flag; set before dispatch |
| `last_inserted_relation` | Borrowed name pointer; set before eval |
| `last_removed_relation` | Borrowed name pointer; set before eval |
| `num_workers` | Immutable after `col_session_create` |
| `stratum_is_monotone[]` | Immutable after `col_session_create` |

**Rule**: Workers MUST NOT write to any [SHARED_RO] field. A write would be a data
race because the main session is concurrently readable by all workers.

### [WORKER_EXCL] — Worker-exclusive (isolated copy per worker)

Each worker receives its own independent instance of these fields. Workers write
freely to their copy; the main session's copy is untouched during dispatch.

| Field | Isolation mechanism |
|-------|-------------------|
| `eval_arena` | New arena per worker via `wl_arena_create(capacity)` |
| `mat_cache` | Copied by value; workers add entries to their copy only |
| `arr_entries`, `arr_count`, `arr_cap` | Zeroed in worker copy (`NULL`, 0, 0) |
| `darr_entries`, `darr_count`, `darr_cap` | Zeroed in worker copy |
| `delta_pool` | New pool per worker via `delta_pool_create()` |
| `wq` | Set to `NULL` in worker copy (prevents nested K-fusion) |

**Rule**: Each [WORKER_EXCL] field must be independently initialized for every
worker before dispatch. Sharing any of these between workers would cause data races.

**mat_cache special case**: The `mat_cache` is copied by *value* (all fields
including the `entries[]` array). Workers inherit existing cache entries for
read-only lookup. At post-barrier cleanup, only entries added by the worker
(index `>= base_count`) are freed — entries `0..base_count-1` share result
pointers with the original session's cache and must not be double-freed.

### [SESSION_ONLY] — Main thread only, outside K-fusion dispatch

These fields are never accessed by workers. They are updated before dispatch
(setup) or after the barrier (accumulation).

| Field | When updated |
|-------|-------------|
| `total_iterations` | Post-eval, after all strata complete |
| `consolidation_ns` | Post-barrier, main thread accumulates timing |
| `kfusion_ns` | Post-barrier, main thread accumulates timing |
| `kfusion_alloc_ns` | Post-barrier (each phase recorded by main thread) |
| `kfusion_dispatch_ns` | Post-barrier |
| `kfusion_merge_ns` | Post-barrier |
| `kfusion_cleanup_ns` | Post-barrier |
| `profile` (`WL_PROFILE`) | Post-barrier, operator profiling counters |

**Rule**: Workers MUST NOT access [SESSION_ONLY] fields. The main session pointer
is not passed to worker functions for fields in this category.

---

## Synchronization Points

```
[PRE-DISPATCH]
  Main thread:
    - Sets all [SHARED_RO] fields (current_iteration, delta_seeded, etc.)
    - Snapshots mat_cache.count as base_count
    - For each worker d in [0, k):
        worker_sess[d] = *sess;              // shallow copy
        worker_sess[d].wq = NULL;            // no nested K-fusion
        worker_sess[d].eval_arena = wl_arena_create(...);  // isolated
        worker_sess[d].arr_entries = NULL;   // isolated
        worker_sess[d].arr_count = 0;
        worker_sess[d].arr_cap = 0;
        worker_sess[d].darr_entries = NULL;  // isolated
        worker_sess[d].darr_count = 0;
        worker_sess[d].darr_cap = 0;
        worker_sess[d].delta_pool = delta_pool_create(...); // isolated

[DISPATCH]
  Workers execute concurrently:
    - Read [SHARED_RO] fields from their session copy (same values)
    - Write only to [WORKER_EXCL] fields in their own copy
    - Call session_find_rel() read-only on shared rels[]

[BARRIER]
  wl_workqueue_wait_all(wq)
    -- all workers have finished before this returns

[POST-BARRIER]
  Main thread:
    - Collects results from each worker's eval_stack
    - Merges K results via col_rel_merge_k()
    - Accumulates [SESSION_ONLY] profiling counters
    - Frees worker-private resources:
        - mat_cache entries added by worker (index >= base_count)
        - arr_entries[], darr_entries[]
        - delta_pool
        - eval_arena
```

---

## Worker Access Rules

1. **Read rels[] only via `session_find_rel()`** — this function takes a
   `wl_col_session_t*` and searches `sess->rels[0..nrels)` by name. Workers call
   this with their session copy (which shares the same `rels` pointer). Since
   `rels[]` is not modified during K-fusion, no lock is needed.

2. **Never write to `sess->rels[]` from a worker** — relation modifications
   (`session_add_rel`, `session_remove_rel`) are main-thread-only operations and
   must not be called within K-fusion worker paths.

3. **Never write to frontier arrays from workers** — `frontiers[]` and
   `rule_frontiers[]` are read for the skip condition
   (`iter > frontiers[stratum_idx].iteration`). Workers MUST NOT update these;
   the main thread updates them post-barrier.

4. **Never access `wq` from workers** — `wq` is set to `NULL` in worker copies.
   Any nested K-fusion attempt within a worker would see `wq == NULL` and fall
   back to the sequential path, preventing deadlock.

5. **Each worker owns its `eval_arena`, `delta_pool`** — these are not shared.
   A worker must not save a pointer to another worker's arena or pool.

---

## Verifying the Contract with TSan

Build with ThreadSanitizer to detect violations at runtime:

```sh
meson setup build-tsan -Db_sanitize=thread -Db_lundef=false
meson compile -C build-tsan
meson test -C build-tsan test_workqueue
```

The `test_session_isolation_contract` test in `tests/test_workqueue.c` exercises
parallel evaluation with 4 workers across three sequential snapshot calls. TSan
will report any data race on shared session state if the isolation contract is
violated.

Expected output: all tests pass with zero TSan reports.

---

## Example: Correct Worker Session Setup

```c
// CORRECT: each worker gets isolated copies of write-capable fields
uint32_t base_count = sess->mat_cache.count;  // snapshot before workers
for (uint32_t d = 0; d < k; d++) {
    worker_sess[d] = *sess;                    // shallow copy [SHARED_RO]
    worker_sess[d].wq = NULL;                  // no nested K-fusion
    worker_sess[d].eval_arena =
        wl_arena_create(sess->eval_arena->capacity); // [WORKER_EXCL]
    worker_sess[d].arr_entries = NULL;         // [WORKER_EXCL] zeroed
    worker_sess[d].arr_count = 0;
    worker_sess[d].arr_cap = 0;
    worker_sess[d].darr_entries = NULL;        // [WORKER_EXCL] zeroed
    worker_sess[d].darr_count = 0;
    worker_sess[d].darr_cap = 0;
    worker_sess[d].delta_pool =
        delta_pool_create(128, sizeof(col_rel_t), 32*1024*1024); // [WORKER_EXCL]
}

// WRONG: sharing eval_arena between workers — data race on arena offset
for (uint32_t d = 0; d < k; d++) {
    worker_sess[d] = *sess;
    // worker_sess[d].eval_arena is still sess->eval_arena — RACE!
}
```

---

## Phase 1 Compliance

Arena isolation was implemented in Phase 1 (issue #99). Prior to Phase 1, all
workers shared the session's single `eval_arena`, causing data races on the arena
offset field. The fix added `worker_sess[d].eval_arena = wl_arena_create(...)` for
each worker, satisfying the [WORKER_EXCL] contract for arena allocation.

The `test_k_fusion_arena_isolation` test verifies correctness by comparing sorted
output rows across K=1,2,4,8 workers. TSan verifies absence of races when built
with `-Db_sanitize=thread`.
