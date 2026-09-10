# wirelog Threading and Concurrency

This document is the **canonical reference for wirelog's concurrency
model**: backend selection, the thread abstraction surface, the
atomics audit, the lock-free delta queue, the K-fusion parallel
dispatch threshold, the compound-arena epoch contract under
worker-borrow semantics, and the signal-safety stance.

It is the deliverable for issue **#734** under epic **#681**
(v0.41 ABI Infrastructure).

Status: Current. Pinned at v0.40.99; any drift between this document
and the source is mechanically caught by
`scripts/ci/check-threading-doc.sh` (`meson test --suite abi:threading_doc`).

---

## 1. Scope and audience

Audience: **downstream embedders** integrating `libwirelog.so.1` into
multi-threaded host processes; **internal contributors** changing
worker-loop code, atomic state, or arena ownership.

**In scope:** every `atomic_*` call site in `wirelog/` production
sources, the threading backend selection chain, K-fusion dispatch
gating, the compound-arena epoch boundary contract, signal-safety.

**Out of scope (explicitly):**

- `examples/`: consumer code, not part of the library.
- `subprojects/` (nanoarrow, xxhash, mbedtls): third-party deps,
  audited under their own projects.
- `tests/`: test scaffolding, not part of the shipped surface.
- `bench/`: benchmark harness; perf numbers are cited, not reproduced.
- `build*/`: artifact directories.

**There is no `wirelog/util/atomics.h` header.** The issue body
referenced one; in fact the atomics surface is layered as documented
in [§4](#4-atomics-layering). Creating a consolidated header is not
part of #734 and would be a separate atomic-commit-sized API change.

### Terminology disambiguation

In this document and in the source tree, **"TDD"** means **"Tuple-at-a-
time Differential Dataflow"** (the differential-dataflow-style
worker model that drives recursive evaluation). It is **not** related
to Test-Driven Development (the methodology mandated by the project's
`CLAUDE.md`). The collision is real and unfortunate; this document
always means the former. Look for `tdd_init_workers_hybrid`,
`tdd_exchange_deltas`, and the `tdd_*_ns` profiling counters in
`wirelog/columnar/internal.h` and `wirelog/columnar/eval.c` for the
implementation.

---

## 2. Threading backend selection

wirelog ships three threading backends in `wirelog/thread.{h,c11.c,
posix.c,msvc.c}`:

| Backend | Source | Selected when |
|---|---|---|
| C11 `<threads.h>` | `wirelog/thread_c11.c` | `WL_HAVE_C11_THREADS` defined (probed via `cc.links()`) |
| Windows MSVC | `wirelog/thread_msvc.c` | `_WIN32` and `WL_HAVE_C11_THREADS` not defined |
| POSIX pthreads | `wirelog/thread_posix.c` | Default fallback |

The detection precedence at compile time (`wirelog/thread.h:62-94`)
is `WL_HAVE_C11_THREADS > _WIN32 > POSIX`. This is the **auto-detected
order** under the default build option `-Dthreads=native`.

### Build-time override

`meson_options.txt:14-19` exposes a `threads` combo option:

```
option('threads', type: 'combo', choices: ['native', 'posix'],
       value: 'native',
       description: 'Threading backend: native (auto-detect C11/POSIX) or
                     posix (force pthreads, required for TSan)')
```

- `-Dthreads=native` (default): `wirelog/meson.build:57-72` runs a
  `cc.links()` probe for `<threads.h>` and sets `-DWL_HAVE_C11_THREADS`
  if the probe succeeds; otherwise the macro is omitted and the header
  falls back to pthreads (or Windows on `_WIN32`).
- `-Dthreads=posix`: the probe is skipped and `pthreads` is forced
  unconditionally. **This is required for ThreadSanitizer** because
  TSan only provides reliable race diagnostics for wirelog when the
  worker threads are created through the pthread backend. The gating
  TSan CI leg at `.github/workflows/ci-pr.yml` invokes meson with
  `-Dthreads=posix`.

### Public API

Every thread/sync primitive that wirelog uses internally is wrapped
behind `thread_t`, `mutex_t`, `cond_t` defined in
`wirelog/thread.h:82-130`. These typedefs are **internal**; they are
not part of the installed public surface, and downstream embedders
must use the threading primitives of their own host process to drive
the library's `wirelog_session_*` calls.

### Mutex semantics

All `mutex_t` instances are **non-recursive** (`wirelog/thread.h:104`).
Windows `CRITICAL_SECTION` is recursive by default; wirelog code
nevertheless must not rely on self-reentrance — the C11 and POSIX
backends would deadlock.

---

## 3. Thread abstraction surface

The 9 public functions in `wirelog/thread.h` (which is an internal
header, despite the qualifier "public" appearing in the doc comment):

```
int  thread_create(thread_t *t, int (*start)(void *), void *arg);
int  thread_join(thread_t *t, int *result);

int  mutex_init(mutex_t *m);
void mutex_destroy(mutex_t *m);
int  mutex_lock(mutex_t *m);
int  mutex_unlock(mutex_t *m);

int  cond_init(cond_t *c);
void cond_destroy(cond_t *c);
int  cond_wait(cond_t *c, mutex_t *m);
int  cond_signal(cond_t *c);
int  cond_broadcast(cond_t *c);
```

The return-code contract follows pthread conventions: `0` on success,
non-zero on failure. The three backend implementations
(`thread_c11.c`, `thread_posix.c`, `thread_msvc.c`) are structurally
identical wrappers; behavioural divergences are confined to the
mutex-recursion note in §2.

---

## 4. Atomics layering

There is no single atomics header. Atomic operations reach the
hardware through three layers:

### 4.1 Direct `<stdatomic.h>` (GCC/Clang)

On GCC/Clang, every site reads `<stdatomic.h>` directly through
`atomic_load_explicit`, `atomic_store_explicit`,
`atomic_fetch_add_explicit`, `atomic_compare_exchange_weak_explicit`,
`atomic_load`, and `atomic_store`. Each site picks its memory order
explicitly (audit in [§5](#5-atomics-audit)).

### 4.2 MSVC shim in `mem_ledger.h`

`wirelog/columnar/mem_ledger.h:24-89` reimplements the subset of C11
atomics that `mem_ledger.c` needs by routing through
`_InterlockedCompareExchange64`. Notes:

- `atomic_load_explicit(ptr, order)` expands to `(*(ptr))` (a plain
  read of a `volatile uint64_t`). On x86, x86-64, and ARM64 this is
  safe because aligned 64-bit reads are atomic and the volatile
  qualifier prevents the compiler from caching the value.
- `atomic_store_explicit(ptr, val, order)` expands to `(*(ptr) = (val))`
  under the same constraints.
- `atomic_fetch_add_explicit` and `atomic_fetch_sub_explicit` use a
  `_InterlockedCompareExchange64` retry loop; this gives acquire+release
  semantics regardless of the requested order.
- `atomic_compare_exchange_weak_explicit` is `_InterlockedCompareExchange64`
  with one comparison, returning `true` on success.
- **Memory orders are ignored** under this shim. The macros at
  `mem_ledger.h:87-89` define `memory_order_relaxed`/`_release`/`_acquire`
  as integer constants only; the intrinsics provide the order the
  hardware already gives. The portable code in `mem_ledger.c` continues
  to pass explicit orders so the GCC/Clang path is correct; the MSVC
  path is a no-op decoration.

### 4.3 MSVC shim in `lockfree_queue.c`

`wirelog/util/lockfree_queue.c:22-37` provides a smaller shim for
`uint32_t` atomic ring-buffer indices:

- `WL_ATOMIC_LOAD_RELAXED(p)` / `WL_ATOMIC_LOAD_ACQUIRE(p)` both expand
  to `(*(p))` on MSVC.
- `WL_ATOMIC_STORE_RELAXED(p, v)` expands to a plain assignment.
- `WL_ATOMIC_STORE_RELEASE(p, v)` expands to `_WriteBarrier();
  *(p) = (v);`. The `_WriteBarrier` intrinsic is a compiler barrier
  only (not a hardware fence), which is sufficient on x86/x86-64
  (TSO) and ARM64 with sequentially-consistent memory ordering of
  aligned 32-bit stores.

On non-MSVC builds the same macros expand to
`atomic_load_explicit(p, memory_order_relaxed)` and friends
(`lockfree_queue.c:47-57`).

### 4.4 MSVC shim in `intern.c`

`wirelog/intern.c:67-92` provides the same shape of shim for the
`uint32_t` published-entry counter of the shared symbol table
(Issue #958):

- `WL_INTERN_LOAD_ACQUIRE(p)` / `WL_INTERN_LOAD_RELAXED(p)` both expand
  to `(*(p))` on MSVC, a plain read of a `volatile uint32_t`.
- `WL_INTERN_STORE_RELEASE(p, v)` expands to a plain assignment.
  Aligned 32-bit accesses are atomic on x86/x86-64 and ARM64, and
  under MSVC's `/volatile:ms` model -- the default on x86/x64, but not
  on ARM64, where MSVC defaults to `/volatile:iso` -- a volatile store
  carries release
  semantics -- the same assumption `mem_ledger.h` already makes.

On non-MSVC builds (including the MinGW GCC Windows job) the macros
expand to `atomic_load_explicit` / `atomic_store_explicit` with the
orders named above.

### 4.5 Type aliases

- `wirelog/columnar/mem_ledger.h:33-42` defines `wl_atomic_u64`,
  `atomic_bool`, `atomic_uint_fast64_t` as MSVC-compatible aliases.
- `wirelog/util/lockfree_queue.c:31,48` defines `wl_atomic_u32` per
  branch.
- `wirelog/intern.c:78,84` defines `wl_intern_atomic_u32` per branch.

These exist so struct fields can be declared portably; the audit in
§5 below covers only call sites, not type-declaration sites.

---

## 5. Atomics audit

Every `atomic_*` call site in `wirelog/` production sources. Counted
mechanically by `scripts/ci/check-threading-doc.sh`; row count must
match the script's count (currently **96**).

Format: `file:function[#N]` | field | operation | order | justification.

### 5.1 `wirelog/columnar/mem_ledger.c` — accounting (21 rows)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `mem_ledger.c:update_peak` | `*peak_atom` (subsys or global) | `atomic_load_explicit` | `relaxed` | Read-current for monotone peak-update CAS loop; no happens-before edge required |
| `mem_ledger.c:update_peak#2` | `*peak_atom` | `atomic_compare_exchange_weak_explicit` | `relaxed`/`relaxed` | Monotone high-water bump; if another thread won, retry; observed values are non-decreasing |
| `mem_ledger.c:counter_sub_clamped` | `*counter` (subsys or global) | `atomic_load_explicit` | `relaxed` | Read-current for the clamp-to-zero subtract path |
| `mem_ledger.c:counter_sub_clamped#2` | `*counter` | `atomic_compare_exchange_weak_explicit` | `relaxed`/`relaxed` | Clamp-to-zero CAS loop shared by free and set_gauge; loss-of-race retries |
| `mem_ledger.c:wl_mem_ledger_init` | `ledger->total_budget` | `atomic_store_explicit` | `relaxed` | Set-once at init; readers see the value eventually via per-counter relaxed loads |
| `mem_ledger.c:saturating_add` | `*counter` (subsys or global) | `atomic_load_explicit` | `relaxed` | Read-current before overflow-safe saturating increment |
| `mem_ledger.c:saturating_add#2` | `*counter` (subsys or global) | `atomic_compare_exchange_weak_explicit` | `relaxed`/`relaxed` | Atomic saturating increment; retry with the observed value and never wrap |
| `mem_ledger.c:wl_mem_ledger_set_gauge` | `ledger->subsys_bytes[subsys]` | `atomic_exchange_explicit` | `relaxed` | Gauge replace (Issue #1380); returns the previous value so the total can move by the difference |
| `mem_ledger.c:wl_mem_ledger_over_budget` | `ledger->total_budget` | `atomic_load_explicit` | `relaxed` | Query path; no edge required |
| `mem_ledger.c:wl_mem_ledger_over_budget#2` | `ledger->current_bytes` | `atomic_load_explicit` | `relaxed` | Query path |
| `mem_ledger.c:wl_mem_ledger_subsys_over_budget` | `ledger->total_budget` | `atomic_load_explicit` | `relaxed` | Query path |
| `mem_ledger.c:wl_mem_ledger_subsys_over_budget#2` | `ledger->subsys_bytes[subsys]` | `atomic_load_explicit` | `relaxed` | Query path |
| `mem_ledger.c:wl_mem_ledger_should_backpressure` | `ledger->total_budget` | `atomic_load_explicit` | `relaxed` | Query path |
| `mem_ledger.c:wl_mem_ledger_should_backpressure#2` | `ledger->subsys_bytes[subsys]` | `atomic_load_explicit` | `relaxed` | Query path |
| `mem_ledger.c:wl_mem_ledger_bytes_remaining` | `ledger->total_budget` | `atomic_load_explicit` | `relaxed` | Snapshot path |
| `mem_ledger.c:wl_mem_ledger_bytes_remaining#2` | `ledger->current_bytes` | `atomic_load_explicit` | `relaxed` | Snapshot path |
| `mem_ledger.c:wl_mem_ledger_snapshot` | `ledger->total_budget` | `atomic_load_explicit` | `relaxed` | Reporter/stats snapshot path (Issue #1380); `wl_mem_ledger_report` consumes the plain copy |
| `mem_ledger.c:wl_mem_ledger_snapshot#2` | `ledger->current_bytes` | `atomic_load_explicit` | `relaxed` | Snapshot path |
| `mem_ledger.c:wl_mem_ledger_snapshot#3` | `ledger->peak_bytes` | `atomic_load_explicit` | `relaxed` | Snapshot path |
| `mem_ledger.c:wl_mem_ledger_snapshot#4` | `ledger->subsys_bytes[i]` | `atomic_load_explicit` | `relaxed` | Snapshot per-subsys path |
| `mem_ledger.c:wl_mem_ledger_snapshot#5` | `ledger->subsys_peak[i]` | `atomic_load_explicit` | `relaxed` | Snapshot per-subsys path |

The ledger's design accepts **accounting skew** between
`current_bytes` and the sum of `subsys_bytes[]`; this is documented in
`mem_ledger.h:217` and is the reason every counter operation uses
`memory_order_relaxed` instead of any stronger order.

### 5.2 `wirelog/util/lockfree_queue.c` — SPSC ring buffer (4 rows)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `lockfree_queue.c:WL_ATOMIC_LOAD_RELAXED` | (macro `WL_ATOMIC_LOAD_RELAXED`) | `atomic_load_explicit` | `relaxed` | Producer reading its own `tail` cursor / consumer reading own `head` cursor (no synchronization edge needed for own cursor) |
| `lockfree_queue.c:WL_ATOMIC_LOAD_ACQUIRE` | (macro `WL_ATOMIC_LOAD_ACQUIRE`) | `atomic_load_explicit` | `acquire` | Producer reading consumer's `head` cursor (or vice versa) to determine free slots; pairs with peer's release-store and acquires every slot the peer published |
| `lockfree_queue.c:WL_ATOMIC_STORE_RELAXED` | (macro `WL_ATOMIC_STORE_RELAXED`) | `atomic_store_explicit` | `relaxed` | Producer writing its own `tail` (or consumer writing own `head`) **when no slot is being published** in the same step |
| `lockfree_queue.c:WL_ATOMIC_STORE_RELEASE` | (macro `WL_ATOMIC_STORE_RELEASE`) | `atomic_store_explicit` | `release` | Producer publishing a new slot (writes `slots[i]` then `tail`); the release ordering ensures the consumer that acquire-loads `tail` sees the slot contents |

The 64-byte padding between `tail` and `head`
(`lockfree_queue.c:69-81`) prevents false sharing; without it the
cache-line ping-pong between producer and consumer collapses
throughput by 2-10x.

### 5.3 Non-explicit atomic APIs — init and relation identity (5 rows)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `io_adapter.c:init_mutex` | `s_mutex_init_ok` | `atomic_store` | `seq_cst` (default) | One-shot mutex-init publish; the bare API used here gives sequential consistency which is the strongest order and safe for an init publisher |
| `io_adapter.c:ensure_builtins` | `s_mutex_init_ok` | `atomic_load` | `seq_cst` (default) | Pairs with the init publish; gates all later mutex operations |
| `relation.c:col_rel_new_identity` | `wl_next_relation_identity` | `atomic_load_explicit` | `relaxed` | Read the candidate identity before the non-wrapping CAS reservation loop; the counter only has to hand out distinct values, no other memory is published through it |
| `relation.c:col_rel_new_identity#2` | `wl_next_relation_identity` | `atomic_compare_exchange_weak_explicit` | `relaxed`/`relaxed` | Reserve a unique relation identity and retry with the observed value after a lost race; uniqueness comes from the RMW, not from ordering |
| `relation.c:col_rel_test_set_next_identity` | `wl_next_relation_identity` | `atomic_store_explicit` | `relaxed` | Test-only seam for selecting the terminal allocator state; production allocation is not concurrent with this reset |

These are the sites in `wirelog/` that use the **non-explicit** atomic APIs
(`atomic_load`/`atomic_store`); they default to `memory_order_seq_cst`.
The identity allocator uses the same default ordering because the CAS loop
must reserve each relation identity without reuse; the test-only store is
only used to exercise allocator exhaustion.

### 5.4 `wirelog/columnar/join.c` — keyed-join cancel/budget and typed output (19 rows)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `join.c:col_join_output_limit_reached` | `sess->join_output_shared_count` | `atomic_fetch_add_explicit` | `relaxed` | Tuple-budget accumulator across keyed-join workers; counter only |
| `join.c:col_join_keyed_count_worker_fn` | `*ctx->stop` | `atomic_load_explicit` | `relaxed` | Cancellation poll; eventual visibility is acceptable for cooperative cancel |
| `join.c:col_join_keyed_count_worker_fn#2` | `*ctx->stop` | `atomic_load_explicit` | `relaxed` | Cancellation poll |
| `join.c:col_join_keyed_count_worker_fn#3` | `*ctx->shared_count` | `atomic_fetch_add_explicit` | `relaxed` | Cross-worker counter increment |
| `join.c:col_join_keyed_count_worker_fn#4` | `*ctx->stop` | `atomic_store_explicit` | `relaxed` | Cooperative cancel flag — best-effort signal, **not a synchronization point**; readers may observe the previous value for a bounded period |
| `join.c:col_join_keyed_count_worker_fn#5` | `*ctx->shared_count` | `atomic_fetch_add_explicit` | `relaxed` | Cross-worker counter increment |
| `join.c:col_join_keyed_count_worker_fn#6` | `*ctx->stop` | `atomic_store_explicit` | `relaxed` | Cooperative cancel flag (see the preceding cancellation note) |
| `join.c:wl_columnar_join_diff_op` | `stop` (local) | `atomic_load_explicit` | `relaxed` | Compaction-loop cancel poll |
| `join.c:wl_columnar_join_diff_op#2` | `ledger->total_budget` | `atomic_load_explicit` | `relaxed` | Backpressure poll; advisory, no edge required |
| `join.c:wl_columnar_join_diff_op#3` | `ledger->current_bytes` | `atomic_load_explicit` | `relaxed` | Backpressure poll; advisory, no edge required |
| `join.c:col_semijoin_fill_worker_fn` | `*ctx->write_error` | `atomic_load_explicit` | `relaxed` | Cooperative fill cancellation after another worker reports a typed write failure |
| `join.c:col_semijoin_fill_worker_fn#2` | `*ctx->write_error` | `atomic_store_explicit` | `relaxed` | Publish the first typed output-write failure to sibling workers and the coordinator |
| `join.c:col_semijoin_fill_worker_fn#3` | `*ctx->write_error` | `atomic_store_explicit` | `relaxed` | Publish a failure from a later projected column in the same output row |
| `join.c:col_join_cross_fill_worker_fn` | `*ctx->write_error` | `atomic_load_explicit` | `relaxed` | Stop cooperative cross-product filling after a sibling reports a typed write failure |
| `join.c:col_join_cross_fill_worker_fn#2` | `*ctx->write_error` | `atomic_store_explicit` | `relaxed` | Publish a typed output-write failure to sibling workers and the coordinator |
| `join.c:col_join_cross_fill_worker_fn#3` | `*ctx->write_error` | `atomic_store_explicit` | `relaxed` | Publish a failure from a later left-side output column |
| `join.c:col_join_cross_fill_worker_fn#4` | `*ctx->write_error` | `atomic_store_explicit` | `relaxed` | Publish a failure from a right-side output column |
| `join.c:col_join_parallel_cross` | `write_error` | `atomic_load_explicit` | `relaxed` | Coordinator observes worker write status after the workqueue barrier |
| `join.c:wl_columnar_semijoin_op` | `write_error` | `atomic_load_explicit` | `relaxed` | Coordinator observes typed semijoin fill status after the workqueue barrier |

### 5.5 `wirelog/columnar/kfusion.c` — K-fusion shared counter (1 row)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `kfusion.c:col_op_k_fusion_dispatch` | `shared_join_count` | `atomic_store_explicit` | `relaxed` | Issue #959: zeroed before branch tasks are submitted, so the happens-before edge comes from task submission itself -- same argument as `eval.c:399`, which resets the TDD counter before `thread_create()` |

The `stop` flag's `memory_order_relaxed` store is deliberate:
cancellation is **cooperative**, not preemptive. A worker may observe
the previous value for up to one cache-line propagation interval; that
is acceptable because each worker re-polls before every tuple batch
and the wasted work is bounded.

### 5.6 `wirelog/columnar/eval.c` — shared counter (1 row)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `eval.c:wl_columnar_eval_nonrec_relation_parallel` | `shared_join_count` | `atomic_store_explicit` | `relaxed` | Reset before workers spawn; happens-before edge is provided by `thread_create()` itself |

### 5.7 `wirelog/columnar/session.c` — worker budget snapshot (1 row)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `session.c:col_worker_session_create` | per-worker view of `ledger->total_budget` | `atomic_load_explicit` | `relaxed` | Worker session reads coordinator's budget snapshot; advisory, no edge required |

### 5.8 `wirelog/columnar/memory_governor.c` — reservation state (37 rows)

The governor uses one atomic counter for the shared reservation limit and
token state transitions. The CAS admission loop is overflow-safe and a token
reaches released exactly once; owner identity is stored as an atomic pointer
representation so transfer cannot race a release with a data race.
Downsize, release, and transfer claim the token before accessing mutable payload;
contending operations return false when the token is busy. Accounting completes
before publishing the updated committed or released state. Move, reinitialization,
and direct non-atomic payload reads require external synchronization.

| Anchor (file:function[#N]) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `memory_governor.c:wl_columnar_memory_governor_init` | `usable_bytes` | `atomic_store_explicit` | `relaxed` | Set-once before the governor is published |
| `memory_governor.c:wl_columnar_memory_governor_init#2` | `reserved_bytes` | `atomic_store_explicit` | `relaxed` | Set-once before reservations are possible |
| `memory_governor.c:wl_columnar_memory_reservation_init` | `reservation->state` | `atomic_store_explicit` | `relaxed` | Publish the empty state before a caller can reserve or reuse the token |
| `memory_governor.c:wl_columnar_memory_governor_ref_create` | `references` | `atomic_store_explicit` | `relaxed` | Publish the coordinator's initial ownership reference |
| `memory_governor.c:wl_columnar_memory_governor_ref_retain` | `references` | `atomic_fetch_add_explicit` | `relaxed` | Retain the shared governor for a worker |
| `memory_governor.c:wl_columnar_memory_governor_ref_release` | `references` | `atomic_fetch_sub_explicit` | `release` | Release one coordinator or worker ownership reference |
| `memory_governor.c:wl_columnar_memory_governor_ref_release#2` | `references` | `atomic_load_explicit` | `acquire` | Synchronize final reference destruction |
| `memory_governor.c:wl_columnar_memory_governor_ref_is_sole` | `references` | `atomic_load_explicit` | `acquire` | Observe that only the intern table still holds the governor before rebinding it |
| `memory_governor.c:reserve_internal` | `reservation->owner_bits` | `atomic_store_explicit` | `relaxed` | Initialize the caller identity before admission |
| `memory_governor.c:reserve_internal#2` | `reserved_bytes` | `atomic_load_explicit` | `relaxed` | Read current shared admission total for the CAS loop |
| `memory_governor.c:reserve_internal#3` | `usable_bytes` | `atomic_load_explicit` | `relaxed` | Read the immutable ordinary-admission limit |
| `memory_governor.c:reserve_internal#4` | `reserved_bytes` | `atomic_compare_exchange_weak_explicit` | `acquire`/`relaxed` | Linearize the overflow verdict without wrapping the shared counter |
| `memory_governor.c:reserve_internal#5` | `reservation->state` | `atomic_store_explicit` | `release` | Abandon a token after an overflow verdict |
| `memory_governor.c:reserve_internal#6` | `reservation->state` | `atomic_store_explicit` | `release` | Abandon a token that exceeded the usable limit |
| `memory_governor.c:reserve_internal#7` | `reserved_bytes` | `atomic_compare_exchange_weak_explicit` | `relaxed`/`relaxed` | Atomically admit without exceeding the shared limit |
| `memory_governor.c:reserve_internal#8` | `reservation->state` | `atomic_store_explicit` | `release` | Publish the fully initialized reserved token |
| `memory_governor.c:transition_reservation` | `reservation->state` | `atomic_compare_exchange_weak_explicit` | `acq_rel`/`acquire` | Acquire prior payload publication while enforcing a legal state transition and retrying spurious failure |
| `memory_governor.c:claim_reservation_state` | `reservation->state` | `atomic_compare_exchange_weak_explicit` | `acq_rel`/`acquire` | Acquire exclusive access to token payload before downsize, release, or transfer |
| `memory_governor.c:wl_columnar_memory_commit` | `reservation->owner_bits` | `atomic_store_explicit` | `release` | Publish committed ownership before the commit state |
| `memory_governor.c:wl_columnar_memory_commit#2` | `reservation->state` | `atomic_store_explicit` | `release` | Publish the committed state after owner initialization |
| `memory_governor.c:wl_columnar_memory_transfer` | `reservation->state` | `atomic_load_explicit` | `acquire` | Read the active state before claiming transfer |
| `memory_governor.c:wl_columnar_memory_transfer#2` | `reservation->owner_bits` | `atomic_store_explicit` | `release` | Publish the new logical owner |
| `memory_governor.c:wl_columnar_memory_transfer#3` | `reservation->state` | `atomic_store_explicit` | `release` | Restore the active state after owner transfer |
| `memory_governor.c:credit_reservation` | `reserved_bytes` | `atomic_load_explicit` | `relaxed` | Read current total while holding the token claim |
| `memory_governor.c:credit_reservation#2` | `reserved_bytes` | `atomic_compare_exchange_weak_explicit` | `release`/`relaxed` | Return the claimed token's capacity or shrink delta without underflow |
| `memory_governor.c:wl_columnar_memory_reservation_downsize` | `reservation->state` | `atomic_store_explicit` | `release` | Publish updated bytes after crediting the delta, or restore committed state on failure |
| `memory_governor.c:finish_reservation` | `reservation->state` | `atomic_load_explicit` | `acquire` | Observe an eligible stable state before claiming release; reject a busy token |
| `memory_governor.c:finish_reservation#2` | `reservation->state` | `atomic_store_explicit` | `release` | Publish released state only after accounting, or restore the prior state on failure |
| `memory_governor.c:wl_columnar_memory_reserve_growth` | `reservation->state` | `atomic_load_explicit` | `acquire` | Validate the source token before creating a distinct growth reservation |
| `memory_governor.c:wl_columnar_memory_reserved` | `reserved_bytes` | `atomic_load_explicit` | `acquire` | Read a coherent observable reservation total |
| `memory_governor.c:wl_columnar_memory_reservation_move` | `destination->state` | `atomic_load_explicit` | `acquire` | Validate the destination token before moving ownership |
| `memory_governor.c:wl_columnar_memory_reservation_move#2` | `source->state` | `atomic_load_explicit` | `acquire` | Validate the source token before transferring ownership |
| `memory_governor.c:wl_columnar_memory_reservation_move#3` | `destination->owner_bits` | `atomic_store_explicit` | `relaxed` | Copy the logical owner into the destination token before publishing its state |
| `memory_governor.c:wl_columnar_memory_reservation_move#4` | `source->owner_bits` | `atomic_load_explicit` | `relaxed` | Read the source owner while transferring the token |
| `memory_governor.c:wl_columnar_memory_reservation_move#5` | `destination->state` | `atomic_store_explicit` | `release` | Publish the moved reservation after its owner and governor fields are initialized |
| `memory_governor.c:wl_columnar_memory_reservation_move#6` | `source->owner_bits` | `atomic_store_explicit` | `relaxed` | Clear the source owner after ownership has moved |
| `memory_governor.c:wl_columnar_memory_reservation_move#7` | `source->state` | `atomic_store_explicit` | `release` | Publish the empty source state after clearing its ownership |

### 5.9 `wirelog/arena/compound_arena.c` — mutation gate (5 rows)

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `compound_arena.c:compound_gate_load` | `arena->access_gate` | `atomic_load_explicit` | `acquire` | Observe the writer/readers gate before acquiring a read lease or validating mutation teardown |
| `compound_arena.c:compound_gate_store` | `arena->access_gate` | `atomic_store_explicit` | `release` | Initialize the gate before publication; on portable builds this helper is also the release-side gate store |
| `compound_arena.c:wl_compound_arena_borrow` | `arena->access_gate` | `atomic_compare_exchange_weak_explicit` | `acquire`/`relaxed` | Admit one reader only while no writer owns the gate |
| `compound_arena.c:wl_compound_arena_borrow_release` | `arena->access_gate` | `atomic_compare_exchange_weak_explicit` | `release`/`relaxed` | Remove exactly one reader lease without reopening a writer-owned gate |
| `compound_arena.c:wl_compound_arena_mutation_begin` | `arena->access_gate` | `atomic_compare_exchange_weak_explicit` | `acquire`/`relaxed` | Claim exclusive mutation ownership after all reader leases have drained |

On MSVC the helper load/store use interlocked intrinsics because the shared
`wl_atomic_u64` compatibility type cannot use C11 atomic operations directly.

### 5.10 `wirelog/intern.c` — shared symbol table (3 rows)

The intern table is shared, unsynchronized, by every parallel worker
(Issue #958). Writers (`wl_intern_put`, `wl_intern_get`) serialize on
`intern->lock`; `wl_intern_reverse` and `wl_intern_count` are lock-free
because reverse lookup runs once or twice per row in the string
operations and a lock there costs more than the parallelism is worth.
The three sites are the macro bodies; the call sites they serve are
named in the justification.

| Anchor (`file:function[#N]`) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `intern.c:WL_INTERN_LOAD_ACQUIRE` | `intern->count` | `atomic_load_explicit` | `acquire` | `WL_INTERN_LOAD_ACQUIRE`, used by `wl_intern_reverse`/`wl_intern_count`. Pairs with the release store below: an id below the observed count names a fully written entry, and the segment holding it is allocated and never moves |
| `intern.c:WL_INTERN_LOAD_RELAXED` | `intern->count` | `atomic_load_explicit` | `relaxed` | `WL_INTERN_LOAD_RELAXED`, used by `wl_intern_put`, `intern_resize_prepare`, `intern_retained_bytes_locked` and `wl_intern_free`. Every caller holds `intern->lock` (`wl_intern_free` takes it to release the governor reservation, Issue #1431), so no edge is needed |
| `intern.c:WL_INTERN_STORE_RELEASE` | `intern->count` | `atomic_store_explicit` | `release` | `WL_INTERN_STORE_RELEASE`, used by `wl_intern_put` after the string and its segment pointer are written. Publishing the count first would let a lock-free reader dereference an unwritten slot |

### 5.11 Existing inventory total

21 + 4 + 2 + 3 + 19 + 1 + 1 + 1 + 3 + 36 + 5 = **96 atomic call sites**
before the inactive source-access contract below.

### 5.12 `wirelog/columnar/source_access.h` — inactive source gate (8 rows)

This header-only gate is a testable internal contract and is not linked into
the production library. Its state is zero-initialized; reader and writer
tokens are caller-owned, address-bound and thread-confined. Acquire operations
use acquire semantics before consuming protected state; release operations use
release semantics after protected payload publication. The containing owner
must outlive all active tokens. `EBUSY`, `EINVAL` and `EOVERFLOW` leave the
gate and token state unchanged.

| Anchor (file:function[#N]) | Field | Op | Order | Justification |
|---|---|---|---|---|
| `source_access.h:wl_columnar_source_access_gate_init` | `gate->state` | `atomic_store_explicit` | relaxed | Establish inactive zero state before publication |
| `source_access.h:wl_columnar_source_access_reader_acquire` | `gate->state` | `atomic_load_explicit` | acquire | Observe writer release before admitting a reader |
| `source_access.h:wl_columnar_source_access_reader_acquire#2` | `gate->state` | `atomic_compare_exchange_weak_explicit` | acquire/relaxed | Linearize reader admission without overflowing the writer sentinel |
| `source_access.h:wl_columnar_source_access_reader_release` | `gate->state` | `atomic_load_explicit` | acquire | Observe the active gate before releasing this reader |
| `source_access.h:wl_columnar_source_access_reader_release#2` | `gate->state` | `atomic_compare_exchange_weak_explicit` | release/relaxed | Publish reader payload completion and decrement atomically |
| `source_access.h:wl_columnar_source_access_writer_acquire` | `gate->state` | `atomic_compare_exchange_weak_explicit` | acquire/relaxed | Linearize exclusive writer admission and retry spurious failure |
| `source_access.h:wl_columnar_source_access_writer_release` | `gate->state` | `atomic_load_explicit` | acquire | Validate the writer state before terminal publication |
| `source_access.h:wl_columnar_source_access_writer_release#2` | `gate->state` | `atomic_compare_exchange_weak_explicit` | release/relaxed | Publish writer payload completion and retry spurious failure |

21 + 4 + 2 + 3 + 19 + 1 + 1 + 1 + 3 + 36 + 5 + 8 = **104 atomic call sites**.

The `#N` suffix counts all atomic sites in a symbol, regardless of operation;
the first site remains unsuffixed. `scripts/ci/check-threading-doc.sh` uses
`scripts/ci/threading_doc_anchors.py` to discover the same source sites,
ignoring comments, strings, and function-pointer declarations. Each audit
row resolves to a unique `file:function[#N]` or macro anchor, checks the
documented operation, and is compared against the complete source inventory.
A mismatch or stale anchor fails `meson test --suite abi:threading_doc`.

---

## 6. Lock-free SPSC delta queue

The delta-message ring buffer at `wirelog/util/lockfree_queue.{c,h}`
is a per-(producer,consumer)-pair Single-Producer Single-Consumer
ring with the classic two-cursor design (`tail` for producer,
`head` for consumer). Each cursor lives on its own 64-byte cache
line (`lockfree_queue.c:69-81`).

### Ordering contract

- The producer's `WL_ATOMIC_STORE_RELEASE(&q->tail, new_tail)` at
  enqueue publishes both the slot contents and the new tail.
- The consumer's `WL_ATOMIC_LOAD_ACQUIRE(&q->tail)` at dequeue
  acquires every byte the producer published before its release.
- The producer's reads of `q->head` use `acquire` to know how many
  slots are free; the consumer's reads of `q->head` (its own
  cursor) use `relaxed`.

Symmetric for the consumer's `head` cursor.

### W-way fan-in

For `W` workers feeding one coordinator, the layer above this SPSC
queue (`wirelog/columnar/eval.c` worker loop) instantiates `W`
SPSC queues, one per worker. The coordinator round-robins
dequeues across all `W` tails. This is mechanically MPSC but the
implementation is `W` independent SPSCs — never an MPMC.

### W=512 stress

The 64-byte padding matters at scale: with 512 workers, every
coordinator dequeue scans 512 tails. Without padding the tails
share cache lines with their corresponding heads, and the
cache-line ping-pong cost dominates throughput. The padding cost
is `3 * 64 = 192` bytes per ring; benefit is 2-10x throughput.

---

## 7. K-fusion parallel dispatch threshold

K-fusion is the columnar evaluator's strategy for evaluating a body
of K rule atoms in parallel branches. Two thresholds gate it; they
are **not** the same threshold and conflating them is a known
documentation hazard.

### 7.1 Plan-emission threshold (K ≥ 2)

The optimizer's plan generator at `wirelog/exec_plan_gen.c` emits a
`WL_PLAN_OP_K_FUSION` operator when an IDB body has K ≥ 2 atoms
that can be fused. Below K = 2 there is nothing to fuse; the planner
emits a non-fused chain.

### 7.2 Parallel-runtime threshold (K ≥ 4)

`wirelog/columnar/kfusion.c` defines:

```
#define WL_KFUSION_MIN_PARALLEL_K 4
```

At the dispatch site, the runtime picks between
parallel and serial K-fusion:

```c
uint32_t active_workers = live_count < sess->num_workers
    ? live_count : sess->num_workers;
wl_work_queue_t *wq = NULL;
if (active_workers > 1
    && (live_count >= WL_KFUSION_MIN_PARALLEL_K || adaptive_parallel)) {
    /* ... ensure workqueue, dispatch parallel branches ... */
    wq = sess->wq;
}
if (!wq || (live_count < WL_KFUSION_MIN_PARALLEL_K
        && !adaptive_parallel)) {
    return col_op_k_fusion_serial(op, stack, sess);
}
```

So:

- **K = 2 or K = 3**: plan emits `WL_PLAN_OP_K_FUSION`, runtime starts on
  the **serial** branch and may enable parallel dispatch after adaptive
  evidence (`col_op_k_fusion_serial`).
- **K ≥ 4 and `num_workers > 1`**: runtime allocates per-worker
  sessions and dispatches branches in parallel.

### 7.3 Calibration rationale

The serial-vs-parallel cutover at K = 4 was set empirically. Below
that threshold the workqueue dispatch overhead and the per-worker
session allocation (`COL_SESSION(sess)->kfusion_alloc_ns` profiled
at `kfusion.c:633`) exceed the saved tuple work for the CRDT and
DDISASM workloads measured in the v0.40 perf gate
(`tests/test_crdt_perf_gate.c`, `bench/bench_flowlog.c`). The
    `kfusion.c` comment block records the measurement summary:
**DDISASM K = 3 is 14% slower with 8-worker parallel than serial.**

Cross-reference: #731 (CRDT median-time perf gate landing) for the
empirical evidence anchoring K = 4.

### 7.4 Workload-adaptive low-K dispatch

K=2 and K=3 use a session-local conservative policy in
`wirelog/columnar/kfusion_adaptive.c`. Each K-fusion operator starts in
the serial path. The policy records the complete invocation duration,
including branch evaluation, workqueue wait, merge, cleanup, and result
stack effects. After three valid serial observations it permits parallel
probe invocations; three valid parallel observations must beat the serial
EWMA by at least 5% before the policy enters the parallel state.

The policy has three states: `UNKNOWN`, `SERIAL`, and `PARALLEL`. A
cooldown provides hysteresis after transitions, and invalid timing,
dispatch errors, insufficient observations, or ambiguous results return
to the serial fallback. A change in operator, worker count, or relation
size class creates a fresh record, so stale measurements are not reused.

The adaptive records belong only to the coordinator session. K-fusion
worker session copies set the policy pointer to `NULL`; workers never
update or free coordinator state. Session snapshot profiling preserves
the learned records, while session destruction releases them.

The fixed `K >= 4` threshold and its parallel path are unchanged. The
adaptive policy does not change planner metadata, public ABI, merge logic,
or worker cleanup semantics. Deterministic policy tests use synthetic
observations rather than wall-clock sleeps; workload-level calibration
remains a pinned-runner measurement task.

---

## 8. Compound-arena epoch boundary contract

The compound arena (`wirelog/arena/compound_arena.{h,c}`) is a
generational allocator backing variable-width tuple values across
iterations. Its epoch boundary fires at the end of each evaluation
sub-pass and reclaims the previous epoch's storage.

### 8.1 Coordinator-only invariant

Worker sessions **borrow** the coordinator's compound arena and its
fixed-and-growth admission reservations. Workers are forbidden to advance the
arena's epoch counter or to free its memory; only the coordinator may do so.
The coordinator destroys the arena before releasing its shared
memory-governor reference, so the reservation callback always observes a
live governor. The arena's callback context must not be copied or released
by a worker. The live invariant
that enforces this is the `sess->coordinator == NULL` predicate:

Compound payload and entry-array growth must occur in the coordinator's
single-mutator window, before workers borrow the arena or after all workers
have been destroyed. A worker-held lookup pointer must not span a coordinator
growth replacement. Enforcement and deterministic fault-injection coverage
for this window are tracked separately in #1423.

- `wirelog/columnar/kfusion.c:561-569` (K-fusion path):
  ```c
  if (sess->coordinator == NULL
      && sess->compound_arena && sess->rotation_ops
      && sess->rotation_ops->gc_epoch_boundary) {
      sess->rotation_ops->gc_epoch_boundary(sess);
  }
  ```
  The `sess->compound_arena` conjunct is the defensive NULL-pointer
  guard noted in the surrounding `kfusion.c:555-565` comment block;
  the `coordinator == NULL` conjunct is the load-bearing
  coordinator-only invariant.
- `wirelog/columnar/eval_serial.c:788-791` (eval-stratum sub-pass tail): the
  same gate.

The rotation strategy itself does **not** re-check the predicate
(`wirelog/columnar/rotation_standard.c:28-32`,
`rotation_pinned.c:50-56` both unconditionally call
`wl_compound_arena_gc_epoch_boundary`); the caller is responsible
for gating.

### 8.2 Provenance of the gate

The gate was added in `61e081b` (`fix(#579): skip compound arena GC
dispatch in worker context`). Before that fix, ThreadSanitizer
caught a race on `arena->current_epoch` once worker sessions started
borrowing the coordinator's arena under issue #579's compound-arena
sharing refactor.

The regression test pinning the invariant is
`tests/test_worker_arena_borrow.c::test_worker_skips_gc_epoch_boundary_dispatch`.

### 8.3 Freeze guard is NOT yet wired

The compound arena ships a freeze API
(`wl_compound_arena_freeze` / `wl_compound_arena_unfreeze` at
`wirelog/arena/compound_arena.h:254-299`) intended as a long-term
backstop: a frozen arena rejects allocator mutations entirely. As of
v0.40.99 this guard is **not wired around K-fusion or TDD dispatch**.
The `coordinator == NULL` gate, not freeze, is the live invariant.

Refactors that touch the gate must not assume freeze covers them.

### 8.4 TDD parallelism reuses the gate

The TDD (Tuple-at-a-time Differential Dataflow) worker model
launches via `tdd_init_workers_hybrid` / `tdd_exchange_deltas` /
`tdd_bdx_exchange_deltas` (referenced from
`wirelog/exec_plan_gen.c:2086,2090,2225`). TDD workers carry the
same `sess->coordinator != NULL` marker and are therefore
automatically excluded from GC-epoch dispatch by the same gate
above. No separate gate exists for TDD.

---

## 9. Signal-safety

wirelog is **not async-signal-safe**.

- `WL_LOG` (the structured logger declared in `wirelog/util/log.h`)
  is explicitly NOT async-signal-safe; see `docs/ERROR_MODEL.md`
  and the internal comment block in `wirelog/util/log.h`.
- After `fork()`, if the child changes the log sink, the child
  must call `wl_log_init()` again.
- The `mem_ledger`, the SPSC delta queue, the columnar evaluator,
  and the I/O adapter framework all use mutexes and/or allocate
  memory. Mutexes are not async-signal-safe; `malloc` is not
  async-signal-safe. Do not call `wirelog_*` from a signal handler.

If a host process needs to interrupt a long-running
`wirelog_session_step`, the supported pattern is to set the
`wirelog_session_*` cancellation flag from the signal-handling
thread (not from inside the handler itself).

Cross-reference: Risk C6, issue #709 (consolidated in
`docs/ERROR_MODEL.md`).

---

## 10. Fork-safety

wirelog is **not fork-safe**. Specifically, calling `fork()` from a
process that has already initialized any `wirelog_*` state and then
continuing in the child without `exec*()` is **undefined behavior**.

This section documents the contract for embedders that run under
fork-then-exec hosts (CGI, `subprocess.Popen`, classic `system(3)`)
versus fork-without-exec hosts (uWSGI prefork worker model, Apache
`mod_wsgi` prefork, gunicorn `sync` workers, classic `fork()`
daemon-supervisor patterns).

### 10.1 The fork hazard for the I/O adapter registry

The I/O adapter registry at `wirelog/io/io_adapter.c` is a
**process-global, mutex-protected** singleton. The mutex is published
exactly once via `call_once(&s_mutex_once, init_mutex)` (POSIX) or
the equivalent `InterlockedCompareExchange` ladder (Windows), gated
by the `s_mutex_init_ok` atomic from §5.3.

After a `fork()`:

- The child inherits the *value* of `s_mutex_once`, `s_mutex_init_ok`,
  and the mutex bytes themselves.
- If a *different* thread in the parent held `s_mutex` at the moment
  of `fork()`, the child inherits a mutex that is **locked by a
  non-existent thread**. The very next call to
  `wirelog_io_register_adapter()`, `wirelog_io_unregister_adapter()`,
  or `wirelog_io_find_adapter()` in the child will deadlock (POSIX) or
  return an undefined error (Windows).
- Even if no thread held `s_mutex` at fork time, the registry's table
  entries (`scheme` strings copied into the fixed-size buffer, the
  adapter pointer slots) reference memory owned by the parent's
  address-space snapshot. Pointers remain valid in the child until
  the child mutates them, but any host-supplied
  `wirelog_io_adapter_t *` is only guaranteed to remain valid for the
  parent's lifetime.

The CHANGELOG line "thread-safe (process-global mutex)" describes
thread-safety **within a single process image**. It is not a
fork-after-thread contract.

### 10.2 Supported child-process patterns

A child process MUST follow one of these patterns:

1. **fork-then-exec** (recommended). The child calls `execve()` (or
   `execvp`, `posix_spawn`, etc.) before invoking any `wirelog_*`
   entrypoint. After `exec`, the child has a fresh address space and
   all wirelog state is reinitialized from scratch. This is the
   normal `subprocess`-style pattern and is fully supported.
2. **fork-without-exec, no wirelog use in child**. The child uses
   `fork()` to spawn a worker process but never calls any
   `wirelog_*` API. Only the parent retains the wirelog state. This
   is also fully supported.
3. **fork-without-exec, wirelog re-initialized in child**. The child
   treats wirelog as uninitialized after the `fork()` return point
   and re-builds its own state from scratch:
   - The child MUST NOT call any registry function before
     re-initializing.
   - The child MAY register fresh adapters via
     `wirelog_io_register_adapter()` *only if* a process-wide
     `pthread_atfork()` child handler has reset
     `s_mutex_once`, `s_mutex_init_ok`, and re-initialized
     `s_mutex` from the child side. wirelog does **not** install
     such a handler internally: the host must install it, or the
     host must accept that the registry is unusable in the child.
   - Any session, program, easy, or backend objects created in the
     parent (`wirelog_session_*`, `wirelog_easy_*`,
     `wirelog_program_*`, etc.) MUST NOT be reused in the child.
     They are not transferable across the fork boundary; the only
     safe operation is to discard them and rebuild.

The recommended posture for new embedders is pattern (1) or (2).
Pattern (3) is feasible but requires host-side `pthread_atfork`
plumbing and is not validated by the wirelog test suite.

### 10.3 Implications for uWSGI / mod_wsgi / gunicorn

These hosts are the canonical fork-without-exec deployment shape and
deserve an explicit note:

- **uWSGI** with `processes > 1`: each worker is a `fork()` child of
  the master. If the wirelog import (Python binding load, `dlopen` of
  `libwirelog.so.1`, or a Path-A C extension) happens before
  `lazy-apps` (i.e. in the master), every worker inherits the
  parent's registry mutex. The supported configuration is
  `lazy-apps = true` (uWSGI option) so each worker imports
  wirelog in its own post-fork address space.
- **mod_wsgi** prefork MPM: same hazard, same mitigation. Use a
  daemon-mode MPM (`worker` or `event`) or ensure the wirelog import
  happens lazily in the request thread, not at module load.
- **gunicorn sync workers**: same hazard. The supported
  configurations are `--preload=False` (default) or workers that
  invoke `exec`-style spawning (`--worker-class=gevent` with
  `--preload=False`).

The shared principle: **wirelog must be initialized in the same
process that will use it.** Crossing a `fork()` boundary without
`exec*()` is unsupported.

### 10.4 `WL_LOG` and the structured logger

The `wirelog/util/log.h` structured logger has its own fork-safety
note: after `fork()`, if the child changes the log sink, the child
must call `wl_log_init()` again. This is documented in §9 (signal-
safety) and `docs/ERROR_MODEL.md`. It is reproduced here for
completeness: the logger fits the same fork-without-exec hazard
pattern as the I/O adapter registry.

### 10.5 What about `pthread_atfork`?

Installing `pthread_atfork()` handlers from inside `libwirelog.so.1`
was considered and rejected for v1.0:

- It would only fix POSIX hosts. Windows has no `fork()`; uWSGI on
  Windows uses different worker semantics.
- It would interact unpredictably with host-installed `atfork`
  handlers in larger Python/Java stacks.
- The clean child-side reset of the registry mutex requires
  invalidating any in-flight `wirelog_io_*` call that was holding
  the mutex at fork time -- there is no async-safe way to abort a
  mutex holder mid-operation. The host's `atfork` prepare handler
  would still need to acquire the mutex before fork, which is the
  shape every host is already expected to provide.

The 1.0 contract is therefore the explicit set of supported patterns
in §10.2 above, with the explicit non-goal that `libwirelog.so.1`
itself does not register `pthread_atfork()` handlers.

Cross-reference: Risk C13, issue #716 (this section).

---

## 11. Appendix: ThreadSanitizer configuration

To run wirelog under TSan the host must build with
`-Dthreads=posix`:

```
meson setup build-tsan \
    -Db_sanitize=thread \
    -Dthreads=posix \
    -Doptimization=1 \
    -Dwirelog_log_max_level=error
meson test -C build-tsan --suite tsan
```

The `-Dthreads=posix` requirement is non-negotiable for race gating.
Issue #826 showed that the Linux glibc C11 `<threads.h>` backend is
not a reliable TSan runtime signal even though glibc implements the C11
surface over NPTL.  Executing instrumented workers created through
`thrd_create` can crash the sanitizer runtime itself with
`ThreadSanitizer:DEADLYSIGNAL` / SEGV `0x18` before wirelog
synchronization is meaningfully diagnosed.  Suppressions do not apply
to that fatal sanitizer crash.

The posix backend is therefore the only canonical TSan race surface.
The `tsan-native` CI leg under `.github/workflows/ci-pr.yml`
configures and compiles with `-Dthreads=native` plus
`-Db_sanitize=thread`, then runs only a policy/documentation smoke. It
does not execute the full runtime test suite under native C11 TSan.
Runtime coverage for the native C11 backend comes from the ordinary
non-TSan matrix.

### TSan coverage matrix

| Backend | Platform | TSan status | Notes |
|---------|----------|-------------|-------|
| `posix` | all | **gating** | libtsan intercepts pthread_* directly; canonical race surface |
| `native` | Linux glibc | compile-only advisory | CI configures and compiles with TSan, but does not run native C11 worker tests because issue #826 shows sanitizer-runtime SEGVs before race diagnostics |
| `native` | musl | not covered | musl C11 shim does not route through NPTL pthread symbols |
| `native` | Apple libc | not covered | Apple libc lacks `<threads.h>`; C11 backend not buildable (see meson.build:42) |
| `native` | Windows MSVC | not covered | no libtsan; Win32 threads only |

The compile-only native leg exists to keep `wirelog/thread_c11.c`
buildable under sanitizer flags. It is not evidence that libtsan has
observed C11-thread happens-before edges. On all platforms the posix
backend remains the only reliable TSan option.

Cross-references: Risk C5, issue #708 (-Dthreads=native + TSan
advisory leg); issue #826 (native TSan SEGV triage);
`.github/workflows/ci-pr.yml` `tsan-native` job.

---

## 11. Compound-arena read leases and mutation windows (#1423)

`wl_compound_arena_t` has one coordinator mutation gate. A worker must hold a
`wl_compound_arena_borrow_t` while using a lookup result; copying or releasing
the lease twice is rejected. The coordinator cannot allocate, retain, freeze,
unfreeze, advance the epoch, or destroy the arena while any read lease is
active. Coordinator mutation must be completed before worker leases are
opened, and workers must release their leases before coordinator teardown.

This gate protects pointer lifetime across both append-only metadata changes
and replacement-buffer growth. `frozen` remains a semantic read-only guard,
not a synchronization primitive. The unmanaged compound-arena constructor
and standalone fuzz target do not link the memory governor.

## 12. References

- `wirelog/thread.h:1-130` — backend precedence and type definitions.
- `wirelog/meson.build:40-72` — `cc.links()` C11-threads detection.
- `meson_options.txt:14-19` — `threads` build option (`native` |
  `posix`).
- `wirelog/columnar/kfusion.c:10` — `WL_KFUSION_MIN_PARALLEL_K` plus
  the rationale comment block citing the DDISASM K = 3 measurement.
- `wirelog/columnar/kfusion.c:609-623` — K-fusion parallel-dispatch
  gate.
- `wirelog/columnar/kfusion.c:561-569` — compound-arena gate (K-fusion
  side).
- `wirelog/columnar/eval_serial.c:788-791` — compound-arena gate (eval-
  stratum side).
- `wirelog/columnar/mem_ledger.{h,c}` — accounting atomics + MSVC
  shim.
- `wirelog/util/lockfree_queue.{h,c}` — SPSC ring buffer + MSVC
  shim.
- `wirelog/arena/compound_arena.h:135-299` — compound-arena type,
  epoch boundary, freeze API.
- `wirelog/columnar/rotation_standard.c:28-32`,
  `rotation_pinned.c:50-56` — rotation strategy epoch-boundary
  callbacks.
- `wirelog/util/log.h` — internal WL_LOG safety summary.
- `docs/ERROR_MODEL.md` — canonical WL_LOG signal/fork safety policy.
- Issue #681 — v0.41 ABI Infrastructure epic.
- Issue #734 — this document.
- Issue #708 — TSan + `-Dthreads=posix` cross-link.
- Issue #716 — io_adapter fork-safety (§10).
- Issue #709 — error-model and WL_LOG safety documentation.
- Issue #731 — CRDT median-time perf gate (empirical anchor for
  K = 4 parallel threshold).
- Commit `61e081b` — `fix(#579)`: skip compound-arena GC dispatch
  in worker context (the live gate invariant).
- Test pin: `tests/test_worker_arena_borrow.c`.

## 13. Primary arrangement lease contract (#1384)

The primary arrangement cache is not independently thread-safe. Within an
operator's session, a keyed join acquires a `col_arrangement_pin_t` before
using the arrangement and releases it after the probe completes. Eviction and
relation invalidation inspect the lease count: eviction skips pinned entries,
and invalidation is deferred until the final release. This keeps the
arrangement buffers and their embedded cache-entry identity stable for the
borrower's lifetime. The flat registry refuses `realloc()` while any lease is
active; callers that cannot obtain a new cache slot must use their ephemeral
fallback until the lease is released.

The lease is internal and must not be copied or released twice. It does not
cover filtered/differential/sorted/materialization caches or relation
generation checks; those extensions are tracked by issue #1435. No claim of
general multi-threaded access is made by this contract.
