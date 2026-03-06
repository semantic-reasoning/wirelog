# Delta Query Support - Implementation Plan

**Date**: 2026-03-03
**Scope**: Incremental (delta) query support for wirelog
**Complexity**: HIGH
**Phases**: 3 (Backend Abstraction, DD Delta, nanoarrow Backend)

---

## Context

wirelog is a C11 Datalog engine that uses Rust Differential Dataflow (DD) via FFI.
Currently all execution is **one-shot**: parse, build DD dataflow, run to completion,
discard all state. There is no way to update EDB facts and recompute only the
changed IDB tuples.

**Current execution flow** (`driver.c` lines 132-219):
```
parse -> optimize -> dd_plan_generate -> dd_marshal_plan -> worker_create
-> load_edb -> execute_cb -> destroy everything
```

**Key constraint from CLAUDE.md**: "Minimize Rust changes" -- C11-first philosophy.

**Current Rust architecture** (`dataflow.rs`):
- `execute_plan()` calls `timely::execute()` with `new_collection_from()` (static data)
- Each call creates a fresh timely dataflow, runs to completion, tears down
- `WlDdWorker` (`ffi.rs` line 48) is just a HashMap bag of EDB data -- no persistent dataflow

**Target state**: A `wl_session_t` C API where users can:
1. Create a session with a compiled plan
2. Load initial EDB data
3. Step the computation
4. Receive delta output (additions and retractions)
5. Update EDB facts (insert/retract)
6. Re-step to get incremental results
7. Destroy the session

---

## Phase 0: Backend Abstraction (C-side only, no behavior change)

### Objective
Introduce a `wl_backend_t` vtable so the execution path is backend-agnostic.
The current DD execution becomes the first (and only) backend implementation.
All 410 existing tests must pass unchanged.

### Task 0.1 -- Introduce `wl_backend_t` vtable [Size: M]

**New file**: `wirelog/backend/backend.h`

```c
typedef struct wl_backend_t {
    const char *name;                              /* "dd", "nanoarrow" */

    /* Lifecycle */
    void *(*worker_create)(uint32_t num_workers);
    void  (*worker_destroy)(void *worker);

    /* EDB loading */
    int (*load_edb)(void *worker, const char *relation,
                    const int64_t *data, uint32_t num_rows, uint32_t num_cols);

    /* One-shot execution (existing behavior) */
    int (*execute_cb)(const wl_ffi_plan_t *plan, void *worker,
                      wl_dd_on_tuple_fn on_tuple, void *user_data);
} wl_backend_t;
```

**Acceptance criteria**:
- `wl_backend_t` struct defined in `wirelog/backend/backend.h`
- A `wl_dd_backend()` function returns a static `wl_backend_t` with all DD function pointers
- No new Rust code
- Compiles cleanly, all existing tests pass

### Task 0.2 -- Wire driver.c through the vtable [Size: S]

**Modified file**: `wirelog/cli/driver.c`

Change `wl_run_pipeline()` to obtain the backend via `wl_dd_backend()` and call
through the vtable instead of direct `wl_dd_*` function calls.

**Acceptance criteria**:
- `wl_run_pipeline()` uses `wl_backend_t` function pointers
- CLI behavior identical (all `test_cli` tests pass)
- No change to Rust code

### Task 0.3 -- Update meson.build files [Size: S]

**Modified files**: `wirelog/meson.build`, `tests/meson.build`

Add `wirelog/backend/backend.c` (the DD backend vtable init) to both
`wirelog_dd_src` / `ir_src` lists.

**Acceptance criteria**:
- New backend source compiles in both library and test builds
- `meson test` passes all suites

### Phase 0 Files

| Action | File | Notes |
|--------|------|-------|
| CREATE | `wirelog/backend/backend.h` | vtable definition |
| CREATE | `wirelog/backend/backend.c` | DD backend implementation |
| MODIFY | `wirelog/cli/driver.c` | Use vtable instead of direct DD calls |
| MODIFY | `wirelog/meson.build` | Add backend source |
| MODIFY | `tests/meson.build` | Add backend source to test builds |
| NO TOUCH | `rust/**` | No Rust changes |
| NO TOUCH | `wirelog/backend/dd/dd_ffi.h` | FFI boundary unchanged |
| NO TOUCH | `wirelog/backend/dd/dd_plan.h` | Plan types unchanged |

### Phase 0 Naming Decision

Keep `wl_dd_plan_t` / `wl_ffi_plan_t` names as-is. The plan structures are
genuinely DD-flavored (VARIABLE, MAP, JOIN, etc. map to DD operators). A
future nanoarrow backend will consume the same plan structure but interpret
the operators differently. Renaming adds churn with no semantic benefit.
The backend abstraction lives at the *execution* level, not the plan level.

---

## Phase 1: DD Delta Support (Rust changes + C session API)

### Objective
Add a persistent-session API where DD maintains a live dataflow graph.
EDB updates are fed as delta tuples; IDB output reports only changes.
This is where Rust changes are required (but scoped to ~200 lines).

### Task 1.1 -- C-side session API [Size: M]

**New file**: `wirelog/backend/session.h`

```c
typedef struct wl_session wl_session_t;

/* Create a session with a compiled plan. Dataflow is built but not stepped. */
wl_session_t *wl_session_create(const wl_ffi_plan_t *plan,
                                 const wl_backend_t *backend,
                                 uint32_t num_workers);

/* Load initial EDB data (before first step). */
int wl_session_load_edb(wl_session_t *s, const char *relation,
                         const int64_t *data, uint32_t num_rows,
                         uint32_t num_cols);

/* Step the computation. Calls on_delta for each changed tuple.
 * diff > 0 = insertion, diff < 0 = retraction. */
typedef void (*wl_on_delta_fn)(const char *relation, const int64_t *row,
                                uint32_t ncols, int32_t diff,
                                void *user_data);
int wl_session_step(wl_session_t *s, wl_on_delta_fn on_delta,
                     void *user_data);

/* Insert or retract EDB tuples between steps.
 * diff > 0 = insert, diff < 0 = retract. */
int wl_session_update_edb(wl_session_t *s, const char *relation,
                           const int64_t *data, uint32_t num_rows,
                           uint32_t num_cols, int32_t diff);

/* Destroy session and release all resources. */
void wl_session_destroy(wl_session_t *s);
```

**Acceptance criteria**:
- Header compiles cleanly
- Session API documented with ownership model
- Callback includes `diff` parameter for additions (+1) and retractions (-1)

### Task 1.2 -- Rust persistent dataflow [Size: L]

**Modified file**: `rust/wirelog-dd/src/dataflow.rs`
**New file**: `rust/wirelog-dd/src/session.rs`

Create `SessionState` that holds:
- A persistent timely worker
- `InputSession` handles per EDB relation (replaces `new_collection_from`)
- Probe handles for tracking progress
- An output channel for delta tuples

Key Rust changes:
1. New `session.rs` module (~150 lines):
   - `SessionState::new(plan, num_workers)` -- build dataflow with `InputSession`
   - `SessionState::load_edb(relation, rows)` -- feed initial data via `InputSession::insert`
   - `SessionState::step()` -- advance time, step worker, collect output deltas
   - `SessionState::update_edb(relation, rows, diff)` -- insert/retract between steps
   - `SessionState::drop()` -- clean shutdown

2. In `dataflow.rs`, refactor collection creation:
   - Current: `scope.new_collection_from(rows)` (static, not updatable)
   - New: `scope.new_input()` returns `(InputSession, Collection)` (live, updatable)
   - The one-shot `execute_plan()` function is kept as-is for backward compatibility

3. Output collection:
   - Current: `inspect()` callback filters `diff > 0` only (line 149-151)
   - New: `inspect()` captures both positive and negative diffs, sends via channel

**Acceptance criteria**:
- `SessionState` can be created, loaded, stepped, updated, stepped again
- Output correctly reports additions (diff=+1) and retractions (diff=-1)
- Existing `execute_plan()` still works unchanged (one-shot mode)
- All existing 85 Rust tests pass
- New Rust tests for session lifecycle (create, load, step, update, step, destroy)
- New Rust tests for delta correctness (insert edge, step, verify TC additions)

### Task 1.3 -- Rust FFI entry points for sessions [Size: M]

**Modified file**: `rust/wirelog-dd/src/ffi.rs`

New `extern "C"` functions:
```rust
wl_dd_session_create(plan, num_workers) -> *mut SessionState
wl_dd_session_load_edb(session, relation, data, num_rows, num_cols) -> c_int
wl_dd_session_step(session, on_delta, user_data) -> c_int
wl_dd_session_update_edb(session, relation, data, num_rows, num_cols, diff) -> c_int
wl_dd_session_destroy(session)
```

**Modified file**: `wirelog/backend/dd/dd_ffi.h`

Add C declarations for the new Rust-exported session functions.

**Acceptance criteria**:
- FFI functions compile and link
- C side can call session FFI functions
- Memory ownership: C borrows during calls, Rust owns the session state
- NULL-safety on all entry points

### Task 1.4 -- Wire session API to DD backend vtable [Size: S]

**Modified file**: `wirelog/backend/backend.h`

Extend `wl_backend_t` with optional session function pointers:
```c
    /* Session-based execution (delta mode) -- NULL if unsupported */
    void *(*session_create)(const wl_ffi_plan_t *plan, uint32_t num_workers);
    int   (*session_load_edb)(void *session, const char *relation,
                              const int64_t *data, uint32_t nrows, uint32_t ncols);
    int   (*session_step)(void *session, wl_on_delta_fn on_delta, void *user_data);
    int   (*session_update_edb)(void *session, const char *relation,
                                const int64_t *data, uint32_t nrows,
                                uint32_t ncols, int32_t diff);
    void  (*session_destroy)(void *session);
```

**Acceptance criteria**:
- DD backend fills in session vtable entries
- `wl_session_*` C API calls dispatch through vtable
- A backend without session support (NULL pointers) returns an error from `wl_session_create`

### Task 1.5 -- C-side integration tests [Size: M]

**New file**: `tests/test_session.c`

Test cases:
1. Create session, load EDB, step, verify output matches one-shot execution
2. Create session, load EDB, step, update EDB (add edge), step, verify delta output
3. Create session, load EDB, step, update EDB (retract edge), step, verify retraction
4. Transitive closure: add edges incrementally, verify TC grows correctly
5. Session destroy is clean (no leaks under valgrind)
6. Error cases: NULL session, step without load, etc.

**Acceptance criteria**:
- All 6+ test cases pass
- Delta output is correct (verified against full recomputation)
- Tests registered in `tests/meson.build` with `rust_ffi_dep`

### Phase 1 Files

| Action | File | Notes |
|--------|------|-------|
| CREATE | `wirelog/backend/session.h` | Session API |
| CREATE | `wirelog/backend/session.c` | Session impl (dispatches to backend) |
| CREATE | `rust/wirelog-dd/src/session.rs` | Persistent dataflow (~150 lines) |
| CREATE | `tests/test_session.c` | Session integration tests |
| MODIFY | `rust/wirelog-dd/src/lib.rs` | Add `mod session;` |
| MODIFY | `rust/wirelog-dd/src/ffi.rs` | Add session FFI entry points |
| MODIFY | `wirelog/backend/dd/dd_ffi.h` | Add session FFI declarations |
| MODIFY | `wirelog/backend/backend.h` | Add session vtable entries |
| MODIFY | `wirelog/backend/backend.c` | Wire DD session functions |
| MODIFY | `wirelog/meson.build` | Add session source |
| MODIFY | `tests/meson.build` | Add test_session |
| NO TOUCH | `wirelog/cli/driver.c` | One-shot path unchanged |
| NO TOUCH | `rust/wirelog-dd/src/dataflow.rs` | Existing execution unchanged |

### Rust Change Budget

Total new Rust: ~200 lines in `session.rs` + ~80 lines in `ffi.rs` additions.
No changes to existing `dataflow.rs` or `plan_reader.rs`.
This respects the "minimize Rust changes" constraint by adding a new module
rather than refactoring existing code.

---

## Phase 2: nanoarrow Backend (C-only)

### Objective
Implement a C11-native backend using semi-naive delta evaluation.
Same session API, different backend. No Rust involved.

### Task 2.1 -- Semi-naive evaluator in C11 [Size: XL]

**New files**: `wirelog/backend/nanoarrow_backend.c`, `wirelog/backend/eval.c`

Implement a C11 semi-naive fixed-point evaluator that:
- Interprets the same `wl_ffi_plan_t` plan structure
- Uses hash tables or sorted arrays for relation storage
- Implements delta rules: `new_R = R_rules(delta_S, ...) - R`
- Supports all 9 operator types (VARIABLE, MAP, FILTER, JOIN, ANTIJOIN, REDUCE, CONCAT, CONSOLIDATE, SEMIJOIN)

**Acceptance criteria**:
- Produces identical results to DD backend on all 15 benchmark workloads
- Passes all session tests when substituted as backend
- No Rust dependency

### Task 2.2 -- nanoarrow memory layer (optional) [Size: L]

**New files**: `wirelog/backend/columnar.c`

Replace row-major relation storage with Apache Arrow columnar layout
via nanoarrow for memory efficiency and SIMD-friendly access patterns.

**Acceptance criteria**:
- Same results as row-major implementation
- Measurable memory reduction on large workloads
- nanoarrow is an optional build dependency (`-Dnanoarrow=true`)

### Task 2.3 -- Backend selection API [Size: S]

**Modified file**: `wirelog/backend/backend.h`, `wirelog/cli/driver.c`

Add `wl_backend_get(name)` that returns DD or nanoarrow backend.
CLI flag: `--backend=dd|nanoarrow`.

**Acceptance criteria**:
- Runtime backend selection works
- Default backend is DD (backward compatible)
- `wirelog --backend=nanoarrow` uses C-only path

### Phase 2 Files

| Action | File | Notes |
|--------|------|-------|
| CREATE | `wirelog/backend/nanoarrow_backend.c` | C11 semi-naive evaluator |
| CREATE | `wirelog/backend/eval.c` | Core evaluation loop |
| CREATE | `wirelog/backend/columnar.c` | Optional nanoarrow storage |
| MODIFY | `wirelog/backend/backend.h` | Backend registry |
| MODIFY | `wirelog/backend/backend.c` | Backend selection |
| MODIFY | `wirelog/cli/driver.c` | `--backend` flag |
| MODIFY | `wirelog/meson.build` | nanoarrow sources |
| MODIFY | `tests/meson.build` | nanoarrow test builds |
| MODIFY | `meson.build` | `-Dnanoarrow=true` option |
| NO TOUCH | `rust/**` | No Rust changes |

---

## Dependency Graph

```
Phase 0:
  Task 0.1 (vtable)
    └─> Task 0.2 (wire driver)
    └─> Task 0.3 (meson)
  [0.2 and 0.3 can run in parallel after 0.1]

Phase 1 (depends on Phase 0 complete):
  Task 1.1 (C session API)    ─────────────────────────────────────┐
  Task 1.2 (Rust session)     ─────────────────────────────────────┤
    [1.1 and 1.2 can run in parallel]                              │
  Task 1.3 (FFI bridge)       <── depends on 1.1 + 1.2 ───────────┤
  Task 1.4 (vtable wiring)    <── depends on 1.1 + 1.3            │
  Task 1.5 (integration tests) <── depends on 1.3 + 1.4 ──────────┘

Phase 2 (depends on Phase 1 complete):
  Task 2.1 (semi-naive eval)  ─────────────────────────────────────┐
  Task 2.2 (nanoarrow memory) <── depends on 2.1                   │
  Task 2.3 (backend selection) <── depends on 2.1                  │
    [2.2 and 2.3 can run in parallel after 2.1]                    │
```

---

## Risk Mitigation

### Phase 0 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Vtable adds call overhead | Low | Low | Function pointer indirection is negligible vs DD execution time |
| Naming confusion (dd_plan vs exec_plan) | Medium | Low | Document that plan types are shared; backend abstraction is at execution level |

**Breaking changes**: None. Phase 0 is pure refactoring.
**Rollback**: Revert the 3 commits (vtable, driver, meson).

### Phase 1 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `InputSession` API differs from `new_collection_from` | Medium | Medium | Prototype in isolated Rust test first |
| Timely worker lifecycle management (thread safety) | Medium | High | Single-threaded session; document thread model |
| Delta output ordering non-deterministic | Medium | Low | Tests compare sets, not sequences |
| Memory growth in long-running sessions | Low | Medium | Session tracks allocated memory; add `session_compact()` later |

**Breaking changes**: None. New API is additive. One-shot path untouched.
**Rollback**: Delete `session.rs`, revert FFI additions.

### Phase 2 Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Semi-naive evaluator correctness on recursive programs | High | High | Test against DD results on all 15 benchmarks |
| Performance gap vs DD | Medium | Medium | Accept slower initially; optimize with profiling |
| nanoarrow API surface area | Low | Low | nanoarrow is well-documented, minimal API |

**Breaking changes**: None. New backend is opt-in.
**Rollback**: Disable `-Dnanoarrow=true`.

### Testing Strategy

- **Phase 0**: Run all 410 existing tests. Zero tolerance for regressions.
- **Phase 1**: New `test_session.c` (C integration) + new Rust `session` tests.
  Cross-validate: one-shot results == session first-step results for all benchmarks.
- **Phase 2**: Run all 15 benchmarks with both backends. Results must be identical.
  Property-based: random EDB updates, compare DD session vs nanoarrow session.

---

## Effort Estimation

| Task | Size | Estimated Effort | Critical Path? |
|------|------|-----------------|----------------|
| 0.1 Backend vtable | M | 1 day | Yes |
| 0.2 Wire driver | S | 0.5 day | Yes |
| 0.3 Meson updates | S | 0.5 day | No (parallel with 0.2) |
| 1.1 C session API | M | 1 day | Yes (parallel with 1.2) |
| 1.2 Rust persistent dataflow | L | 3 days | Yes (parallel with 1.1) |
| 1.3 FFI bridge | M | 1 day | Yes |
| 1.4 Vtable wiring | S | 0.5 day | Yes |
| 1.5 Integration tests | M | 1.5 days | Yes |
| 2.1 Semi-naive evaluator | XL | 5-7 days | Yes |
| 2.2 nanoarrow memory | L | 3-4 days | No |
| 2.3 Backend selection | S | 0.5 day | No |

**Critical path**: 0.1 -> 0.2 -> 1.1/1.2 -> 1.3 -> 1.4 -> 1.5 -> 2.1
**Minimum viable delta feature**: Phase 0 + Phase 1 (Tasks 0.1-0.3, 1.1-1.5) = ~8 days
**Full implementation**: All phases = ~17-20 days

---

## Key Design Decisions

### Decision 1: Callback reports per-tuple with diff

**Chosen**: `on_delta(relation, row, ncols, diff, user_data)` where diff is +1/-1.

**Rationale**: Per-tuple callbacks match the existing `wl_dd_on_tuple_fn` pattern
(`dd_ffi.h` line 551). Adding a `diff` parameter is the minimal extension. DD
naturally produces `(data, time, diff)` triples (see `dataflow.rs` line 149),
so the mapping is direct.

**Alternative considered**: Batch callback `on_delta(relation, added_rows, added_count, removed_rows, removed_count)`. Rejected because it requires materializing delta sets before delivery, adding memory overhead and breaking the streaming model.

### Decision 2: Batch mode stays separate from session mode

**Chosen**: Keep `wl_dd_execute_cb` (one-shot) as a separate path from
`wl_session_*` (persistent). Do NOT implement batch on top of session.

**Rationale**: One-shot execution via `new_collection_from` is simpler, faster
for single-run programs (no `InputSession` overhead), and is the existing tested
path. The session API is additive.

**Alternative considered**: Remove `execute_plan()` and always use sessions
(batch = create session, load, step, destroy). Rejected because it adds
complexity to the common case and risks regressions.

### Decision 3: Sessions are single-threaded

**Chosen**: One session = one timely worker thread. No concurrent updates.

**Rationale**: DD's `InputSession` is not thread-safe. The C session API
is called sequentially (load, step, update, step). Multi-threaded DD
execution (multiple timely workers) is an orthogonal concern handled by
the `num_workers` parameter to `wl_dd_worker_create`, which controls
DD-internal parallelism within a single execution.

**Alternative considered**: Allow concurrent `update_edb` calls from multiple
C threads. Rejected: requires mutex synchronization around `InputSession`,
adds complexity for a use case that can be served by batching updates before
calling `step`.

### Decision 4: Schema changes between sessions are not supported

**Chosen**: A session is bound to a fixed plan at creation time. To change
the schema (add/remove columns, add/remove rules), destroy and recreate.

**Rationale**: DD builds a fixed dataflow graph from the plan. Changing the
schema requires rebuilding the graph. This is a fundamental DD constraint.

### Decision 5: Plan types keep DD naming

**Chosen**: Keep `wl_dd_plan_t`, `wl_dd_op_t`, etc. Do not rename to
`wl_exec_plan_t`.

**Rationale**: The plan structure reflects DD operator semantics (VARIABLE,
MAP, JOIN, etc.). A nanoarrow backend will interpret the same plan differently
but the operator vocabulary is DD-native. Renaming 50+ references across C
and Rust adds churn without improving clarity. The abstraction boundary is at
the *backend vtable*, not the plan types.

---

## Success Criteria

1. **Phase 0**: All 410 existing tests pass. Backend vtable compiles and is used by driver. Zero behavior change.
2. **Phase 1**: Session API works end-to-end. Delta output is correct for transitive closure, connected components, and SSSP benchmarks. One-shot path unaffected.
3. **Phase 2**: nanoarrow backend produces identical results to DD on all 15 benchmarks. Binary size without Rust < 2MB.
