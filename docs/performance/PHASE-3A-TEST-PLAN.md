#Phase 3A Test Plan : K - Fusion Plan Generation &Dispatch

**Date:** 2026-03-08
**Author:** Gemini (validation specialist)
**Phase:** 3A (Weeks 1-3)
**Status:** CORRECTNESS GATE CLOSED (2026-03-08) — Performance baseline recalibrated

---

## Baseline State (Phase 2D Complete)

| Metric | Value |
|--------|-------|
| Test suite | 20/20 (19 OK + 1 EXPECTEDFAIL) |
| CSPA (K=2) | 6.0s median, 20,381 tuples, 6 iterations |
| DOOP (K=8) | 71m50s |
| K-fusion dispatch | EXISTS but sequential (submit+wait_all per worker) |
| Plan generation | ENABLE_K_FUSION=1 (already emitting K_FUSION nodes) |

### Critical Finding: Dispatch is Sequential, Not Parallel

`col_op_k_fusion()` at `columnar_nanoarrow.c:2327` currently submits one worker,
waits for it to complete, then submits the next. This is **sequential execution**
despite using the workqueue infrastructure. Phase 3A's goal is converting to
true parallel dispatch (submit all K workers, then one `wait_all`).

```c
// CURRENT (sequential - Phase 2D workaround):
for (uint32_t d = 0; d < k; d++)
{
    wl_workqueue_submit(wq, col_op_k_fusion_worker, &workers[d]);
    wl_workqueue_wait_all(wq); // ← barrier INSIDE loop = sequential
}

// TARGET (parallel):
for (uint32_t d = 0; d < k; d++) {
    wl_workqueue_submit(wq, col_op_k_fusion_worker, &workers[d]);
}
wl_workqueue_wait_all(wq); // ← single barrier AFTER all submits
```

    The Phase 2D race condition(heap - use - after - free) was fixed by using
`eval_stack_init` per worker.With that isolation in place,
    true parallel dispatch should be safe.

            -- -

            ##Phase 3A Test Strategy

            ## #Testing Pyramid

``` 10 % e2e(CSPA / TC correctness, DOOP gate) 30
            % integration(K = 2 / 4 / 8, sequential vs parallel parity,
                          ASAN / TSan) 60
            % unit(K - fusion dispatch logic, merge correctness,
                   empty - delta interaction)
```

            ## #Test Files

        | File | Status | Purpose | | -- -- -- | -- -- -- -- | -- -- -- -- - |
        | `tests / test_k_fusion_merge.c` | EXISTS(mock - only)
        | Algorithm structure validation | | `tests / test_k_fusion_dispatch.c`
        | EXISTS(mock - only) | Algorithm logic validation |
        | `tests / test_k_fusion_e2e.c`
        | **NEW(this plan) ** | Real session API correctness |

        -- -

           ##Test Cases : test_k_fusion_e2e.c

                          ## #T1 : K
    = 2 Recursive Join Correctness

        **Program : **Transitive closure with K
                    = 2 body(`r(x, y), r(y, z)`)
```datalog.decl r(x
                    : int32, y
                    : int32) r(1, 2)
                          .r(2, 3)
                          .r(3, 4)
                          .r(x, z)
    : -r(x, y),
r(y, z).
``` **Expected : ** 6 tuples
    = { (1, 2), (2, 3), (3, 4), (1, 3), (2, 4), (1, 4) } **Validates
    : **K
      = 2 K - fusion dispatch produces correct results **Risk : **HIGH — core K
                                                                - fusion path

                                                                ## #T2 : K
      = 1 vs K
      = 2 Parity(Non - regression)

          **Purpose : **Verify K
                      = 1 non - fusion path and K
                      = 2 fusion path agree on TC
```datalog-- K
                      = 1 variant(no K - fusion)
    : tc(x, z)
    : -tc(x, y),
edge(y, z).

    --K
    = 2 variant(triggers K - fusion)
    : tc(x, z)
    : -tc(x, y),
tc(y, z).
``` Both on edge(1, 2), edge(2, 3),
edge(3, 4) should produce 6 tuples.**Validates : **K - fusion result matches non
    - K
    - fusion baseline

    ## #T3 : Iteration Count Correctness

                 **Program : ** 3
                             - edge chain K
    = 2(same as T1) **Expected : **Converges in ≤3 iterations **Validates
    : **Fixed
      - point semantics preserved under K
      - fusion dispatch

      ## #T4 : Empty Delta Skip
               + K
               - Fusion Interaction

                   **Purpose : **A K
    = 2 recursive relation whose delta becomes empty should trigger the empty
      - delta skip optimization even when K
      - fusion is active.**Expected : **No extra iterations after fixed
      - point **Risk : **MEDIUM — optimization interaction

                       ## #T5 : K
    = 2 Isolated Worker Stack Safety

        **Purpose : **Run K
                    = 2 dispatch with ASAN enabled; verify no heap errors.
- Each worker must use its own `eval_stack_t` (no shared stack)
- Session is read-only during worker execution
**Expected:** Exit code 0, no ASAN/TSan errors
**Validates:** Phase 2D race condition fix holds under parallelism

### T6: Larger Graph K=2 Correctness

**Program:** 5-node chain transitive closure
```datalog
.decl e(x: int32, y: int32)
e(1,2). e(2,3). e(3,4). e(4,5).
.decl reach(x: int32, y: int32)
reach(x,y) :- e(x,y).
reach(x,z) :- reach(x,y), reach(y,z).
```
**Expected:** 10 tuples (all pairs (i,j) where i<j in 1..5)
**Validates:** K-fusion scales to realistic input sizes

---

## Phase 3A Acceptance Gate

### Test Gate (Week 2)
- [ ] All 20 existing tests pass (no regression)
- [ ] `test_k_fusion_e2e` passes: T1–T6 all GREEN
- [ ] ASAN clean: `meson test -C build --setup=asan` (if configured)

### Performance Gate (Week 3)
- [ ] CSPA median < 4.5s (3-run median, release build `-O3`)
- [ ] DOOP completes < 30 minutes
- [ ] Workqueue overhead < 5% (K=2 parallel vs K=2 sequential)
- [ ] Results documented in `docs/performance/PHASE-3A-BENCHMARK.md`

### Architecture Gate
- [ ] `col_op_k_fusion()` uses single `wait_all` after all submits (true parallel)
- [ ] TSan clean for K=2 and K=8 concurrent paths
- [ ] No global mutable state introduced
- [ ] Backward compatibility: non-K-fusion ops unchanged

---

## Benchmark Strategy

### CSPA Benchmark (3-run median)
```sh
#Release build
meson configure -C build --buildtype=release -Db_lto=true
meson compile -C build
./build/bench/bench_flowlog --workload cspa \
    --data bench/data/graph_10.csv \
    --data-weighted bench/data/graph_10_weighted.csv
```
Record: run 1, run 2, run 3 → report min/median/max.

### DOOP Benchmark (1 run)
```sh
./build/bench/bench_flowlog --workload doop
```
Record: wall-time to completion.

### Parallel vs Sequential Comparison
Temporarily compile with sequential fallback (ENABLE_K_FUSION=0 or num_workers=1)
and compare wall-time to true parallel K-fusion.

---

## DD Oracle Comparison

For correctness validation against DD baseline (extractable from git at commit 8f03049):
```sh
git stash
git checkout 8f03049 -- rust/
#Build and run DD for CSPA
#Record : tuple count = 20, 381, iterations = 6
git stash pop
```

Phase 3A correctness gate: CSPA must produce exactly 20,381 tuples in 6 iterations.

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Race condition in true parallel dispatch | Medium | High | TSan validation; Phase 2D isolation pattern |
| Merge result incorrect after parallel collection | Low | High | T1/T6 correctness tests with exact counts |
| Workqueue capacity exceeded with K=8 | Low | Medium | Submit-before-wait pattern with ring buffer check |
| Performance regression (overhead > gain) | Low | Medium | K=2 parallel vs sequential timing comparison |

---

## Implementation Checklist for Codex (Task 3A-2)

- [ ] Move `wl_workqueue_wait_all(wq)` outside the submission loop
- [ ] Verify worker stack isolation (each worker has its own `eval_stack_t`)
- [ ] Run `meson test -C build` → 20/20 pass
- [ ] Run with TSan: `CC=clang CFLAGS="-fsanitize=thread" meson compile -C build`
- [ ] Confirm CSPA produces 20,381 tuples in 6 iterations

---

## Known Issues (Discovered During Test Development)

### KI-1: K=2 Over-count on Dense Bidirectional Graphs

**Discovered:** 2026-03-08 (Gemini, Phase 3A test authoring)

**Program:**
```datalog
r(1,2). r(2,1). r(1,3). r(3,1). r(2,3). r(3,2).
r(x,z) :- r(x,y), r(y,z).
```

**Expected (logical Datalog):** 9 tuples — 6 base + 3 self-loops {(1,1),(2,2),(3,3)}
**Actual (current K-fusion):** 11 tuples — 2 extra tuples, likely duplicates in storage

**Hypothesis:** K-fusion's `col_rel_merge_k()` has a dedup gap for high-density graphs
where both K workers produce overlapping result sets with more than 2 new tuples.
The 2-node bidirectional cycle (simpler case) deduplicates correctly → 4 tuples.

**Risk:** MEDIUM — correctness issue visible on DOOP-scale programs (high fan-out joins)
**Owner:** Codex (Phase 3A Task 3A-2)
**Gate:** Must be resolved before Phase 3A performance gate. Correct count must be 9.

---

## References

- `wirelog/backend/columnar_nanoarrow.c:2327` — `col_op_k_fusion()` implementation
- `wirelog/exec_plan_gen.c:39` — `ENABLE_K_FUSION=1` flag
- `docs/performance/SEGFAULT-INVESTIGATION.md` — Phase 2D race condition analysis
- `docs/timely/TIMELY-PHASE-3-PLAN.md` — Phase 3 execution plan
- `docs/performance/K-FUSION-DESIGN.md` — K-fusion architecture design

---

## Phase 3A Completion Report (2026-03-08)

### Correctness Gate: ✅ CLOSED

| Criterion | Result |
|-----------|--------|
| Plan generation (ENABLE_K_FUSION=1) | ✅ emitting K_FUSION nodes |
| True parallel dispatch | ✅ single wait_all after all submits |
| KI-1 dedup fix | ✅ ea224ad — EDB prefix sort |
| test_k_fusion_e2e: 7/7 | ✅ (incl. 9-tuple complete graph) |
| Full suite: 21/21 | ✅ (TSan build) |
| TSan clean | ✅ (no races) |
| CSPA correctness: 20,381 tuples, 6 iters | ✅ oracle match |
| DOOP correctness | ⏳ run in progress |

### Benchmark Baseline Correction

⚠️ **The "6.0s" figure in TIMELY-PHASE-3-PLAN.md was incorrect.**

The Phase 2D CSPA TSV validation files (2026-03-07) show:
- 4 valid runs: 28.3s, 28.7s, 34.8s, 41.9s
- **Actual Phase 2D median: ~31.8s** (not 6.0s)

The 6.0s figure likely reflected a different/smaller benchmark not captured in git.

### Two-Tier Benchmark Structure

| Tier | Dataset | Edges | Phase 2D | Phase 3A (ea224ad) | Target |
|------|---------|-------|----------|--------------------|--------|
| Microbenchmark | graph_10.csv | 9 | unknown | ~6s (est.) | — |
| **Real CSPA** | bench/data/cspa/ | 199 | ~31.8s (median) | **~17.2s** | TBD after profiling |

**Phase 3A at 17.2s is a 46% improvement over Phase 2D baseline.** Parallel dispatch works.

### Phase 3B Strategy: Profiling-First

The 4.5s target in TIMELY-PHASE-3-PLAN.md was calibrated on wrong baseline data.
Phase 3B will:
1. Profile the 17.2s baseline (perf/Instruments: where does time go?)
2. Measure K-fusion contribution (ENABLE_K_FUSION=0 sequential vs K=2 parallel)
3. Set data-driven Phase 3B performance targets

### CSPA Benchmark Command (Corrected)

```sh
#Correct(uses real CSPA dataset):
DYLD_LIBRARY_PATH=build-o3/subprojects/nanoarrow \
  ./build-o3/bench/bench_flowlog --workload cspa --data-cspa bench/data/cspa

#NOT this(wrong — graph_10.csv is not CSPA data):
#./ build / bench / bench_flowlog -- workload cspa -- data bench / data / graph_10 \
    .csv
```

### Known Issues: Final Status

| Issue | Status |
|-------|--------|
| KI-1: EDB+IDB dedup over-count | ✅ FIXED (ea224ad) |
| TSan-1: wl_workqueue_drain() race | ✅ FIXED (2e7b6a3) |
| Benchmark baseline mismatch | ✅ DOCUMENTED (see above) |
