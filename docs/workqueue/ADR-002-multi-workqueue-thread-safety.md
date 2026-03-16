# ADR-002: Multi-Workqueue Thread-Safety Design

**Status**: Approved (Consensus Planning Complete)
**Date**: 2026-03-16
**Decision Drivers**: Correctness under load, architectural coherence, incremental evaluation interaction

## Problem

DOOP benchmark experiences catastrophic memory amplification (2045GB vs 1.5GB baseline) when using K=8 workers due to:
- Shared eval_arena with concurrent mutations (race condition)
- Shallow-copy worker sessions sharing mutable state
- Delta relation accumulation across iterations
- Unbounded mat_cache growth
- Incremental delta pre-seeding overhead

Current architecture violates workqueue.h design intent (per-worker arena isolation). Need thread-safe design for general multi-worker scenarios.

## Decision: Option B — Selective Isolation

Workers share read-only EDB/IDB relations (`rels[]`), each worker gets exclusive:
- `eval_arena` (fresh, not shared)
- `eval_stack` (independent context)
- Per-worker cache (mat_cache isolated)
- Arrangement snapshots (read-only)

Results collected in worker buffer, merged sequentially (collect-then-merge pattern).

## Rationale

### Why Option B
1. **Minimal memory overhead**: Only per-worker arenas (256MB/worker acceptable)
2. **Eliminates race condition**: Exclusive arena allocation per worker
3. **Preserves incremental evaluation**: Shared read-only relations avoid duplication
4. **TSan clean**: No concurrent writes to shared state
5. **Proven pattern**: Documented in workqueue.h, ADR-001, shared-state-hazards.md
6. **Extends naturally**: Future multi-worker scenarios (non-recursive strata, FPGA)

### Why Not Alternatives
- **Option A (Full session cloning)**: 200% memory overhead, kills incremental evaluation goals
- **Option C (Coarse-grain locking)**: High contention, architectural regression from ADR-001
- **Option D (Single-worker only)**: Defeats performance goals, architectural dead-end

## Consequences

### Benefits
- DOOP completes: <15GB memory (vs 2045GB), <5 min wall-time (vs timeout)
- CSPA speedup: 1.5-2x with 4 workers (Amdahl's law P~0.7)
- Thread-safe for general multi-worker dispatch
- Zero data races (TSan validated)
- Supports future architectural extensions (FPGA, non-recursive parallelism)

### Trade-offs
- Per-worker cache may have lower hit rate (workers diverge on rule copies)
- Requires careful implementation (shallow copy must correctly isolate mutable fields)
- Delta lifecycle requires explicit management (scratch pool)
- Must audit all shared access patterns (EDB relations read-only assumption)

## Implementation (6 Phases, 15-20h)

**Phase 1** (CRITICAL): Per-worker arena isolation in col_op_k_fusion()
- Create fresh arena before worker submit
- Free all worker arenas after barrier
- Test: K=8 arena isolation, zero TSan races

**Phase 2**: Session structure audit
- Document shared (rels[], nrels, rel_cap) vs exclusive (eval_arena, mat_cache, frontiers[])
- Audit col_op_k_fusion loop, verify read-only assumption
- Create session-isolation-contract.md

**Phase 3**: Delta lifecycle management
- Implement scratch pool for pre-seeded deltas (not in sess->rels[])
- Verify no accumulation across iterations
- Test: delta cleanup validation

**Phase 4**: mat_cache eviction policy
- Implement LRU eviction when cache exceeds 512MB per worker
- Measure cache hit rate
- Verify memory bounded at 512MB per worker

**Phase 5**: TSan validation & testing
- Extend test_workqueue.c: K=1,2,4,8 verification
- Run full suite with TSan enabled
- Verify output byte-for-byte match, measure speedup curve

**Phase 6**: Documentation & issue creation
- Create ADR-002 decision record (this document)
- Update ARCHITECTURE.md with thread-safety section
- Create GitHub issue #173 with findings and acceptance criteria

## Verification

### Correctness
- Zero TSan data races on K-fusion dispatch (K=1,2,4,8)
- Output byte-for-byte match: single-worker vs multi-worker on CSPA/DOOP
- AddressSanitizer pass (heap-use-after-free, buffer overflows)
- Incremental evaluation semantics preserved

### Performance
- CSPA speedup: 1.5-2x with 4 workers (P~0.7 from Amdahl's law)
- DOOP speedup: 2-3x with 8 workers
- Memory regression: CSPA ≤2GB, DOOP <15GB
- Scaling curve: wall-time decreases with K∈{1,2,4,8}

### Code Quality
- Per-worker arena: clean, documented lifecycle
- Session isolation: explicit shared/exclusive field documentation
- Delta scratch pool: explicit creation, cleanup, no accumulation
- mat_cache: LRU policy, bounded size monitoring

## Future Work

- **Non-recursive stratum parallelization**: Extend collect-then-merge to multiple strata (K-fusion already proven)
- **FPGA integration**: Multi-worker design provides foundation for heterogeneous compute
- **Incremental evaluation optimization**: Delta-seeding + per-worker arenas enables cost-based selectivity in multi-worker scenarios

## References

- GitHub Issue #173: Multi-workqueue thread-safety design & DOOP enablement
- Root cause analysis: columnar_nanoarrow.c lines 3000, 5316-5340, 2960-3175
- Workqueue API: workqueue.h, design intent vs implementation gap
- ADR-001: Collect-then-merge pattern for non-recursive strata
- shared-state-hazards.md: Thread-safety hazard catalog

---

**Related Issues**: #62 (Boolean Specialization), #67 (Incremental Evaluation), #83 (Frontier Reset)
**Approved By**: Consensus planning (Planner/Architect/Critic review complete)
