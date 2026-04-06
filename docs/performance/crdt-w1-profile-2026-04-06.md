# CRDT W=1 Profiling Results (2026-04-06)

## Context

Follow-up investigation for Issue #367 (CRDT W=1 regression: 317s vs 225s baseline).

Tool: macOS `sample` (1ms interval, 30-second window during steady-state execution)
Binary: `bench_flowlog` (Oz, LTO, ARM64, without WL_RADIX_BENCH)
Dataset: bench/data/crdt/ (259,778 edges, 14,148 iterations, 2,156,530 tuples)

## Baseline Before Fix

```
workload  edges   workers  min_ms   tuples   iterations  status
crdt      259778  1        288900   2156530  14148       OK
```

~289s with commit c939733 (snap hoist only, EDB skip not yet extended to W=1).

## Root Cause Identified (commit c939733)

`rewrite_multiway_delta()` only expands plans with k≥2 IDB delta positions.
CRDT recursive rules have k=1 (single recursive variable), so no FORCE_DELTA
expansion occurs. The EDB base rule (e.g. `nextSiblingAnc :- nextSibling`) had
`WL_DELTA_AUTO` and was re-evaluated every sub-pass, producing 104K rows that
went through UNION CONSOLIDATE K-way merge each iteration.

Evidence from WL_RADIX_BENCH output:
- 28,301 k=16 radix sorts × nrows=104,852 × ~3.75ms = ~106s (36.4% of 291s)
- Exactly 2.0 sorts per sub-pass (2 recursive strata with EDB base rules)

Fix: Remove `tdd_subpass_active` gate from EDB base-case VARIABLE skip so it
applies to W=1 sequential evaluation (was TDD-workers-only in Issue #361).

## CPU Profile (sample, 15,084 samples)

| Rank | Function | Samples | % | Category |
|------|----------|---------|---|----------|
| 1 | `hash_int64_keys_neon` | ~8,608 | 57.1% | Join probe - hash computation |
| 2 | `col_rel_row_copy_out` | ~2,901 | 19.2% | Join probe - row materialization |
| 3 | `keys_match_neon` | ~502 | 3.3% | Join probe - key comparison |
| 4 | `session_find_rel` | ~41 | 0.3% | Relation lookup |
| 5 | `col_op_consolidate_kway_merge` | ~22 | 0.1% | Consolidation |
| 6 | `col_rel_radix_sort` | ~14 | 0.09% | Sort (incl. WL_RADIX_BENCH fprintf) |
| 7 | `snprintf` (in pool_new_auto) | ~9 | 0.06% | Relation name formatting |

All samples rooted in `col_eval_relation_plan` → join probe loop
(offsets 7376/7308/7352/7340/7392/7404 are different instructions in the
same arrangement probe hot path).

## Key Findings

**Join probe dominates (>80%):**
- `hash_int64_keys_neon` + `col_rel_row_copy_out` + `keys_match_neon` = ~80%
- This is the arrangement probe loop in `col_eval_relation_plan`

**Sort is negligible (0.09%):**
- skip-pass already eliminates 75% of passes for nc=4 (24/32 skipped in k=8)
- Hybrid sort strategy (ncols≥3 → qsort_r) would have no measurable effect

**Consolidation is negligible (0.1%):**
- The EDB base-case fix (c939733) already eliminates the 104K-row re-sort

**malloc/snprintf overhead is negligible (<0.2%):**
- snap[] hoist saves ~3-6ms on 289s run (0.002%)
- #285 snprintf caching confirmed negligible here

## Optimization Impact Summary

| Change | Expected Impact | Actual |
|--------|----------------|--------|
| EDB base-case skip W=1 (c939733) | -106s (k=16 sorts eliminated) | TBD benchmark |
| snap[] hoist (c939733) | -3-6ms | Negligible |
| Hybrid sort ncols≥3 | ~0% (sort is 0.09%) | N/A |
| Arena allocator for TDD | ~0% (malloc is <0.2%) | N/A |

## Next Investigation (Task #2-2)

Join probe (80% CPU) is the remaining bottleneck. Areas to investigate:
1. Per-rule JOIN call breakdown: which rules (valueFlow, valueAlias, etc.) dominate
2. Arrangement reuse effectiveness across iterations
3. Row materialization cost: `col_rel_row_copy_out` called per output row
4. Potential: avoid full row copy in join output (pointer/index-based)
