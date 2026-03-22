# CRDT Baseline After DEBUG Removal (#284)

## Executive Summary

After merging #277 (DEBUG fprintf and getenv removal), CRDT benchmark baseline has been re-established.

**Execution Time:** ~258.7 seconds (median of 3 runs)
**Peak Memory:** 377 MB
**Tuples Generated:** 2.1M
**Iterations:** 14,148

## Benchmark Configuration

- **Workload:** CRDT (Insert/Remove operations)
- **Dataset:** bench/data/crdt/
  - Insert_input.csv: 3.0 MB
  - Remove_input.csv: 635 KB
- **Workers:** 1 (single-threaded)
- **Repeats:** 3
- **Platform:** macOS (Apple Silicon)

## Results

| Metric | Value |
|--------|-------|
| Min Time | 257,342.4 ms |
| Median Time | 258,705.9 ms |
| Max Time | 263,166.0 ms |
| Variance | ~2.4% |
| Peak RSS | 377,248 KB |
| Edges Processed | 259,778 |
| Tuples Generated | 2,156,530 |
| Iterations | 14,148 |
| Status | OK |

## Observations

1. **Consistency:** Results are tightly clustered (variance < 2.5%) — good for establishing reliable baseline
2. **Performance:** Total execution time dominated by evaluation iterations (14K iterations at ~18ms each)
3. **Memory:** Peak memory usage at 377 MB indicates efficient columnar storage relative to tuple volume
4. **Scaling:** Single-threaded execution; comparison point for multi-worker optimization (#279-#282)

## Next Steps

This baseline is required before evaluating performance improvements from:
- #279: K-fusion threshold optimization
- #280: Memory ledger and soft truncation
- #281: SIMD consolidation enhancements
- #282: Stride-based evaluation

## Timestamp

- **Date:** 2026-03-22
- **Commit:** c82eb69 (fix: unsigned underflow in kway_merge)
- **#277 Status:** Merged (DEBUG fprintf and getenv removed)
