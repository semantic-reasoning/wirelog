# ARM NEON kc<2 Dispatch Overhead Analysis (Issue #234)

## Problem Statement

ARM NEON JOIN dispatch uses ternary macros (ops.c:1393-1398) that check
`kc < 2` at every call site in the hot join loop. Since kc=1 accounts for
~98% of DOOP joins, this is the dominant code path.

## Current Dispatch Mechanism

```c
#define hash_int64_keys_fast(row, key_cols, kc)                        \
    ((kc) < 2 ? hash_int64_keys_scalar_inline((row), (key_cols), (kc)) \
              : hash_int64_keys_neon((row), (key_cols), (kc)))
#define keys_match_fast(lrow, lk, rrow, rk, kc)                            \
    ((kc) < 2 ? keys_match_scalar_inline((lrow), (lk), (rrow), (rk), (kc)) \
              : keys_match_neon((lrow), (lk), (rrow), (rk), (kc)))
```

## Call Sites in Hot Loop (col_op_join)

| Line | Function              | Loop Context                         |
|------|-----------------------|--------------------------------------|
| 1727 | hash_int64_keys_fast  | Ephemeral hash BUILD (all right rows)|
| 1768 | keys_match_fast       | Arrangement PROBE (inner loop)       |
| 1800 | hash_int64_keys_fast  | Ephemeral hash PROBE (all left rows) |
| 1806 | keys_match_fast       | Ephemeral hash PROBE (inner loop)    |

Key observation: `kc` is set once (line 1495) and is **loop-invariant** within
`col_op_join`. The ternary check is redundant on every iteration after the first.

## Root Cause Chain

1. **Original NEON (cc8b822)**: Simple alias `#define hash_int64_keys_fast hash_int64_keys_neon`.
   For kc=1, `hash_int64_keys_neon` internally falls back to `hash_int64_keys()`,
   causing double function-call overhead. **24.5% regression** (17.3s vs 13.9s).

2. **Inline dispatch fix (ca0cf9a)**: Added `_scalar_inline` helpers and ternary
   macros to bypass NEON for kc<2. This eliminates the double call but introduces:
   - Per-iteration branch at 4 call sites (well-predicted but non-zero cost)
   - Code size bloat (ternary expanded 4x in the hot loop)
   - I-cache pressure from duplicated inline expansions

## Overhead Analysis

### Per-iteration costs of ternary dispatch:
- Branch instruction: ~1 cycle (well-predicted on Apple M-series)
- Duplicated at 4 call sites = 4 branches per left-row iteration
- For DOOP with ~6.4M tuples, millions of iterations

### Why it matters:
- `hash_int64_keys_neon` is `static` (NOT `inline`) at line 1315
- `keys_match_neon` is `static inline` at line 1354
- The scalar helpers are `static inline` and will be fully inlined
- The ternary creates an asymmetry: inlined scalar vs function-call NEON

## Proposed Fix Direction

Make the NEON functions internally dispatch to `_scalar_inline` helpers for
kc<2, then use simple alias macros (like AVX2 does). This:
1. Eliminates per-call-site ternary branches
2. Preserves zero function-call overhead for kc<2 (scalar_inline is inlined)
3. Reduces code size (single dispatch point inside each function)
4. Matches AVX2 pattern (simple alias, internal fallback)

## Baseline Benchmark

| Workload | Median (ms) | Peak RSS (KB) | Tuples    |
|----------|-------------|---------------|-----------|
| DOOP     | 82646.6     | 4,304,384     | 6,393,962 |
