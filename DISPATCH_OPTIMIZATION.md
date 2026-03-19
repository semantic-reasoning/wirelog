# ARM NEON kc<2 Dispatch Optimization Design (Issue #234)

## Chosen Strategy: Internal Inline Fallback

Modify `hash_int64_keys_neon` and `keys_match_neon` to call the `_scalar_inline`
helpers directly for kc<2, then use simple alias macros (no ternary dispatch).

## Design

### Before (ternary macro dispatch):
```
call site → ternary(kc<2) → scalar_inline (inlined)
                           → hash_int64_keys_neon (function call)
```
4 ternary branches per left-row iteration.

### After (internal inline fallback):
```
call site → hash_int64_keys_neon → if(kc<2) scalar_inline (inlined by compiler)
                                 → else NEON SIMD path
```
0 branches at call sites. Single dispatch inside each function.

## Changes Required

### 1. `hash_int64_keys_neon` (line 1315-1345)
Change: `if (kc < 2) return hash_int64_keys(row, key_cols, kc);`
To:     `if (kc < 2) return hash_int64_keys_scalar_inline(row, key_cols, kc);`

Also add `inline` qualifier to allow compiler to inline the whole function.

### 2. `keys_match_neon` (line 1354-1379)
Change: Add early return for kc<2 using `keys_match_scalar_inline`.
Currently has no kc<2 shortcut (relies on ternary macro to bypass).

### 3. Dispatcher macros (lines 1393-1398)
Change from ternary macros to simple aliases:
```c
#define hash_int64_keys_fast hash_int64_keys_neon
#define keys_match_fast keys_match_neon
```

### 4. Remove `_scalar_inline` standalone declarations
Keep the functions (they're now called from within NEON functions) but remove
the `#ifdef __ARM_NEON__` guard duplication since they're only used internally.

## Why This Works

- `hash_int64_keys_scalar_inline` is `static inline` → compiler inlines it
  into `hash_int64_keys_neon` at the kc<2 branch
- Single branch point (inside function) vs 4 branch points (at call sites)
- Matches AVX2 pattern: `hash_int64_keys_avx2` has `if (kc<4) return hash_int64_keys(...)`
- No behavior change: same hash values, same match results

## Correctness Guarantees

- kc=0: `_scalar_inline` loop body doesn't execute → returns initial hash/true (cross product)
- kc=1: `_scalar_inline` executes once → identical to `hash_int64_keys` for kc=1
- kc>=2: NEON SIMD path unchanged
- All existing tests must pass unchanged
