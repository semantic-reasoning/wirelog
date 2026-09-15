/*
 * columnar/merge.c - wirelog Columnar Concatenation and Consolidation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#if defined(_MSC_VER)
#define WL_OPS_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define WL_OPS_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define WL_OPS_ALWAYS_INLINE inline
#endif

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

static void *
col_op_consolidate_malloc(size_t size, const char *site)
{
#ifdef WL_TEST_CONSOLIDATE_ALLOC_HOOK
    if (wl_columnar_consolidate_alloc_hook
        && wl_columnar_consolidate_alloc_hook(site))
        return NULL;
#else
    (void)site;
#endif
    return malloc(size);
}

static void *
col_op_consolidate_calloc(size_t count, size_t size, const char *site)
{
#ifdef WL_TEST_CONSOLIDATE_ALLOC_HOOK
    if (wl_columnar_consolidate_alloc_hook
        && wl_columnar_consolidate_alloc_hook(site))
        return NULL;
#else
    (void)site;
#endif
    return calloc(count, size);
}

static void *
col_op_consolidate_realloc(void *ptr, size_t size, const char *site)
{
#ifdef WL_TEST_CONSOLIDATE_ALLOC_HOOK
    if (wl_columnar_consolidate_alloc_hook
        && wl_columnar_consolidate_alloc_hook(site))
        return NULL;
#else
    (void)site;
#endif
    return realloc(ptr, size);
}

/* The operand roles are load-bearing at the merge-output sizing site: the
 * `right != 0` test below is one of the two constructs that would pin that
 * allocation's extent to a constant and revive a false
 * clang-analyzer-security.ArrayBound there.  Rewriting this guard in terms
 * of `left` is semantically identical but breaks the tidy ratchet 700 lines
 * away; see the comment in the `seg_count >= 2` block and issue #1592. */
static bool
col_op_consolidate_size_multiply(size_t left, size_t right, size_t *out)
{
    if (!out || (right != 0 && left > SIZE_MAX / right))
        return false;
    *out = left * right;
    return true;
}

static bool
col_op_consolidate_size_add(size_t left, size_t right, size_t *out)
{
    if (!out || left > SIZE_MAX - right)
        return false;
    *out = left + right;
    return true;
}

/* --- CONCAT -------------------------------------------------------------- */

int
col_op_concat(eval_stack_t *stack, wl_col_session_t *sess)
{
    if (stack->top < 2)
        return 0; /* single-item passthrough for K-copy boundary marker */

    eval_entry_t b_e;
    eval_entry_t a_e;
    int pop_rc = eval_stack_pop_relation(stack, &b_e);
    if (pop_rc != 0)
        return pop_rc;
    pop_rc = eval_stack_pop_relation(stack, &a_e);
    if (pop_rc != 0) {
        eval_entry_dispose(&b_e);
        return pop_rc;
    }
    col_rel_t *a = a_e.rel;
    col_rel_t *b = b_e.rel;

    if (!a || !b || a->ncols != b->ncols) {
        eval_entry_dispose(&a_e);
        eval_entry_dispose(&b_e);
        return EINVAL;
    }

#ifdef WL_PROFILE
    uint64_t _t0_concat = now_ns();
    sess->profile.concat_calls++;
#endif

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$concat", a);
    if (!out) {
        eval_entry_dispose(&a_e);
        eval_entry_dispose(&b_e);
        return ENOMEM;
    }

    int rc = col_rel_append_all(out, a, NULL);
    if (rc == 0)
        rc = col_rel_append_all(out, b, NULL);

    if (rc != 0) {
        eval_entry_dispose(&a_e);
        eval_entry_dispose(&b_e);
        col_rel_destroy(out);
        return rc;
    }

    /* Track segment boundaries for K-way merge optimization. */
    uint32_t left_segs = a_e.seg_count > 0 ? a_e.seg_count : 1;
    uint32_t right_segs = b_e.seg_count > 0 ? b_e.seg_count : 1;
    uint32_t total_segs = left_segs + right_segs;

    uint32_t *out_boundaries
        = (uint32_t *)malloc((total_segs + 1) * sizeof(uint32_t));
    if (!out_boundaries) {
        eval_entry_dispose(&a_e);
        eval_entry_dispose(&b_e);
        col_rel_destroy(out);
        return ENOMEM;
    }

    /* Copy left boundaries */
    if (a_e.seg_boundaries != NULL) {
        memcpy(out_boundaries, a_e.seg_boundaries,
            (left_segs + 1) * sizeof(uint32_t));
    } else {
        out_boundaries[0] = 0;
        out_boundaries[1] = a->nrows;
    }

    /* Adjust and append right boundaries */
    uint32_t right_offset = a->nrows;
    if (b_e.seg_boundaries != NULL) {
        for (uint32_t i = 0; i <= right_segs; i++)
            out_boundaries[left_segs + i]
                = b_e.seg_boundaries[i] + right_offset;
    } else {
        out_boundaries[left_segs] = right_offset;
        out_boundaries[left_segs + 1] = out->nrows;
    }

    /* Clean up input boundaries */
    eval_entry_dispose(&a_e);
    eval_entry_dispose(&b_e);

#ifdef WL_PROFILE
    if (out->nrows == 0)
        sess->profile.concat_empty_out++;
    sess->profile.concat_ns += now_ns() - _t0_concat;
#endif

    rc = eval_stack_push(stack, out, true);
    if (rc != 0) {
        free(out_boundaries);
        col_rel_destroy(out);
        return rc;
    }

    /* Attach boundary metadata to the pushed entry */
    stack->items[stack->top - 1].seg_boundaries = out_boundaries;
    stack->items[stack->top - 1].seg_count = total_segs;
    return 0;
}
/* --- CONSOLIDATE --------------------------------------------------------- */

/* Issue #197: SIMD row comparison functions moved here so kway_row_cmp and
 * all callers in the consolidate/merge paths use the optimized dispatcher. */

/* Helper: lexicographic int64_t row comparison (-1/0/+1).
 * Compares rows a and b with ncols columns using int64_t values (not bytes).
 * Required for correct little-endian int64_t comparisons.
 */
static int UNUSED
row_cmp_lex(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        if (a[c] < b[c])
            return -1;
        if (a[c] > b[c])
            return 1;
    }
    return 0;
}

#ifdef __AVX2__
/* row_cmp_simd_avx2 - AVX2-accelerated lexicographic int64_t row comparison.
 *
 * Compares rows a and b (each ncols int64_t values) and returns -1, 0, or +1,
 * identical in semantics to row_cmp_lex().  Processes 4 elements per SIMD
 * iteration then falls back to scalar for the remainder.
 *
 * No alignment assumptions: unaligned loads (_mm256_loadu_si256) are used.
 */
static inline int
row_cmp_simd_avx2(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    uint32_t i = 0;

    /* Process 4 int64_t elements per iteration (256-bit vectors). */
    for (; i + 4 <= ncols; i += 4) {
        __m256i va = _mm256_loadu_si256((const __m256i *)(a + i));
        __m256i vb = _mm256_loadu_si256((const __m256i *)(b + i));

        /* eq_mask: 0xFFFFFFFFFFFFFFFF for equal lanes, 0 otherwise. */
        __m256i eq_mask = _mm256_cmpeq_epi64(va, vb);

        /* Collapse equality mask to 4-bit scalar (one bit per byte-group of 8).
         * movemask gives one bit per byte; equal lane -> 8 bits set -> 0xFF.
         * We check for a fully-equal lane by looking at 8-bit groups. */
        int eq_bits = _mm256_movemask_epi8(eq_mask); /* 32 bits, 8 per lane */

        if (eq_bits == (int)0xFFFFFFFF) {
            /* All 4 lanes are equal; continue to next chunk. */
            continue;
        }

        /* At least one lane differs.  Find the lowest-index differing lane.
         * eq_bits has 8 consecutive bits set for an equal lane.
         * Lane k occupies bits [8k .. 8k+7].  A differing lane has at least
         * one of those bits clear, so (~eq_bits) has a set bit in that range.
         */
        int neq = ~eq_bits;
        /* ctz gives the position of the first differing byte; divide by 8
         * gives the lane index within this 4-element chunk. */
        int lane = __builtin_ctz((unsigned int)neq) / 8;
        int64_t av = a[i + (uint32_t)lane];
        int64_t bv = b[i + (uint32_t)lane];
        return (av < bv) ? -1 : 1;
    }

    /* Scalar fallback for the remaining ncols % 4 elements. */
    for (; i < ncols; i++) {
        if (a[i] < b[i])
            return -1;
        if (a[i] > b[i])
            return 1;
    }
    return 0;
}
#endif /* __AVX2__ */

#ifdef __ARM_NEON__
/* row_cmp_simd_neon - NEON-accelerated lexicographic int64_t row comparison.
 *
 * Compares rows a and b (each ncols int64_t values) and returns -1, 0, or +1,
 * identical in semantics to row_cmp_lex().  Processes 2 elements per SIMD
 * iteration then falls back to scalar for the remainder.
 *
 * No alignment assumptions: unaligned loads (vld1q_s64) are used.
 */
static inline int
row_cmp_simd_neon(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    uint32_t i = 0;

    /* Process 2 int64_t elements per iteration (128-bit vectors). */
    for (; i + 2 <= ncols; i += 2) {
        int64x2_t va = vld1q_s64(a + i);
        int64x2_t vb = vld1q_s64(b + i);

        /* eq_mask: all-ones (0xFFFFFFFFFFFFFFFF) for equal lanes, 0 otherwise. */
        uint64x2_t eq_mask = vceqq_s64(va, vb);

        /* Optimized lane extraction: check lane 0 first, avoid ternary operator.
         * This improves instruction scheduling and reduces branch prediction stalls. */
        uint64_t eq0 = vgetq_lane_u64(eq_mask, 0);
        if (!eq0) {
            /* Lane 0 differs; extract and compare. */
            int64_t av = vgetq_lane_s64(va, 0);
            int64_t bv = vgetq_lane_s64(vb, 0);
            return (av < bv) ? -1 : 1;
        }

        /* Lane 0 is equal; check lane 1. */
        uint64_t eq1 = vgetq_lane_u64(eq_mask, 1);
        if (eq1) {
            /* Both lanes equal; continue to next pair. */
            continue;
        }

        /* Lane 1 differs; extract and compare. */
        int64_t av = vgetq_lane_s64(va, 1);
        int64_t bv = vgetq_lane_s64(vb, 1);
        return (av < bv) ? -1 : 1;
    }

    /* Scalar fallback for the remaining ncols % 2 element. */
    if (i < ncols) {
        if (a[i] < b[i])
            return -1;
        if (a[i] > b[i])
            return 1;
    }
    return 0;
}
#endif /* __ARM_NEON__ */

/* Dispatcher: Select best row comparison at compile time.
 * Automatically chooses AVX2, NEON, or scalar fallback.
 */
#ifdef __AVX2__
#define row_cmp_optimized row_cmp_simd_avx2
#elif defined(__ARM_NEON__)
#define row_cmp_optimized row_cmp_simd_neon
#else
#define row_cmp_optimized row_cmp_lex
#endif

/* Issue #279: Fully-unrolled specializations for the two most common widths. */
static inline int
row_cmp_ncols2(const int64_t *a, const int64_t *b)
{
    if (a[0] != b[0])
        return (a[0] < b[0]) ? -1 : 1;
    if (a[1] != b[1])
        return (a[1] < b[1]) ? -1 : 1;
    return 0;
}

static inline int
row_cmp_ncols4(const int64_t *a, const int64_t *b)
{
    if (a[0] != b[0])
        return (a[0] < b[0]) ? -1 : 1;
    if (a[1] != b[1])
        return (a[1] < b[1]) ? -1 : 1;
    if (a[2] != b[2])
        return (a[2] < b[2]) ? -1 : 1;
    if (a[3] != b[3])
        return (a[3] < b[3]) ? -1 : 1;
    return 0;
}

int
row_cmp_dispatch(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    if (ncols == 2)
        return row_cmp_ncols2(a, b);
    if (ncols == 4)
        return row_cmp_ncols4(a, b);
    /* Fallback: call the compile-time SIMD selection directly. */
#ifdef __AVX2__
    return row_cmp_simd_avx2(a, b, ncols);
#elif defined(__ARM_NEON__)
    return row_cmp_simd_neon(a, b, ncols);
#else
    return row_cmp_lex(a, b, ncols);
#endif
}

/* Issue #197: kway_row_cmp now delegates to row_cmp_dispatch so all 10+
 * call sites in the consolidate/merge hot paths use the SIMD dispatcher.
 * Issue #279: row_cmp_dispatch adds loop-free fast paths for ncols=2/4. */
static inline int
kway_row_cmp(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    return row_cmp_dispatch(a, b, ncols);
}

/* Compare a relation row against a raw row buffer (for merge operations
 * where one operand is in a temp buffer). Phase B, Issue #330.
 * Direct column access for cache efficiency (Issue #334). */
static inline int
col_rel_row_cmp_raw(const col_rel_t *r, uint32_t row_idx,
    const int64_t *raw_row)
{
    for (uint32_t c = 0; c < r->ncols; c++) {
        int64_t value = r->columns[c][row_idx];
        if (r->column_types && r->column_types[c] == WIRELOG_TYPE_FLOAT) {
            int cmp = wl_columnar_float_compare_bits(value, raw_row[c]);
            if (cmp != 0)
                return cmp;
            continue;
        }
        if (value < raw_row[c])
            return -1;
        if (value > raw_row[c])
            return 1;
    }
    return 0;
}

/*
 * col_op_consolidate_hash_dedup - Hash-based deduplication for consolidation.
 *
 * When the total row count greatly exceeds the unique count (common in
 * recursive Datalog joins), hash-based dedup is O(N) vs O(N * passes)
 * for radix sort + O(N) merge. After dedup, the small unique set is
 * sorted in place with heapsort.
 *
 * Returns 0 on success, -1 when the unique-count heuristic requests fallback,
 * and ENOMEM for allocation failures. The relation is untouched on failure.
 */
static uint32_t
col_op_consolidate_hash_row(const col_rel_t *rel, const int64_t *row,
    uint32_t nc)
{
    uint32_t hash = 2166136261u;
    for (uint32_t c = 0; c < nc; c++)
        hash = wl_columnar_hash_value(hash, rel, c, row[c]);
    return hash;
}

static bool
col_op_consolidate_hash_rows_equal(const col_rel_t *rel,
    const int64_t *left, const int64_t *right, uint32_t nc)
{
    for (uint32_t c = 0; c < nc; c++) {
        if (!wl_columnar_value_equal(rel, c, left[c], rel, c, right[c]))
            return false;
    }
    return true;
}

static int
col_op_consolidate_hash_rows_compare(const col_rel_t *rel,
    const int64_t *left, const int64_t *right, uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        if (rel->column_types
            && rel->column_types[c] == WIRELOG_TYPE_FLOAT) {
            int cmp = wl_columnar_float_compare_bits(left[c], right[c]);
            if (cmp != 0)
                return cmp;
        } else {
            if (left[c] < right[c])
                return -1;
            if (left[c] > right[c])
                return 1;
        }
    }
    return 0;
}

static void
col_op_consolidate_hash_rows_swap(int64_t *rows, uint32_t left,
    uint32_t right, uint32_t ncols)
{
    int64_t *left_row = rows + (size_t)left * ncols;
    int64_t *right_row = rows + (size_t)right * ncols;
    for (uint32_t c = 0; c < ncols; c++) {
        int64_t value = left_row[c];
        left_row[c] = right_row[c];
        right_row[c] = value;
    }
}

static void
col_op_consolidate_hash_rows_sift_down(const col_rel_t *rel, int64_t *rows,
    uint32_t root, uint32_t count, uint32_t ncols)
{
    while (root < count / 2u) {
        uint32_t child = root * 2u + 1u;
        if (child + 1u < count
            && col_op_consolidate_hash_rows_compare(rel,
            rows + (size_t)child * ncols,
            rows + (size_t)(child + 1u) * ncols, ncols) < 0)
            child++;
        if (col_op_consolidate_hash_rows_compare(rel,
            rows + (size_t)root * ncols,
            rows + (size_t)child * ncols, ncols) >= 0)
            return;
        col_op_consolidate_hash_rows_swap(rows, root, child, ncols);
        root = child;
    }
}

static void
col_op_consolidate_hash_rows_sort(const col_rel_t *rel, int64_t *rows,
    uint32_t count, uint32_t ncols)
{
    for (uint32_t root = count / 2u; root > 0;)
        col_op_consolidate_hash_rows_sift_down(rel, rows, --root, count,
            ncols);
    for (uint32_t end = count; end > 1u;) {
        col_op_consolidate_hash_rows_swap(rows, 0, --end, ncols);
        col_op_consolidate_hash_rows_sift_down(rel, rows, 0, end, ncols);
    }
}

static int
col_op_consolidate_hash_dedup(col_rel_t *rel,
    const wl_columnar_source_access_writer_t *writer,
    bool *alias_release_pending)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;
    size_t row_bytes = (size_t)nc * sizeof(int64_t);

    /* Hash table: open addressing, power-of-2 capacity */
    uint32_t ht_cap = 8192;
    uint32_t ht_mask = ht_cap - 1;
    int64_t *ht_vals = (int64_t *)col_op_consolidate_malloc(
        (size_t)ht_cap * row_bytes, "hash_table");
    uint8_t *ht_used = (uint8_t *)col_op_consolidate_calloc(ht_cap, 1,
            "hash_table_used");
    if (!ht_vals || !ht_used) {
        free(ht_vals);
        free(ht_used);
        return ENOMEM;
    }

    /* Unique row buffer (row-major flat array) */
    uint32_t uniq_cap = 4096;
    uint32_t uniq_count = 0;
    int64_t *uniq_buf = (int64_t *)col_op_consolidate_malloc(
        (size_t)uniq_cap * row_bytes, "hash_unique_rows");
    if (!uniq_buf) {
        free(ht_vals);
        free(ht_used);
        return ENOMEM;
    }

    int64_t _rb[COL_STACK_MAX];
    int64_t *rb = nc <= COL_STACK_MAX ? _rb
        : (int64_t *)col_op_consolidate_malloc((size_t)nc * sizeof(int64_t),
            "hash_row_scratch");
    if (!rb) {
        free(ht_vals);
        free(ht_used);
        free(uniq_buf);
        return ENOMEM;
    }

    for (uint32_t r = 0; r < nr; r++) {
        /* Read row from column-major storage */
        for (uint32_t c = 0; c < nc; c++)
            rb[c] = rel->columns[c][r];

        uint32_t h = col_op_consolidate_hash_row(rel, rb, nc);

        uint32_t slot = (uint32_t)(h & ht_mask);
        bool found = false;
        while (ht_used[slot]) {
            int64_t *sv = ht_vals + (size_t)slot * nc;
            if (col_op_consolidate_hash_rows_equal(rel, sv, rb, nc)) {
                found = true;
                break;
            }
            slot = (slot + 1) & ht_mask;
        }

        if (!found) {
            /* Check if rehash needed (load > 50%) */
            if (uniq_count * 2 >= ht_cap) {
                /* If unique count already > nr/4, hash dedup not worth it */
                if (uniq_count > nr / 4) {
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    return -1;
                }

                /* Rehash to 2x capacity */
                uint32_t new_cap = ht_cap * 2;
                uint32_t new_mask = new_cap - 1;
                int64_t *new_vals = (int64_t *)col_op_consolidate_malloc(
                    (size_t)new_cap * row_bytes, "hash_rehash_rows");
                uint8_t *new_used = (uint8_t *)col_op_consolidate_calloc(
                    new_cap, 1, "hash_rehash_used");
                if (!new_vals || !new_used) {
                    free(new_vals);
                    free(new_used);
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    return ENOMEM;
                }

                for (uint32_t s = 0; s < ht_cap; s++) {
                    if (ht_used[s]) {
                        int64_t *sv = ht_vals + (size_t)s * nc;
                        uint32_t rh = col_op_consolidate_hash_row(rel, sv, nc);
                        uint32_t ns = rh & new_mask;
                        while (new_used[ns])
                            ns = (ns + 1) & new_mask;
                        memcpy(new_vals + (size_t)ns * nc, sv,
                            row_bytes);
                        new_used[ns] = 1;
                    }
                }

                free(ht_vals);
                free(ht_used);
                ht_vals = new_vals;
                ht_used = new_used;
                ht_cap = new_cap;
                ht_mask = new_mask;

                /* Re-probe for current row in new table */
                slot = (uint32_t)(h & ht_mask);
                while (ht_used[slot])
                    slot = (slot + 1) & ht_mask;
            }

            /* Insert into hash table */
            memcpy(ht_vals + (size_t)slot * nc, rb, row_bytes);
            ht_used[slot] = 1;

            /* Grow unique buffer if needed */
            if (uniq_count >= uniq_cap) {
                uniq_cap *= 2;
                int64_t *nb = (int64_t *)col_op_consolidate_realloc(uniq_buf,
                        (size_t)uniq_cap * row_bytes, "hash_unique_grow");
                if (!nb) {
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    return ENOMEM;
                }
                uniq_buf = nb;
            }

            memcpy(uniq_buf + (size_t)uniq_count * nc, rb, row_bytes);
            uniq_count++;
        }
    }

    if (rb != _rb) free(rb);
    free(ht_vals);
    free(ht_used);

    /* Sort staged rows in place before publishing any mutation. */
    if (uniq_count > 1)
        col_op_consolidate_hash_rows_sort(rel, uniq_buf, uniq_count, nc);

    /* Detach only after hash/sort staging is complete.  All emitted rows
     * came from the validated source relation, so publish has no expected
     * validation failure and performs no allocation. */
    if (rel->col_shared) {
        int cow_rc = col_rel_cow_unshare_with_source_writer(rel, writer);
        if (cow_rc != 0) {
            free(uniq_buf);
            return cow_rc;
        }
        *alias_release_pending = true;
    }

    /* Publish the complete sorted result only after all fallible work. */
    for (uint32_t r = 0; r < uniq_count; r++)
        col_rel_row_copy_in_raw(rel, r, uniq_buf + (size_t)r * nc);
    rel->nrows = uniq_count;
    wl_columnar_relation_touch_view(rel);
    free(uniq_buf);

    return 0;
}

/*
 * col_op_consolidate_kway_merge - K-way merge with per-segment sort and dedup.
 *
 * Sorts each segment in-place, then merges K sorted segments using a min-heap.
 * Deduplicates on-the-fly during merge. Writes merged result back into rel.
 *
 * For K=1: just sort + dedup in-place.
 * For K>=2: min-heap merge with O(M log K) comparisons. K=2 uses a
 * stack-allocated heap to avoid a separate heap allocation.
 *
 * @rel            Relation containing K concatenated segments.
 * @seg_boundaries Array of (seg_count+1) offsets [s0, s1, ..., sK].
 * @seg_count      Number of segments K.
 * @return         0 on success; EBUSY if a source reader or storage alias
 *                 prevents exclusive mutation; ENOMEM on allocation failure.
 */
static int
col_op_consolidate_kway_merge_impl(col_rel_t *rel,
    const uint32_t *seg_boundaries, uint32_t seg_count,
    const wl_columnar_source_access_writer_t *writer,
    bool *alias_release_pending)
{
    typedef struct {
        uint32_t seg;    /* segment index */
        uint32_t cursor; /* current row index within rel->data */
        uint32_t end;    /* one-past-end row index for this segment */
    } heap_entry_t;

    if (!wl_columnar_relation_float_values_valid(rel))
        return EINVAL;
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr <= 1)
        return 0;
    if (!seg_boundaries || seg_count == 0
        || seg_boundaries[0] != 0 || seg_boundaries[seg_count] != nr)
        return EINVAL;
    for (uint32_t s = 0; s < seg_count; s++) {
        if (seg_boundaries[s] > seg_boundaries[s + 1]
            || seg_boundaries[s + 1] > nr)
            return EINVAL;
    }
    size_t segment_bytes;
    size_t heap_bytes;
    size_t row_bytes = 0;
    size_t merged_bytes = 0;
    size_t merged_alloc_bytes = 0;
    size_t segment_total_bytes;
    size_t additional_scratch_bytes;
    wl_columnar_radix_workspace_t sort_workspace = { 0 };
    uint32_t *seg_starts = NULL;
    uint32_t *seg_ends = NULL;
    int64_t *merged = NULL;
    heap_entry_t stack_heap[2];
    heap_entry_t *heap = stack_heap;
    heap_entry_t *heap_storage = NULL;
    int result = 0;
    if (!col_op_consolidate_size_multiply(seg_count, sizeof(uint32_t),
        &segment_bytes)
        || !col_op_consolidate_size_multiply(seg_count, sizeof(heap_entry_t),
        &heap_bytes))
        return ENOMEM;
    if (!col_op_consolidate_size_multiply(segment_bytes, 2u,
        &segment_total_bytes))
        return ENOMEM;
    additional_scratch_bytes = segment_total_bytes;
    if (seg_count >= 2) {
        /* Both the zero test inside col_op_consolidate_size_multiply's
         * `right != 0` guard and a `merged_bytes == 0` test would pin the
         * merge-output extent to a small constant, after which the
         * analyzer can no longer see that nc is the same width the buffer
         * was sized with, and reports the raw_row[c] reads in
         * col_rel_row_cmp_raw as out of bounds.  Two things keep it
         * symbolic, and they are pinned by different checks.  Keeping nr
         * -- known > 1 from the early return above -- as the right operand
         * is what the tidy ratchet guards: swapping it back reintroduces
         * clang-analyzer-security.ArrayBound, and merge.c is an
         * allowlisted file the ratchet holds clean.  Replacing the
         * unconditional add with a `merged_bytes == 0` test does the same.
         * Deleting the slack outright is analyzer-clean, so the ratchet
         * would not catch it; what it reintroduces is the zero-sized
         * allocation below, which the zero-arity merge test pins instead.
         *
         * The slack also earns its place without the analyzer: a zero-arity
         * relation (.decl p()) reaches this path with nc == 0, and
         * col_op_consolidate_malloc is a thin malloc() wrapper whose NULL
         * is read as exhaustion below, so a zero-sized merge output would
         * be a spurious ENOMEM on a libc where malloc(0) returns NULL.
         *
         * This is defensive, not curative: it keeps the extent symbolic
         * rather than giving the analyzer the width invariant that
         * col_rel_row_cmp_raw actually relies on.  Issue #1592 tracks
         * removing the need for both constructs. */
        if (!col_op_consolidate_size_multiply(nc, sizeof(int64_t),
            &row_bytes)
            || !col_op_consolidate_size_multiply(row_bytes, nr,
            &merged_bytes)
            || !col_op_consolidate_size_add(merged_bytes, sizeof(int64_t),
            &merged_alloc_bytes))
            return ENOMEM;
        if (!col_op_consolidate_size_add(additional_scratch_bytes,
            merged_alloc_bytes, &additional_scratch_bytes))
            return ENOMEM;
    }
    if (seg_count >= 3
        && !col_op_consolidate_size_add(additional_scratch_bytes, heap_bytes,
        &additional_scratch_bytes))
        return ENOMEM;

    /* Hash-based dedup for large datasets (#369): O(N) scan + O(U log U) sort
     * where U is the unique count.  When U << N (common in recursive Datalog
     * joins), this is much faster than sorting all N rows. */
    if (nr > 10000) {
        int rc = col_op_consolidate_hash_dedup(rel, writer,
                alias_release_pending);
        if (rc == 0)
            return 0;
        if (rc != -1)
            return rc;
        /* The unique-count heuristic requests the ordinary sort+merge path. */
    }

    int workspace_rc = wl_columnar_radix_workspace_prepare(rel,
            seg_boundaries, seg_count, additional_scratch_bytes,
            &sort_workspace);
    if (workspace_rc != 0)
        return workspace_rc;

    /* Allocate every buffer that can fail before the first in-place sort or
     * dedup mutation.  The workspace reservation includes these buffers. */
    seg_starts = (uint32_t *)col_op_consolidate_malloc(
        segment_bytes, "segment_starts");
    seg_ends = (uint32_t *)col_op_consolidate_malloc(
        segment_bytes, "segment_ends");
    if (seg_count >= 2)
        merged = (int64_t *)col_op_consolidate_malloc(
            merged_alloc_bytes, "merge_output");
    if (seg_count >= 3)
        heap_storage = (heap_entry_t *)col_op_consolidate_malloc(heap_bytes,
                "merge_heap");
    if (heap_storage)
        heap = heap_storage;
    if (!seg_starts || !seg_ends || (seg_count >= 2 && !merged)
        || (seg_count >= 3 && !heap_storage)) {
        result = ENOMEM;
        goto cleanup;
    }

    /* Every buffer allocation is now complete.  A shared view may detach
     * before the first segment mutation; later sorting uses only workspace. */
    if (rel->col_shared) {
        int cow_rc = col_rel_cow_unshare_with_source_writer(rel, writer);
        if (cow_rc != 0) {
            result = cow_rc;
            goto cleanup;
        }
        *alias_release_pending = true;
    }

    /* Sort each segment in-place using radix sort.
     * Optimization (#369): skip sort for already-sorted segments (e.g.,
     * from consolidated IDB reads). Also dedup within each segment after
     * sort to reduce merge input. Track per-segment unique counts. */

    for (uint32_t s = 0; s < seg_count; s++) {
        uint32_t start = seg_boundaries[s];
        uint32_t end = seg_boundaries[s + 1];
        uint32_t count = end - start;
        seg_starts[s] = start;

        if (count > 1) {
            /* Quick sorted-check: bail on first out-of-order pair */
            bool already_sorted = true;
            for (uint32_t r = start + 1; r < end; r++) {
                if (col_rel_row_cmp(rel, r - 1, r) > 0) {
                    already_sorted = false;
                    break;
                }
            }
            if (!already_sorted) {
                int sort_rc =
                    wl_columnar_relation_radix_sort_with_workspace(rel,
                        start, count, writer, &sort_workspace);
                if (sort_rc != 0) {
                    result = sort_rc;
                    goto cleanup;
                }
            }

            /* Intra-segment dedup: compact unique rows to reduce merge */
            uint32_t out_r = start + 1;
            for (uint32_t r = start + 1; r < end; r++) {
                if (col_rel_row_cmp(rel, out_r - 1, r) != 0) {
                    if (out_r != r)
                        col_rel_row_move_raw(rel, out_r, r);
                    out_r++;
                }
            }
            seg_ends[s] = out_r;
        } else {
            seg_ends[s] = end;
        }
    }

    /* K=1: already sorted+deduped by the loop above */
    if (seg_count == 1) {
        rel->nrows = seg_ends[0];
        wl_columnar_relation_touch_view(rel);
        goto cleanup;
    }

    /* Merge K sorted segments with a min-heap. K=2 uses stack storage;
     * larger merges use the admitted heap allocation.
     *
     * Heap entries: (segment_index, current_row_pointer).
     * Heap property: parent row <= child rows (lexicographic).
     */
    /* Build initial heap from non-empty segments */

    uint32_t heap_size = 0;
    for (uint32_t s = 0; s < seg_count; s++) {
        if (seg_starts[s] < seg_ends[s]) {
            heap[heap_size].seg = s;
            heap[heap_size].cursor = seg_starts[s];
            heap[heap_size].end = seg_ends[s];
            heap_size++;
        }
    }

    /* Sift-down helper using col_rel_row_cmp (Phase B, Issue #330) */
#define HEAP_SIFT_DOWN(start, size)                                          \
        do {                                                                 \
            uint32_t _p = (start);                                           \
            while (2 * _p + 1 < (size)) {                                    \
                uint32_t _c = 2 * _p + 1;                                    \
                if (_c + 1 < (size)                                          \
                    && col_rel_row_cmp(rel, heap[_c + 1].cursor,             \
                    heap[_c].cursor)                                  \
                    < 0)                                              \
                _c++;                                                    \
                if (col_rel_row_cmp(rel, heap[_p].cursor,                    \
                    heap[_c].cursor)                                     \
                    <= 0)                                                    \
                break;                                                   \
                heap_entry_t _tmp = heap[_p];                                \
                heap[_p] = heap[_c];                                         \
                heap[_c] = _tmp;                                             \
                _p = _c;                                                     \
            }                                                                \
        } while (0)

    /* Build min-heap (heapify) */
    if (heap_size > 1) {
        for (int32_t i = (int32_t)(heap_size / 2) - 1; i >= 0; i--)
            HEAP_SIFT_DOWN((uint32_t)i, heap_size);
    }

    /* Extract-min loop with dedup */
    uint32_t out = 0;
    int64_t *last_row = NULL;

    while (heap_size > 0) {
        /* Dedup: skip if same as last emitted row */
        if (last_row == NULL
            || col_rel_row_cmp_raw(rel, heap[0].cursor, last_row) != 0) {
            col_rel_row_copy_out(rel, heap[0].cursor,
                merged + (size_t)out * nc);
            last_row = merged + (size_t)out * nc;
            out++;
        }

        /* Advance cursor of min segment */
        heap[0].cursor++;
        if (heap[0].cursor >= heap[0].end) {
            /* Segment exhausted: replace root with last element */
            heap[0] = heap[heap_size - 1];
            heap_size--;
        }
        if (heap_size > 0)
            HEAP_SIFT_DOWN(0, heap_size);
    }

#undef HEAP_SIFT_DOWN

    /* Scatter flat merged buffer back into column-major */
    for (uint32_t r = 0; r < out; r++)
        col_rel_row_copy_in_raw(rel, r, merged + (size_t)r * nc);
    rel->nrows = out;
    wl_columnar_relation_touch_view(rel);

cleanup:
    free(merged);
    free(heap_storage);
    free(seg_starts);
    free(seg_ends);
    wl_columnar_radix_workspace_destroy(&sort_workspace);
    return result;
}

int
col_op_consolidate_kway_merge(col_rel_t *rel, const uint32_t *seg_boundaries,
    uint32_t seg_count)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    col_rel_t *owner = NULL;
    bool alias_release_pending = false;
    int rc;

    if (!rel)
        return EINVAL;
    rc = col_rel_source_writer_acquire(rel, &writer);
    if (rc != 0)
        return rc;
    if (!wl_columnar_relation_float_values_valid(rel)) {
        rc = EINVAL;
        goto release_writer;
    }
    if (rel->nrows <= 1) {
        rc = 0;
        goto release_writer;
    }
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0)
        goto release_writer;
    if (rel == owner
        && atomic_load_explicit(&owner->storage_alias_borrows,
        memory_order_acquire) > 0) {
        rc = EBUSY;
        goto release_writer;
    }

    rc = col_op_consolidate_kway_merge_impl(rel, seg_boundaries, seg_count,
            &writer, &alias_release_pending);
release_writer:
    if (alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(rel);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (wl_columnar_source_access_writer_release(&writer) != 0 && rc == 0)
        rc = EINVAL;
    return rc;
}

int
col_op_consolidate(eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e;
    int pop_rc = eval_stack_pop_relation(stack, &e);
    if (pop_rc != 0)
        return pop_rc;

    col_rel_t *in = e.rel;
    if (!wl_columnar_relation_float_values_valid(in)) {
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        if (e.owned)
            col_rel_destroy(in);
        return EINVAL;
    }
    uint32_t nc = in->ncols;
    uint32_t nr = in->nrows;

    if (nr <= 1) {
        /* Nothing to deduplicate */
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        in->sorted_nrows = nr;
        in->run_count = 1;
        in->run_ends[0] = nr;
        return eval_stack_push(stack, in, e.owned);
    }

    /* Sort in-place if we own the relation, otherwise copy first */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work = col_rel_pool_new_like(sess->delta_pool, "$consol", in);
        if (!work) {
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        int append_rc = col_rel_append_all(work, in, NULL);
        if (append_rc != 0) {
            col_rel_destroy(work);
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return append_rc;
        }
        work_owned = true;
    }

    /* Dispatch: K-way merge when segment boundaries are available */
    uint32_t k = e.seg_count > 0 ? e.seg_count : 1;
    if (k >= 2 && e.seg_boundaries != NULL) {
        int rc = col_op_consolidate_kway_merge(work, e.seg_boundaries, k);
        free(e.seg_boundaries);
        if (rc != 0) {
            if (work_owned)
                col_rel_destroy(work);
            return rc;
        }
        work->sorted_nrows = work->nrows;
        work->run_count = 1;
        work->run_ends[0] = work->nrows;
        return eval_stack_push(stack, work, work_owned);
    }

    if (e.seg_boundaries)
        free(e.seg_boundaries);

    /* Issue #94: Incremental merge when a sorted prefix exists.
     * data[0..sorted_nrows) is already sorted+unique from a prior
     * consolidation.  Sort only the unsorted suffix and merge. */
    uint32_t sn = work->sorted_nrows;
    if (sn > 0 && sn < nr) {
        uint32_t delta_count = nr - sn;

        /* Phase 1: sort only the unsorted suffix using radix sort */
        int sort_rc = col_rel_radix_sort(work, sn, delta_count);
        if (sort_rc != 0) {
            if (work_owned)
                col_rel_destroy(work);
            return sort_rc;
        }

        /* Phase 1b: dedup within suffix */
        uint32_t d_unique = 1;
        for (uint32_t i = 1; i < delta_count; i++) {
            if (col_rel_row_cmp(work, sn + i - 1, sn + i) != 0) {
                col_rel_row_move_raw(work, sn + d_unique, sn + i);
                d_unique++;
            }
        }

        /* Phase 2: merge sorted prefix with sorted suffix */
        uint32_t max_rows = sn + d_unique;

        /* Reuse persistent merge buffer when possible (column-major) */
        int64_t **merged_cols;
        bool used_merge_buf = false;
        if (work->merge_columns && work->merge_buf_cap >= max_rows) {
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        } else {
            /* Grow persistent buffer */
            uint32_t new_cap = max_rows > work->merge_buf_cap * 2
                                   ? max_rows
                                   : work->merge_buf_cap * 2;
            if (new_cap < max_rows)
                new_cap = max_rows;
            if (work->merge_columns) {
                if (col_columns_realloc(work->merge_columns, nc,
                    new_cap) != 0) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            } else {
                work->merge_columns = col_columns_alloc(nc, new_cap);
                if (!work->merge_columns) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            }
            work->merge_buf_cap = new_cap;
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        }

        uint32_t oi = 0, di = 0, out = 0;
        while (oi < sn && di < d_unique) {
            int cmp = col_rel_row_cmp(work, oi, sn + di);
            if (cmp < 0) {
                col_columns_copy_row(merged_cols, out, work->columns, oi, nc);
                oi++;
            } else if (cmp == 0) {
                col_columns_copy_row(merged_cols, out, work->columns, oi, nc);
                oi++;
                di++;
            } else {
                col_columns_copy_row(merged_cols, out, work->columns,
                    sn + di, nc);
                di++;
            }
            out++;
        }
        while (oi < sn) {
            col_columns_copy_row(merged_cols, out, work->columns, oi, nc);
            oi++;
            out++;
        }
        while (di < d_unique) {
            col_columns_copy_row(merged_cols, out, work->columns,
                sn + di, nc);
            di++;
            out++;
        }

        /* Swap merge_columns and columns to avoid O(N) memcpy (issue #218). */
        if (used_merge_buf) {
            int64_t **old_cols = work->columns;
            uint32_t old_cap = work->capacity;
            work->columns = work->merge_columns;
            work->capacity = work->merge_buf_cap;
            work->merge_columns = old_cols;
            work->merge_buf_cap = old_cap;
            wl_columnar_relation_touch_storage(work);
        }
        work->nrows = out;
        wl_columnar_relation_touch_view(work);
        work->sorted_nrows = out;
        work->run_count = 1;
        work->run_ends[0] = out;

        /* Right-size columns after dedup (issue #218). */
        if (out > 0 && work->capacity > out + out / 4) {
            uint32_t tight = out + out / 4;
            if (tight < COL_REL_INIT_CAP)
                tight = COL_REL_INIT_CAP;
            if (col_columns_realloc(work->columns, nc, tight) == 0) {
                work->capacity = tight;
                wl_columnar_relation_touch_storage(work);
            }
        }

        return eval_stack_push(stack, work, work_owned);
    }

    /* Fallback: radix sort + dedup (sorted_nrows == 0 or full re-sort).
     * The dedup below keeps only rows that differ from their predecessor,
     * which is a correct compaction only over a sorted relation, and it
     * publishes sorted_nrows unconditionally.  A refused sort must therefore
     * fail the operation rather than silently drop rows and mark the result
     * sorted. */
    {
        int sort_rc = col_rel_radix_sort_int64(work);
        if (sort_rc != 0) {
            /* Do not destroy work here.  The refusal that is most likely
             * to land here is EBUSY from a live source reader, and
             * col_rel_destroy_checked() refuses on exactly that predicate,
             * so the destroy would be a guaranteed no-op and work -- already
             * off the stack -- would leak unconditionally.  Handing it back
             * defers the free to the evaluator's drain, which frees it
             * whenever the reader has been released by then.  The drain's
             * own destroy can still be refused while a reader is live; that
             * residual belongs to eval_entry_dispose and is tracked
             * separately. */
            if (eval_stack_push(stack, work, work_owned) != 0 && work_owned)
                col_rel_destroy(work);
            return sort_rc;
        }
    }

    /* Compact: keep only unique rows */
    uint32_t out_r = 1; /* first row always kept */
    for (uint32_t r = 1; r < nr; r++) {
        if (col_rel_row_cmp(work, r - 1, r) != 0) {
            col_rel_row_move_raw(work, out_r, r);
            out_r++;
        }
    }
    work->nrows = out_r;
    wl_columnar_relation_touch_view(work);
    work->sorted_nrows = out_r;
    work->run_count = 1;
    work->run_ends[0] = out_r;

    return eval_stack_push(stack, work, work_owned);
}

/*
 * col_op_consolidate_incremental:
 * Incremental sort+dedup for semi-naive evaluation.
 *
 * Precondition: rel->data[0..old_nrows) is already sorted+unique from
 * the previous iteration's consolidation. New rows appended during this
 * iteration live in [old_nrows..rel->nrows).
 *
 * Algorithm:
 *   1. Sort only the delta rows: O(D log D)
 *   2. Dedup within delta: O(D)
 *   3. Merge sorted old with sorted delta, skipping duplicates: O(N + D)
 *
 * Total: O(D log D + N) vs O(N log N) for full re-sort.
 */
static int UNUSED
col_op_consolidate_incremental(col_rel_t *rel, uint32_t old_nrows)
{
    if (!wl_columnar_relation_float_values_valid(rel))
        return EINVAL;
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;

    if (nr <= 1 || old_nrows >= nr)
        return 0; /* nothing new or trivially sorted */

    uint32_t delta_count = nr - old_nrows;

    /* Phase 1: sort only the new delta rows using radix sort */
    int sort_rc = col_rel_radix_sort(rel, old_nrows, delta_count);
    if (sort_rc != 0)
        return sort_rc;

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (col_rel_row_cmp(rel, old_nrows + i - 1, old_nrows + i) != 0) {
            col_rel_row_move_raw(rel, old_nrows + d_unique, old_nrows + i);
            d_unique++;
        }
    }

    /* Phase 2: merge sorted old [0..old_nrows) with sorted+unique delta.
     * Allocate temporary buffer for merge output. */
    size_t max_rows = (size_t)old_nrows + d_unique;
    int64_t *merged = (int64_t *)malloc(max_rows * nc * sizeof(int64_t));
    if (!merged)
        return ENOMEM;

    uint32_t oi = 0, di = 0, out = 0;
    while (oi < old_nrows && di < d_unique) {
        int cmp = col_rel_row_cmp(rel, oi, old_nrows + di);
        if (cmp < 0) {
            col_rel_row_copy_out(rel, oi, merged + (size_t)out * nc);
            oi++;
            out++;
        } else if (cmp == 0) {
            col_rel_row_copy_out(rel, oi, merged + (size_t)out * nc);
            oi++;
            di++;
            out++; /* skip duplicate from delta */
        } else {
            col_rel_row_copy_out(rel, old_nrows + di,
                merged + (size_t)out * nc);
            di++;
            out++;
        }
    }
    /* Copy remaining from old */
    while (oi < old_nrows) {
        col_rel_row_copy_out(rel, oi, merged + (size_t)out * nc);
        oi++;
        out++;
    }
    /* Copy remaining from delta */
    while (di < d_unique) {
        col_rel_row_copy_out(rel, old_nrows + di,
            merged + (size_t)out * nc);
        di++;
        out++;
    }

    /* Scatter flat merged buffer back into column-major */
    for (uint32_t r = 0; r < out; r++)
        col_rel_row_copy_in_raw(rel, r, merged + (size_t)r * nc);
    free(merged);
    rel->nrows = out;
    wl_columnar_relation_touch_view(rel);
    return 0;
}

/*
 * col_rel_compact_runs - K-way merge of tiered sorted runs (#369).
 *
 * Merges all independently sorted+unique runs into a single sorted run.
 * Uses min-heap merge (runs are already sorted, no per-segment sort needed).
 * Writes merged result back into rel using flat buffer + scatter.
 *
 * @return 0 on success, ENOMEM on allocation failure.
 */
static int
col_rel_compact_runs(col_rel_t *rel)
{
    if (rel->run_count <= 1)
        return 0;

    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;
    uint32_t K = rel->run_count;

    /* Build segment boundaries from run_ends */
    uint32_t seg_bounds[COL_MAX_RUNS + 1];
    seg_bounds[0] = 0;
    for (uint32_t i = 0; i < K; i++)
        seg_bounds[i + 1] = rel->run_ends[i];

    /* Allocate merge output buffer (flat row-major) */
    int64_t *merged = (int64_t *)malloc((size_t)nr * nc * sizeof(int64_t));
    if (!merged)
        return ENOMEM;

    /* Min-heap entries (stack-allocated, K <= COL_MAX_RUNS = 8).
     *
     * lead_key[i] shadows columns[0][heap[i].cursor] so the inner
     * comparator can short-circuit on the leading column without
     * indirecting through col_rel_row_cmp() and its column-array
     * dereference.  When the leading keys differ (the common case for
     * sorted runs whose interleaving is determined by the leading
     * sort key), the heap relation falls out of a register-resident
     * scalar compare; only equal-leading rows pay the full row_cmp
     * cost.  lead_key is kept consistent with heap[].cursor at every
     * mutation: cursor advance, end-of-run replacement, and sift-down
     * swap. */
    typedef struct {
        uint32_t cursor;
        uint32_t end;
    } compact_he_t;

    compact_he_t heap[COL_MAX_RUNS];
    int64_t lead_key[COL_MAX_RUNS];
    uint32_t heap_size = 0;
    const int64_t *lead_col = rel->columns[0];

    for (uint32_t s = 0; s < K; s++) {
        if (seg_bounds[s] < seg_bounds[s + 1]) {
            heap[heap_size].cursor = seg_bounds[s];
            heap[heap_size].end = seg_bounds[s + 1];
            lead_key[heap_size] = lead_col[seg_bounds[s]];
            heap_size++;
        }
    }

#define COMPACT_LT_(_a, _b) \
        (lead_key[_a] < lead_key[_b]                                             \
        || (lead_key[_a] == lead_key[_b]                                        \
        && col_rel_row_cmp(rel, heap[_a].cursor, heap[_b].cursor) < 0))

#define COMPACT_LE_(_a, _b) \
        (lead_key[_a] < lead_key[_b]                                             \
        || (lead_key[_a] == lead_key[_b]                                        \
        && col_rel_row_cmp(rel, heap[_a].cursor, heap[_b].cursor) <= 0))

#define COMPACT_SIFT_DOWN(start, size)                                       \
        do {                                                                     \
            uint32_t _p = (start);                                               \
            while (2 * _p + 1 < (size)) {                                        \
                uint32_t _c = 2 * _p + 1;                                        \
                if (_c + 1 < (size) && COMPACT_LT_(_c + 1, _c))                  \
                _c++;                                                            \
                if (COMPACT_LE_(_p, _c))                                         \
                break;                                                           \
                compact_he_t _tmp = heap[_p];                                    \
                heap[_p] = heap[_c];                                             \
                heap[_c] = _tmp;                                                 \
                int64_t _kt = lead_key[_p];                                      \
                lead_key[_p] = lead_key[_c];                                     \
                lead_key[_c] = _kt;                                              \
                _p = _c;                                                         \
            }                                                                    \
        } while (0)

    /* Build min-heap */
    if (heap_size > 1) {
        for (int32_t i = (int32_t)(heap_size / 2) - 1; i >= 0; i--)
            COMPACT_SIFT_DOWN((uint32_t)i, heap_size);
    }

    /* Extract-min loop (cross-run uniqueness is guaranteed, but dedup
     * defensively in case of edge cases) */
    uint32_t out = 0;
    int64_t *last_row = NULL;

    while (heap_size > 0) {
        if (last_row == NULL
            || col_rel_row_cmp_raw(rel, heap[0].cursor, last_row) != 0) {
            col_rel_row_copy_out(rel, heap[0].cursor,
                merged + (size_t)out * nc);
            last_row = merged + (size_t)out * nc;
            out++;
        }
        heap[0].cursor++;
        if (heap[0].cursor >= heap[0].end) {
            heap[0] = heap[heap_size - 1];
            lead_key[0] = lead_key[heap_size - 1];
            heap_size--;
        } else {
            lead_key[0] = lead_col[heap[0].cursor];
        }
        if (heap_size > 0)
            COMPACT_SIFT_DOWN(0, heap_size);
    }

#undef COMPACT_SIFT_DOWN
#undef COMPACT_LT_
#undef COMPACT_LE_

    /* Scatter flat merged buffer back into column-major */
    for (uint32_t r = 0; r < out; r++)
        col_rel_row_copy_in_raw(rel, r, merged + (size_t)r * nc);
    free(merged);

    rel->nrows = out;
    wl_columnar_relation_touch_view(rel);
    rel->sorted_nrows = out;
    rel->run_count = 1;
    rel->run_ends[0] = out;
    return 0;
}

/*
 * col_op_consolidate_incremental_delta - Incremental consolidation with delta output
 *
 * PURPOSE:
 *   Merge pre-sorted old data with newly appended delta rows, while simultaneously
 *   emitting the set of truly-new rows (R_new - R_old) as a byproduct.
 *   This eliminates separate post-iteration merge walk needed for delta computation.
 *
 * PRECONDITIONS:
 *   - rel->data[0..old_nrows) is already sorted and unique (invariant)
 *   - rel->data[old_nrows..rel->nrows) contains newly appended delta rows (unsorted)
 *   - old_nrows <= rel->nrows
 *
 * POSTCONDITIONS:
 *   - rel->data[0..rel->nrows) is sorted and unique (new invariant)
 *   - delta_out->data contains exactly R_new - R_old (truly new rows)
 *   - delta_out->data is sorted in same order as rel->data
 *   - rel->nrows reflects final merged count
 *
 * ALGORITHM (#369):
 *   Binary-search dedup with tiered sorted runs when D << N.
 *   Falls back to 2-pointer merge when D is large relative to N.
 *   Compacts all runs via K-way merge when run_count >= COL_MAX_RUNS.
 *
 * ALGORITHM COMPLEXITY:
 *   - Time: O(D log D + D) fast-path (all delta > all old, common for CRDT)
 *           O(D log D + D*K*log(N/K)) binary-search path (D << N)
 *           O(D log D + N + D) fallback merge walk (D ~ N)
 *   - Space: O(N + D) for merge buffer (fallback) or O(D) (binary-search path)
 */
static int
col_op_consolidate_incremental_delta_fail(col_rel_t *delta_out,
    uint32_t initial_nrows, int rc)
{
    if (delta_out && delta_out->nrows != initial_nrows) {
        delta_out->nrows = initial_nrows;
        wl_columnar_relation_touch_view(delta_out);
    }
    return rc;
}

static int
col_op_consolidate_incremental_delta_append_locked(col_rel_t *delta_out,
    const int64_t *row, wl_columnar_source_access_writer_t *delta_writer,
    bool *test_hook_called)
{
#ifdef WL_TEST_CONSOLIDATE_HOOK
    if (test_hook_called && !*test_hook_called
        && wl_columnar_consolidation_transition_hook) {
        *test_hook_called = true;
        wl_columnar_consolidation_transition_hook(delta_out,
            WL_COLUMNAR_CONSOLIDATION_TEST_APPEND_AFTER_DETACH);
    }
#else
    (void)test_hook_called;
#endif
    return col_rel_append_row_locked(delta_out, row, delta_writer);
}

static int
col_op_consolidate_incremental_delta_impl(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out, int *out_fast_path,
    wl_columnar_source_access_writer_t *delta_writer,
    const wl_columnar_source_access_writer_t *rel_writer,
    bool *out_rel_alias_release_pending)
{
    if (!out_rel_alias_release_pending)
        return EINVAL;
    *out_rel_alias_release_pending = false;
    if (!wl_columnar_relation_float_values_valid(rel)
        || (delta_out && !wl_columnar_relation_float_values_valid(delta_out)))
        return EINVAL;
    if (delta_out == rel)
        return EINVAL;
    uint32_t delta_initial_nrows = delta_out ? delta_out->nrows : 0;
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;
    bool delta_append_hook_called = false;

    if (nr == 0 || old_nrows >= nr) {
        if (out_fast_path)
            *out_fast_path = 1; /* trivially fast: no data to process */
        return 0;              /* nothing new */
    }

    uint32_t delta_count = nr - old_nrows;

    /* Phase 1: sort only the new delta rows using radix sort.  Sorting can
     * allocate permutation buffers, so do not continue with an unsorted
     * source if admission fails. */
    /* rel_writer, never delta_writer: the latter is a lease on delta_out's
     * canonical owner, which is a different gate whenever the two relations
     * do not share one. */
    int sort_rc = col_rel_radix_sort_locked(rel, old_nrows, delta_count,
            rel_writer, true, out_rel_alias_release_pending);
    if (sort_rc != 0)
        return col_op_consolidate_incremental_delta_fail(delta_out,
                   delta_initial_nrows, sort_rc);

    /* Phase 1b: dedup within delta */
    uint32_t d_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (col_rel_row_cmp(rel, old_nrows + i - 1, old_nrows + i) != 0) {
            col_rel_row_move_raw(rel, old_nrows + d_unique, old_nrows + i);
            d_unique++;
        }
    }

    /* Initialize or repair run tracking (#369, #376).
     * After retraction/re-eval the relation may be cleared (nrows reduced)
     * without resetting run_count/run_ends, leaving stale metadata.
     * Normalize to a single run covering [0, old_nrows) when inconsistent. */
    if (rel->run_count == 0
        || rel->run_ends[rel->run_count - 1] != old_nrows) {
        rel->run_count = (old_nrows > 0) ? 1 : 0;
        if (old_nrows > 0)
            rel->run_ends[0] = old_nrows;
    }

    /* Fast-path (Issue #239): if all delta rows sort after max of all runs,
     * skip the merge/search and directly append as new run. */
    int fast_path = 0;
    if (old_nrows == 0) {
        fast_path = 1;
    } else {
        /* Check against max (last row) of ALL runs (#369 C1) */
        bool all_less = true;
        for (uint32_t i = 0; i < rel->run_count && all_less; i++) {
            uint32_t end = rel->run_ends[i];
            if (end > 0
                && col_rel_row_cmp(rel, end - 1, old_nrows) >= 0)
                all_less = false;
        }
        if (all_less)
            fast_path = 1;
    }

    if (fast_path) {
        /* All d_unique rows are novel. Emit to delta_out and append as run. */
        if (delta_out) {
            col_row_buf_t drb;
            int64_t *const dr = col_row_buf_init(&drb, nc);
            if (!dr)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, ENOMEM);
            for (uint32_t k = 0; k < d_unique; k++) {
                for (uint32_t c = 0; c < nc; c++)
                    dr[c] = rel->columns[c][old_nrows + k];
                int rc = col_op_consolidate_incremental_delta_append_locked(
                    delta_out, dr, delta_writer,
                    &delta_append_hook_called);
                if (rc != 0) {
                    col_row_buf_release(&drb);
                    return col_op_consolidate_incremental_delta_fail(
                        delta_out, delta_initial_nrows, rc);
                }
            }
            col_row_buf_release(&drb);
        }
        rel->nrows = old_nrows + d_unique;
        wl_columnar_relation_touch_view(rel);
        rel->sorted_nrows = rel->nrows;

        /* Register as new run */
        if (rel->run_count < COL_MAX_RUNS) {
            rel->run_ends[rel->run_count] = rel->nrows;
            rel->run_count++;
        } else {
            /* Temporarily extend last run to include new data, then compact */
            rel->run_ends[rel->run_count - 1] = rel->nrows;
            int rc = col_rel_compact_runs(rel);
            if (rc != 0)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, rc);
        }

        if (rel->timestamps) {
            free(rel->timestamps);
            rel->timestamps = NULL;
            wl_columnar_relation_touch_storage(rel);
        }
        if (out_fast_path)
            *out_fast_path = 1;
        return 0;
    }

    /* Adaptive dispatch (#369): use binary-search dedup when D << N,
     * fall back to 2-pointer merge when D is large (first iterations). */
    if (d_unique <= old_nrows / 16 && rel->run_count > 0) {
        /* Binary-search dedup path: O(D * K * log(N/K)) */
        uint32_t novel_count = 0;
        col_row_buf_t drb = { 0 };
        int64_t *const dr = delta_out ? col_row_buf_init(&drb, nc) : NULL;
        if (delta_out && !dr)
            return col_op_consolidate_incremental_delta_fail(delta_out,
                       delta_initial_nrows, ENOMEM);

        for (uint32_t i = 0; i < d_unique; i++) {
            uint32_t row_idx = old_nrows + i;
            bool found = false;

            /* Search each existing run */
            for (uint32_t r = 0; r < rel->run_count && !found; r++) {
                uint32_t run_start = (r == 0) ? 0 : rel->run_ends[r - 1];
                uint32_t run_end = rel->run_ends[r];
                if (col_rel_binary_search_row(rel, run_start, run_end,
                    row_idx))
                    found = true;
            }

            if (!found) {
                /* Novel row: compact to front of delta region */
                if (novel_count != i)
                    col_rel_row_move_raw(rel, old_nrows + novel_count, row_idx);
                if (delta_out) {
                    for (uint32_t c = 0; c < nc; c++)
                        dr[c] = rel->columns[c][old_nrows + novel_count];
                    int rc
                        = col_op_consolidate_incremental_delta_append_locked(
                            delta_out, dr, delta_writer,
                            &delta_append_hook_called);
                    if (rc != 0) {
                        col_row_buf_release(&drb);
                        return col_op_consolidate_incremental_delta_fail(
                            delta_out, delta_initial_nrows, rc);
                    }
                }
                novel_count++;
            }
        }
        col_row_buf_release(&drb);

        if (novel_count > 0) {
            rel->nrows = old_nrows + novel_count;
            wl_columnar_relation_touch_view(rel);
            /* Register novel rows as new run */
            if (rel->run_count < COL_MAX_RUNS) {
                rel->run_ends[rel->run_count] = rel->nrows;
                rel->run_count++;
            } else {
                /* Compact existing runs, preserving novel rows (#376).
                 * Novel rows at [old_nrows..old_nrows+novel_count) are not
                 * in any run yet.  compact_runs only merges run-bounded
                 * data so novel rows are physically untouched.  Relocate
                 * them adjacent to the compacted prefix afterwards. */
                int rc = col_rel_compact_runs(rel);
                if (rc != 0)
                    return col_op_consolidate_incremental_delta_fail(
                        delta_out, delta_initial_nrows, rc);
                uint32_t compacted = rel->nrows;
                for (uint32_t j = 0; j < novel_count; j++)
                    col_rel_row_move_raw(rel, compacted + j, old_nrows + j);
                rel->nrows = compacted + novel_count;
                wl_columnar_relation_touch_view(rel);
                rel->run_ends[rel->run_count] = rel->nrows;
                rel->run_count++;
            }
        } else {
            rel->nrows = old_nrows; /* no new rows */
            wl_columnar_relation_touch_view(rel);
        }
        rel->sorted_nrows = rel->nrows;

        if (rel->timestamps) {
            free(rel->timestamps);
            rel->timestamps = NULL;
        }
        if (out_fast_path)
            *out_fast_path = 0;
        return 0;
    }

    /* Fallback: 2-pointer merge when D is large relative to N.
     * After merge, reset to single run. */
    uint32_t max_rows = old_nrows + d_unique;

    if (rel->merge_buf_cap < max_rows) {
        uint32_t new_cap = max_rows > rel->merge_buf_cap * 2
                               ? max_rows
                               : rel->merge_buf_cap * 2;
        if (new_cap < max_rows)
            new_cap = max_rows;
        if (nc == 0) {
            rel->merge_buf_cap = new_cap;
        } else if (rel->merge_columns) {
            if (col_columns_realloc(rel->merge_columns, nc, new_cap) != 0)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, ENOMEM);
        } else {
            rel->merge_columns = col_columns_alloc(nc, new_cap);
            if (!rel->merge_columns)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, ENOMEM);
        }
        rel->merge_buf_cap = new_cap;
    }
    int64_t **merged_cols = rel->merge_columns;

    col_row_buf_t delta_rb;
    if (!col_row_buf_init(&delta_rb, nc))
        return col_op_consolidate_incremental_delta_fail(delta_out,
                   delta_initial_nrows, ENOMEM);
    int64_t *delta_row = delta_rb.ptr;

    /* For fallback merge, we need a single sorted prefix.
     * If multiple runs exist, compact first (#377 fix).
     * compact_runs only merges run-bounded data; delta rows at
     * [old_nrows..old_nrows+d_unique) are physically untouched.
     * Relocate them adjacent to the compacted prefix afterwards. */
    if (rel->run_count > 1) {
        uint32_t delta_phys = old_nrows; /* physical location of delta */
        int rc = col_rel_compact_runs(rel);
        if (rc != 0) {
            col_row_buf_release(&delta_rb);
            return col_op_consolidate_incremental_delta_fail(delta_out,
                       delta_initial_nrows, rc);
        }
        uint32_t compacted = rel->nrows;
        for (uint32_t j = 0; j < d_unique; j++)
            col_rel_row_move_raw(rel, compacted + j, delta_phys + j);
        old_nrows = compacted;
        rel->nrows = compacted + d_unique;
        wl_columnar_relation_touch_view(rel);
    }

    uint32_t oi = 0, di = 0, out = 0;

    while (oi < old_nrows && di < d_unique) {
        int cmp = col_rel_row_cmp(rel, oi, old_nrows + di);

        if (cmp == 0) {
            col_columns_copy_row(merged_cols, out, rel->columns, oi, nc);
            oi++;
            di++;
        } else if (cmp < 0) {
            col_columns_copy_row(merged_cols, out, rel->columns, oi, nc);
            oi++;
        } else {
            col_columns_copy_row(merged_cols, out, rel->columns,
                old_nrows + di, nc);
            if (delta_out) {
                for (uint32_t c = 0; c < nc; c++)
                    delta_row[c] = merged_cols[c][out];
                int rc = col_op_consolidate_incremental_delta_append_locked(
                    delta_out, delta_row, delta_writer,
                    &delta_append_hook_called);
                if (rc != 0) {
                    col_row_buf_release(&delta_rb);
                    return col_op_consolidate_incremental_delta_fail(
                        delta_out, delta_initial_nrows, rc);
                }
            }
            di++;
        }
        out++;
    }
    while (oi < old_nrows) {
        col_columns_copy_row(merged_cols, out, rel->columns, oi, nc);
        oi++;
        out++;
    }
    while (di < d_unique) {
        col_columns_copy_row(merged_cols, out, rel->columns,
            old_nrows + di, nc);
        if (delta_out) {
            for (uint32_t c = 0; c < nc; c++)
                delta_row[c] = merged_cols[c][out];
            int rc = col_op_consolidate_incremental_delta_append_locked(
                delta_out, delta_row, delta_writer,
                &delta_append_hook_called);
            if (rc != 0) {
                col_row_buf_release(&delta_rb);
                return col_op_consolidate_incremental_delta_fail(
                    delta_out, delta_initial_nrows, rc);
            }
        }
        di++;
        out++;
    }

    col_row_buf_release(&delta_rb);

    /* Swap merge_columns and columns to avoid O(N) memcpy (issue #218). */
    {
        int64_t **old_cols = rel->columns;
        uint32_t old_cap = rel->capacity;
        rel->columns = rel->merge_columns;
        rel->capacity = rel->merge_buf_cap;
        rel->merge_columns = old_cols;
        rel->merge_buf_cap = old_cap;
        wl_columnar_relation_touch_storage(rel);
    }
    rel->nrows = out;
    wl_columnar_relation_touch_view(rel);
    rel->sorted_nrows = out;
    rel->run_count = 1;
    rel->run_ends[0] = out;

    /* Phase 3b: Right-size columns after dedup (issue #218). */
    if (out > 0 && rel->capacity > out + out / 4) {
        uint32_t tight = out + out / 4;
        if (tight < COL_REL_INIT_CAP)
            tight = COL_REL_INIT_CAP;
        if (col_columns_realloc(rel->columns, nc, tight) == 0)
            wl_columnar_relation_touch_storage(rel);
        rel->capacity = tight;
    }

    if (rel->timestamps) {
        free(rel->timestamps);
        rel->timestamps = NULL;
        wl_columnar_relation_touch_storage(rel);
    }
    if (out_fast_path)
        *out_fast_path = 0;
    return 0;
}

int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out, int *out_fast_path)
{
    col_rel_t *rel_owner = NULL;
    col_rel_t *delta_owner = NULL;
    wl_columnar_source_access_writer_t rel_writer = { 0 };
    wl_columnar_source_access_writer_t delta_writer = { 0 };
    wl_columnar_source_access_writer_t *delta_writer_ptr = NULL;
    bool rel_acquired = false;
    bool delta_acquired = false;
    bool rel_alias_release_pending = false;
    bool delta_alias_release_pending = false;
    int rc;

    if (!wl_columnar_relation_float_values_valid(rel)
        || (delta_out && !wl_columnar_relation_float_values_valid(delta_out)))
        return EINVAL;
    if (delta_out == rel
        || (delta_out && delta_out->ncols != rel->ncols))
        return EINVAL;
    rc = col_rel_storage_owner_resolve(rel, &rel_owner);
    if (rc != 0)
        return rc;
    if (rel_owner == rel
        && atomic_load_explicit(&rel->storage_alias_borrows,
        memory_order_acquire) > 0)
        return EBUSY;
    if (delta_out) {
        rc = col_rel_storage_owner_resolve(delta_out, &delta_owner);
        if (rc != 0)
            return rc;
        if (delta_owner == delta_out
            && atomic_load_explicit(&delta_out->storage_alias_borrows,
            memory_order_acquire) > 0)
            return EBUSY;
    }

    /* Acquire the two canonical owners in address order.  The same owner is
     * acquired only once when rel and delta_out alias the same relation. */
    if (!delta_owner || rel_owner == delta_owner) {
        rc = wl_columnar_source_access_writer_acquire(
            &rel_owner->source_access, &rel_writer);
        if (rc != 0)
            return rc;
        rel_acquired = true;
        delta_writer_ptr = delta_out ? &rel_writer : NULL;
    } else if ((uintptr_t)rel_owner < (uintptr_t)delta_owner) {
        rc = wl_columnar_source_access_writer_acquire(
            &rel_owner->source_access, &rel_writer);
        if (rc != 0)
            return rc;
        rel_acquired = true;
        rc = wl_columnar_source_access_writer_acquire(
            &delta_owner->source_access, &delta_writer);
        if (rc != 0)
            goto cleanup;
        delta_acquired = true;
        delta_writer_ptr = &delta_writer;
    } else {
        rc = wl_columnar_source_access_writer_acquire(
            &delta_owner->source_access, &delta_writer);
        if (rc != 0)
            return rc;
        delta_acquired = true;
        rc = wl_columnar_source_access_writer_acquire(
            &rel_owner->source_access, &rel_writer);
        if (rc != 0)
            goto cleanup;
        rel_acquired = true;
        delta_writer_ptr = &delta_writer;
    }

    if (delta_out) {
        uint32_t delta_count = rel->nrows > old_nrows
            ? rel->nrows - old_nrows : 0;
        rc = col_rel_reserve_rows_locked(delta_out, delta_count,
                delta_writer_ptr, &delta_alias_release_pending);
        if (rc != 0)
            goto cleanup;
    }

    rc = col_op_consolidate_incremental_delta_impl(rel, old_nrows,
            delta_out, out_fast_path, delta_writer_ptr, &rel_writer,
            &rel_alias_release_pending);

cleanup:
    /* Keep each old canonical-owner gate closed until every mutation is
     * complete, then end the deferred aliases before releasing its writer. */
    if (delta_alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(delta_out);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (rel_alias_release_pending) {
        int alias_rc = col_rel_storage_alias_release(rel);
        if (alias_rc != 0 && rc == 0)
            rc = alias_rc;
    }
    if (delta_acquired
        && wl_columnar_source_access_writer_release(&delta_writer) != 0
        && rc == 0)
        rc = EINVAL;
    if (rel_acquired
        && wl_columnar_source_access_writer_release(&rel_writer) != 0
        && rc == 0)
        rc = EINVAL;
    return rc;
}
