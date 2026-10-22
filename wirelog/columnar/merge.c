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

#ifdef WL_SESSION_TEST_HOOKS
bool wl_columnar_merge_test_fail_copy_alloc;
bool wl_columnar_merge_test_fail_copy_append;
int (*wl_columnar_merge_test_after_cons_copy)(col_rel_t *);
bool wl_columnar_merge_test_fail_concat_boundaries_alloc;
#endif

static void *
wl_columnar_merge_concat_boundaries_alloc(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_merge_test_fail_concat_boundaries_alloc) {
        wl_columnar_merge_test_fail_concat_boundaries_alloc = false;
        return NULL;
    }
#endif
    return malloc(bytes);
}

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

static int
col_rel_merge_scratch_reserve(col_rel_t *rel, uint64_t bytes,
    wl_columnar_memory_reservation_t *reservation)
{
    wl_columnar_memory_admission_status_t status;
    if (!reservation)
        return EINVAL;
    wl_columnar_memory_reservation_init(reservation);
    if (!rel || !rel->memory_governor || bytes == 0)
        return 0;
    status = wl_columnar_memory_reserve_checked(
        wl_columnar_memory_governor_ref_get(rel->memory_governor), bytes,
        reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED) {
        rel->memory_budget_denial_pending = true;
        return ENOMEM;
    }
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

static void
col_rel_merge_scratch_release(wl_columnar_memory_reservation_t *reservation)
{
    if (reservation)
        wl_columnar_memory_release(reservation);
}

static int
col_op_concat_cleanup(eval_stack_t *stack, eval_entry_t *a,
    eval_entry_t *b, int primary_rc)
{
    int cleanup_rc;

    cleanup_rc = eval_stack_dispose_entry(stack, a);
    if (primary_rc == 0 && cleanup_rc != 0)
        primary_rc = cleanup_rc;
    cleanup_rc = eval_stack_dispose_entry(stack, b);
    if (primary_rc == 0 && cleanup_rc != 0)
        primary_rc = cleanup_rc;
    return primary_rc;
}

static int
col_op_dispose_entry_primary(eval_stack_t *stack, eval_entry_t *entry,
    int primary_rc)
{
    int cleanup_rc = eval_stack_dispose_entry(stack, entry);
    return primary_rc != 0 ? primary_rc : cleanup_rc;
}

/* Before input consumption starts, both popped slots remain available and
 * the candidate has never been published or borrowed. Preserve complete
 * entries, including segment ownership, for a retry of the same operator. */
static int
wl_columnar_merge_concat_abort(eval_stack_t *stack, wl_col_session_t *sess,
    eval_entry_t *a, eval_entry_t *b, col_rel_t *candidate, int rc)
{
    if (rc == ENOMEM && candidate && candidate->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC && sess)
        sess->memory_budget_denied = true;
    col_rel_destroy(candidate);
    int restore_rc = eval_stack_repush_entry(stack, a);
    if (restore_rc == 0)
        restore_rc = eval_stack_repush_entry(stack, b);
    return restore_rc != 0 ? restore_rc : rc;
}

static int
col_op_cleanup_owned_relation(eval_stack_t *stack, eval_entry_t *entry,
    col_rel_t *rel, bool owned, int primary_rc)
{
    int cleanup_rc;

    if (!owned)
        return primary_rc;
    cleanup_rc = col_rel_destroy_checked(rel);
    if (cleanup_rc == 0) {
        free(entry->seg_boundaries);
        entry->seg_boundaries = NULL;
        entry->seg_count = 0;
        return primary_rc;
    }
    if (cleanup_rc != 0) {
        eval_entry_t retained = *entry;
        int push_rc;
        retained.rel = rel;
        retained.owned = true;
        push_rc = eval_stack_repush_entry(stack, &retained);
        if (push_rc != 0)
            return primary_rc != 0 ? primary_rc : push_rc;
        if (primary_rc == 0)
            primary_rc = cleanup_rc;
    }
    return primary_rc;
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
        return col_op_concat_cleanup(stack, &(eval_entry_t){ 0 }, &b_e,
                   pop_rc);
    }
    col_rel_t *a = a_e.rel;
    col_rel_t *b = b_e.rel;

    if (!a || !b || a->ncols != b->ncols) {
        return col_op_concat_cleanup(stack, &a_e, &b_e, EINVAL);
    }

#ifdef WL_PROFILE
    uint64_t _t0_concat = now_ns();
    sess->profile.concat_calls++;
#endif

    wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
        ? sess->memory_governor : a->memory_governor
        ? a->memory_governor : b->memory_governor;
    col_rel_t *out = NULL;
    int rc = wl_columnar_relation_pool_new_like_governed_checked(&out,
            sess->delta_pool, "$concat", a, governor);
    if (rc != 0)
        return wl_columnar_merge_concat_abort(stack, sess, &a_e, &b_e,
                   out, rc);

    rc = col_rel_append_all(out, a, NULL);
    if (rc == 0)
        rc = col_rel_append_all(out, b, NULL);

    if (rc != 0)
        return wl_columnar_merge_concat_abort(stack, sess, &a_e, &b_e,
                   out, rc);

    /* Track segment boundaries for K-way merge optimization. */
    uint32_t left_segs = a_e.seg_count > 0 ? a_e.seg_count : 1;
    uint32_t right_segs = b_e.seg_count > 0 ? b_e.seg_count : 1;
    uint32_t total_segs = left_segs + right_segs;

    uint32_t *out_boundaries
        = (uint32_t *)wl_columnar_merge_concat_boundaries_alloc(
            (total_segs + 1) * sizeof(uint32_t));
    if (!out_boundaries)
        return wl_columnar_merge_concat_abort(stack, sess, &a_e, &b_e,
                   out, ENOMEM);

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

    /* Clean up input relations and boundaries before publishing the output.
     * A refused checked destroy puts the exact entry back on @stack. */
    rc = col_op_concat_cleanup(stack, &a_e, &b_e, 0);
    if (rc != 0) {
        free(out_boundaries);
        col_rel_destroy(out);
        return rc;
    }

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
    const int64_t *raw_row, uint32_t row_width)
{
    for (uint32_t c = 0; c < row_width; c++) {
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
    wl_columnar_relation_mutation_lease_t *lease)
{
    uint32_t nc = rel->ncols;
    uint32_t nr = rel->nrows;
    size_t row_bytes;
    if (!col_op_consolidate_size_multiply(nc, sizeof(int64_t), &row_bytes))
        return EOVERFLOW;
    size_t allocated_row_bytes = row_bytes ? row_bytes : sizeof(int64_t);
    uint64_t scratch_bytes = 0;
    uint64_t bytes = 0;
    uint64_t max_ht_cap = 8192;
    uint64_t max_uniq_cap = 4096;
    uint64_t max_hash_live_cap;
    uint64_t max_uniq_live_cap;
    wl_columnar_memory_reservation_t reservation;

    while (max_ht_cap <= (uint64_t)nr * 2u
        && max_ht_cap <= UINT32_MAX / 2u)
        max_ht_cap *= 2u;
    while (max_uniq_cap < nr && max_uniq_cap <= UINT32_MAX / 2u)
        max_uniq_cap *= 2u;
    if (max_uniq_cap < nr)
        return EOVERFLOW;
    /* Rehash allocates the doubled table while the old table remains live. */
    max_hash_live_cap = max_ht_cap
        + (max_ht_cap > 8192u ? max_ht_cap / 2u : 0u);
    max_uniq_live_cap = max_uniq_cap
        + (max_uniq_cap > 4096u ? max_uniq_cap / 2u : 0u);
    if (max_hash_live_cap < max_ht_cap || max_uniq_live_cap < max_uniq_cap
        || !wl_columnar_memory_size_mul(max_hash_live_cap,
        allocated_row_bytes, &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, max_hash_live_cap,
        &scratch_bytes)
        || !wl_columnar_memory_size_mul(max_uniq_live_cap,
        allocated_row_bytes, &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || (nc > COL_STACK_MAX
        && (!wl_columnar_memory_size_mul(nc, sizeof(int64_t), &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes))))
        return EOVERFLOW;
    if (scratch_bytes > SIZE_MAX)
        return EOVERFLOW;
    int admission_rc = col_rel_merge_scratch_reserve(rel, scratch_bytes,
            &reservation);
    if (admission_rc != 0)
        return admission_rc;
#define HASH_RELEASE_SCRATCH() col_rel_merge_scratch_release(&reservation)

    /* Hash table: open addressing, power-of-2 capacity */
    uint32_t ht_cap = 8192;
    uint32_t ht_mask = ht_cap - 1;
    int64_t *ht_vals = (int64_t *)col_op_consolidate_malloc(
        (size_t)ht_cap * allocated_row_bytes, "hash_table");
    uint8_t *ht_used = (uint8_t *)col_op_consolidate_calloc(ht_cap, 1,
            "hash_table_used");
    if (!ht_vals || !ht_used) {
        free(ht_vals);
        free(ht_used);
        HASH_RELEASE_SCRATCH();
        return ENOMEM;
    }

    /* Unique row buffer (row-major flat array) */
    uint32_t uniq_cap = 4096;
    uint32_t uniq_count = 0;
    int64_t *uniq_buf = (int64_t *)col_op_consolidate_malloc(
        (size_t)uniq_cap * allocated_row_bytes, "hash_unique_rows");
    if (!uniq_buf) {
        free(ht_vals);
        free(ht_used);
        HASH_RELEASE_SCRATCH();
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
        HASH_RELEASE_SCRATCH();
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
                    HASH_RELEASE_SCRATCH();
                    return -1;
                }

                /* Rehash to 2x capacity */
                uint32_t new_cap = ht_cap * 2;
                uint32_t new_mask = new_cap - 1;
                int64_t *new_vals = (int64_t *)col_op_consolidate_malloc(
                    (size_t)new_cap * allocated_row_bytes,
                    "hash_rehash_rows");
                uint8_t *new_used = (uint8_t *)col_op_consolidate_calloc(
                    new_cap, 1, "hash_rehash_used");
                if (!new_vals || !new_used) {
                    free(new_vals);
                    free(new_used);
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    HASH_RELEASE_SCRATCH();
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
                        (size_t)uniq_cap * allocated_row_bytes,
                        "hash_unique_grow");
                if (!nb) {
                    if (rb != _rb) free(rb);
                    free(ht_vals);
                    free(ht_used);
                    free(uniq_buf);
                    HASH_RELEASE_SCRATCH();
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
    if (lease->owner != rel) {
        int cow_rc = wl_columnar_relation_privatize_shared_view_with_lease(
            rel, lease);
        if (cow_rc != 0) {
            free(uniq_buf);
            HASH_RELEASE_SCRATCH();
            return cow_rc;
        }
    }

    /* Publish the complete sorted result only after all fallible work. */
    lease->set->published = true;
    for (uint32_t r = 0; r < uniq_count; r++)
        col_rel_row_copy_in_raw(rel, r, uniq_buf + (size_t)r * nc);
    rel->nrows = uniq_count;
    wl_columnar_relation_touch_view(rel);
    free(uniq_buf);
    HASH_RELEASE_SCRATCH();

#undef HASH_RELEASE_SCRATCH
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
/* Stable segment order makes equal tuples keep the earliest incoming record.
 * Stable sorting within each segment preserves its original representative. */
static int
wl_columnar_merge_heap_compare(const col_rel_t *rel, uint32_t left,
    uint32_t right, uint32_t left_segment, uint32_t right_segment)
{
    int cmp = col_rel_row_cmp(rel, left, right);
    if (cmp == 0 && rel->timestamps)
        return (left_segment > right_segment) - (left_segment < right_segment);
    return cmp;
}

static int
col_op_consolidate_kway_merge_impl(col_rel_t *rel,
    const uint32_t *seg_boundaries, uint32_t seg_count,
    wl_columnar_relation_mutation_lease_t *lease)
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

    if (!seg_boundaries || seg_count == 0
        || seg_boundaries[0] != 0 || seg_boundaries[seg_count] != nr)
        return EINVAL;
    for (uint32_t s = 0; s < seg_count; s++) {
        if (seg_boundaries[s] > seg_boundaries[s + 1]
            || seg_boundaries[s + 1] > nr)
            return EINVAL;
    }
    if (nr <= 1)
        return 0;
    size_t segment_bytes;
    size_t heap_bytes;
    size_t row_bytes = 0;
    size_t merged_bytes = 0;
    size_t merged_alloc_bytes = 0;
    size_t timestamp_bytes = 0;
    size_t aggregate_scratch_bytes;
    wl_columnar_memory_reservation_t merge_reservation;
    wl_columnar_relation_radix_sequence_t sort_sequence = { 0 };
    uint32_t *seg_starts = NULL;
    uint32_t *seg_ends = NULL;
    int64_t *merged = NULL;
    col_delta_timestamp_t *merged_timestamps = NULL;
    heap_entry_t stack_heap[2];
    heap_entry_t *heap = stack_heap;
    heap_entry_t *heap_storage = NULL;
    int result = 0;
    wl_columnar_memory_reservation_init(&merge_reservation);
    if (!col_op_consolidate_size_multiply(seg_count, sizeof(uint32_t),
        &segment_bytes)
        || !col_op_consolidate_size_multiply(seg_count, sizeof(heap_entry_t),
        &heap_bytes))
        return EOVERFLOW;
    size_t segment_total_bytes;
    if (!col_op_consolidate_size_multiply(segment_bytes, 2u,
        &segment_total_bytes))
        return EOVERFLOW;
    aggregate_scratch_bytes = segment_total_bytes;
    if (seg_count >= 2) {
        /* Keep one element of storage for zero-arity relations: malloc(0)
         * is permitted to return NULL, which would otherwise look like
         * ENOMEM to the allocation check below. */
        if (!col_op_consolidate_size_multiply(nc, sizeof(int64_t),
            &row_bytes)
            || !col_op_consolidate_size_multiply(row_bytes, nr,
            &merged_bytes)
            || !col_op_consolidate_size_add(merged_bytes, sizeof(int64_t),
            &merged_alloc_bytes))
            return EOVERFLOW;
        if (!col_op_consolidate_size_add(aggregate_scratch_bytes,
            merged_alloc_bytes, &aggregate_scratch_bytes))
            return EOVERFLOW;
    }
    if (rel->timestamps && seg_count >= 2) {
        if (!col_op_consolidate_size_multiply(nr,
            sizeof(*merged_timestamps), &timestamp_bytes)
            || !col_op_consolidate_size_add(aggregate_scratch_bytes,
            timestamp_bytes, &aggregate_scratch_bytes))
            return EOVERFLOW;
    }
    if (seg_count >= 3) {
        size_t total;
        if (!col_op_consolidate_size_add(aggregate_scratch_bytes,
            heap_bytes, &total))
            return EOVERFLOW;
        aggregate_scratch_bytes = total;
    }

    /* Hash-based dedup for large datasets (#369): O(N) scan + O(U log U) sort
     * where U is the unique count.  When U << N (common in recursive Datalog
     * joins), this is much faster than sorting all N rows. */
    /* Timestamped set normalization preserves the first incoming record,
     * including signed/zero multiplicity. The hash shortcut does not carry
     * that provenance, so retain stable segment sorting for these inputs. */
    if (nr > 10000 && !rel->timestamps) {
        int rc = col_op_consolidate_hash_dedup(rel, lease);
        if (rc == 0)
            return 0;
        if (rc != -1)
            return rc;
        /* The unique-count heuristic requests the ordinary sort+merge path. */
    }

    int workspace_rc = wl_columnar_relation_radix_sequence_prepare_with_lease(
        rel, seg_boundaries, seg_count, &sort_sequence, lease);
    if (workspace_rc != 0)
        return workspace_rc;
    uint64_t publication_events = (uint64_t)sort_sequence.needs_sort_count + 1u;
    if (rel->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID
        - publication_events) {
        result = EOVERFLOW;
        goto cleanup;
    }
    if (lease->owner != rel && rel->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        result = EOVERFLOW;
        goto cleanup;
    }
    int admission_rc = col_rel_merge_scratch_reserve(rel,
            aggregate_scratch_bytes, &merge_reservation);
    if (admission_rc != 0) {
        result = admission_rc;
        goto cleanup;
    }

    /* Allocate every buffer that can fail before the first in-place sort or
     * dedup mutation.  The workspace reservation includes these buffers. */
    seg_starts = (uint32_t *)col_op_consolidate_malloc(
        segment_bytes, "segment_starts");
    seg_ends = (uint32_t *)col_op_consolidate_malloc(
        segment_bytes, "segment_ends");
    if (seg_count >= 2)
        merged = (int64_t *)col_op_consolidate_malloc(
            merged_alloc_bytes, "merge_output");
    if (timestamp_bytes > 0)
        merged_timestamps = col_op_consolidate_malloc(timestamp_bytes,
                "merge_timestamps");
    if (seg_count >= 3)
        heap_storage = (heap_entry_t *)col_op_consolidate_malloc(heap_bytes,
                "merge_heap");
    if (heap_storage)
        heap = heap_storage;
    if (!seg_starts || !seg_ends || (seg_count >= 2 && !merged)
        || (seg_count >= 3 && !heap_storage)
        || (timestamp_bytes > 0 && !merged_timestamps)) {
        result = ENOMEM;
        goto cleanup;
    }

    /* Every buffer and reservation is live before the first mutation. */
    result = wl_columnar_relation_radix_sequence_execute_with_lease(
        &sort_sequence, lease);
    if (result != 0)
        goto cleanup;
    if (lease->owner != rel && !lease->detached) {
        result = wl_columnar_relation_privatize_shared_view_with_lease(rel,
                lease);
        if (result != 0)
            goto cleanup;
    }

    /* Sort is complete. Deduplicate each segment to reduce merge input. */

    lease->set->published = true;
    for (uint32_t s = 0; s < seg_count; s++) {
        uint32_t start = sort_sequence.boundaries[s];
        uint32_t end = sort_sequence.boundaries[s + 1];
        uint32_t count = end - start;
        seg_starts[s] = start;

        if (count > 1) {
            /* Intra-segment dedup: compact unique rows to reduce merge */
            uint32_t out_r = start + 1;
            for (uint32_t r = start + 1; r < end; r++) {
                if (col_rel_row_cmp(rel, out_r - 1, r) != 0) {
                    if (out_r != r)
                        col_rel_row_move_raw(rel, out_r, r);
                    if (rel->timestamps && out_r != r)
                        rel->timestamps[out_r] = rel->timestamps[r];
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
                    && wl_columnar_merge_heap_compare(rel, heap[_c + 1].cursor,             \
                    heap[_c].cursor, heap[_c + 1].seg, heap[_c].seg)       \
                    < 0)                                              \
                _c++;                                                    \
                if (wl_columnar_merge_heap_compare(rel, heap[_p].cursor,                    \
                    heap[_c].cursor, heap[_p].seg, heap[_c].seg)          \
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
            || col_rel_row_cmp_raw(rel, heap[0].cursor, last_row, nc) != 0) {
            col_rel_row_copy_out(rel, heap[0].cursor,
                merged + (size_t)out * nc);
            /* Test the source too: clang-analyzer cannot follow that
             * merged_timestamps is allocated only when rel->timestamps is
             * non-NULL, because the link runs through timestamp_bytes. */
            if (merged_timestamps && rel->timestamps)
                merged_timestamps[out] = rel->timestamps[heap[0].cursor];
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
    if (merged_timestamps && rel->timestamps)
        memcpy(rel->timestamps, merged_timestamps,
            (size_t)out * sizeof(*merged_timestamps));
    rel->nrows = out;
    wl_columnar_relation_touch_view(rel);

cleanup:
    free(merged);
    free(merged_timestamps);
    free(heap_storage);
    free(seg_starts);
    free(seg_ends);
    wl_columnar_relation_radix_sequence_destroy(&sort_sequence);
    col_rel_merge_scratch_release(&merge_reservation);
    return result;
}

static int
wl_columnar_merge_consolidate_checked(col_rel_t *rel,
    const uint32_t *seg_boundaries, uint32_t seg_count, bool finalize,
    bool *out_published)
{
    wl_columnar_relation_mutation_set_t set = { 0 };
    wl_columnar_relation_mutation_role_t role = { 0 };
    wl_columnar_relation_mutation_descriptor_t descriptor = { 0 };
    wl_columnar_relation_mutation_owner_t owners[2] = { 0 };
    wl_columnar_relation_mutation_lease_t lease = { 0 };
    wl_columnar_relation_mutation_initialization_t initialization = { 0 };
    col_rel_t *owner = NULL;
    int rc;

    if (out_published)
        *out_published = false;
    if (!rel)
        return EINVAL;
    role.relation = rel;
    role.role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION;
    rc = col_rel_mutation_set_acquire(&set, &role, 1, &descriptor, 1,
            owners, 2, &lease, 1, &initialization, 1);
    if (rc != 0)
        return rc;
    rel->memory_budget_denial_pending = false;
    if (!wl_columnar_relation_float_values_valid(rel)) {
        rc = EINVAL;
        goto finish;
    }
    if (rel->nrows > rel->capacity
        || (rel->timestamps
        && rel->timestamp_capacity < rel->nrows)
        || (rel->ncols && !rel->columns)) {
        rc = EINVAL;
        goto finish;
    }
    for (uint32_t c = 0; c < rel->ncols; c++)
        if (!rel->columns[c]) {
            rc = EINVAL;
            goto finish;
        }
    if (!seg_boundaries || !seg_count || seg_count == UINT32_MAX) {
        rc = seg_count == UINT32_MAX ? EOVERFLOW : EINVAL;
        goto finish;
    }
    if (seg_boundaries[0] != 0
        || seg_boundaries[seg_count] != rel->nrows) {
        rc = EINVAL;
        goto finish;
    }
    for (uint32_t s = 0; s < seg_count; s++)
        if (seg_boundaries[s] > seg_boundaries[s + 1]
            || seg_boundaries[s + 1] > rel->nrows) {
            rc = EINVAL;
            goto finish;
        }
    if (rel->nrows <= 1 && !finalize) {
        rc = 0;
        goto finish;
    }
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc != 0)
        goto finish;
    if (rel == owner
        && col_rel_storage_alias_borrow_count(owner) > 0) {
        rc = EBUSY;
        goto finish;
    }
    if (!wl_columnar_relation_generation_valid(rel->view_generation)) {
        rc = EINVAL;
        goto finish;
    }
    if (lease.owner != rel && rel->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u) {
        rc = EOVERFLOW;
        goto finish;
    }
    /* Hash candidate publication has one view event and may privatize one
     * alias. Check both before it reads or allocates candidate state. */
    if (rel->nrows > 10000 && !rel->timestamps
        && (rel->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || (lease.owner != rel && rel->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u))) {
        rc = EOVERFLOW;
        goto finish;
    }

    rc = col_op_consolidate_kway_merge_impl(rel, seg_boundaries, seg_count,
            &lease);
    if (rc == 0 && finalize) {
        set.published = true;
        rel->sorted_nrows = rel->nrows;
        rel->run_count = 1;
        rel->run_ends[0] = rel->nrows;
    }
    if (rc == 0 && out_published)
        *out_published = true;
finish:
    {
        int finish_rc = col_rel_mutation_set_finish(&set, false);
        if (rc == 0)
            rc = finish_rc;
    }
    return rc;
}

int
col_op_consolidate_kway_merge(col_rel_t *rel, const uint32_t *seg_boundaries,
    uint32_t seg_count)
{
    return wl_columnar_merge_consolidate_checked(rel, seg_boundaries,
               seg_count, false, NULL);
}

/* A borrowed input stays untouched until its private candidate has completed
 * consolidation. Capture denial evidence before checked destruction can erase
 * it; a refused destruction must retain the candidate as a reachable owner. */
static int
wl_columnar_merge_consolidate_borrowed_abort(eval_stack_t *stack,
    wl_col_session_t *sess, eval_entry_t entry, col_rel_t *copy, int rc)
{
    if (rc == ENOMEM && copy && copy->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC && sess)
        sess->memory_budget_denied = true;
    int cleanup_rc = col_rel_destroy_checked(copy);
    if (cleanup_rc != 0) {
        entry.rel = copy;
        entry.owned = true;
    }
    stack->items[stack->top++] = entry;
    return rc != 0 ? rc : cleanup_rc;
}

static int
wl_columnar_merge_consolidate_borrowed(eval_stack_t *stack,
    wl_col_session_t *sess, eval_entry_t entry,
    wl_columnar_memory_governor_ref_t *governor)
{
    col_rel_t *copy = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    int rc = col_rel_source_reader_acquire(entry.rel, &reader);
    if (rc != 0)
        goto failed;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_merge_test_fail_copy_alloc)
        rc = ENOMEM;
    else
#endif
    rc = wl_columnar_relation_pool_new_like_governed_checked(&copy,
            sess ? sess->delta_pool : NULL, "$consol", entry.rel, governor);
    if (rc != 0)
        goto failed;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_merge_test_fail_copy_append)
        rc = ENOMEM;
    else
#endif
    rc = col_rel_append_all(copy, entry.rel, NULL);
    if (rc != 0)
        goto failed;
    rc = col_rel_source_reader_release(&reader);
    if (rc != 0)
        goto failed;

#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_merge_test_after_cons_copy) {
        rc = wl_columnar_merge_test_after_cons_copy(copy);
        if (rc != 0)
            goto failed;
    }
#endif

    /* Admit the complete sort workspace before mutating the private copy. */
    uint32_t whole[] = { 0, copy->nrows };
    const uint32_t *boundaries = whole;
    uint32_t segments = 1;
    if (entry.seg_boundaries && entry.seg_count >= 2) {
        boundaries = entry.seg_boundaries;
        segments = entry.seg_count;
    }
    rc = wl_columnar_merge_consolidate_checked(copy, boundaries, segments,
            true, NULL);
    if (rc != 0)
        goto failed;
    free(entry.seg_boundaries);
    entry.seg_boundaries = NULL;
    entry.seg_count = 0;
    entry.rel = copy;
    entry.owned = true;
    stack->items[stack->top++] = entry;
    return 0;

failed:
    if (reader.owner) {
        int release_rc = col_rel_source_reader_release(&reader);
        if (release_rc != 0)
            rc = release_rc;
    }
    return wl_columnar_merge_consolidate_borrowed_abort(stack, sess, entry,
               copy, rc);
}

/* Timestamped SET and governed owned consolidation share the checked engine.
 * Keep the popped entry intact until success: its original stack slot remains
 * available even when the caller entered with a completely full stack. */
static int
wl_columnar_merge_consolidate_entry(eval_stack_t *stack,
    wl_col_session_t *sess, eval_entry_t entry)
{
    col_rel_t *work = entry.rel;
    col_rel_t *copy = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_memory_governor_ref_t *governor = NULL;
    int rc = 0;
    bool published = false;
    if (!entry.owned) {
        rc = col_rel_source_reader_acquire(entry.rel, &reader);
        if (rc != 0)
            goto failed;
        governor = sess && sess->memory_governor
            ? sess->memory_governor : entry.rel->memory_governor;
        /* Use a governed heap clone so constructor, timestamp, growth, and
         * merge scratch allocations are admitted before any mutation. */
        rc = wl_columnar_relation_new_like_governed_checked(&copy,
                "$consol", entry.rel, governor);
        if (rc != 0) {
            if (rc == ENOSPC && sess)
                sess->memory_budget_denied = true;
            goto failed;
        }
        rc = col_rel_enable_timestamps(copy);
        if (rc == 0)
            rc = col_rel_append_all(copy, entry.rel, NULL);
        int release_rc = col_rel_source_reader_release(&reader);
        if (rc == 0)
            rc = release_rc;
        if (rc != 0)
            goto failed;
        work = copy;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_merge_test_after_cons_copy) {
            rc = wl_columnar_merge_test_after_cons_copy(copy);
            if (rc != 0)
                goto failed;
        }
#endif
    }
    uint32_t local_boundaries[3] = { 0, work->nrows, work->nrows };
    const uint32_t *boundaries = local_boundaries;
    uint32_t segments = 1;
    if (entry.seg_boundaries && entry.seg_count >= 2) {
        boundaries = entry.seg_boundaries;
        segments = entry.seg_count;
    } else if (entry.rel->sorted_nrows > 0
        && entry.rel->sorted_nrows < work->nrows) {
        local_boundaries[1] = entry.rel->sorted_nrows;
        segments = 2;
    }
    rc = wl_columnar_merge_consolidate_checked(work, boundaries, segments,
            true, &published);
    if (rc != 0 && !published)
        goto failed;
    free(entry.seg_boundaries);
    entry.seg_boundaries = NULL;
    entry.seg_count = 0;
    entry.rel = work;
    entry.owned = true;
    if (!work->timestamps)
        entry.is_delta = false;
    /* Publication is complete even if releasing a lease reports an
     * invariant error. Retain that result, not obsolete segment boundaries. */
    stack->items[stack->top++] = entry;
    return rc;
failed:
    if (entry.owned && rc == ENOMEM && work->memory_budget_denial_pending) {
        rc = ENOSPC;
        if (sess)
            sess->memory_budget_denied = true;
    }
    if (reader.owner) {
        int release_rc = col_rel_source_reader_release(&reader);
        if (release_rc != 0)
            rc = release_rc;
    }
    if (!entry.owned)
        return wl_columnar_merge_consolidate_borrowed_abort(stack, sess,
                   entry, copy, rc);
    stack->items[stack->top++] = entry;
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
    if (in->timestamps)
        return wl_columnar_merge_consolidate_entry(stack, sess, e);
    if (!wl_columnar_relation_float_values_valid(in)) {
        return col_op_dispose_entry_primary(stack, &e, EINVAL);
    }
    uint32_t nc = in->ncols;
    uint32_t nr = in->nrows;

    if (nr <= 1) {
        if (!e.owned || (in->sorted_nrows == nr && in->run_count == 1
            && in->run_ends[0] == nr && e.seg_boundaries == NULL))
            return eval_stack_repush_entry(stack, &e);
        wl_columnar_source_access_writer_t writer = { 0 };
        int writer_rc = col_rel_source_writer_acquire(in, &writer);
        if (writer_rc != 0)
            return eval_stack_repush_entry(stack,
                       &e) == 0 ? writer_rc : ENOBUFS;
        if (col_rel_storage_alias_borrow_count(in) != 0) {
            (void)wl_columnar_source_access_writer_release(&writer);
            return eval_stack_repush_entry(stack, &e) == 0 ? EBUSY : ENOBUFS;
        }
        /* Nothing to deduplicate */
        if (e.seg_boundaries) {
            free(e.seg_boundaries);
            e.seg_boundaries = NULL;
            e.seg_count = 0;
        }
        in->sorted_nrows = nr;
        in->run_count = 1;
        in->run_ends[0] = nr;
        (void)wl_columnar_source_access_writer_release(&writer);
        return eval_stack_repush_entry(stack, &e);
    }

    wl_columnar_memory_governor_ref_t *governor = sess && sess->memory_governor
        ? sess->memory_governor : in->memory_governor;
    if (!e.owned && governor)
        return wl_columnar_merge_consolidate_borrowed(stack, sess, e,
                   governor);
    if (e.owned && in->memory_governor)
        return wl_columnar_merge_consolidate_entry(stack, sess, e);

    /* Sort in-place if we own the relation, otherwise copy first */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work =
#ifdef WL_SESSION_TEST_HOOKS
            wl_columnar_merge_test_fail_copy_alloc
                ? NULL :
#endif
            col_rel_pool_new_like(sess ? sess->delta_pool : NULL, "$consol",
                in);
        if (!work) {
            /* Keep the borrowed input and its segment metadata available for
             * a retry after transient allocation pressure. */
            if (eval_stack_repush_entry(stack, &e) != 0)
                return ENOBUFS;
            return ENOMEM;
        }
        int append_rc =
#ifdef WL_SESSION_TEST_HOOKS
            wl_columnar_merge_test_fail_copy_append
                ? ENOMEM :
#endif
            col_rel_append_all(work, in, NULL);
        if (append_rc != 0) {
            int cleanup_rc = col_rel_destroy_checked(work);
            if (cleanup_rc != 0) {
                /* The copied relation becomes the retry owner when its
                 * cleanup is refused.  Preserve the original k-way
                 * boundaries on the retained entry. */
                eval_entry_t retained = e;
                retained.rel = work;
                retained.owned = true;
                (void)eval_stack_repush_entry(stack, &retained);
            } else {
                /* The borrowed source remains usable and retains its
                 * metadata for a later copy attempt. */
                (void)eval_stack_repush_entry(stack, &e);
            }
            return append_rc;
        }
        work_owned = true;
    }

    /* Dispatch: K-way merge when segment boundaries are available */
    uint32_t k = e.seg_count > 0 ? e.seg_count : 1;
    if (k >= 2 && e.seg_boundaries != NULL) {
        int rc = col_op_consolidate_kway_merge(work, e.seg_boundaries, k);
        if (rc != 0) {
            if (!e.owned) {
                int cleanup_rc = col_rel_destroy_checked(work);
                if (cleanup_rc == 0) {
                    /* The private copy is gone; retain the original
                     * borrowed entry and its boundaries for retry. */
                    (void)eval_stack_repush_entry(stack, &e);
                } else {
                    eval_entry_t retained = e;
                    retained.rel = work;
                    retained.owned = true;
                    (void)eval_stack_repush_entry(stack, &retained);
                }
                return rc;
            }
            return col_op_cleanup_owned_relation(stack, &e, work,
                       work_owned, rc);
        }
        free(e.seg_boundaries);
        e.seg_boundaries = NULL;
        e.seg_count = 0;
        work->sorted_nrows = work->nrows;
        work->run_count = 1;
        work->run_ends[0] = work->nrows;
        return eval_stack_push(stack, work, work_owned);
    }

    if (e.seg_boundaries)
        free(e.seg_boundaries);
    e.seg_boundaries = NULL;
    e.seg_count = 0;

    /* Issue #94: Incremental merge when a sorted prefix exists.
     * data[0..sorted_nrows) is already sorted+unique from a prior
     * consolidation.  Sort only the unsorted suffix and merge. */
    uint32_t sn = work->sorted_nrows;
    if (sn > 0 && sn < nr) {
        uint32_t delta_count = nr - sn;

        /* Phase 1: sort only the unsorted suffix using radix sort */
        int sort_rc = col_rel_radix_sort(work, sn, delta_count);
        if (sort_rc != 0) {
            return col_op_cleanup_owned_relation(stack, &e, work,
                       work_owned, sort_rc);
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
            int grid_rc = col_rel_reserve_merge_grid(work, new_cap);
            if (grid_rc != 0)
                return col_op_cleanup_owned_relation(stack, &e, work,
                           work_owned, grid_rc);
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
        if (out > 0) {
            uint64_t target = (uint64_t)out + out / 4;
            if (target < COL_REL_INIT_CAP)
                target = COL_REL_INIT_CAP;
            if (!work->memory_governor && target < work->capacity
                && col_columns_realloc_atomic(work->columns, nc,
                work->capacity, (uint32_t)target) == 0) {
                work->capacity = (uint32_t)target;
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
            return col_op_cleanup_owned_relation(stack, &e, work,
                       work_owned, sort_rc);
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

/* Flat row-major merge buffer for run compaction, with its governor credit.
 * Acquiring it is the only fallible step of a compaction (#2051), so callers
 * that must not publish a partial state acquire it first. */
typedef struct col_rel_compact_scratch {
    int64_t *merged;
    uint32_t rows;
    wl_columnar_memory_reservation_t reservation;
} col_rel_compact_scratch_t;

/*
 * col_rel_compact_scratch_prepare - reserve and allocate room for `rows`
 * merged rows.
 *
 * @return 0, EOVERFLOW when the size does not fit, the admission error
 *         (ENOMEM with memory_budget_denial_pending set, EOVERFLOW or
 *         EINVAL), or ENOMEM on allocation failure.  On failure nothing is
 *         held and rel's rows, runs and generations are unchanged.
 */
static int
col_rel_compact_scratch_prepare(col_rel_t *rel, uint32_t rows,
    col_rel_compact_scratch_t *scratch)
{
    uint64_t merged_bytes = 0;

    scratch->merged = NULL;
    scratch->rows = 0;
    wl_columnar_memory_reservation_init(&scratch->reservation);
    if (!wl_columnar_memory_size_mul(rows,
        (uint64_t)rel->ncols * sizeof(int64_t),
        &merged_bytes))
        return EOVERFLOW;
    if (merged_bytes > SIZE_MAX)
        return EOVERFLOW;
    int admission_rc = col_rel_merge_scratch_reserve(rel, merged_bytes,
            &scratch->reservation);
    if (admission_rc != 0)
        return admission_rc;
    scratch->merged = (int64_t *)malloc((size_t)merged_bytes);
    if (!scratch->merged) {
        col_rel_merge_scratch_release(&scratch->reservation);
        return ENOMEM;
    }
    scratch->rows = rows;
    return 0;
}

static void
col_rel_compact_scratch_release(col_rel_compact_scratch_t *scratch)
{
    if (!scratch->merged)
        return;
    free(scratch->merged);
    scratch->merged = NULL;
    scratch->rows = 0;
    col_rel_merge_scratch_release(&scratch->reservation);
}

/*
 * col_rel_compact_runs_prepared - K-way merge of tiered sorted runs (#369).
 *
 * Merges all independently sorted+unique runs into a single sorted run.
 * Uses min-heap merge (runs are already sorted, no per-segment sort needed).
 * Writes merged result back into rel using the prepared flat buffer +
 * scatter, then releases the scratch.  Cannot fail: the caller must hold a
 * prepared scratch covering every run-bounded row.
 */
static void
col_rel_compact_runs_prepared(col_rel_t *rel,
    col_rel_compact_scratch_t *scratch)
{
    if (rel->run_count <= 1 || !scratch->merged
        || rel->run_ends[rel->run_count - 1] > scratch->rows)
        abort();

    uint32_t nc = rel->ncols;
    uint32_t K = rel->run_count;
    int64_t *merged = scratch->merged;

    /* Build segment boundaries from run_ends */
    uint32_t seg_bounds[COL_MAX_RUNS + 1];
    seg_bounds[0] = 0;
    for (uint32_t i = 0; i < K; i++)
        seg_bounds[i + 1] = rel->run_ends[i];

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
            || col_rel_row_cmp_raw(rel, heap[0].cursor, last_row, nc) != 0) {
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
    col_rel_compact_scratch_release(scratch);

    rel->nrows = out;
    wl_columnar_relation_touch_view(rel);
    rel->sorted_nrows = out;
    rel->run_count = 1;
    rel->run_ends[0] = out;
}

/*
 * col_rel_compact_runs - prepare a scratch for rel->nrows rows and compact.
 *
 * @return 0 on success, or a col_rel_compact_scratch_prepare error, in which
 *         case rel's rows, runs and generations are unchanged.
 */
static int
col_rel_compact_runs(col_rel_t *rel)
{
    col_rel_compact_scratch_t scratch;

    if (rel->run_count <= 1)
        return 0;
    int rc = col_rel_compact_scratch_prepare(rel, rel->nrows, &scratch);
    if (rc != 0)
        return rc;
    col_rel_compact_runs_prepared(rel, &scratch);
    return 0;
}

/* Reserve the storage-generation advances an incremental consolidation may
 * make before it mutates anything (Issue #2046).  Each successful advance
 * from g needs g + 1 < WL_COLUMNAR_REL_GENERATION_INVALID, so `steps`
 * advances fit only while g < INVALID - steps; otherwise a late touch would
 * poison the counter after rows, runs, or timestamp ownership were already
 * published.
 *
 * The path is chosen only after the delta sort, which itself advances the
 * view generation, so the bound is taken over every path still reachable:
 * one advance to retire timestamps, one to detach a shared view (by the
 * sort, before the fast path's run compaction, or before the
 * binary/fallback mutation; the first detach clears col_shared), and one for
 * the fallback's column swap whenever the delta is too large to guarantee
 * the fast or binary path. */
static uint32_t
col_op_consolidate_storage_steps(const col_rel_t *rel, uint32_t old_nrows,
    uint32_t delta_count)
{
    return (rel->timestamps ? 1u : 0u) + (rel->col_shared ? 1u : 0u)
           + (old_nrows > 0 && delta_count > old_nrows / 16 ? 1u : 0u);
}

/* Reserve the view-generation advances an incremental consolidation may
 * make before it mutates anything (Issue #2049), on the same g < INVALID -
 * steps bound as col_op_consolidate_storage_steps.  The delta sort makes one
 * advance when it sorts two or more delta rows of a relation with columns.
 * After it, each path publishes nrows once, except where it compacts runs:
 * the fast path's compaction makes two advances, and the binary path's (at
 * COL_MAX_RUNS, with novel rows) and the fallback's (over more than one
 * run) make three.  Whether a path may compact depends on the repaired run
 * count, which the sort does not change, so it is known here; the path is
 * not.  At COL_MAX_RUNS three advances are therefore reserved, even for a
 * delta the sort will route to the fast path.  Below it, only the fallback
 * compacts, and only a delta too large to guarantee the binary path can
 * reach it. */
static uint32_t
col_op_consolidate_view_steps(const col_rel_t *rel, uint32_t old_nrows,
    uint32_t delta_count)
{
    uint32_t runs = rel->run_count;
    if (runs == 0 || rel->run_ends[runs - 1] != old_nrows)
        runs = old_nrows > 0 ? 1u : 0u;
    bool compacting = runs >= COL_MAX_RUNS
        || (runs > 1 && delta_count > old_nrows / 16);
    return (rel->ncols > 0 && delta_count > 1 ? 1u : 0u)
           + (compacting ? 3u : 1u);
}

static int
col_op_consolidate_incremental_delta_generation_preflight(
    const col_rel_t *rel, uint32_t old_nrows)
{
    if (rel->nrows == 0 || old_nrows >= rel->nrows)
        return 0;
    uint32_t delta_count = rel->nrows - old_nrows;
    uint32_t storage_steps = col_op_consolidate_storage_steps(rel,
            old_nrows, delta_count);
    uint32_t view_steps = col_op_consolidate_view_steps(rel, old_nrows,
            delta_count);
    if ((storage_steps > 0
        && rel->storage_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - (uint64_t)storage_steps)
        || rel->view_generation
        >= WL_COLUMNAR_REL_GENERATION_INVALID - (uint64_t)view_steps)
        return EOVERFLOW;
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
col_op_consolidate_incremental_delta_append_with_lease(col_rel_t *delta_out,
    const int64_t *row, wl_columnar_relation_mutation_lease_t *delta_lease,
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
    return col_rel_append_row_with_lease(delta_out, row, delta_lease);
}

static int
col_op_consolidate_incremental_delta_impl(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out, int *out_fast_path,
    wl_columnar_relation_mutation_lease_t *rel_lease,
    wl_columnar_relation_mutation_lease_t *delta_lease)
{
    if (!rel_lease || (delta_out && !delta_lease))
        return EINVAL;
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

    /* Sort only the new delta rows after preparing all permutation scratch.
     * The exact source lease covers the relation being sorted, regardless of
     * whether delta_out shares its owner. */
    wl_columnar_radix_workspace_t sort_workspace = { 0 };
    int sort_rc = wl_columnar_relation_radix_workspace_prepare_with_lease(rel,
            old_nrows, delta_count, &sort_workspace, rel_lease);
    if (sort_rc == 0)
        sort_rc = wl_columnar_relation_radix_sort_consolidation_with_lease(
            rel, old_nrows, delta_count, &sort_workspace, rel_lease);
    wl_columnar_radix_workspace_destroy(&sort_workspace);
    if (sort_rc != 0)
        return col_op_consolidate_incremental_delta_fail(delta_out,
                   delta_initial_nrows, sort_rc);

    /* From here on, partial progress is intentionally retained on errors. */
    rel_lease->set->published = true;

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
        /* Compaction below rewrites rel->columns in place; on a shared view
         * those are the canonical owner's buffers (#2048).  Detach before
         * any emission or metadata change so a refused copy leaves rel and
         * delta_out untouched.  A delta the sort reordered was detached by
         * the sort itself, and the non-compacting fast path writes no
         * borrowed column buffer, so it keeps the borrow. */
        bool compact = rel->run_count >= COL_MAX_RUNS;
        col_rel_compact_scratch_t scratch;
        if (rel->col_shared && compact) {
            int cow_rc = col_rel_cow_unshare_with_lease(rel, rel_lease);
            if (cow_rc != 0)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, cow_rc);
        }

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
                int rc = col_op_consolidate_incremental_delta_append_with_lease(
                    delta_out, dr, delta_lease,
                    &delta_append_hook_called);
                if (rc != 0) {
                    col_row_buf_release(&drb);
                    return col_op_consolidate_incremental_delta_fail(
                        delta_out, delta_initial_nrows, rc);
                }
            }
            col_row_buf_release(&drb);
        }

        /* Acquire the compaction scratch before publishing rows or runs
         * (#2051): a refusal after nrows and the last run were extended
         * would let a retry's run repair fold the interleaved runs into one
         * "sorted" run.  The refusal itself publishes no rows, runs or
         * generations.  As for any failure after the sort, what came before
         * it stays in effect: an admitted detach, a run repair, the sort's
         * view-generation advance, and an unpublished delta suffix that may
         * be reordered and deduplicated (its value set is kept); delta_out
         * is rolled back. */
        if (compact) {
            int rc = col_rel_compact_scratch_prepare(rel,
                    old_nrows + d_unique, &scratch);
            if (rc != 0)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, rc);
        }
        rel->nrows = old_nrows + d_unique;
        wl_columnar_relation_touch_view(rel);
        rel->sorted_nrows = rel->nrows;

        /* Register as new run */
        if (!compact) {
            rel->run_ends[rel->run_count] = rel->nrows;
            rel->run_count++;
        } else {
            /* Extend the last run to include new data, then compact */
            rel->run_ends[rel->run_count - 1] = rel->nrows;
            col_rel_compact_runs_prepared(rel, &scratch);
        }

        if (rel->timestamps) {
            wl_columnar_relation_terminal_sequence_t terminal = { 0 };
            int terminal_rc = wl_columnar_relation_terminal_sequence_begin(
                &terminal, rel_lease,
                WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE);
            if (terminal_rc != 0)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, terminal_rc);
            free(rel->timestamps);
            rel->timestamps = NULL;
            rel->timestamp_capacity = 0;
            col_rel_retire_payload_credit(rel);
            wl_columnar_relation_terminal_sequence_publish_timestamp_retirement(
                &terminal);
            if (wl_columnar_relation_terminal_sequence_finish(&terminal) != 0)
                abort();
        }
        if (out_fast_path)
            *out_fast_path = 1;
        return 0;
    }

    /* A one-row deferred sort is a valid no-op, but the fallback and binary
     * search paths below still mutate relation storage.  Detach a shared
     * view before those mutations so the deferred cleanup can retire its
     * alias borrow while the owner writer remains held. */
    if (rel->col_shared) {
        int cow_rc = col_rel_cow_unshare_with_lease(rel, rel_lease);
        if (cow_rc != 0)
            return col_op_consolidate_incremental_delta_fail(delta_out,
                       delta_initial_nrows, cow_rc);
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
                        = col_op_consolidate_incremental_delta_append_with_lease
                        (
                            delta_out, dr, delta_lease,
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
            bool compact = rel->run_count >= COL_MAX_RUNS;
            col_rel_compact_scratch_t scratch;
            /* Acquire the compaction scratch before publishing rows
             * (#2053), as the fast path does (#2051); it covers the rows
             * published below, the size compaction reserved here before.
             * The refusal itself publishes no rows, runs or generations.
             * What came before it stays in effect: an admitted detach, the
             * sort's view-generation advance, and an unconsolidated delta
             * suffix whose novel rows were compacted to its front (its
             * union with the published rows is kept); delta_out is rolled
             * back. */
            if (compact) {
                int rc = col_rel_compact_scratch_prepare(rel,
                        old_nrows + novel_count, &scratch);
                if (rc != 0)
                    return col_op_consolidate_incremental_delta_fail(
                        delta_out, delta_initial_nrows, rc);
            }
            rel->nrows = old_nrows + novel_count;
            wl_columnar_relation_touch_view(rel);
            /* Register novel rows as new run */
            if (!compact) {
                rel->run_ends[rel->run_count] = rel->nrows;
                rel->run_count++;
            } else {
                /* Compact existing runs, preserving novel rows (#376).
                 * Novel rows at [old_nrows..old_nrows+novel_count) are not
                 * in any run yet.  The compaction only merges run-bounded
                 * data so novel rows are physically untouched.  Relocate
                 * them adjacent to the compacted prefix afterwards. */
                col_rel_compact_runs_prepared(rel, &scratch);
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
            wl_columnar_relation_terminal_sequence_t terminal = { 0 };
            int terminal_rc = wl_columnar_relation_terminal_sequence_begin(
                &terminal, rel_lease,
                WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE);
            if (terminal_rc != 0)
                return col_op_consolidate_incremental_delta_fail(delta_out,
                           delta_initial_nrows, terminal_rc);
            free(rel->timestamps);
            rel->timestamps = NULL;
            rel->timestamp_capacity = 0;
            col_rel_retire_payload_credit(rel);
            wl_columnar_relation_terminal_sequence_publish_timestamp_retirement(
                &terminal);
            if (wl_columnar_relation_terminal_sequence_finish(&terminal) != 0)
                abort();
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
        int grid_rc = col_rel_reserve_merge_grid_with_lease(rel, new_cap,
                rel_lease);
        if (grid_rc != 0)
            return col_op_consolidate_incremental_delta_fail(delta_out,
                       delta_initial_nrows, grid_rc);
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
                int rc = col_op_consolidate_incremental_delta_append_with_lease(
                    delta_out, delta_row, delta_lease,
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
            int rc = col_op_consolidate_incremental_delta_append_with_lease(
                delta_out, delta_row, delta_lease,
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
        wl_columnar_relation_terminal_sequence_t terminal = { 0 };
        uint32_t terminal_events = WL_COLUMNAR_RELATION_TERMINAL_GRID_SWAP;
        if (rel->timestamps)
            terminal_events |= WL_COLUMNAR_RELATION_TERMINAL_TIMESTAMP_RETIRE;
        int terminal_rc = wl_columnar_relation_terminal_sequence_begin(
            &terminal, rel_lease, terminal_events);
        if (terminal_rc != 0)
            return col_op_consolidate_incremental_delta_fail(delta_out,
                       delta_initial_nrows, terminal_rc);
        int64_t **old_cols = rel->columns;
        uint32_t old_cap = rel->capacity;
        rel->columns = rel->merge_columns;
        rel->capacity = rel->merge_buf_cap;
        rel->merge_columns = old_cols;
        rel->merge_buf_cap = old_cap;
        wl_columnar_relation_terminal_sequence_publish_grid_swap(&terminal);
        if (rel->timestamps) {
            free(rel->timestamps);
            rel->timestamps = NULL;
            rel->timestamp_capacity = 0;
            col_rel_retire_payload_credit(rel);
            wl_columnar_relation_terminal_sequence_publish_timestamp_retirement(
                &terminal);
        }
        if (wl_columnar_relation_terminal_sequence_finish(&terminal) != 0)
            abort();
    }
    rel->nrows = out;
    wl_columnar_relation_touch_view(rel);
    rel->sorted_nrows = out;
    rel->run_count = 1;
    rel->run_ends[0] = out;

    if (out_fast_path)
        *out_fast_path = 0;
    return 0;
}

int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out, int *out_fast_path)
{
    wl_columnar_relation_mutation_role_t roles[2] = { 0 };
    wl_columnar_relation_mutation_descriptor_t descriptors[2] = { 0 };
    wl_columnar_relation_mutation_owner_t owners[4] = { 0 };
    wl_columnar_relation_mutation_lease_t leases[2] = { 0 };
    wl_columnar_relation_mutation_initialization_t initializations[2] = { 0 };
    wl_columnar_relation_mutation_set_t set = { 0 };
    size_t role_count = delta_out ? 2u : 1u;
    int rc;

    if (!rel || delta_out == rel)
        return EINVAL;
    roles[0] = (wl_columnar_relation_mutation_role_t) {
        .relation = rel,
        .role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
    };
    if (delta_out)
        roles[1] = (wl_columnar_relation_mutation_role_t) {
            .relation = delta_out,
            .role_flags = WL_COLUMNAR_RELATION_PAYLOAD_MUTATION
        };
    rc = col_rel_mutation_set_acquire(&set, roles, role_count,
            descriptors, 2, owners, 4, leases, 2, initializations, 2);
    if (rc != 0)
        return rc;
    rel->memory_budget_denial_pending = false;
    if (delta_out)
        delta_out->memory_budget_denial_pending = false;
    if (!wl_columnar_relation_float_values_valid(rel)
        || (delta_out
        && (!wl_columnar_relation_float_values_valid(delta_out)
        || delta_out->ncols != rel->ncols))) {
        rc = EINVAL;
        goto finish;
    }

    rc = col_op_consolidate_incremental_delta_generation_preflight(rel,
            old_nrows);
    if (rc != 0)
        goto finish;

    if (delta_out && rel->nrows > old_nrows) {
        uint32_t delta_count = rel->nrows > old_nrows
            ? rel->nrows - old_nrows : 0;
        /* Reserve delta_out's view-generation advances before its first
         * mutation (Issue #2057): each emitted row is a locked append that
         * advances the view once, at most delta_count of them, and a
         * failure after emission rolls delta_out back with one more. */
        if (delta_count > 0
            && delta_out->view_generation
            >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
            - (uint64_t)delta_count) {
            rc = EOVERFLOW;
            goto finish;
        }
        rc = col_rel_reserve_rows_with_lease(delta_out, delta_count,
                &leases[1]);
        if (rc != 0)
            goto finish;
    }

    rc = col_op_consolidate_incremental_delta_impl(rel, old_nrows,
            delta_out, out_fast_path, &leases[0],
            delta_out ? &leases[1] : NULL);

finish:
    {
        int finish_rc = col_rel_mutation_set_finish(&set, rc == 0);
        if (rc == 0)
            rc = finish_rc;
    }
    return rc;
}

/* Private controlled incremental consolidation (#1821). This attempt keeps
 * all candidate storage private until commit. Its per-call work budget counts
 * row/cell copies and row comparisons; allocations/admission are polled on
 * both sides but remain non-preemptible. */
static int
wl_columnar_merge_incremental_delta_attempt_validate(col_rel_t *source,
    wl_columnar_merge_incremental_delta_attempt_t *attempt,
    wl_evaluation_control_t *control)
{
    if (!source || !attempt || attempt->identity != (uintptr_t)attempt
        || attempt->transaction.source != source
        || !attempt->transaction.image || !attempt->delta
        || attempt->transaction.writer.owner != &source->source_access
        || attempt->transaction.writer.identity
        != (uintptr_t)&attempt->transaction.writer
        || !wl_columnar_source_access_writer_thread_equal(
            &attempt->transaction.writer)
        || attempt->control != control || !control
        || !attempt->control_attempt_id
        || wl_evaluation_control_active_attempt_id(control)
        != attempt->control_attempt_id)
        return EINVAL;
    if (source->relation_identity != attempt->source_snapshot.relation_identity
        || source->view_generation != attempt->source_snapshot.view_generation
        || source->storage_generation
        != attempt->source_snapshot.storage_generation
        || source->nrows != attempt->source_nrows)
        return EBUSY;
    if (attempt->external_candidate) {
        if (!attempt->candidate || !attempt->candidate_reader_held
            || attempt->candidate_reader.identity
            != (uintptr_t)&attempt->candidate_reader
            || attempt->candidate_reader.owner
            != &attempt->candidate->source_access
            || attempt->candidate_reader.secondary_owner
            != &attempt->candidate->descriptor_access
            || source->sorted_nrows < source->nrows
            || (source->nrows && (!source->columns || !source->columns[0]))
            || (attempt->candidate->nrows
            && (!attempt->candidate->columns
            || !attempt->candidate->columns[0]))
            || !wl_columnar_relation_snapshot_equal(
                attempt->candidate_snapshot,
                wl_columnar_relation_snapshot(attempt->candidate)))
            return EBUSY;
    }
    return 0;
}

static void
wl_columnar_merge_incremental_delta_attempt_discard_live(
    wl_columnar_merge_incremental_delta_attempt_t *attempt)
{
    if (attempt->delta)
        col_rel_destroy(attempt->delta);
    attempt->delta = NULL;
    /* Release the two relation protections in reverse acquisition order. */
    if (attempt->candidate_first) {
        if (attempt->transaction.source)
            col_rel_mutable_image_discard(&attempt->transaction);
        if (attempt->candidate_reader_held) {
            if (col_rel_source_reader_release(&attempt->candidate_reader) != 0)
                abort();
            attempt->candidate_reader_held = false;
        }
    } else {
        if (attempt->candidate_reader_held) {
            if (col_rel_source_reader_release(&attempt->candidate_reader) != 0)
                abort();
            attempt->candidate_reader_held = false;
        }
        if (attempt->transaction.source)
            col_rel_mutable_image_discard(&attempt->transaction);
    }
    memset(attempt, 0, sizeof(*attempt));
}

static int
wl_columnar_merge_incremental_delta_charge(
    wl_columnar_merge_incremental_delta_attempt_t *attempt, uint64_t *remaining,
    uint64_t work)
{
    if (work > *remaining)
        return EAGAIN;
    int rc = wl_evaluation_control_charge(attempt->control, work);
    if (rc == 0)
        *remaining -= work;
    return rc;
}

/* Compare one tuple incrementally.  A wide row can otherwise hide an
 * unbounded column scan inside a single "row comparison" charge. */
enum {
    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID = 0,
    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID = 1,
    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_MERGE_GRID = 2,
    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_CANDIDATE_GRID = 3,
};

static int
wl_columnar_merge_incremental_delta_compare_step(
    wl_columnar_merge_incremental_delta_attempt_t *attempt, uint64_t *remaining,
    const col_rel_t *source, uint8_t left_grid, uint32_t left_row,
    uint8_t right_grid, uint32_t right_row, int *out_cmp)
{
    if (!attempt || !source || !out_cmp)
        return EINVAL;
    if (!attempt->compare_active) {
        attempt->compare_active = true;
        attempt->compare_zero_charged = false;
        attempt->compare_left_grid = left_grid;
        attempt->compare_right_grid = right_grid;
        attempt->compare_left_row = left_row;
        attempt->compare_right_row = right_row;
        attempt->compare_column = 0;
    } else if (attempt->compare_left_grid != left_grid
        || attempt->compare_right_grid != right_grid
        || attempt->compare_left_row != left_row
        || attempt->compare_right_row != right_row) {
        return EINVAL;
    }

    const col_rel_t *image = attempt->transaction.image;
    if (source->ncols != image->ncols
        || left_grid > WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_CANDIDATE_GRID
        || right_grid > WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_CANDIDATE_GRID
        || left_row >=
        (left_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID
            ? source->nrows
            : left_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_CANDIDATE_GRID
                ? attempt->candidate_nrows : image->nrows)
        || right_row >=
        (right_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID
            ? source->nrows
            : right_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_CANDIDATE_GRID
                ? attempt->candidate_nrows : image->nrows))
        return EINVAL;
    while (attempt->compare_column < source->ncols) {
        int rc = wl_columnar_merge_incremental_delta_charge(attempt, remaining,
                1);
        if (rc != 0)
            return rc;
        uint32_t c = attempt->compare_column;
        int64_t **left = left_grid ==
            WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID
            ? source->columns
            : left_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID
                ? image->columns
            : left_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_CANDIDATE_GRID
                ? attempt->candidate->columns : image->merge_columns;
        int64_t **right = right_grid ==
            WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID
            ? source->columns
            : right_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID
                ? image->columns
            : right_grid == WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_CANDIDATE_GRID
                ? attempt->candidate->columns : image->merge_columns;
        if (!left || !right)
            return EINVAL;
        int64_t a = left[c][left_row];
        int64_t b = right[c][right_row];
        wirelog_column_type_t type = source->column_types
            ? source->column_types[c] : WIRELOG_TYPE_INT64;
        int cmp;
        if (type == WIRELOG_TYPE_FLOAT)
            cmp = wl_columnar_float_compare_bits(a, b);
        else
            cmp = a < b ? -1 : a > b ? 1 : 0;
        if (cmp != 0) {
            attempt->compare_active = false;
            *out_cmp = cmp;
            return 0;
        }
        attempt->compare_column++;
    }
    if (source->ncols == 0 && !attempt->compare_zero_charged) {
        int rc = wl_columnar_merge_incremental_delta_charge(attempt, remaining,
                1);
        if (rc != 0)
            return rc;
        attempt->compare_zero_charged = true;
    }
    attempt->compare_active = false;
    *out_cmp = 0;
    return 0;
}

static int64_t **
wl_columnar_merge_incremental_delta_sort_source(
    wl_columnar_merge_incremental_delta_attempt_t *attempt)
{
    return attempt->sort_source_primary
        ? attempt->transaction.image->columns
        : attempt->transaction.image->merge_columns;
}

static int64_t **
wl_columnar_merge_incremental_delta_sort_destination(
    wl_columnar_merge_incremental_delta_attempt_t *attempt)
{
    return attempt->sort_source_primary
        ? attempt->transaction.image->merge_columns
        : attempt->transaction.image->columns;
}

static void
wl_columnar_merge_incremental_delta_sort_swap_image_grids(col_rel_t *image)
{
    int64_t **columns = image->columns;
    uint32_t capacity = image->capacity;
    image->columns = image->merge_columns;
    image->capacity = image->merge_buf_cap;
    image->merge_columns = columns;
    image->merge_buf_cap = capacity;
    wl_columnar_relation_touch_storage(image);
}

static void
wl_columnar_merge_incremental_delta_run_advance(
    const wl_columnar_merge_incremental_delta_attempt_t *attempt, uint32_t *run,
    uint32_t *low, uint32_t *high)
{
    uint32_t start = *run == 0 ? 0 : attempt->run_ends[*run - 1];
    *low = start;
    *high = attempt->run_ends[*run];
}

static bool
wl_columnar_merge_staged_arrow_schema_equal(
    const struct ArrowSchema *left, const struct ArrowSchema *right,
    uint32_t depth)
{
    if (depth > 8 || !left || !right
        || ((left->format == NULL) != (right->format == NULL))
        || ((left->name == NULL) != (right->name == NULL))
        || ((left->metadata == NULL) != (right->metadata == NULL))
        || left->metadata != NULL || right->metadata != NULL
        || left->flags != right->flags
        || left->n_children != right->n_children
        || (left->n_children && (!left->children || !right->children)))
        return false;
    if ((left->format && strcmp(left->format, right->format) != 0)
        || (left->name && strcmp(left->name, right->name) != 0))
        return false;
    if ((left->dictionary == NULL) != (right->dictionary == NULL))
        return false;
    if (left->dictionary
        && !wl_columnar_merge_staged_arrow_schema_equal(left->dictionary,
        right->dictionary, depth + 1u))
        return false;
    for (int64_t i = 0; i < left->n_children; i++)
        if (!wl_columnar_merge_staged_arrow_schema_equal(left->children[i],
            right->children[i], depth + 1u))
            return false;
    return true;
}

static bool
wl_columnar_merge_staged_metadata_equal(const col_rel_t *source,
    const col_rel_t *candidate)
{
    if (source->ncols != candidate->ncols
        || source->declared_ncols != candidate->declared_ncols
        || source->schema_ok != candidate->schema_ok
        || source->has_graph_column != candidate->has_graph_column
        || source->graph_col_idx != candidate->graph_col_idx
        || source->compound_kind != candidate->compound_kind
        || source->compound_count != candidate->compound_count
        || source->compound_arity_len != candidate->compound_arity_len
        || source->inline_physical_offset != candidate->inline_physical_offset
        || (source->compound_arity_len
        && (!source->compound_arity_map
        || !candidate->compound_arity_map))
        || (source->schema_ok
        && !wl_columnar_merge_staged_arrow_schema_equal(&source->schema,
        &candidate->schema, 0)))
        return false;
    for (uint32_t c = 0; c < source->ncols; c++) {
        wirelog_column_type_t source_type = source->column_types
            ? source->column_types[c] : WIRELOG_TYPE_INT64;
        wirelog_column_type_t candidate_type = candidate->column_types
            ? candidate->column_types[c] : WIRELOG_TYPE_INT64;
        const char *source_name = source->col_names
            ? source->col_names[c] : NULL;
        const char *candidate_name = candidate->col_names
            ? candidate->col_names[c] : NULL;
        if (source_type != candidate_type
            || ((source_name == NULL) != (candidate_name == NULL))
            || (source_name && strcmp(source_name, candidate_name) != 0))
            return false;
    }
    for (uint32_t i = 0; i < source->compound_arity_len; i++)
        if (source->compound_arity_map[i]
            != candidate->compound_arity_map[i])
            return false;
    return true;
}

static int
wl_columnar_merge_incremental_delta_attempt_begin_internal(col_rel_t *source,
    const col_rel_t *candidate, uint32_t old_nrows,
    wl_columnar_merge_incremental_delta_attempt_t *attempt,
    wl_evaluation_control_t *control)
{
    uint64_t control_id;
    uint32_t capacity;
    int rc;

    if (!source || !attempt || !control)
        return EINVAL;
    if (attempt->identity)
        return EBUSY;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        return rc;
    control_id = wl_evaluation_control_active_attempt_id(control);
    if (!control_id)
        return EINVAL;

    memset(attempt, 0, sizeof(*attempt));
    attempt->identity = (uintptr_t)attempt;
    attempt->control = control;
    attempt->control_attempt_id = control_id;
    attempt->candidate = candidate;
    attempt->external_candidate = candidate != NULL;
    if (candidate) {
        /* Pointer identity is immutable and can be rejected without reading
        * relation metadata. Other admission checks run under both gates. */
        if (candidate == source) {
            rc = EINVAL;
            goto fail;
        }
        attempt->candidate_first = (uintptr_t)candidate < (uintptr_t)source;
    }

    /* Gates are nonblocking. A stable address order prevents reciprocal
     * source/candidate attempts from each retaining the other's needed gate. */
    if (candidate && attempt->candidate_first) {
        rc = col_rel_source_reader_acquire(candidate,
                &attempt->candidate_reader);
        if (rc != 0)
            goto fail;
        attempt->candidate_reader_held = true;
    }
    rc = wl_columnar_relation_mutable_image_prepare_empty(source,
            &attempt->transaction, 0);
    if (rc != 0)
        goto fail;
    if (candidate && !attempt->candidate_first) {
        rc = col_rel_source_reader_acquire(candidate,
                &attempt->candidate_reader);
        if (rc != 0)
            goto fail;
        attempt->candidate_reader_held = true;
    }
    bool all_int64 = source->ncols > 0;
    for (uint32_t c = 0; candidate && c < source->ncols; c++)
        if (source->column_types
            && source->column_types[c] != WIRELOG_TYPE_INT64)
            all_int64 = false;
    if (candidate && (source->storage_owner != source
        || candidate->storage_owner != candidate
        || !all_int64
        || source->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || source->compound_arity_len != 0 || source->has_graph_column
        || !wl_columnar_merge_staged_metadata_equal(source, candidate)
        || source->timestamps || candidate->timestamps
        || source->dedup_slots || candidate->dedup_slots
        || candidate->pool_owned || candidate->arena_owned
        || candidate->col_shared || source->sorted_nrows < source->nrows
        || (source->nrows && (!source->columns || !source->columns[0]))
        || (candidate->nrows
        && (!candidate->columns || !candidate->columns[0])))) {
        rc = ENOTSUP;
        goto fail;
    }
    if (candidate && candidate->nrows > UINT32_MAX - source->nrows) {
        rc = EOVERFLOW;
        goto fail;
    }
    attempt->source_snapshot = wl_columnar_relation_snapshot(source);
    if (candidate)
        attempt->candidate_snapshot = wl_columnar_relation_snapshot(candidate);
    attempt->source_nrows = source->nrows;
    attempt->old_nrows = candidate ? source->nrows : old_nrows;
    attempt->candidate_nrows = candidate ? candidate->nrows : 0;
    attempt->input_nrows = candidate
        ? source->nrows + candidate->nrows : source->nrows;
    attempt->no_op = candidate
        ? candidate->nrows == 0 : old_nrows >= source->nrows;
    attempt->phase = attempt->no_op ? WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DONE
        : WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_COPY;
    attempt->fast_path = attempt->no_op;
    attempt->sort_width = 1;
    attempt->sort_source_primary = true;

    capacity = !attempt->no_op && attempt->input_nrows
        ? attempt->input_nrows : source->ncols ? 1u : 0u;
    if (capacity > 0) {
        bool denied = false;
        rc = col_rel_reserve_capacity_admitted(attempt->transaction.image,
                capacity, &denied);
        if (rc == ENOMEM && denied)
            rc = ENOSPC;
        if (rc != 0)
            goto fail;
    }

    if (attempt->old_nrows > 0 && !attempt->no_op) {
        uint32_t end = 0;
        if (source->run_count > 0 && source->run_count <= COL_MAX_RUNS
            && source->run_ends[source->run_count - 1]
            == attempt->old_nrows) {
            attempt->run_count = source->run_count;
            for (uint32_t i = 0; i < source->run_count; i++) {
                uint32_t next = source->run_ends[i];
                if (next <= end || next > attempt->old_nrows) {
                    rc = EINVAL;
                    goto fail;
                }
                attempt->run_ends[i] = next;
                end = next;
            }
        } else if (source->sorted_nrows >= attempt->old_nrows) {
            attempt->run_count = 1;
            attempt->run_ends[0] = attempt->old_nrows;
        } else {
            rc = EINVAL;
            goto fail;
        }
    }

    col_rel_t *image = attempt->transaction.image;
    if (!attempt->no_op) {
        rc = wl_columnar_eval_dedup_set_clone_begin(image, source);
        if (rc != 0)
            goto fail;
        attempt->dedup_copy_count = source->dedup_cap;
        if (attempt->dedup_copy_count != 0)
            attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_COPY_DEDUP;
    }
    if (!attempt->no_op && attempt->input_nrows > 0 && source->ncols > 0) {
        rc = col_rel_reserve_merge_grid(image, attempt->input_nrows);
        if (rc != 0)
            goto fail;
    }
    rc = wl_columnar_relation_new_empty_governed_like_locked(source,
            &attempt->delta);
    if (rc != 0)
        goto fail;
    capacity = source->ncols ? 1u : 0u;
    if (capacity) {
        bool denied = false;
        rc = col_rel_reserve_capacity_admitted(attempt->delta, capacity,
                &denied);
        if (rc == ENOMEM && denied)
            rc = ENOSPC;
        if (rc != 0)
            goto fail;
    }
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        goto fail;
    return 0;

fail:
    if (attempt->transaction.source || attempt->candidate_reader_held)
        wl_columnar_merge_incremental_delta_attempt_discard_live(attempt);
    else
        memset(attempt, 0, sizeof(*attempt));
    return rc;
}

int
wl_columnar_merge_incremental_delta_attempt_begin(col_rel_t *source,
    uint32_t old_nrows, wl_columnar_merge_incremental_delta_attempt_t *attempt,
    wl_evaluation_control_t *control)
{
    return wl_columnar_merge_incremental_delta_attempt_begin_internal(source,
               NULL, old_nrows, attempt, control);
}

int
wl_columnar_merge_incremental_delta_attempt_begin_candidate(col_rel_t *source,
    const col_rel_t *candidate,
    wl_columnar_merge_incremental_delta_attempt_t *attempt,
    wl_evaluation_control_t *control)
{
    if (!candidate)
        return EINVAL;
    return wl_columnar_merge_incremental_delta_attempt_begin_internal(source,
               candidate, 0, attempt, control);
}

int
wl_columnar_merge_incremental_delta_attempt_advance(col_rel_t *source,
    wl_columnar_merge_incremental_delta_attempt_t *attempt,
    wl_evaluation_control_t *control, uint64_t work_budget)
{
    uint64_t remaining = work_budget
        > WL_EVALUATION_CONTROL_QUANTUM
        ? WL_EVALUATION_CONTROL_QUANTUM : work_budget;
    int rc = wl_columnar_merge_incremental_delta_attempt_validate(source,
            attempt,
            control);
    if (rc == EBUSY) {
        wl_columnar_merge_incremental_delta_attempt_discard_live(attempt);
        return rc;
    }
    if (rc != 0)
        return rc;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0)
        goto terminal;

    col_rel_t *image = attempt->transaction.image;
    while (attempt->phase != WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DONE) {
        switch (attempt->phase) {
        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_COPY_DEDUP:
            if (attempt->dedup_copy_cursor >= attempt->dedup_copy_count) {
                if (attempt->dedup_copy_occupied != source->dedup_count) {
                    rc = EINVAL;
                    goto terminal;
                }
                attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_COPY;
                continue;
            }
            rc = wl_columnar_merge_incremental_delta_charge(attempt,
                    &remaining, 1);
            if (rc != 0)
                goto sliced_or_terminal;
            rc = wl_columnar_eval_dedup_set_clone_slot(image, source,
                    attempt->dedup_copy_cursor);
            if (rc != 0)
                goto terminal;
            attempt->dedup_copy_occupied +=
                source->dedup_slots[attempt->dedup_copy_cursor] != 0;
            attempt->dedup_copy_cursor++;
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_COPY:
            if (attempt->copy_row >= attempt->input_nrows) {
                image->nrows = attempt->input_nrows;
                image->base_nrows = source->base_nrows;
                attempt->phase = attempt->external_candidate
                    ? WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SORT
                    : WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_FAST_OLD_MAX;
                attempt->max_old_row = 0;
                attempt->fast_scan_row = 1;
                continue;
            }
            if (source->ncols == 0) {
                rc = wl_columnar_merge_incremental_delta_charge(attempt,
                        &remaining, 1);
                if (rc != 0)
                    goto sliced_or_terminal;
                attempt->copy_row++;
                continue;
            }
            rc = wl_columnar_merge_incremental_delta_charge(attempt, &remaining,
                    1);
            if (rc != 0)
                goto sliced_or_terminal;
            const col_rel_t *copy_source = attempt->external_candidate
                && attempt->copy_row >= attempt->source_nrows
                ? attempt->candidate : source;
            uint32_t source_row = copy_source == source
                ? attempt->copy_row : attempt->copy_row - attempt->source_nrows;
            int64_t value = copy_source->columns[attempt->copy_column]
                [source_row];
            if (copy_source->column_types
                && copy_source->column_types[attempt->copy_column]
                == WIRELOG_TYPE_FLOAT) {
                if (!wl_columnar_float_bits_valid(value)) {
                    rc = EINVAL;
                    goto terminal;
                }
                if (wl_columnar_float_bits_zero(value))
                    value = 0;
            }
            image->columns[attempt->copy_column][attempt->copy_row] = value;
            attempt->copy_column++;
            if (attempt->copy_column == source->ncols) {
                attempt->copy_column = 0;
                attempt->copy_row++;
            }
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_FAST_OLD_MAX:
            if (attempt->old_nrows <= 1
                || attempt->fast_scan_row >= attempt->old_nrows) {
                attempt->fast_path = true;
                attempt->fast_scan_row = attempt->old_nrows;
                attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_FAST_CHECK;
                continue;
            }
            int cmp_old;
            rc = wl_columnar_merge_incremental_delta_compare_step(attempt,
                    &remaining,
                    source, WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID,
                    attempt->max_old_row,
                    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID,
                    attempt->fast_scan_row, &cmp_old);
            if (rc != 0)
                goto sliced_or_terminal;
            if (cmp_old < 0)
                attempt->max_old_row = attempt->fast_scan_row;
            attempt->fast_scan_row++;
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_FAST_CHECK:
            if (attempt->old_nrows == 0) {
                attempt->fast_path = true;
                attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SORT;
                continue;
            }
            if (attempt->fast_scan_row >= attempt->source_nrows) {
                attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SORT;
                continue;
            }
            int cmp_fast;
            rc = wl_columnar_merge_incremental_delta_compare_step(attempt,
                    &remaining,
                    source, WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID,
                    attempt->fast_scan_row,
                    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID,
                    attempt->max_old_row, &cmp_fast);
            if (rc != 0)
                goto sliced_or_terminal;
            if (cmp_fast <= 0)
                attempt->fast_path = false;
            attempt->fast_scan_row++;
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SORT: {
            if (attempt->input_nrows <= 1) {
                attempt->dedup_input = attempt->input_nrows ? 1u : 0u;
                attempt->dedup_write = attempt->dedup_input;
                attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DEDUP;
                continue;
            }
            if (attempt->sort_left >= attempt->input_nrows) {
                if (attempt->sort_width >= attempt->input_nrows
                    - attempt->sort_width) {
                    attempt->sort_source_primary
                        = !attempt->sort_source_primary;
                    if (!attempt->sort_source_primary)
                        wl_columnar_merge_incremental_delta_sort_swap_image_grids
                            (image);
                    attempt->dedup_input = 1;
                    attempt->dedup_write = 1;
                    attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DEDUP;
                } else {
                    attempt->sort_width *= 2u;
                    attempt->sort_source_primary
                        = !attempt->sort_source_primary;
                    attempt->sort_left = 0;
                }
                continue;
            }
            if (!attempt->sort_run_active) {
                uint64_t mid = (uint64_t)attempt->sort_left
                    + attempt->sort_width;
                uint64_t right = mid + attempt->sort_width;
                attempt->sort_mid = mid < attempt->input_nrows
                    ? (uint32_t)mid : attempt->input_nrows;
                attempt->sort_right = right < attempt->input_nrows
                    ? (uint32_t)right : attempt->input_nrows;
                attempt->sort_i = attempt->sort_left;
                attempt->sort_j = attempt->sort_mid;
                attempt->sort_out = attempt->sort_left;
                attempt->sort_run_active = true;
            }
            if (attempt->sort_copy_active) {
                if (image->ncols == 0) {
                    rc = wl_columnar_merge_incremental_delta_charge(attempt,
                            &remaining, 1);
                    if (rc != 0)
                        goto sliced_or_terminal;
                    attempt->sort_copy_active = false;
                    if (attempt->sort_copy_row == attempt->sort_i)
                        attempt->sort_i++;
                    else
                        attempt->sort_j++;
                    attempt->sort_out++;
                    continue;
                }
                rc = wl_columnar_merge_incremental_delta_charge(attempt,
                        &remaining, 1);
                if (rc != 0)
                    goto sliced_or_terminal;
                int64_t **dst =
                    wl_columnar_merge_incremental_delta_sort_destination(
                    attempt);
                int64_t **src =
                    wl_columnar_merge_incremental_delta_sort_source(attempt);
                dst[attempt->sort_copy_column][attempt->sort_out]
                    = src[attempt->sort_copy_column][attempt->sort_copy_row];
                attempt->sort_copy_column++;
                if (attempt->sort_copy_column == image->ncols) {
                    attempt->sort_copy_column = 0;
                    attempt->sort_copy_active = false;
                    if (attempt->sort_copy_row == attempt->sort_i)
                        attempt->sort_i++;
                    else
                        attempt->sort_j++;
                    attempt->sort_out++;
                }
                continue;
            }
            if (attempt->sort_out >= attempt->sort_right) {
                attempt->sort_left = attempt->sort_right;
                attempt->sort_run_active = false;
                continue;
            }
            uint32_t pick;
            if (attempt->sort_i >= attempt->sort_mid) {
                pick = attempt->sort_j;
            } else if (attempt->sort_j >= attempt->sort_right) {
                pick = attempt->sort_i;
            } else {
                uint8_t sort_grid = attempt->sort_source_primary
                    ? WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID
                    : WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_MERGE_GRID;
                int cmp_sort;
                rc = wl_columnar_merge_incremental_delta_compare_step(attempt,
                        &remaining, source, sort_grid, attempt->sort_i,
                        sort_grid, attempt->sort_j, &cmp_sort);
                if (rc != 0)
                    goto sliced_or_terminal;
                pick = cmp_sort <= 0 ? attempt->sort_i : attempt->sort_j;
            }
            attempt->sort_copy_row = pick;
            attempt->sort_copy_active = true;
            continue;
        }

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DEDUP:
            if (attempt->dedup_input >= attempt->input_nrows) {
                image->nrows = attempt->dedup_write;
                image->base_nrows = source->base_nrows > image->nrows
                    ? image->nrows : source->base_nrows;
                image->sorted_nrows = image->nrows;
                image->run_count = image->nrows ? 1u : 0u;
                if (image->run_count)
                    image->run_ends[0] = image->nrows;
                attempt->novel_row = 0;
                attempt->novel_run = 0;
                attempt->novel_search_active = false;
                attempt->novel_found = false;
                attempt->phase =
                    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_COUNT_NOVEL;
                continue;
            }
            if (!attempt->dedup_copy_active) {
                int cmp_duplicate;
                rc = wl_columnar_merge_incremental_delta_compare_step(attempt,
                        &remaining, source,
                        WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID,
                        attempt->dedup_write - 1u,
                        WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID,
                        attempt->dedup_input, &cmp_duplicate);
                if (rc != 0)
                    goto sliced_or_terminal;
                if (cmp_duplicate == 0) {
                    attempt->dedup_input++;
                    continue;
                }
                if (attempt->dedup_write == attempt->dedup_input) {
                    attempt->dedup_write++;
                    attempt->dedup_input++;
                    continue;
                }
                attempt->dedup_copy_active = true;
                attempt->dedup_copy_column = 0;
                continue;
            }
            if (image->ncols == 0) {
                rc = wl_columnar_merge_incremental_delta_charge(attempt,
                        &remaining, 1);
                if (rc != 0)
                    goto sliced_or_terminal;
                attempt->dedup_copy_active = false;
                attempt->dedup_write++;
                attempt->dedup_input++;
                continue;
            }
            rc = wl_columnar_merge_incremental_delta_charge(attempt, &remaining,
                    1);
            if (rc != 0)
                goto sliced_or_terminal;
            image->columns[attempt->dedup_copy_column]
            [attempt->dedup_write]
                = image->columns[attempt->dedup_copy_column]
                [attempt->dedup_input];
            attempt->dedup_copy_column++;
            if (attempt->dedup_copy_column == image->ncols) {
                attempt->dedup_copy_column = 0;
                attempt->dedup_copy_active = false;
                attempt->dedup_write++;
                attempt->dedup_input++;
            }
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_COUNT_NOVEL:
            if (attempt->novel_row >= image->nrows) {
                attempt->phase =
                    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_ALLOC_DELTA;
                continue;
            }
            if (attempt->novel_found) {
                attempt->novel_row++;
                attempt->novel_run = 0;
                attempt->novel_search_active = false;
                attempt->novel_found = false;
                continue;
            }
            if (attempt->novel_run >= attempt->run_count) {
                rc = wl_columnar_merge_incremental_delta_charge(attempt,
                        &remaining, 1);
                if (rc != 0)
                    goto sliced_or_terminal;
                attempt->novel_count++;
                attempt->novel_row++;
                attempt->novel_run = 0;
                attempt->novel_search_active = false;
                continue;
            }
            if (!attempt->novel_search_active) {
                wl_columnar_merge_incremental_delta_run_advance(attempt,
                    &attempt->novel_run, &attempt->search_low,
                    &attempt->search_high);
                attempt->novel_search_active = true;
                continue;
            }
            if (attempt->search_low >= attempt->search_high) {
                attempt->novel_run++;
                attempt->novel_search_active = false;
                continue;
            }
            uint32_t mid = attempt->search_low
                + (attempt->search_high - attempt->search_low) / 2u;
            int cmp_novel;
            rc = wl_columnar_merge_incremental_delta_compare_step(attempt,
                    &remaining,
                    source, WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID,
                    mid,
                    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID,
                    attempt->novel_row, &cmp_novel);
            if (rc != 0) {
                if (rc == EAGAIN)
                    goto sliced_or_terminal;
                goto terminal;
            }
            if (cmp_novel == 0)
                attempt->novel_found = true;
            else if (cmp_novel < 0)
                attempt->search_low = mid + 1u;
            else
                attempt->search_high = mid;
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_ALLOC_DELTA:
            rc = wl_evaluation_control_charge(control, 0);
            if (rc != 0)
                goto terminal;
            if (attempt->novel_count > 0) {
                bool denied = false;
                uint32_t capacity = attempt->novel_count;
                if (capacity < (image->ncols ? 1u : 0u))
                    capacity = 1u;
                rc = col_rel_reserve_capacity_admitted(attempt->delta,
                        capacity, &denied);
                if (rc == ENOMEM && denied)
                    rc = ENOSPC;
                if (rc != 0)
                    goto terminal;
            }
            rc = wl_evaluation_control_charge(control, 0);
            if (rc != 0)
                goto terminal;
            attempt->emit_input = 0;
            attempt->emit_run = 0;
            attempt->emit_search_active = false;
            attempt->emit_found = false;
            attempt->emit_count = 0;
            attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_EMIT_DELTA;
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_EMIT_DELTA:
            if (attempt->emit_copy_active) {
                if (image->ncols == 0) {
                    rc = wl_columnar_merge_incremental_delta_charge(attempt,
                            &remaining, 1);
                    if (rc != 0)
                        goto sliced_or_terminal;
                    attempt->emit_copy_active = false;
                    attempt->emit_count++;
                    attempt->emit_input++;
                    attempt->emit_search_active = false;
                    attempt->emit_run = 0;
                    continue;
                }
                rc = wl_columnar_merge_incremental_delta_charge(attempt,
                        &remaining, 1);
                if (rc != 0)
                    goto sliced_or_terminal;
                attempt->delta->columns[attempt->emit_copy_column]
                [attempt->emit_count]
                    = image->columns[attempt->emit_copy_column]
                    [attempt->emit_input];
                attempt->emit_copy_column++;
                if (attempt->emit_copy_column == image->ncols) {
                    attempt->emit_copy_active = false;
                    attempt->emit_count++;
                    attempt->emit_input++;
                    attempt->emit_copy_column = 0;
                    attempt->emit_search_active = false;
                    attempt->emit_run = 0;
                }
                continue;
            }
            if (attempt->emit_input >= image->nrows) {
                attempt->delta->nrows = attempt->emit_count;
                attempt->delta->sorted_nrows = attempt->emit_count;
                attempt->delta->run_count = attempt->emit_count ? 1u : 0u;
                if (attempt->emit_count)
                    attempt->delta->run_ends[0] = attempt->emit_count;
                if (attempt->emit_count)
                    wl_columnar_relation_touch_view(attempt->delta);
                attempt->phase = WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DONE;
                continue;
            }
            if (attempt->emit_found) {
                attempt->emit_found = false;
                attempt->emit_search_active = false;
                attempt->emit_run = 0;
                attempt->emit_input++;
                continue;
            }
            if (attempt->emit_run >= attempt->run_count) {
                attempt->emit_copy_active = true;
                attempt->emit_copy_column = 0;
                continue;
            }
            if (!attempt->emit_search_active) {
                wl_columnar_merge_incremental_delta_run_advance(attempt,
                    &attempt->emit_run, &attempt->emit_low,
                    &attempt->emit_high);
                attempt->emit_search_active = true;
                continue;
            }
            if (attempt->emit_low >= attempt->emit_high) {
                attempt->emit_run++;
                attempt->emit_search_active = false;
                continue;
            }
            uint32_t emit_mid = attempt->emit_low
                + (attempt->emit_high - attempt->emit_low) / 2u;
            int emit_cmp;
            rc = wl_columnar_merge_incremental_delta_compare_step(attempt,
                    &remaining,
                    source, WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_SOURCE_GRID,
                    emit_mid,
                    WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_IMAGE_GRID,
                    attempt->emit_input, &emit_cmp);
            if (rc != 0) {
                if (rc == EAGAIN)
                    goto sliced_or_terminal;
                goto terminal;
            }
            if (emit_cmp == 0)
                attempt->emit_found = true;
            else if (emit_cmp < 0)
                attempt->emit_low = emit_mid + 1u;
            else
                attempt->emit_high = emit_mid;
            continue;

        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DONE:
        case WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_INERT:
        default:
            rc = EINVAL;
            goto terminal;
        }
    }
    return 0;

sliced_or_terminal:
    if (rc == EAGAIN)
        return EAGAIN;
terminal:
    wl_columnar_merge_incremental_delta_attempt_discard_live(attempt);
    return rc;
}

int
wl_columnar_merge_incremental_delta_attempt_commit(col_rel_t *source,
    wl_columnar_merge_incremental_delta_attempt_t *attempt,
    col_rel_t **delta_out,
    int *out_fast_path, wl_evaluation_control_t *control)
{
    int rc = wl_columnar_merge_incremental_delta_attempt_validate(source,
            attempt,
            control);
    if (rc == EBUSY) {
        wl_columnar_merge_incremental_delta_attempt_discard_live(attempt);
        return rc;
    }
    if (rc != 0)
        return rc;
    if (!delta_out || *delta_out ||
        attempt->phase != WL_COLUMNAR_MERGE_INCREMENTAL_DELTA_DONE)
        return EINVAL;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc != 0) {
        wl_columnar_merge_incremental_delta_attempt_discard_live(attempt);
        return rc;
    }
    if (attempt->no_op) {
        col_rel_mutable_image_discard(&attempt->transaction);
    } else {
        rc = col_rel_mutable_image_commit(&attempt->transaction);
        if (rc != 0) {
            wl_columnar_merge_incremental_delta_attempt_discard_live(attempt);
            return rc;
        }
    }
    if (attempt->candidate_reader_held) {
        rc = col_rel_source_reader_release(&attempt->candidate_reader);
        if (rc != 0)
            abort();
        attempt->candidate_reader_held = false;
    }
    *delta_out = attempt->delta;
    attempt->delta = NULL;
    if (out_fast_path)
        *out_fast_path = attempt->fast_path ? 1 : 0;
    memset(attempt, 0, sizeof(*attempt));
    return 0;
}

int
wl_columnar_merge_incremental_delta_attempt_abort(col_rel_t *source,
    wl_columnar_merge_incremental_delta_attempt_t *attempt)
{
    if (!source || !attempt || attempt->identity != (uintptr_t)attempt
        || attempt->transaction.source != source
        || attempt->transaction.writer.owner != &source->source_access
        || attempt->transaction.writer.identity
        != (uintptr_t)&attempt->transaction.writer
        || !wl_columnar_source_access_writer_thread_equal(
            &attempt->transaction.writer))
        return EINVAL;
    wl_columnar_merge_incremental_delta_attempt_discard_live(attempt);
    return 0;
}

/* --- Private original-attempt scalar CONCAT (#1821) --- */
enum {
    WL_COLUMNAR_MERGE_CONCAT_WORK = 192,
    WL_COLUMNAR_MERGE_CONCAT_CELL = 128,
    /* Each payment partitions128 current +64 future. Bootstrap prepays
     * distinct BINARY parent and ACK/reset first capacities (2*256).
     * Then batch18 + sources2 + scratch4 + DECIDE1 =25*256, before any
     * resource owner. A binary candidate/retired/pin/ACK ledger is separate. */
    WL_COLUMNAR_MERGE_CONCAT_RESET_PAYMENTS = 8,
    WL_COLUMNAR_MERGE_CONCAT_RESOURCE_PAYMENTS = 100,
    WL_COLUMNAR_MERGE_CONCAT_HOST = 384,
    WL_COLUMNAR_MERGE_CONCAT_RUNNING = 1,
    WL_COLUMNAR_MERGE_CONCAT_BATCH,
    WL_COLUMNAR_MERGE_CONCAT_LEFT,
    WL_COLUMNAR_MERGE_CONCAT_RIGHT,
    WL_COLUMNAR_MERGE_CONCAT_PHYSICAL,
    WL_COLUMNAR_MERGE_CONCAT_CREDIT,
    WL_COLUMNAR_MERGE_CONCAT_REFERENCE,
    WL_COLUMNAR_MERGE_CONCAT_CLEAR,
    WL_COLUMNAR_MERGE_CONCAT_DECIDE,
    WL_COLUMNAR_MERGE_CONCAT_BINARY,
    WL_COLUMNAR_MERGE_CONCAT_DONE
};
#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_merge_test_concat_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
int wl_columnar_merge_test_concat_fail_allocation = -1;
#endif

static int
wl_columnar_merge_concat_charge(wl_evaluation_control_t *control, uint64_t id,
    uint64_t work)
{
    if (!control || !id
        || wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
    return wl_evaluation_control_charge(control, work);
}

static int
wl_columnar_merge_concat_tail(wl_evaluation_control_t *control, uint64_t id,
    unsigned phase, uint64_t progress)
{
    if (!control || !id
        || wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_merge_test_concat_checkpoint)
        wl_columnar_merge_test_concat_checkpoint(control, phase, progress);
#else
    (void)phase; (void)progress;
#endif
    return wl_columnar_merge_concat_charge(control, id, 0);
}

static int
wl_columnar_merge_concat_return(wl_evaluation_control_t *control, uint64_t id,
    int result)
{
    int tail = wl_columnar_merge_concat_charge(control, id, 0);
    return tail > 0 ? tail : result ? result : tail;
}

static int
wl_columnar_merge_concat_fund(wl_evaluation_control_t *control,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (funding == control) return EINVAL;
    return wl_columnar_merge_concat_charge(funding, funding_id,
               WL_COLUMNAR_MERGE_CONCAT_HOST);
}

static bool
wl_columnar_merge_concat_thread(const wl_columnar_merge_concat_context_t *ctx)
{
#if defined(WL_HAVE_C11_THREADS)
    return ctx->thread_valid && thrd_equal(ctx->owner_thread, thrd_current());
#elif defined(_WIN32) || defined(_WIN64)
    return ctx->thread_valid && ctx->owner_thread == GetCurrentThreadId();
#else
    return ctx->thread_valid && pthread_equal(ctx->owner_thread,
               pthread_self());
#endif
}

static int
wl_columnar_merge_concat_valid(wl_columnar_merge_concat_context_t *ctx,
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    if (ctx->identity != ctx || ctx->frame != frame || !ctx->session
        || !ctx->control || !ctx->attempt_id || !ctx->phase
        || ctx->numeric_owner != owner || !wl_columnar_merge_concat_thread(ctx))
        return EINVAL;
    return ctx->busy ? EBUSY : 0;
}

static void
wl_columnar_merge_concat_status(const wl_columnar_merge_concat_context_t *ctx,
    wl_columnar_filter_status_t *status)
{
    status->progress = ctx->phase == WL_COLUMNAR_MERGE_CONCAT_DONE
        ? WL_COLUMNAR_FILTER_PROGRESS_DONE : ctx->cleanup_result
        ? WL_COLUMNAR_FILTER_PROGRESS_RETAINED :
        WL_COLUMNAR_FILTER_PROGRESS_YIELD;
    status->execution_result = ctx->execution_result;
    status->cleanup_result = ctx->cleanup_result;
    status->semantic_result = ctx->execution_result
        ? ctx->execution_result : ctx->cleanup_result;
    status->committed = ctx->committed;
}

/* Each variable row/lane visit has its own admission, including non-FLOAT.
* Left scan precedes right compatibility, as raw left-then-right append. */
static int
wl_columnar_merge_concat_finite(wl_columnar_relation_scalar_source_t *source,
    wl_evaluation_control_t *control, uint64_t id, unsigned side)
{
    for (uint32_t row = 0; row < source->nrows; ++row) {
        int rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_CELL);
        if (rc) return rc;
        for (uint32_t c = 0; c < source->width; ++c) {
            rc = wl_columnar_merge_concat_charge(control, id,
                    WL_COLUMNAR_MERGE_CONCAT_CELL);
            if (rc) return rc;
            bool valid = !source->types ||
                source->types[c] != WIRELOG_TYPE_FLOAT
                || wl_columnar_float_bits_valid(source->columns[c][row]);
            rc = wl_columnar_merge_concat_tail(control, id, 10 + side, c);
            if (rc > 0 || valid) {
                if (rc) return rc;
            } else return EINVAL;
        }
        rc = wl_columnar_merge_concat_tail(control, id, 12 + side, row);
        if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_merge_concat_scratch(wl_columnar_join_scratch_t *scratch,
    uint64_t bytes, wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control, uint64_t id)
{
    int rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) return rc;
    wl_columnar_memory_reservation_init(&scratch->credit);
    scratch->initialized = true;
    rc = wl_columnar_merge_concat_tail(control, id, 20, bytes);
    if (rc) return rc;
    if (governor) {
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) return rc;
        scratch->governor = governor;
        wl_columnar_memory_governor_ref_retain(governor);
        scratch->ref_held = true;
        rc = wl_columnar_merge_concat_tail(control, id, 21, bytes);
        if (rc) return rc;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(governor), bytes,
            &scratch->credit, &status, control, id);
        if (rc) return rc;
        scratch->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
        rc = wl_columnar_merge_concat_tail(control, id, 27, bytes);
        if (rc) return rc;
        int admission = scratch->admitted ? 0
            : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status ==
            WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
        rc = wl_columnar_merge_concat_return(control, id, admission);
        if (rc) return rc;
    }
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    bool fail = wl_columnar_merge_test_concat_fail_allocation == 0;
    if (wl_columnar_merge_test_concat_fail_allocation > 0)
        --wl_columnar_merge_test_concat_fail_allocation;
    scratch->physical = fail ? NULL : malloc((size_t)bytes);
#else
    scratch->physical = malloc((size_t)bytes);
#endif
    scratch->allocated = scratch->physical != NULL;
    rc = wl_columnar_merge_concat_tail(control, id, 22, bytes);
    if (rc > 0 || scratch->allocated) {
        if (rc) return rc;
    } else return ENOMEM;
    if (scratch->admitted) {
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) return rc;
        rc = wl_columnar_merge_concat_tail(control, id, 23, bytes);
        if (rc) return rc;
        rc = wl_columnar_memory_governor_commit_attempt_controlled(
            &scratch->credit, scratch->physical, &scratch->committed, control,
            id);
        rc = wl_columnar_merge_concat_return(control, id, rc);
    }
    return rc;
}

int
wl_columnar_merge_concat_attempt_start(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status, wl_evaluation_control_t *funding,
    uint64_t funding_id)
{
    return wl_columnar_merge_concat_attempt_start_owned(op, frame, session,
               control, id, status, funding, funding_id, NULL);
}

int
wl_columnar_merge_concat_attempt_start_owned(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status, wl_evaluation_control_t *funding,
    uint64_t funding_id, const void *owner)
{
    if (!op || !status || (owner && !control)) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, session,
            &stack, owner);
    if (rc) return rc;
    if (owner) {
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        if (record->control != control ||
            record->attempt_id != id) return EINVAL;
    }
    rc = wl_columnar_merge_concat_fund(control, funding, funding_id);
    if (rc) return rc;
    if (!control) {
        int result = op->op == WL_PLAN_OP_CONCAT ? col_op_concat(stack, session)
            : EINVAL;
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = result; status->semantic_result = result;
        status->cleanup_result = 0; status->committed = result == 0;
        return 0;
    }
    wl_columnar_merge_concat_context_t *ctx = NULL;
    rc = wl_columnar_eval_stack_concat_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    if (ctx->identity || ctx->phase) return EBUSY;
    for (unsigned i = 0; i < WL_COLUMNAR_MERGE_CONCAT_RESET_PAYMENTS; ++i) {
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) goto inert;
        rc = wl_columnar_merge_concat_tail(control, id, 1, i);
        if (rc) goto inert;
    }
    rc = wl_columnar_eval_stack_binary_begin_owned(frame, session, control, id,
            owner);
    if (rc) goto inert;
    /* Only fixed context ownership is published here. Its first reset share
     * was admitted before binary_begin; all resource markers are still false. */
    ctx->identity = ctx; ctx->frame = frame; ctx->session = session;
    ctx->control = control; ctx->attempt_id = id; ctx->numeric_owner = owner;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->committed = false; ctx->busy = true; ctx->private_output = NULL;
    ctx->source_initialized[0] = false; ctx->source_initialized[1] = false;
    ctx->batch_initialized = false;
    ctx->scratch.initialized = false; ctx->scratch.allocated = false;
    ctx->scratch.admitted = false; ctx->scratch.committed = false;
    ctx->scratch.ref_held = false; ctx->scratch.physical = NULL;
    ctx->scratch.governor = NULL;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true; ctx->phase = WL_COLUMNAR_MERGE_CONCAT_RUNNING;
    rc = wl_columnar_merge_concat_return(control, id, 0);
    if (rc) goto done;
    for (unsigned i = 0; i < WL_COLUMNAR_MERGE_CONCAT_RESOURCE_PAYMENTS; ++i) {
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) goto done;
        rc = wl_columnar_merge_concat_tail(control, id, 2, i);
        if (rc) goto done;
    }
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) goto done;
    bool supported = op->op == WL_PLAN_OP_CONCAT && !op->materialized
        && !session->current_iteration && !session->diff_operators_active
        && !session->retraction_seeded && !session->retraction_right_pass
        && !session->tdd_input_run && !session->tdd_subpass_active
        && !session->tdd_outbound_only_active && !session->join_batch_bytes
        && !session->join_output_shared_count && !session->coordinator
        && session->num_workers <= 1;
    rc = wl_columnar_merge_concat_tail(control, id, 3, 0);
    if (rc > 0 || supported) {
        if (rc) goto done;
    } else {
        rc = ENOTSUP; goto done;
    }
    const col_rel_t *inputs[2] = { NULL, NULL };
    const eval_entry_t *entries[2] = { NULL, NULL };
    col_rel_t **out_slot = NULL;
    for (unsigned side = 0; side < 2; ++side) {
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) goto done;
        rc = wl_columnar_eval_stack_binary_input_owned(frame, side,
                &entries[side], owner);
        rc = wl_columnar_merge_concat_return(control, id, rc);
        if (rc) goto done;
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) goto done;
        inputs[side] = entries[side]->rel;
        rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
            &ctx->sources[side], &ctx->source_initialized[side], control, id);
        rc = wl_columnar_merge_concat_return(control, id, rc);
        if (rc) goto done;
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) goto done;
        rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &ctx->sources[side], inputs[side], false);
        rc = wl_columnar_merge_concat_return(control, id, rc);
        if (rc) goto done;
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_WORK);
        if (rc) goto done;
        bool no_ts = !ctx->sources[side].timestamps;
        rc = wl_columnar_merge_concat_tail(control, id, 4, side);
        if (rc > 0 || no_ts) {
            if (rc) goto done;
        } else {
            rc = ENOTSUP; goto done;
        }
    }
    wl_columnar_relation_scalar_source_t *left = &ctx->sources[0];
    wl_columnar_relation_scalar_source_t *right = &ctx->sources[1];
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) goto done;
    bool widths = left->width == right->width;
    rc = wl_columnar_merge_concat_tail(control, id, 5, left->width);
    if (rc > 0 || widths) {
        if (rc) goto done;
    } else {
        rc = EINVAL; goto done;
    }
    rc = wl_columnar_merge_concat_finite(left, control, id, 0);
    rc = wl_columnar_merge_concat_return(control, id, rc);
    if (rc) goto done;
    for (uint32_t c = 0; right->nrows && c < left->width; ++c) {
        rc = wl_columnar_merge_concat_charge(control, id,
                WL_COLUMNAR_MERGE_CONCAT_CELL);
        if (rc) goto done;
        wirelog_column_type_t a =
            left->types ? left->types[c] : WIRELOG_TYPE_INT64;
        wirelog_column_type_t b =
            right->types ? right->types[c] : WIRELOG_TYPE_INT64;
        bool compatible = a == b ||
            (a != WIRELOG_TYPE_FLOAT && b != WIRELOG_TYPE_FLOAT);
        rc = wl_columnar_merge_concat_tail(control, id, 6, c);
        if (rc > 0 || compatible) {
            if (rc) goto done;
        } else {
            rc = EINVAL; goto done;
        }
    }
    rc = wl_columnar_merge_concat_finite(right, control, id, 1);
    rc = wl_columnar_merge_concat_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) goto done;
    uint64_t bytes = 0;
    bool sized = right->nrows <= UINT32_MAX - left->nrows
        && wl_columnar_memory_size_mul(left->width ? left->width : 1,
            sizeof(int64_t), &bytes) && bytes <= SIZE_MAX;
    rc = wl_columnar_merge_concat_tail(control, id, 7, bytes);
    if (rc > 0 || sized) {
        if (rc) goto done;
    } else {
        rc = EOVERFLOW; goto done;
    }
    wl_columnar_memory_governor_ref_t *governor = session->memory_governor
        ? session->memory_governor : inputs[0]->memory_governor
        ? inputs[0]->memory_governor : inputs[1]->memory_governor;
    rc = wl_columnar_merge_concat_scratch(&ctx->scratch, bytes, governor,
            control, id);
    rc = wl_columnar_merge_concat_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_binary_output_owned(frame, &out_slot, owner);
    rc = wl_columnar_merge_concat_return(control, id, rc);
    if (rc) goto done;
    const wirelog_column_type_t *types = left->types ? left->types
        : right->nrows ? right->types : NULL;
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) goto done;
    rc =
        wl_columnar_relation_scalar_source_new_like_typed_operator_attempt_controlled
        (
        left, "$concat", governor, types, out_slot);
    rc = wl_columnar_merge_concat_return(control, id, rc);
    if (rc) goto done;
    ctx->private_output = *out_slot;
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) goto done;
    wl_columnar_filter_attempt_batch_init(&ctx->batch);
    wl_columnar_relation_private_batch_drain_init(&ctx->drain);
    ctx->batch_initialized = true;
    rc = wl_columnar_merge_concat_tail(control, id, 24, 0);
    if (rc) goto done;
    rc = wl_columnar_merge_concat_charge(control, id,
            WL_COLUMNAR_MERGE_CONCAT_WORK);
    if (rc) goto done;
    rc = wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            *out_slot, control);
    rc = wl_columnar_merge_concat_return(control, id, rc);
    if (rc) goto done;
    for (unsigned side = 0; side < 2; ++side) {
        for (uint32_t row = 0; row < ctx->sources[side].nrows; ++row) {
            rc = wl_columnar_merge_concat_charge(control, id,
                    WL_COLUMNAR_MERGE_CONCAT_WORK);
            if (rc) goto done;
            rc = wl_columnar_relation_scalar_source_gather_attempt_controlled(
                &ctx->sources[side], row, ctx->scratch.physical, left->width);
            rc = wl_columnar_merge_concat_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_merge_concat_charge(control, id,
                    WL_COLUMNAR_MERGE_CONCAT_WORK);
            if (rc) goto done;
            rc =
                wl_columnar_relation_append_batch_numeric_row_operator_controlled
                (
                &ctx->batch, ctx->scratch.physical, control, id);
            rc = wl_columnar_merge_concat_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_merge_concat_tail(control, id, 25 + side, row);
            if (rc) goto done;
        }
    }
done:
    if (rc == ENOMEM && ctx->private_output
        && ctx->private_output->memory_budget_denial_pending) rc = ENOSPC;
    if (rc == ENOSPC) session->memory_budget_denied = true;
    ctx->execution_result = rc; ctx->busy = false;
    /* A partial resource-prepayment prefix owns only the fully prefunded
     * fixed context and A transaction. Do not enter any unpaid empty stages. */
    bool resources = ctx->source_initialized[0] || ctx->source_initialized[1]
        || ctx->batch_initialized || ctx->scratch.initialized;
    ctx->phase = resources ? WL_COLUMNAR_MERGE_CONCAT_BATCH
        : WL_COLUMNAR_MERGE_CONCAT_BINARY;
    wl_columnar_merge_concat_status(ctx, status);
    return 0;
inert:
    status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
    status->execution_result = rc; status->semantic_result = rc;
    status->cleanup_result = 0; status->committed = false;
    return 0;
}

int
wl_columnar_merge_concat_attempt_step_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_status_t *status, wl_evaluation_control_t *funding,
    uint64_t funding_id, const void *owner)
{
    if (!status) return EINVAL;
    wl_columnar_merge_concat_context_t *ctx = NULL;
    int rc = wl_columnar_eval_stack_concat_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    rc = wl_columnar_merge_concat_valid(ctx, frame, owner);
    if (rc) return rc;
    rc = wl_columnar_merge_concat_fund(ctx->control, funding, funding_id);
    if (rc) return rc;
    ctx->busy = true;
    switch (ctx->phase) {
    case WL_COLUMNAR_MERGE_CONCAT_BATCH:
        if (ctx->batch_initialized) {
            bool done = false;
            rc = wl_columnar_relation_private_batch_drain_step(&ctx->drain,
                    &ctx->batch, ctx->execution_result, &done);
            if (rc) {
                ctx->cleanup_result = rc; break;
            }
            ctx->cleanup_result = 0;
            if (!done) break;
            ctx->batch_initialized = false;
        }
        ctx->phase = WL_COLUMNAR_MERGE_CONCAT_LEFT;
        break;
    case WL_COLUMNAR_MERGE_CONCAT_LEFT:
    case WL_COLUMNAR_MERGE_CONCAT_RIGHT: {
        unsigned side = ctx->phase == WL_COLUMNAR_MERGE_CONCAT_LEFT ? 0 : 1;
        if (ctx->source_initialized[side]) {
            (void)wl_columnar_relation_scalar_source_end(&ctx->sources[side],
                ctx->execution_result);
            if (ctx->sources[side].phase !=
                WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED)
                abort();
            ctx->source_initialized[side] = false;
        }
        ++ctx->phase;
        break;
    }
    case WL_COLUMNAR_MERGE_CONCAT_PHYSICAL:
        if (ctx->scratch.allocated) {
            void *physical = ctx->scratch.physical;
            ctx->scratch.physical = NULL; ctx->scratch.allocated = false;
            free(physical);
        }
        ctx->phase = WL_COLUMNAR_MERGE_CONCAT_CREDIT;
        break;
    case WL_COLUMNAR_MERGE_CONCAT_CREDIT:
        if (ctx->scratch.admitted) {
            bool released = ctx->scratch.committed
                ? wl_columnar_memory_release(&ctx->scratch.credit)
                : wl_columnar_memory_rollback(&ctx->scratch.credit);
            if (!released) {
                ctx->cleanup_result = EINVAL; break;
            }
            ctx->scratch.admitted = false;
        }
        ctx->cleanup_result = 0;
        ctx->phase = WL_COLUMNAR_MERGE_CONCAT_REFERENCE;
        break;
    case WL_COLUMNAR_MERGE_CONCAT_REFERENCE:
        if (ctx->scratch.ref_held) {
            wl_columnar_memory_governor_ref_t *governor = ctx->scratch.governor;
            ctx->scratch.governor = NULL; ctx->scratch.ref_held = false;
            wl_columnar_memory_governor_ref_release(governor);
        }
        ctx->phase = WL_COLUMNAR_MERGE_CONCAT_CLEAR;
        break;
    case WL_COLUMNAR_MERGE_CONCAT_CLEAR:
        ctx->scratch.initialized = false; ctx->scratch.committed = false;
        ctx->phase = WL_COLUMNAR_MERGE_CONCAT_DECIDE;
        break;
    case WL_COLUMNAR_MERGE_CONCAT_DECIDE:
        if (!ctx->execution_result) {
            rc = wl_columnar_merge_concat_charge(ctx->control, ctx->attempt_id,
                    0);
            if (!rc) rc = wl_columnar_eval_stack_binary_commit_owned(frame,
                        &ctx->committed, owner);
            ctx->execution_result = rc;
        }
        ctx->private_output = NULL;
        ctx->phase = WL_COLUMNAR_MERGE_CONCAT_BINARY;
        break;
    case WL_COLUMNAR_MERGE_CONCAT_BINARY: {
        /* Child status is consumed on this later host action; no further
         * child or disposal occurs after its return in this dispatch. */
        wl_columnar_filter_status_t child;
        ctx->busy = false;
        rc = wl_columnar_eval_stack_binary_finish_owned(frame,
                ctx->execution_result, &child, funding, funding_id, owner);
        if (rc) return rc;
        ctx->cleanup_result = child.cleanup_result;
        if (!ctx->execution_result) ctx->execution_result =
                child.execution_result;
        if (child.semantic_result && !child.cleanup_result)
            ctx->execution_result = child.semantic_result;
        if (child.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE)
            ctx->phase = WL_COLUMNAR_MERGE_CONCAT_DONE;
        *status = child;
        return 0;
    }
    case WL_COLUMNAR_MERGE_CONCAT_DONE:
        break;
    default:
        ctx->busy = false;
        return EBUSY;
    }
    ctx->busy = false;
    wl_columnar_merge_concat_status(ctx, status);
    return 0;
}

int
wl_columnar_merge_concat_attempt_ack_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *funding, uint64_t funding_id, const void *owner)
{
    wl_columnar_merge_concat_context_t *ctx = NULL;
    int rc = wl_columnar_eval_stack_concat_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    if (!ctx->identity && !ctx->phase) {
        /* The getter already authenticated the original numeric owner. Even
         * an inert ACK must not debit that operation or its successor. */
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        return wl_columnar_merge_concat_fund(record ? record->control : NULL,
                   funding, funding_id);
    }
    rc = wl_columnar_merge_concat_valid(ctx, frame, owner);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_MERGE_CONCAT_DONE) return EBUSY;
    rc = wl_columnar_merge_concat_fund(ctx->control, funding, funding_id);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_binary_ack_owned(frame, funding, funding_id,
            owner);
    if (rc) return rc;
    ctx->identity = NULL; ctx->frame = NULL; ctx->session = NULL;
    ctx->control = NULL; ctx->attempt_id = 0; ctx->numeric_owner = NULL;
    ctx->phase = 0; ctx->busy = false; ctx->committed = false;
    ctx->thread_valid = false; ctx->execution_result = 0;
    ctx->cleanup_result = 0;
    return 0;
}
