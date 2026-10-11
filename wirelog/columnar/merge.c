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

static int
wl_columnar_merge_consolidate_tail(wl_evaluation_control_t *c)
{
    return wl_evaluation_control_charge(c, 0);
}

static int
wl_columnar_merge_consolidate_reset(wl_columnar_consolidate_record_t *record,
    wl_evaluation_control_t *c)
{
    if (!record || !record->ready) return EINVAL;
    if (record->phase == WL_COLUMNAR_CONSOLIDATE_RESETTING) return EBUSY;
    if (record->phase != WL_COLUMNAR_CONSOLIDATE_INERT
        && record->phase != WL_COLUMNAR_CONSOLIDATE_RETIRED) return EBUSY;
    int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
    record->phase = WL_COLUMNAR_CONSOLIDATE_RESETTING;
    record->lane = WL_COLUMNAR_CONSOLIDATE_LANE_INITIALIZING;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    for (size_t offset = 0;
        offset < offsetof(wl_columnar_consolidate_record_t, phase);){
        size_t bytes = offsetof(wl_columnar_consolidate_record_t,
                phase) - offset;
        if (bytes > 128) bytes = 128;
        rc = wl_evaluation_control_charge(c, bytes); if (rc) return rc;
        memset((char *)record + offset, 0, bytes); offset += bytes;
        wl_columnar_relation_consolidate_checkpoint(c, 20, offset, record,
            NULL);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    record->sequence.metadata_admission.identity =
        &record->sequence.metadata_admission;
    atomic_store_explicit(&record->sequence.metadata_admission.state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_relaxed);
    record->sequence.workspace.admission.identity =
        &record->sequence.workspace.admission;
    atomic_store_explicit(&record->sequence.workspace.admission.state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_relaxed);
    record->merge_admission.identity = &record->merge_admission;
    atomic_store_explicit(&record->merge_admission.state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_relaxed);
    record->lane = WL_COLUMNAR_CONSOLIDATE_RADIX;
    record->phase = WL_COLUMNAR_CONSOLIDATE_PREPARING;
    return wl_columnar_merge_consolidate_tail(c);
}

/* The tagged heuristic is separate from errno and control dispositions. */
typedef enum {
    WL_COLUMNAR_MERGE_HASH_COMPLETE = 0,
    WL_COLUMNAR_MERGE_HASH_FALLBACK
} wl_columnar_merge_hash_outcome_t;

static int
wl_columnar_merge_hash_step(wl_columnar_consolidate_record_t *record,
    wl_evaluation_control_t *c, int stage, uint64_t progress)
{
    wl_columnar_relation_consolidate_checkpoint(c, stage, progress, record,
        record->hash.relation);
    return wl_columnar_merge_consolidate_tail(c);
}

int
wl_columnar_merge_hash_retire(wl_columnar_consolidate_record_t *record,
    int primary, wl_evaluation_control_t *c)
{
    if (!record || !record->ready ||
        record->lane != WL_COLUMNAR_CONSOLIDATE_HASH)
        return primary > 0 ? primary : EINVAL;
    if (c) {
        int rc = wl_evaluation_control_charge(c, 8);
        if (rc) return primary > 0 ? primary : rc;
    }
    record->phase = WL_COLUMNAR_CONSOLIDATE_RETIRING;
    wl_columnar_merge_hash_record_t *h = &record->hash;
    /* Healthy disposal is engine work. Mandatory disposal never needs fresh
     * admission, and every original payload is NULL before credit retirement. */
#define WL_COLUMNAR_HASH_FREE(field) do { \
            if (c) { int stop = wl_evaluation_control_charge(c, 8); \
                     if (stop) return primary > 0 ? primary : stop; } \
            free(h->field); h->field = NULL; \
            if (c) { int stop = wl_columnar_merge_consolidate_tail(c); \
                     if (stop) return primary > 0 ? primary : stop; } \
} while (0)
    WL_COLUMNAR_HASH_FREE(values);
    WL_COLUMNAR_HASH_FREE(new_values);
    WL_COLUMNAR_HASH_FREE(used);
    WL_COLUMNAR_HASH_FREE(new_used);
    WL_COLUMNAR_HASH_FREE(unique);
    WL_COLUMNAR_HASH_FREE(new_unique);
    WL_COLUMNAR_HASH_FREE(row_scratch);
#undef WL_COLUMNAR_HASH_FREE
    if (c) {
        int rc = wl_evaluation_control_charge(c, 64);
        if (rc) return primary > 0 ? primary : rc;
    }
    wl_columnar_relation_consolidate_checkpoint(c, 99, 0, record, h->relation);
    if (c) {
        int rc = wl_columnar_merge_consolidate_tail(c);
        if (rc) return primary > 0 ? primary : rc;
    }
    if (h->admission_active) {
        uint64_t state = atomic_load_explicit(&h->admission.state,
                memory_order_relaxed);
        bool released = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            ? wl_columnar_memory_release(&h->admission)
            : state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            && wl_columnar_memory_rollback(&h->admission);
        if (!released) return primary > 0 ? primary : EINVAL;
        h->admission_active = false;
    }
    record->phase = WL_COLUMNAR_CONSOLIDATE_RETIRED;
    return primary;
}

static int
wl_columnar_merge_hash_initialize(wl_columnar_consolidate_record_t *record,
    wl_evaluation_control_t *c)
{
    int rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    wl_columnar_relation_radix_sequence_t *seq = &record->sequence;
    wl_columnar_radix_workspace_t *w = &seq->workspace;
    if (record->lane != WL_COLUMNAR_CONSOLIDATE_RADIX
        || record->phase != WL_COLUMNAR_CONSOLIDATE_PREPARING
        || seq->prepared || seq->consumed || seq->boundaries || seq->needs_sort
        || seq->metadata_admission_active || w->admission_active
        || w->perm_a || w->perm_b || w->bucket_values || w->byte_values
        || w->short_values || w->count16 || w->temp_column
        || w->insertion_rows || w->timestamps
        || seq->metadata_admission.identity != &seq->metadata_admission
        || w->admission.identity != &w->admission
        || atomic_load(&seq->metadata_admission.state)
        != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        || atomic_load(&w->admission.state)
        != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY) return EBUSY;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    record->lane = WL_COLUMNAR_CONSOLIDATE_LANE_INITIALIZING;
    for (size_t at = 0; at < sizeof(record->sequence);) {
        size_t bytes = sizeof(record->sequence) - at;
        if (bytes > 128u) bytes = 128u;
        rc = wl_evaluation_control_charge(c, bytes); if (rc) return rc;
        memset((char *)&record->sequence + at, 0, bytes); at += bytes;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    wl_columnar_memory_reservation_init(&record->hash.admission);
    record->lane = WL_COLUMNAR_CONSOLIDATE_HASH;
    return wl_columnar_merge_consolidate_tail(c);
}

static int
wl_columnar_merge_hash_validate(wl_columnar_consolidate_record_t *record,
    wl_evaluation_control_t *c)
{
    wl_columnar_merge_hash_record_t *h = &record->hash;
    int rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    wl_columnar_relation_mutation_lease_t *lease = h->lease;
    col_rel_t *r = h->relation;
    if (!h->prepared || !r || !lease || lease->set != h->set
        || !h->set || h->set->lease_count != 1 || h->set->leases != lease
        || h->set->descriptor_count != 1 || h->set->owner_count != 1
        || lease->acquisition_nonce != h->nonce || lease->owner != r
        || lease->owner_identity != h->owner_identity
        || lease->owner_generation != h->owner_generation
        || col_rel_mutation_lease_validate(lease, r)) return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (r->relation_identity != h->identity
        || r->storage_generation != h->storage_generation
        || r->view_generation != h->view_generation
        || r->columns != (int64_t **)h->columns || r->column_types != h->types
        || r->nrows != h->nrows || r->ncols != h->ncols
        || r->capacity != h->capacity || r->timestamps || r->col_shared
        || r->arena_owned || r->nrows > r->capacity
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    uint64_t fingerprint;
    rc = wl_columnar_relation_radix_type_fingerprint_controlled(r,
            &fingerprint, c); if (rc) return rc;
    return fingerprint == h->type_fingerprint ? 0 : EINVAL;
}

static int
wl_columnar_merge_hash_prepare(wl_columnar_consolidate_record_t *record,
    col_rel_t *r, wl_columnar_relation_mutation_lease_t *lease,
    wl_evaluation_control_t *c)
{
    int rc = wl_columnar_merge_hash_initialize(record, c); if (rc) return rc;
    wl_columnar_merge_hash_record_t *h = &record->hash;
    rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (!r || !lease || !lease->set || lease->set->lease_count != 1
        || lease->set->leases != lease || lease->set->descriptor_count != 1
        || lease->set->owner_count != 1 || lease->owner != r
        || col_rel_mutation_lease_validate(lease, r)
        || !wl_columnar_relation_generation_valid(
            r->view_generation)) return EINVAL;
    if (r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
        return EOVERFLOW;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (!col_op_consolidate_size_multiply(r->ncols, sizeof(int64_t),
        &h->row_bytes)) return EOVERFLOW;
    h->allocated_row_bytes = h->row_bytes ? h->row_bytes : sizeof(int64_t);
    h->relation = r;
    uint64_t max_ht_cap = 8192u, max_uniq_cap = 4096u;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    while (max_ht_cap <= (uint64_t)r->nrows * 2u
        && max_ht_cap <= UINT32_MAX / 2u) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        max_ht_cap *= 2u;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    while (max_uniq_cap < r->nrows && max_uniq_cap <= UINT32_MAX / 2u) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        max_uniq_cap *= 2u;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    if (max_uniq_cap < r->nrows) return EOVERFLOW;
    uint64_t ht_live = max_ht_cap + (max_ht_cap > 8192u ? max_ht_cap / 2u : 0u);
    uint64_t unique_live = max_uniq_cap
        + (max_uniq_cap > 4096u ? max_uniq_cap / 2u : 0u);
    uint64_t bytes, scratch = 0;
    if (ht_live < max_ht_cap || unique_live < max_uniq_cap
        || !wl_columnar_memory_size_mul(ht_live, h->allocated_row_bytes, &bytes)
        || !wl_columnar_memory_size_add(scratch, bytes, &scratch)
        || !wl_columnar_memory_size_add(scratch, ht_live, &scratch)
        || !wl_columnar_memory_size_mul(unique_live, h->allocated_row_bytes,
        &bytes)
        || !wl_columnar_memory_size_add(scratch, bytes, &scratch)
        || (r->ncols > COL_STACK_MAX
        && !wl_columnar_memory_size_add(scratch, h->row_bytes, &scratch))
        || scratch > SIZE_MAX) return EOVERFLOW;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc = wl_columnar_merge_hash_step(record, c, 78, scratch);
    if (rc) return rc;
    wl_columnar_memory_admission_status_t status;
    rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(r->memory_governor), scratch,
        &h->admission, &status, c); if (rc) return rc;
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            r->memory_budget_denial_pending = true;
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOMEM
            : status ==
               WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
    }
    h->admission_active = true;
    rc = wl_evaluation_control_charge(c, 128); if (rc) return rc;
    h->relation = r; h->lease = lease; h->set = lease->set;
    h->nonce = lease->acquisition_nonce; h->identity = r->relation_identity;
    h->storage_generation = r->storage_generation;
    h->view_generation = r->view_generation;
    h->owner_identity = lease->owner_identity;
    h->owner_generation = lease->owner_generation;
    h->columns = (const int64_t *const *)r->columns; h->types = r->column_types;
    h->nrows = r->nrows; h->ncols = r->ncols; h->capacity = r->capacity;
    h->hash_capacity = 8192u; h->unique_capacity = 4096u;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc = wl_columnar_relation_radix_type_fingerprint_controlled(r,
            &h->type_fingerprint, c); if (rc) return rc;
    rc = wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&h->values, 8192u * h->allocated_row_bytes, c);
    if (rc) return rc;
    rc = wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&h->used, 8192u, c); if (rc) return rc;
    rc = wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&h->unique, 4096u * h->allocated_row_bytes, c);
    if (rc) return rc;
    if (r->ncols > COL_STACK_MAX) {
        rc = wl_columnar_relation_consolidate_allocate_controlled(
            (void **)&h->row_scratch, h->row_bytes, c); if (rc) return rc;
    }
    rc = wl_columnar_merge_hash_step(record, c, 77, scratch);
    if (rc) return rc;
    rc = wl_columnar_relation_consolidate_commit_controlled(&h->admission,
            h->admission_active, h, c); if (rc) return rc;
    rc = wl_columnar_merge_hash_step(record, c, 79, scratch);
    if (rc) return rc;
    for (uint32_t slot = 0; slot < h->hash_capacity; slot++) {
        rc = wl_evaluation_control_charge(c, 1); if (rc) return rc;
        h->used[slot] = 0;
        rc = wl_columnar_merge_hash_step(record, c, 80, slot + 1u);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
    h->prepared = true;
    rc = wl_columnar_merge_hash_validate(record, c); if (rc) return rc;
    return wl_columnar_merge_hash_step(record, c, 98, h->nrows);
}

static int
wl_columnar_merge_hash_cells(wl_columnar_consolidate_record_t *record,
    const int64_t *row, uint32_t *hash, wl_evaluation_control_t *c,
    uint64_t *visits)
{
    *hash = 2166136261u;
    for (uint32_t col = 0; col < record->hash.ncols; col++) {
        int rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
        *hash = wl_columnar_hash_value(*hash, record->hash.relation, col,
                row[col]);
        rc = wl_columnar_merge_hash_step(record, c, 82, ++*visits);
        if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_merge_hash_copy(wl_columnar_consolidate_record_t *record,
    int64_t *dst, const int64_t *src, uint32_t count,
    wl_evaluation_control_t *c, int stage, uint64_t *visits)
{
    for (uint32_t col = 0; col < count; col++) {
        int rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        dst[col] = src[col];
        rc = wl_columnar_merge_hash_step(record, c, stage, ++*visits);
        if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_merge_hash_compare(wl_columnar_consolidate_record_t *record,
    const int64_t *left, const int64_t *right, int *cmp,
    wl_evaluation_control_t *c, uint64_t *visits)
{
    col_rel_t *r = record->hash.relation;
    for (uint32_t col = 0; col < r->ncols; col++) {
        int rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
        *cmp = r->column_types && r->column_types[col] == WIRELOG_TYPE_FLOAT
            ? wl_columnar_float_compare_bits(left[col], right[col])
            : (left[col] > right[col]) - (left[col] < right[col]);
        rc = wl_columnar_merge_hash_step(record, c, 94, ++*visits);
        if (rc) return rc;
        if (*cmp) return 0;
    }
    *cmp = 0; return 0;
}

static int
wl_columnar_merge_hash_swap(wl_columnar_consolidate_record_t *record,
    uint32_t left, uint32_t right, wl_evaluation_control_t *c, uint64_t *visits)
{
    wl_columnar_merge_hash_record_t *h = &record->hash;
    for (uint32_t col = 0; col < h->ncols; col++) {
        int rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
        size_t a = (size_t)left * h->ncols + col;
        size_t b = (size_t)right * h->ncols + col;
        int64_t v = h->unique[a]; h->unique[a] = h->unique[b]; h->unique[b] = v;
        rc = wl_columnar_merge_hash_step(record, c, 95, ++*visits);
        if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_merge_hash_sift(wl_columnar_consolidate_record_t *record,
    uint32_t root, uint32_t count, wl_evaluation_control_t *c,
    uint64_t *compares, uint64_t *swaps)
{
    wl_columnar_merge_hash_record_t *h = &record->hash;
    while (root < count / 2u) {
        int rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        uint32_t child = root * 2u + 1u;
        rc = wl_columnar_merge_hash_step(record, c, 93, child);
        if (rc) return rc;
        int cmp;
        if (child + 1u < count) {
            rc = wl_columnar_merge_hash_compare(record,
                    h->unique + (size_t)child * h->ncols,
                    h->unique + (size_t)(child + 1u) * h->ncols, &cmp, c,
                    compares);
            if (rc) return rc;
            if (cmp < 0) child++;
        }
        rc = wl_columnar_merge_hash_compare(record,
                h->unique + (size_t)root * h->ncols,
                h->unique + (size_t)child * h->ncols, &cmp, c, compares);
        if (rc) return rc;
        if (cmp >= 0) return 0;
        rc = wl_columnar_merge_hash_swap(record, root, child, c, swaps);
        if (rc) return rc;
        root = child;
    }
    return 0;
}

static int
wl_columnar_merge_hash_execute(wl_columnar_consolidate_record_t *record,
    wl_evaluation_control_t *c, wl_columnar_merge_hash_outcome_t *outcome)
{
    wl_columnar_merge_hash_record_t *h = &record->hash;
    int rc = wl_columnar_merge_hash_validate(record, c); if (rc) return rc;
    *outcome = WL_COLUMNAR_MERGE_HASH_COMPLETE;
    int64_t local_row[COL_STACK_MAX];
    int64_t *row = h->ncols > COL_STACK_MAX ? h->row_scratch : local_row;
    uint64_t gathers = 0, hashes = 0, probes = 0, equalities = 0, copies = 0;
    for (uint32_t r = 0; r < h->nrows; r++) {
        rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        for (uint32_t col = 0; col < h->ncols; col++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            row[col] = h->relation->columns[col][r];
            rc = wl_columnar_merge_hash_step(record, c, 81, ++gathers);
            if (rc) return rc;
        }
        uint32_t hash;
        rc = wl_columnar_merge_hash_cells(record, row, &hash, c, &hashes);
        if (rc) return rc;
        uint32_t slot = hash & (h->hash_capacity - 1u);
        bool found = false;
        for (;;) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            bool used = h->used[slot] != 0;
            rc = wl_columnar_merge_hash_step(record, c, 83, ++probes);
            if (rc) return rc;
            if (!used) break;
            bool equal = true;
            for (uint32_t col = 0; col < h->ncols; col++) {
                rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
                equal = wl_columnar_value_equal(h->relation, col,
                        h->values[(size_t)slot * h->ncols + col], h->relation,
                        col, row[col]);
                rc = wl_columnar_merge_hash_step(record, c, 84, ++equalities);
                if (rc) return rc;
                if (!equal) break;
            }
            if (equal) {
                found = true; break;
            }
            slot = (slot + 1u) & (h->hash_capacity - 1u);
        }
        if (found) continue;
        if ((uint64_t)h->unique_count * 2u >= h->hash_capacity) {
            rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
            if (h->unique_count > h->nrows / 4u) {
                *outcome = WL_COLUMNAR_MERGE_HASH_FALLBACK;
                return wl_columnar_merge_hash_step(record, c, 85,
                           h->unique_count);
            }
            uint32_t cap = h->hash_capacity * 2u;
            rc = wl_columnar_relation_consolidate_allocate_controlled(
                (void **)&h->new_values, (size_t)cap * h->allocated_row_bytes,
                c);
            if (rc) return rc;
            rc = wl_columnar_relation_consolidate_allocate_controlled(
                (void **)&h->new_used, cap, c); if (rc) return rc;
            for (uint32_t s = 0; s < cap; s++) {
                rc = wl_evaluation_control_charge(c, 1); if (rc) return rc;
                h->new_used[s] = 0;
                rc = wl_columnar_merge_hash_step(record, c, 86, s + 1u);
                if (rc) return rc;
            }
            for (uint32_t s = 0; s < h->hash_capacity; s++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                bool used = h->used[s] != 0;
                rc = wl_columnar_merge_hash_step(record, c, 87, s + 1u);
                if (rc) return rc;
                if (!used) continue;
                int64_t *src = h->values + (size_t)s * h->ncols;
                uint32_t rh;
                rc = wl_columnar_merge_hash_cells(record, src, &rh, c, &hashes);
                if (rc) return rc;
                uint32_t ns = rh & (cap - 1u);
                for (;;) {
                    rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                    bool occupied = h->new_used[ns] != 0;
                    rc = wl_columnar_merge_hash_step(record, c, 88, ++probes);
                    if (rc) return rc;
                    if (!occupied) break;
                    ns = (ns + 1u) & (cap - 1u);
                }
                rc = wl_columnar_merge_hash_copy(record,
                        h->new_values + (size_t)ns * h->ncols, src, h->ncols,
                        c, 89, &copies); if (rc) return rc;
                rc = wl_evaluation_control_charge(c, 1); if (rc) return rc;
                h->new_used[ns] = 1;
                rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
            free(h->values); h->values = NULL;
            free(h->used); h->used = NULL;
            h->values = h->new_values; h->new_values = NULL;
            h->used = h->new_used; h->new_used = NULL; h->hash_capacity = cap;
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
            slot = hash & (cap - 1u);
            for (;;) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                bool occupied = h->used[slot] != 0;
                rc = wl_columnar_merge_hash_step(record, c, 88, ++probes);
                if (rc) return rc;
                if (!occupied) break;
                slot = (slot + 1u) & (cap - 1u);
            }
        }
        rc = wl_columnar_merge_hash_copy(record,
                h->values + (size_t)slot * h->ncols, row, h->ncols, c, 90,
                &copies);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 1); if (rc) return rc;
        h->used[slot] = 1;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        if (h->unique_count >= h->unique_capacity) {
            uint32_t cap = h->unique_capacity * 2u;
            rc = wl_columnar_relation_consolidate_allocate_controlled(
                (void **)&h->new_unique, (size_t)cap * h->allocated_row_bytes,
                c);
            if (rc) return rc;
            for (uint32_t r = 0; r < h->unique_count; r++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                rc = wl_columnar_merge_hash_copy(record,
                        h->new_unique + (size_t)r * h->ncols,
                        h->unique + (size_t)r * h->ncols, h->ncols, c, 91,
                        &copies);
                if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
            free(h->unique); h->unique = NULL;
            h->unique = h->new_unique; h->new_unique = NULL;
            h->unique_capacity = cap;
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        }
        rc = wl_columnar_merge_hash_copy(record,
                h->unique + (size_t)h->unique_count * h->ncols, row, h->ncols,
                c, 92, &copies); if (rc) return rc;
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        h->unique_count++;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    uint64_t compares = 0, swaps = 0;
    for (uint32_t root = h->unique_count / 2u; root;) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        rc = wl_columnar_merge_hash_sift(record, --root, h->unique_count,
                c, &compares, &swaps); if (rc) return rc;
    }
    for (uint32_t end = h->unique_count; end > 1u;) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        rc = wl_columnar_merge_hash_swap(record, 0, --end, c, &swaps);
        if (rc) return rc;
        rc = wl_columnar_merge_hash_sift(record, 0, end, c, &compares, &swaps);
        if (rc) return rc;
    }
    rc = wl_columnar_merge_hash_validate(record, c); if (rc) return rc;
    uint64_t writes = 0;
    for (uint32_t r = 0; r < h->unique_count; r++) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        for (uint32_t col = 0; col < h->ncols; col++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            h->relation->columns[col][r] = h->unique[(size_t)r * h->ncols +
                    col];
            rc = wl_columnar_merge_hash_step(record, c, 96, ++writes);
            if (rc) return rc;
        }
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    rc = wl_columnar_merge_hash_validate(record, c); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 48); if (rc) return rc;
    rc = wl_columnar_merge_hash_step(record, c, 97, h->unique_count);
    if (rc) return rc;
    col_rel_t *r = h->relation;
    r->nrows = h->unique_count; r->sorted_nrows = h->unique_count;
    r->run_count = 1; r->run_ends[0] = h->unique_count; r->view_generation++;
    h->lease->set->published = true;
    record->phase = WL_COLUMNAR_CONSOLIDATE_CONSUMED;
    return 0;
}

static int
wl_columnar_merge_consolidate_shape(col_rel_t *r, wl_evaluation_control_t *c)
{
    int rc = wl_evaluation_control_charge(c, 48); if (rc) return rc;
    if (!r || r->nrows > r->capacity ||
        (r->ncols && !r->columns)) return EINVAL;
    if (r->timestamps || r->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || r->compound_arity_map || r->compound_arity_len) return ENOTSUP;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    uint64_t visited = 0;
    for (uint32_t col = 0; col < r->ncols; col++) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        uint32_t type =
            r->column_types ? r->column_types[col] : WIRELOG_TYPE_INT64;
        if (!r->columns[col] || (type != WIRELOG_TYPE_INT64
            && type != WIRELOG_TYPE_FLOAT &&
            type != WIRELOG_TYPE_STRING)) return EINVAL;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        if (type == WIRELOG_TYPE_FLOAT) {
            for (uint32_t row = 0; row < r->nrows; row++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                if (!wl_columnar_float_bits_valid(
                        r->columns[col][row])) return EINVAL;
                wl_columnar_relation_consolidate_checkpoint(c, 21, ++visited,
                    NULL, r);
                rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
            }
        }
    }
    return 0;
}

/* Fresh actual MAP output only; borrowed shape validation remains separate. */
static int
wl_columnar_merge_consolidate_owned_shape(col_rel_t *r,
    wl_evaluation_control_t *c)
{
    int rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    if (r->storage_owner != r || !r->relation_identity || r->arena_owned
        || r->col_shared || col_rel_storage_alias_borrow_count(r)
        || r->storage_owner_identity != r->relation_identity
        || r->storage_owner_generation != r->storage_generation)
        return ENOTSUP;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    return wl_columnar_merge_consolidate_shape(r, c);
}

int
wl_columnar_merge_concat_controlled(eval_stack_t *stack, wl_col_session_t *sess,
    wl_evaluation_control_t *c)
{
    if (!stack || !sess || !c) return EINVAL;
    int rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    wl_columnar_relation_consolidate_checkpoint(c, 117, stack->top, NULL, NULL);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    if (stack->top < 2u) return wl_columnar_merge_consolidate_tail(c);
    if (stack->top > COL_STACK_MAX) return EINVAL;
    wl_columnar_relation_private_batch_t *writer = NULL;
    rc = wl_columnar_eval_stack_cleanup_private_batch_owner(sess, stack,
            &writer, c);
    if (rc) return rc;
    wl_columnar_relation_source_group_t *group;
    eval_entry_t *result;
    rc = wl_columnar_eval_stack_cleanup_join_owner(sess, stack, &group,
            &result);
    if (rc) return rc;
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(
        sess->cleanup_active);
    if (!bulk || bulk->active) return EBUSY;
    uint32_t top = stack->top;
    eval_entry_t *a = &stack->items[top - 2u], *b = &stack->items[top - 1u];
    if (a->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || b->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || !a->rel || !b->rel
        || a->is_delta || b->is_delta || a->continuation || b->continuation
        || (a->rel == b->rel && (a->owned || b->owned))) return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    col_rel_t *left = a->rel, *right = b->rel, *out = NULL;
    rc = wl_columnar_relation_source_group_acquire_controlled(group, left,
            right, c);
    if (rc) goto done;
    wl_columnar_relation_consolidate_checkpoint(c, 118, 0, NULL, left);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
    rc = wl_columnar_merge_consolidate_owned_shape(left, c); if (rc) goto done;
    rc = wl_columnar_merge_consolidate_owned_shape(right, c); if (rc) goto done;
    rc = wl_evaluation_control_charge(c, 64); if (rc) goto done;
    if (left->ncols != right->ncols){
        rc = ENOTSUP; goto done;
    }
    if (right->nrows > UINT32_MAX - left->nrows) {
        rc = EOVERFLOW; goto done;
    }
    uint32_t rows = left->nrows + right->nrows;
    uint32_t left_count = a->seg_boundaries ? a->seg_count : 1u;
    uint32_t right_count = b->seg_boundaries ? b->seg_count : 1u;
    if ((!a->seg_boundaries && a->seg_count > 1u)
        || (!b->seg_boundaries && b->seg_count > 1u)
        || !left_count || !right_count) {
        rc = EINVAL; goto done;
    }
    if (right_count > UINT32_MAX - left_count) {
        rc = EOVERFLOW; goto done;
    }
    uint32_t count = left_count + right_count;
    uint64_t bytes;
    if (!wl_columnar_memory_size_mul((uint64_t)count + 1u, 4u, &bytes)
        || bytes > SIZE_MAX) {
        rc = EOVERFLOW; goto done;
    }
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
    eval_entry_t *inputs[2] = {a, b};
    for (unsigned side = 0; side < 2u; side++) {
        eval_entry_t *entry = inputs[side];
        uint32_t n = side ? right_count : left_count;
        if (entry->seg_boundaries) {
            rc = wl_evaluation_control_charge(c, 16); if (rc) goto done;
            if (entry->seg_boundaries[0]
                || entry->seg_boundaries[n] != entry->rel->nrows) {
                rc = EINVAL; goto done;
            }
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
            for (uint32_t i = 0; i < n; i++) {
                rc = wl_evaluation_control_charge(c, 16); if (rc) goto done;
                if (entry->seg_boundaries[i] > entry->seg_boundaries[i + 1u]
                    || entry->seg_boundaries[i + 1u] > entry->rel->nrows) {
                    rc = EINVAL; goto done;
                }
                rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
            }
        }
    }
    rc = wl_columnar_relation_pool_new_like_held_controlled(&out,
            sess->delta_pool,
            "$concat", left, sess->memory_governor, c);
    if (out) *result = (eval_entry_t){.rel = out, .owned = true,
                                      .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION};
    if (rc) goto done;
    rc = wl_evaluation_control_charge(c, 8); if (rc) goto done;
    result->seg_boundaries =
        wl_columnar_merge_concat_boundaries_alloc((size_t)bytes);
    if (!result->seg_boundaries) {
        rc = ENOMEM; goto done;
    }
    result->seg_count = count;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
    for (unsigned side = 0; side < 2u; side++) {
        eval_entry_t *entry = inputs[side];
        uint32_t n = side ? right_count : left_count;
        uint32_t offset = side ? left->nrows : 0u;
        uint32_t base = side ? left_count : 0u;
        for (uint32_t i = 0; i <= n; i++) {
            rc = wl_evaluation_control_charge(c, 16); if (rc) goto done;
            uint32_t boundary = entry->seg_boundaries ? entry->seg_boundaries[i]
                : i ? entry->rel->nrows : 0u;
            result->seg_boundaries[base + i] = offset + boundary;
            wl_columnar_relation_consolidate_checkpoint(c, 113,
                (uint64_t)base + i + 1u, NULL, out);
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
        }
    }
    rc = wl_columnar_relation_source_group_drain(group, 0); if (rc) goto done;
    rc = wl_columnar_relation_bulk_append_controlled(bulk, out, left, c);
    if (rc) goto done;
    wl_columnar_relation_consolidate_checkpoint(c, 114, out->nrows, NULL, out);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
    rc = wl_columnar_relation_bulk_append_controlled(bulk, out, right, c);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(c, 96); if (rc) goto done;
    if (bulk->active || group->descriptor_count || group->owner_count
        || !wl_columnar_relation_private_batch_disposable(writer)
        || stack->top != top || a->rel != left || b->rel != right
        || result->rel != out || !result->owned || out->nrows != rows
        || result->seg_count != count ||
        result->seg_boundaries[count] != rows){
        rc = EINVAL; goto done;
    }
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
    for (unsigned i = 0; i < 2u; i++) {
        rc = wl_evaluation_control_charge(c, 16); if (rc) goto done;
        wl_columnar_relation_consolidate_checkpoint(c, 115, i, NULL, out);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
        /* Existing checked physical cleanup exclusion: arbitrary width/Arrow/
         * dedup teardown is not a prepaid bounded forward region. */
        rc = eval_entry_dispose(inputs[i]); if (rc) goto done;
        /* Successful disposal consumes borrowed entries too; the legacy
         * disposer retains their non-owning pointer for its other callers. */
        inputs[i]->rel = NULL;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
        wl_columnar_relation_consolidate_checkpoint(c, 116, (uint64_t)i + 1u,
            NULL, out);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
    }
    rc = wl_columnar_relation_private_append_batch_begin_controlled(writer, out,
            c);
    if (rc) goto done;
    rc = wl_columnar_relation_private_append_batch_end_controlled(writer);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(c, 3u * sizeof(*result) + 8u);
    if (rc) goto done;
    if (stack->top != top || a->rel || b->rel || a->owned || b->owned
        || !wl_columnar_relation_private_batch_disposable(writer)) {
        rc = EINVAL; goto done;
    }
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto done;
    *a = *result; *result = (eval_entry_t){0}; *b = (eval_entry_t){0};
    stack->top = top - 1u;
    return 0;
done:
    if (rc == ENOMEM && out && out->memory_budget_denial_pending) {
        sess->memory_budget_denied = true; rc = ENOSPC;
    }
    rc = wl_columnar_relation_bulk_append_retire(bulk, rc);
    rc = wl_columnar_relation_private_batch_retire(writer, rc);
    int cleanup = wl_columnar_relation_source_group_drain(group, rc);
    if (rc <= 0 && cleanup > 0) rc = cleanup;
    /* Actual inputs and candidate stay in original slots for frame drain. */
    return rc;
}

static int
wl_columnar_merge_consolidate_dedup(wl_columnar_consolidate_record_t *record,
    wl_columnar_relation_private_batch_t *writer, col_rel_t *r,
    wl_evaluation_control_t *c)
{
    wl_columnar_relation_radix_sequence_t *seq = &record->sequence;
    uint64_t expected_view = seq->view_generation + seq->needs_sort_count;
    int rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    if (record->phase != WL_COLUMNAR_CONSOLIDATE_CONSUMED || !seq->consumed
        || r->view_generation != expected_view
        || r->view_generation >= WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        || writer->phase != WL_COLUMNAR_RELATION_PRIVATE_HELD
        || writer->batch.relation != r || !writer->batch.active) return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    uint32_t kept = r->nrows ? 1u : 0u;
    uint64_t compares = 0, moves = 0;
    for (uint32_t row = 1; row < r->nrows; row++) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        bool equal = true;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        for (uint32_t col = 0; col < r->ncols; col++) {
            rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
            int64_t a = r->columns[col][kept - 1u], b = r->columns[col][row];
            equal = r->column_types &&
                r->column_types[col] == WIRELOG_TYPE_FLOAT
                ? wl_columnar_float_compare_bits(a, b) == 0 : a == b;
            wl_columnar_relation_consolidate_checkpoint(c, 22, ++compares,
                record,
                r);
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
            if (!equal) break;
        }
        if (!equal) {
            for (uint32_t col = 0; col < r->ncols; col++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                r->columns[col][kept] = r->columns[col][row];
                wl_columnar_relation_consolidate_checkpoint(c, 23, ++moves,
                    record, r);
                rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            kept++;
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        }
    }
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    wl_columnar_relation_mutation_lease_t *lease = &writer->batch.single.lease;
    if (lease->set != &writer->batch.single.set || lease->set->leases != lease
        || lease->set->lease_count != 1 || lease->set->descriptor_count != 1
        || lease->set->owner_count != 1 ||
        col_rel_mutation_lease_validate(lease, r)) return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    if (r->view_generation != expected_view || r->nrows != seq->nrows
        || r->ncols != seq->ncols || r->columns != (int64_t **)seq->columns
        || r->storage_generation != seq->relation_generation) return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 48); if (rc) return rc;
    wl_columnar_relation_consolidate_checkpoint(c, 24, kept, record, r);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    r->nrows = kept; r->sorted_nrows = kept;
    r->run_count = 1; r->run_ends[0] = kept;
    r->view_generation++;
    lease->set->published = true;
    return 0;
}

typedef struct wl_columnar_merge_segment_heap_entry {
    uint32_t segment, cursor, end;
} wl_columnar_merge_segment_heap_entry_t;
_Static_assert(sizeof(wl_columnar_merge_segment_heap_entry_t) == 12u,
    "segment heap must preserve raw admission");

static int
wl_columnar_merge_segment_compare(wl_columnar_consolidate_record_t *record,
    col_rel_t *r, uint32_t left, uint32_t right, const int64_t *saved,
    int *cmp, wl_evaluation_control_t *c)
{
    for (uint32_t col = 0; col < r->ncols; col++) {
        int rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
        int64_t a = r->columns[col][left],
            b = saved ? saved[col] : r->columns[col][right];
        *cmp = r->column_types && r->column_types[col] == WIRELOG_TYPE_FLOAT
            ? wl_columnar_float_compare_bits(a, b) : (a > b) - (a < b);
        wl_columnar_relation_consolidate_checkpoint(c, 106, (uint64_t)col + 1u,
            record, r);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        if (*cmp) return 0;
    }
    *cmp = 0; return 0;
}

static int
wl_columnar_merge_segment_sift(wl_columnar_consolidate_record_t *record,
    col_rel_t *r, wl_columnar_merge_segment_heap_entry_t *heap,
    uint32_t parent, uint32_t size, wl_evaluation_control_t *c)
{
    while (parent < size / 2u) {
        int rc = wl_evaluation_control_charge(c, 16); if (rc) return rc;
        uint32_t child = parent * 2u + 1u;
        if (child >= size) break;
        int cmp;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        if (child + 1u < size) {
            rc = wl_columnar_merge_segment_compare(record, r,
                    heap[child + 1u].cursor,
                    heap[child].cursor, NULL, &cmp, c); if (rc) return rc;
            if (cmp < 0) child++;
        }
        rc = wl_columnar_merge_segment_compare(record, r, heap[parent].cursor,
                heap[child].cursor, NULL, &cmp, c); if (rc) return rc;
        if (cmp <= 0) break;
        rc = wl_evaluation_control_charge(c, 3u * sizeof(*heap) + 8u);
        if (rc) return rc;
        wl_columnar_merge_segment_heap_entry_t swap = heap[parent];
        heap[parent] = heap[child]; heap[child] = swap; parent = child;
        wl_columnar_relation_consolidate_checkpoint(c, 107, parent, record, r);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_merge_segment_allocate(wl_columnar_consolidate_record_t *record,
    col_rel_t *r, wl_evaluation_control_t *c)
{
    uint32_t count = record->sequence.seg_count;
    int rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    uint64_t cells, merged, bytes,
        heap = count >= 3u ? (uint64_t)count * 12u : 0u;
    if (count < 2u || !wl_columnar_memory_size_mul(r->nrows, r->ncols, &cells)
        || !wl_columnar_memory_size_mul(cells, 8u, &merged)
        || !wl_columnar_memory_size_add(merged, 8u, &merged)
        || !wl_columnar_memory_size_add((uint64_t)count * 8u, merged, &bytes)
        || !wl_columnar_memory_size_add(bytes, heap,
        &bytes) || bytes > SIZE_MAX)
        return EOVERFLOW;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    wl_columnar_memory_admission_status_t status;
    rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(r->memory_governor), bytes,
        &record->merge_admission, &status, c); if (rc) return rc;
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
        && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            r->memory_budget_denial_pending = true;
        return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status ==
               WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
    }
    /* Install alias disposition before publishing any physical pointer. */
    record->merge_admission_active = true;
    rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
    record->lane = WL_COLUMNAR_CONSOLIDATE_RADIX_MERGE;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&record->starts,
        (size_t)bytes, c); if (rc) return rc;
    rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
    record->ends = record->starts + count;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    rc =
        wl_columnar_relation_consolidate_commit_controlled(
        &record->merge_admission,
        record->merge_admission_active, record, c); if (rc) return rc;
    wl_columnar_relation_consolidate_checkpoint(c, 104, bytes, record, r);
    return wl_columnar_merge_consolidate_tail(c);
}

static int
wl_columnar_merge_segment_finish(wl_columnar_consolidate_record_t *record,
    wl_columnar_relation_private_batch_t *writer, col_rel_t *r,
    wl_evaluation_control_t *c)
{
    wl_columnar_relation_radix_sequence_t *seq = &record->sequence;
    uint32_t count = seq->seg_count;
    int rc = wl_evaluation_control_charge(c, 96); if (rc) return rc;
    uint64_t generation = seq->view_generation + seq->needs_sort_count;
    wl_columnar_relation_mutation_lease_t *lease = &writer->batch.single.lease;
    if (record->lane != WL_COLUMNAR_CONSOLIDATE_RADIX_MERGE || count < 2u
        || record->phase != WL_COLUMNAR_CONSOLIDATE_CONSUMED || !seq->consumed
        || !record->starts || record->ends != record->starts + count
        || !record->merge_admission_active || writer->batch.relation != r
        || writer->phase != WL_COLUMNAR_RELATION_PRIVATE_HELD
        || r->view_generation != generation || r->nrows != seq->nrows
        || r->columns != (int64_t **)seq->columns || r->ncols != seq->ncols
        || r->storage_generation != seq->relation_generation
        || col_rel_mutation_lease_validate(lease, r)) return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    int64_t *merged = (int64_t *)(record->starts + (size_t)count * 2u);
    wl_columnar_merge_segment_heap_entry_t local_heap[2];
    wl_columnar_merge_segment_heap_entry_t *heap = count < 3u ? local_heap
        : (wl_columnar_merge_segment_heap_entry_t *)((char *)merged
        + (size_t)r->nrows * r->ncols * 8u + 8u);
    for (uint32_t i = 0; i < count; i++) {
        rc = wl_evaluation_control_charge(c, 24); if (rc) return rc;
        uint32_t start = seq->boundaries[i], end = seq->boundaries[i + 1u];
        uint32_t kept = start < end ? start + 1u : end;
        record->starts[i] = start;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        for (uint32_t row = kept; row < end; row++) {
            int cmp;
            rc = wl_columnar_merge_segment_compare(record, r, kept - 1u, row,
                    NULL, &cmp, c); if (rc) return rc;
            if (!cmp) continue;
            for (uint32_t col = 0; col < r->ncols; col++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                r->columns[col][kept] = r->columns[col][row];
                wl_columnar_relation_consolidate_checkpoint(c, 105, col + 1u,
                    record, r);
                rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            kept++;
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        }
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        record->ends[i] = kept;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    uint32_t size = 0;
    for (uint32_t i = 0; i < count; i++) {
        rc = wl_evaluation_control_charge(c, sizeof(*heap) + 16u);
        if (rc) return rc;
        if (record->starts[i] < record->ends[i]) {
            heap[size++] = (wl_columnar_merge_segment_heap_entry_t){i,
                                                                    record->
                                                                    starts[i],
                                                                    record->ends
                                                                    [i]};
        }
        wl_columnar_relation_consolidate_checkpoint(c, count >= 3u ? 108 : 109,
            (uint64_t)i + 1u, record, r);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    }
    for (uint32_t i = size / 2u; i;) {
        rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
        i--;
        rc = wl_columnar_merge_segment_sift(record, r, heap, i, size, c);
        if (rc) return rc;
    }
    uint32_t output = 0;
    while (size) {
        int cmp = 1;
        if (output) {
            rc = wl_columnar_merge_segment_compare(record, r, heap[0].cursor,
                    0, merged + (size_t)(output - 1u) * r->ncols, &cmp, c);
            if (rc) return rc;
        }
        if (cmp) {
            for (uint32_t col = 0; col < r->ncols; col++) {
                rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
                merged[(size_t)output * r->ncols +
                    col] = r->columns[col][heap[0].cursor];
                wl_columnar_relation_consolidate_checkpoint(c, 110, col + 1u,
                    record, r);
                rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            output++;
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        }
        rc = wl_evaluation_control_charge(c, sizeof(*heap) + 16u);
        if (rc) return rc;
        heap[0].cursor++;
        if (heap[0].cursor == heap[0].end) heap[0] = heap[--size];
        wl_columnar_relation_consolidate_checkpoint(c, 111, output, record, r);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        if (size) {
            rc = wl_columnar_merge_segment_sift(record, r, heap, 0, size, c);
            if (rc) return rc;
        }
    }
    for (uint32_t row = 0; row < output; row++)
        for (uint32_t col = 0; col < r->ncols; col++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) return rc;
            r->columns[col][row] = merged[(size_t)row * r->ncols + col];
            wl_columnar_relation_consolidate_checkpoint(c, 112,
                (uint64_t)row * r->ncols + col + 1u, record, r);
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
        }
    rc = wl_evaluation_control_charge(c, 64); if (rc) return rc;
    if (r->view_generation != generation || r->nrows != seq->nrows
        || col_rel_mutation_lease_validate(lease, r)) return EINVAL;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    r->nrows = output; r->sorted_nrows = output;
    r->run_count = 1; r->run_ends[0] = output;
    r->view_generation++; lease->set->published = true;
    return 0;
}

static int
wl_columnar_merge_consolidate_controlled(eval_stack_t *stack,
    wl_col_session_t *sess, wl_evaluation_control_t *c)
{
    if (!stack || !sess || !c) return EINVAL;
    if (sess->num_workers > 1 || sess->diff_operators_active ||
        sess->tdd_input_run
        || sess->tdd_subpass_active || sess->tdd_outbound_only_active
        || sess->delta_seeded || sess->retraction_seeded ||
        sess->retraction_right_pass
        || sess->join_output_shared_count
        || (sess->current_iteration && !(sess->recursive.active
        && sess->recursive.ready && !sess->diff_enabled)) ||
        sess->delta_observer ||
        !sess->memory_governor) return ENOTSUP;
    if (sess->join_batch_bytes) {
        int admission = wl_evaluation_control_charge(c, 64);
        if (admission) return admission;
        wl_columnar_join_batch_record_t *batch =
            wl_columnar_eval_stack_cleanup_join_batch(sess->cleanup_active);
        if (batch && (batch->phase != WL_COLUMNAR_JOIN_BATCH_INERT
            || batch->producer || batch->continuation || batch->left_keys
            || batch->right_keys || batch->types)) return EBUSY;
        admission = wl_evaluation_control_charge(c, 0);
        if (admission) return admission;
    }
    wl_columnar_relation_private_batch_t *writer = NULL;
    int rc = wl_columnar_eval_stack_cleanup_private_batch_owner(sess, stack,
            &writer, c);
    if (rc) return rc;
    rc =
        wl_columnar_eval_stack_cleanup_consolidate_prepare(sess->cleanup_active,
            c);
    if (rc) return rc;
    wl_columnar_consolidate_record_t *record =
        wl_columnar_eval_stack_cleanup_consolidate(sess->cleanup_active);
    if (!record || !record->ready) return EINVAL;
    if (record->phase != WL_COLUMNAR_CONSOLIDATE_INERT
        && record->phase != WL_COLUMNAR_CONSOLIDATE_RETIRED) return EBUSY;
    wl_columnar_relation_source_group_t *group;
    eval_entry_t *result;
    rc = wl_columnar_eval_stack_cleanup_join_owner(sess, stack, &group,
            &result);
    if (rc) return rc;
    if (!stack->top || stack->top > COL_STACK_MAX
        || stack->items[stack->top - 1u].kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || !stack->items[stack->top - 1u].rel) return EINVAL;
    uint32_t slot = stack->top - 1u;
    eval_entry_t *original = &stack->items[slot];
    if (original->is_delta) return ENOTSUP;
    bool owned = original->owned;
    /* Scalar producer binding only: no stale prior payload dereference. */
    rc = wl_evaluation_control_charge(c, 32); if (rc) return rc;
    if (owned && (writer->phase != WL_COLUMNAR_RELATION_PRIVATE_RETIRED
        || writer->batch.relation != original->rel)) return ENOTSUP;
    wl_columnar_relation_consolidate_checkpoint(c, 42, 0, record,
        original->rel);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) return rc;
    uint32_t *segments = original->seg_boundaries;
    uint32_t segment_count = original->seg_count;
    col_rel_t *in = original->rel, *out = NULL;
    bool committed = false;
    rc = wl_columnar_relation_source_group_acquire_controlled(group, in, NULL,
            c);
    wl_columnar_relation_consolidate_checkpoint(c, 25, 0, record, in);
    if (rc) goto cleanup;
    rc = owned ? wl_columnar_merge_consolidate_owned_shape(in, c)
        : wl_columnar_merge_consolidate_shape(in, c);
    if (rc) goto cleanup;
    rc = wl_evaluation_control_charge(c, 32); if (rc) goto cleanup;
    bool normalized_small = in->nrows <= 1u && in->sorted_nrows == in->nrows
        && in->run_count == 1 && in->run_ends[0] == in->nrows && !segments;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    if (in->nrows <= 1u && (!owned || normalized_small)) {
        rc = wl_columnar_relation_source_group_release_readers(group, 0);
        if (rc) goto cleanup;
        rc = wl_evaluation_control_charge(c, 32); if (rc) goto cleanup;
        if (stack->top != slot + 1u || original->rel != in ||
            original->owned != owned) {
            rc = EBUSY; goto cleanup;
        }
        rc = wl_columnar_relation_source_group_validate_writers(group);
        if (rc) goto cleanup;
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        rc = wl_evaluation_control_charge(c, 84); if (rc) goto cleanup;
        wl_columnar_relation_consolidate_checkpoint(c, 26, 0, record, in);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        /* Do not commit the lazy source journal on original-entry no-op. */
        return wl_columnar_relation_source_group_drain(group, 0);
    }
    rc = wl_evaluation_control_charge(c, 32); if (rc) goto cleanup;
    if ((!segments && segment_count > 1u) || (segments && !segment_count)
        || (segments &&
        (segments[0] || segments[segment_count] != in->nrows))) {
        rc = EINVAL; goto cleanup;
    }
    for (uint32_t i = 0; segments && i < segment_count; i++) {
        rc = wl_evaluation_control_charge(c, 16); if (rc) goto cleanup;
        if (segments[i] > segments[i + 1u] || segments[i + 1u] > in->nrows) {
            rc = EINVAL; goto cleanup;
        }
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    }
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    if (owned) {
        /* Readonly authority must be completely drained before mutation. */
        rc = wl_columnar_relation_source_group_drain(group, 0);
        if (rc) goto cleanup;
        rc = wl_evaluation_control_charge(c, 64); if (rc) goto cleanup;
        if (stack->top != slot + 1u || original->rel != in
            || !original->owned || original->is_delta
            || original->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || original->seg_boundaries != segments
            || original->seg_count != segment_count) {
            rc = EBUSY; goto cleanup;
        }
        wl_columnar_relation_consolidate_checkpoint(c, 38, 0, record, in);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        /* Charge both entry writes; no poll between the sole-owner move. */
        rc = wl_evaluation_control_charge(c, 2u * sizeof(*result));
        if (rc) goto cleanup;
        *result = *original; *original = (eval_entry_t){0}; out = in;
        wl_columnar_relation_consolidate_checkpoint(c, 39, 0, record, out);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        rc = wl_columnar_relation_private_append_batch_begin_controlled(writer,
                out, c); if (rc) goto cleanup;
        rc = wl_columnar_merge_consolidate_owned_shape(out, c);
        if (rc) goto cleanup;
        rc = wl_evaluation_control_charge(c, 32); if (rc) goto cleanup;
        if (result->seg_boundaries != segments ||
            result->seg_count != segment_count) {
            rc = EINVAL; goto cleanup;
        }
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        if (out->nrows <= 1u) {
            rc = wl_evaluation_control_charge(c, 64); if (rc) goto cleanup;
            if (col_rel_mutation_lease_validate(&writer->batch.single.lease,
                out)) {
                rc = EINVAL; goto cleanup;
            }
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
            rc = wl_evaluation_control_charge(c, 32); if (rc) goto cleanup;
            wl_columnar_relation_consolidate_checkpoint(c, 40, 0, record, out);
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
            free(result->seg_boundaries); result->seg_boundaries = NULL;
            result->seg_count = 0;
            out->sorted_nrows = out->nrows; out->run_count = 1;
            out->run_ends[0] = out->nrows;
            goto owned_finish;
        }
        rc = wl_columnar_merge_consolidate_reset(record, c);
        if (rc) goto cleanup;
        goto prepare;
    }
    rc = wl_columnar_merge_consolidate_reset(record, c); if (rc) goto cleanup;
    rc = wl_columnar_relation_pool_new_like_held_controlled(&out,
            sess->delta_pool,
            "$consol", in, sess->memory_governor, c);
    if (out) *result = (eval_entry_t){.rel = out, .owned = true,
                                      .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION};
    if (rc) goto cleanup;
    rc = wl_columnar_relation_private_append_batch_begin_controlled(writer, out,
            c);
    if (rc) goto cleanup;
    for (uint32_t row = 0; row < in->nrows; row++) {
        rc = wl_evaluation_control_charge(c, 32); if (rc) goto cleanup;
        int64_t empty = 0;
        int64_t *tuple = in->ncols ? out->row_scratch : &empty;
        if (out->ncols != in->ncols || (in->ncols && !tuple)) {
            rc = EINVAL; goto cleanup;
        }
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        for (uint32_t col = 0; col < in->ncols; col++) {
            rc = wl_evaluation_control_charge(c, 8); if (rc) goto cleanup;
            tuple[col] = in->columns[col][row];
            wl_columnar_relation_consolidate_checkpoint(c, 27,
                (uint64_t)row * in->ncols + col + 1u, record, out);
            rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        }
        rc = wl_columnar_relation_private_append_batch_row_controlled(writer,
                tuple, c);
        if (rc) goto cleanup;
        wl_columnar_relation_consolidate_checkpoint(c, 28, row + 1u, record,
            out);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    }
    wl_columnar_relation_consolidate_checkpoint(c, 29, 0, record, out);
    rc = wl_columnar_relation_private_append_batch_end_controlled(writer);
    if (rc) goto cleanup;
    rc = wl_columnar_relation_private_append_batch_begin_controlled(writer, out,
            c); if (rc) goto cleanup;
prepare:;
    wl_columnar_relation_mutation_lease_t *lease = &writer->batch.single.lease;
    if (out->nrows > 10000u) {
        rc = wl_columnar_merge_hash_prepare(record, out, lease, c);
        if (rc) goto cleanup;
        wl_columnar_merge_hash_outcome_t outcome;
        rc = wl_columnar_merge_hash_execute(record, c, &outcome);
        if (rc) goto cleanup;
        rc = wl_columnar_merge_hash_retire(record, 0, c);
        if (rc) goto cleanup;
        if (outcome == WL_COLUMNAR_MERGE_HASH_COMPLETE) goto owned_finish;
        /* Actual physical and credit drain precedes reuse at original addresses.
         * Continue the same attempt without resetting or refunding consumption. */
        rc = wl_columnar_merge_consolidate_reset(record, c);
        if (rc) goto cleanup;
    }
    uint32_t local_bounds[3] = {0, out->nrows, out->nrows};
    const uint32_t *sort_bounds = local_bounds;
    uint32_t sort_count = 1;
    if (segments && segment_count >= 2u) {
        sort_bounds = segments; sort_count = segment_count;
    } else if (owned && out->sorted_nrows && out->sorted_nrows < out->nrows) {
        local_bounds[1] = out->sorted_nrows; sort_count = 2;
    }
    if (sort_count > 1u) {
        wl_columnar_relation_consolidate_checkpoint(c, 121, sort_count, record,
            out);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        rc = wl_columnar_relation_radix_sequence_prepare_with_lease_controlled(
            out, sort_bounds, sort_count, &record->sequence, lease, c);
        if (rc) goto cleanup;
        rc = wl_columnar_merge_segment_allocate(record, out, c);
        if (rc) goto cleanup;
        goto prepared;
    }
    wl_columnar_memory_admission_status_t status;
    rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(out->memory_governor), 8,
        &record->merge_admission, &status, c);
    if (rc) goto cleanup;
    if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK &&
        status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY){
        if (status ==
            WL_COLUMNAR_MEMORY_ADMISSION_DENIED) sess->memory_budget_denied =
                true;
        rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status ==
            WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : ENOMEM;
        goto cleanup;
    }
    record->merge_admission_active = true;
    rc =
        wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&record->starts,
        sizeof(uint32_t), c); if (rc) goto cleanup;
    rc =
        wl_columnar_relation_consolidate_allocate_controlled(
        (void **)&record->ends,
        sizeof(uint32_t), c); if (rc) goto cleanup;
    rc =
        wl_columnar_relation_consolidate_commit_controlled(
        &record->merge_admission,
        record->merge_admission_active, record, c); if (rc) goto cleanup;
    rc = wl_evaluation_control_charge(c, 8); if (rc) goto cleanup;
    record->starts[0] = 0; record->ends[0] = out->nrows;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    uint32_t bounds[] = {0, out->nrows};
    wl_columnar_relation_consolidate_checkpoint(c, 37, 0, record, out);
    rc = wl_columnar_relation_radix_sequence_prepare_with_lease_controlled(
        out, bounds, 1, &record->sequence, lease, c); if (rc) goto cleanup;
prepared:
    rc = wl_evaluation_control_charge(c, 8); if (rc) goto cleanup;
    record->phase = WL_COLUMNAR_CONSOLIDATE_PREPARED;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    wl_columnar_relation_consolidate_checkpoint(c, 30, 0, record, out);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    rc = wl_evaluation_control_charge(c, 8); if (rc) goto cleanup;
    record->phase = WL_COLUMNAR_CONSOLIDATE_EXECUTING;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    rc =
        wl_columnar_relation_radix_sequence_execute_with_lease_controlled(
        &record->sequence, lease, c);
    if (rc) goto cleanup;
    rc = wl_evaluation_control_charge(c, 8); if (rc) goto cleanup;
    record->phase = WL_COLUMNAR_CONSOLIDATE_CONSUMED;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    wl_columnar_relation_consolidate_checkpoint(c, 31, out->view_generation,
        record, out);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    rc = record->sequence.seg_count > 1u
        ? wl_columnar_merge_segment_finish(record, writer, out, c)
        : wl_columnar_merge_consolidate_dedup(record, writer, out, c);
    if (rc) goto cleanup;
    wl_columnar_relation_consolidate_checkpoint(c, 32, out->view_generation,
        record, out);
owned_finish:
    rc = wl_columnar_relation_private_append_batch_end_controlled(writer);
    if (rc) goto cleanup;
    rc = wl_columnar_relation_consolidate_retire(record, 0);
    if (rc) goto cleanup;
    if (owned) {
        rc = wl_evaluation_control_charge(c, 96); if (rc) goto cleanup;
        if (!wl_columnar_relation_private_batch_disposable(writer)
            || (record->phase != WL_COLUMNAR_CONSOLIDATE_RETIRED
            && record->phase != WL_COLUMNAR_CONSOLIDATE_INERT)
            || stack->top != slot + 1u || original->rel || original->owned
            || original->seg_boundaries || original->continuation
            || result->rel != out || !result->owned
            || group->descriptor_count || group->owner_count) {
            rc = EBUSY; goto cleanup;
        }
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        /* Two entry writes plus segment disposal/transport scalar writes. */
        rc = wl_evaluation_control_charge(c, 2u * sizeof(*result) + 32u);
        if (rc) goto cleanup;
        wl_columnar_relation_consolidate_checkpoint(c, 41, 0, record, out);
        rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
        /* Segment transport is no longer needed after whole-boundary SET. */
        free(result->seg_boundaries); result->seg_boundaries = NULL;
        result->seg_count = 0;
        /* No fallible operation between the two ownership writes. */
        *original = *result; *result = (eval_entry_t){0};
        return 0;
    }
    rc = wl_columnar_relation_source_group_release_readers(group, 0);
    if (rc) goto cleanup;
    rc = wl_evaluation_control_charge(c, 64); if (rc) goto cleanup;
    if (!wl_columnar_relation_private_batch_disposable(writer)
        || record->phase != WL_COLUMNAR_CONSOLIDATE_RETIRED) {
        rc = EINVAL; goto cleanup;
    }
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    rc = wl_evaluation_control_charge(c, 32); if (rc) goto cleanup;
    if (stack->top != slot + 1u || original->rel != in || original->owned
        || result->rel != out || !result->owned) {
        rc = EBUSY; goto cleanup;
    }
    rc = wl_columnar_relation_source_group_validate_writers(group);
    if (rc) goto cleanup;
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    rc = wl_evaluation_control_charge(c, 84); if (rc) goto cleanup;
    wl_columnar_relation_consolidate_checkpoint(c, 33, 0, record, out);
    rc = wl_columnar_merge_consolidate_tail(c); if (rc) goto cleanup;
    wl_columnar_relation_source_group_commit(group); committed = true;
    rc = wl_columnar_relation_source_group_drain(group, 0);
    if (rc) goto cleanup;
    wl_columnar_relation_consolidate_checkpoint(NULL, 34, 0, record, out);
    rc = eval_entry_dispose(original); if (rc) goto cleanup;
    *original = *result; *result = (eval_entry_t){0};
    return 0;
cleanup:
    if (rc == ENOMEM && out && out->memory_budget_denial_pending) {
        sess->memory_budget_denied = true; rc = ENOSPC;
    }
    rc = wl_columnar_relation_private_batch_retire(writer, rc);
    if (wl_columnar_relation_private_batch_disposable(writer))
        rc = wl_columnar_relation_consolidate_retire(record, rc);
    int cleanup = wl_columnar_relation_source_group_drain(group, rc);
    if (cleanup > 0 && rc <= 0) rc = cleanup;
    if (!committed && !group->descriptor_count && !group->owner_count
        && wl_columnar_relation_private_batch_disposable(writer)
        && (record->phase == WL_COLUMNAR_CONSOLIDATE_INERT
        || record->phase == WL_COLUMNAR_CONSOLIDATE_RETIRED
        || record->phase == WL_COLUMNAR_CONSOLIDATE_RESETTING)){
        cleanup = eval_entry_dispose(result);
        if (cleanup > 0 && rc <= 0) rc = cleanup;
    }
    return rc;
}

int
col_op_consolidate(eval_stack_t *stack, wl_col_session_t *sess)
{
    if (sess && sess->base.evaluation_control)
        return wl_columnar_merge_consolidate_controlled(stack, sess,
                   sess->base.evaluation_control);
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

/* The original scratch lane is reachable through every stopped disposer.
* Physical pointers become NULL before the corresponding original credit is
* retired. A refused release leaves that exact token and lane pending. */
static int
wl_columnar_merge_incremental_scratch_retire(
    wl_columnar_merge_incremental_scratch_t *scratch, int primary)
{
    if (!scratch || !scratch->ready)
        return primary > 0 ? primary : EINVAL;
    if (scratch->lane == 1) {
        wl_columnar_relation_radix_sequence_t *seq = &scratch->sort;
        wl_columnar_radix_workspace_t *w = &seq->workspace;
        free(seq->boundaries); seq->boundaries = NULL;
        free(seq->needs_sort); seq->needs_sort = NULL;
        free(w->perm_a); w->perm_a = NULL;
        free(w->perm_b); w->perm_b = NULL;
        free(w->temp_column); w->temp_column = NULL;
        free(w->bucket_values); w->bucket_values = NULL;
        w->byte_values = NULL; w->short_values = NULL;
        free(w->count16); w->count16 = NULL;
        free(w->insertion_rows); w->insertion_rows = NULL;
        free(w->timestamps); w->timestamps = NULL;
        wl_columnar_memory_reservation_t *tokens[2] = {
            &seq->metadata_admission, &w->admission
        };
        bool *flags[2] = {&seq->metadata_admission_active,
                          &w->admission_active};
        for (unsigned i = 0; i < 2; i++) {
            if (!*flags[i]) continue;
            uint64_t state = atomic_load_explicit(&tokens[i]->state,
                    memory_order_acquire);
            bool retired = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                ? wl_columnar_memory_release(tokens[i])
                : state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
                && wl_columnar_memory_rollback(tokens[i]);
            if (!retired) return primary > 0 ? primary : EINVAL;
            *flags[i] = false;
        }
        scratch->lane = 0;
    } else if (scratch->lane == 3) {
        col_row_buf_release(&scratch->row);
        free(scratch->compacted); scratch->compacted = NULL;
        if (scratch->admission_active) {
            uint64_t state = atomic_load_explicit(
                &scratch->compaction_admission.state, memory_order_acquire);
            bool retired = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                ? wl_columnar_memory_release(&scratch->compaction_admission)
                : state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
                && wl_columnar_memory_rollback(&scratch->compaction_admission);
            if (!retired) return primary > 0 ? primary : EINVAL;
            scratch->admission_active = false;
        }
        scratch->lane = 0;
    } else if (scratch->lane == 4) {
        if (scratch->terminal.armed) {
            int cleanup = wl_columnar_relation_terminal_sequence_finish(
                &scratch->terminal);
            if (cleanup) return primary > 0 ? primary : cleanup;
        }
        scratch->lane = 0;
    } else if (scratch->lane != 0 && scratch->lane != 2)
        return primary > 0 ? primary : EINVAL;
    return primary;
}

static int
wl_columnar_merge_incremental_compare(const col_rel_t *rel,
    uint32_t left, uint32_t right, const int64_t *flat, int *out,
    wl_evaluation_control_t *control)
{
    *out = 0;
    for (uint32_t c = 0; c < rel->ncols; c++) {
        int rc = wl_evaluation_control_charge(control, 24);
        if (rc) return rc;
        int64_t a = rel->columns[c][left];
        int64_t b = flat ? flat[c] : rel->columns[c][right];
        int cmp = rel->column_types
            && rel->column_types[c] == WIRELOG_TYPE_FLOAT
            ? wl_columnar_float_compare_bits(a, b) : (a > b) - (a < b);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (cmp) {
            *out = cmp; return 0;
        }
    }
    return 0;
}

static int
wl_columnar_merge_incremental_scratch_rows(
    wl_columnar_merge_incremental_scratch_t *scratch,
    uint32_t ncols, bool row_needed, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_merge_incremental_scratch_retire(scratch, 0);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    scratch->lane = 2;
    for (size_t at = 0; at < offsetof(wl_columnar_merge_incremental_scratch_t,
        lane);){
        size_t n = offsetof(wl_columnar_merge_incremental_scratch_t, lane) - at;
        if (n > 128) n = 128;
        rc = wl_evaluation_control_charge(control, n); if (rc) return rc;
        memset((char *)scratch + at, 0, n); at += n;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 96); if (rc) return rc;
    wl_columnar_memory_reservation_init(&scratch->compaction_admission);
    scratch->lane = 3;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (!row_needed) return 0;
    uint64_t bytes;
    rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
    if (!wl_columnar_memory_size_mul(ncols, sizeof(int64_t), &bytes)
        || bytes > SIZE_MAX) return EOVERFLOW;
    if (ncols <= COL_STACK_MAX) scratch->row.ptr = scratch->row.stack;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (ncols <= COL_STACK_MAX) return 0;
    /* Raw wide row scratch is an uncredited heap allocation. */
    rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
    scratch->row.ptr = malloc((size_t)bytes);
    if (!scratch->row.ptr) return ENOMEM;
    return wl_evaluation_control_charge(control, 0);
}

static int
wl_columnar_merge_incremental_compact_prepare(col_rel_t *rel, uint32_t rows,
    wl_columnar_merge_incremental_scratch_t *scratch,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!scratch || !scratch->ready || scratch->lane != 3
        || scratch->compacted || scratch->admission_active) return EINVAL;
    uint64_t bytes;
    if (!wl_columnar_memory_size_mul(rows, rel->ncols, &bytes)
        || !wl_columnar_memory_size_mul(bytes, sizeof(int64_t), &bytes)
        || bytes > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (rel->memory_governor && bytes) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(rel->memory_governor), bytes,
            &scratch->compaction_admission, &status, control);
        scratch->admission_active = scratch->compaction_admission.governor !=
            NULL;
        if (rc) return rc;
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                rel->memory_budget_denial_pending = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOMEM
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
        }
    }
    rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
    scratch->compacted = malloc((size_t)bytes);
    if (!scratch->compacted) return ENOMEM;
    scratch->capacity = rows;
    return wl_evaluation_control_charge(control, 0);
}

static int
wl_columnar_merge_incremental_sift(col_rel_t *rel,
    wl_columnar_merge_incremental_scratch_t *scratch,
    uint32_t parent, uint32_t size, wl_evaluation_control_t *control)
{
    while (2u * parent + 1u < size) {
        int rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        uint32_t child = 2u * parent + 1u;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        int cmp;
        if (child + 1u < size) {
            rc = wl_columnar_merge_incremental_compare(rel,
                    scratch->positions[child + 1u], scratch->positions[child],
                    NULL, &cmp, control);
            if (rc) return rc;
            if (cmp < 0) child++;
        }
        rc = wl_columnar_merge_incremental_compare(rel,
                scratch->positions[parent], scratch->positions[child], NULL,
                &cmp, control);
        if (rc) return rc;
        if (cmp <= 0) break;
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        uint32_t position = scratch->positions[parent],
            end = scratch->ends[parent];
        scratch->positions[parent] = scratch->positions[child];
        scratch->ends[parent] = scratch->ends[child];
        scratch->positions[child] = position; scratch->ends[child] = end;
        parent = child;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_merge_incremental_compact(col_rel_t *rel,
    wl_columnar_merge_incremental_scratch_t *scratch,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (rel->run_count < 2 || rel->run_count > COL_MAX_RUNS
        || !scratch->compacted
        || rel->run_ends[rel->run_count - 1] > scratch->capacity) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    wl_columnar_relation_consolidate_checkpoint(control, 125, rel->run_count,
        NULL, rel);
    uint32_t size = 0, start = 0;
    for (uint32_t run = 0; run < rel->run_count; run++) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        uint32_t end = rel->run_ends[run];
        if (start > end || end > rel->nrows) return EINVAL;
        if (start < end) {
            scratch->positions[size] = start;
            scratch->ends[size++] = end;
        }
        start = end;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    for (uint32_t i = size / 2u; i > 0; i--) {
        rc = wl_columnar_merge_incremental_sift(rel, scratch, i - 1u, size,
                control); if (rc) return rc;
    }
    uint32_t out = 0;
    while (size) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        uint32_t row = scratch->positions[0];
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        int cmp = 1;
        if (out) {
            rc = wl_columnar_merge_incremental_compare(rel, row, 0,
                    scratch->compacted + (size_t)(out - 1u) * rel->ncols,
                    &cmp, control); if (rc) return rc;
        }
        if (cmp) {
            for (uint32_t c = 0; c < rel->ncols; c++) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) return rc;
                scratch->compacted[(size_t)out * rel->ncols + c] =
                    rel->columns[c][row];
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) return rc;
            }
            out++;
        }
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        scratch->positions[0]++;
        if (scratch->positions[0] >= scratch->ends[0]) {
            size--;
            scratch->positions[0] = scratch->positions[size];
            scratch->ends[0] = scratch->ends[size];
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (size) {
            rc = wl_columnar_merge_incremental_sift(rel, scratch, 0, size,
                    control); if (rc) return rc;
        }
    }
    for (uint32_t row = 0; row < out; row++)
        for (uint32_t c = 0; c < rel->ncols; c++) {
            rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
            rel->columns[c][row] = scratch->compacted[(size_t)row * rel->ncols +
                    c];
            wl_columnar_relation_consolidate_checkpoint(control, 126,
                (uint64_t)row * rel->ncols + c + 1u, NULL, rel);
            rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        }
    rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    free(scratch->compacted); scratch->compacted = NULL;
    if (scratch->admission_active) {
        uint64_t state =
            atomic_load_explicit(&scratch->compaction_admission.state,
                memory_order_acquire);
        bool retired = state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
            ? wl_columnar_memory_rollback(&scratch->compaction_admission)
            : state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            && wl_columnar_memory_release(&scratch->compaction_admission);
        if (!retired) return EINVAL;
        scratch->admission_active = false;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    rel->nrows = out; wl_columnar_relation_touch_view(rel);
    rel->sorted_nrows = out; rel->run_count = 1; rel->run_ends[0] = out;
    scratch->output_rows = out;
    return wl_evaluation_control_charge(control, 0);
}

int
wl_columnar_merge_incremental_complete_storage(
    wl_columnar_merge_incremental_authority_t *authority,
    wl_columnar_merge_incremental_scratch_t *scratch,
    wl_columnar_relation_bulk_append_t *bulk, bool swap,
    wl_evaluation_control_t *control)
{
    if (!authority || !scratch || !bulk || !control || !scratch->ready)
        return EINVAL;
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    /* Healthy forward retirement has admitted readiness. Physical frees use
     * the existing exclusion; original refused credit still blocks overlay. */
    rc = wl_columnar_merge_incremental_scratch_retire(scratch, 0);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    scratch->lane = 2;
    for (size_t at = 0; at < sizeof(scratch->terminal);) {
        size_t n = sizeof(scratch->terminal) - at;
        if (n > 128u) n = 128u;
        rc = wl_evaluation_control_charge(control, n); if (rc) return rc;
        memset((char *)&scratch->terminal + at, 0, n); at += n;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    scratch->lane = 4;
    return wl_columnar_relation_incremental_terminal(bulk, authority,
               &scratch->terminal, swap, control);
}

int
wl_columnar_merge_incremental_retire(
    wl_columnar_merge_incremental_authority_t *authority,
    wl_columnar_merge_incremental_scratch_t *scratch, int primary)
{
    int rc = primary;
    if (authority && authority->ready && authority->active
        && authority->phase == 2u && !authority->consumed
        && !authority->delta_rolled_back && authority->delta) {
        col_rel_t *delta = authority->delta;
        int cleanup = authority->set.lease_count == 2u
            && authority->set.leases == authority->leases
            ? col_rel_mutation_lease_validate(&authority->leases[1],
                delta) : EINVAL;
        if (cleanup) return primary > 0 ? primary : cleanup;
        if (delta->nrows != authority->delta_initial_nrows) {
            if (delta->view_generation >=
                WL_COLUMNAR_REL_GENERATION_INVALID - 1u)
                return primary > 0 ? primary : EOVERFLOW;
            delta->nrows = authority->delta_initial_nrows;
            wl_columnar_relation_touch_view(delta);
            authority->set.published = true;
        }
        authority->delta_rolled_back = true;
    }
    rc = wl_columnar_merge_incremental_scratch_retire(scratch, rc);
    if (scratch && scratch->ready && scratch->lane != 0
        && scratch->lane != 2) return rc;
    return wl_columnar_relation_incremental_finish(authority, rc);
}

static int
wl_columnar_merge_incremental_move(col_rel_t *rel, uint32_t to, uint32_t from,
    wl_evaluation_control_t *control)
{
    if (to == from) return 0;
    for (uint32_t col = 0; col < rel->ncols; col++) {
        int rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        rel->columns[col][to] = rel->columns[col][from];
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_merge_incremental_emit(
    wl_columnar_merge_incremental_authority_t *authority,
    wl_columnar_merge_incremental_scratch_t *scratch, int64_t **columns,
    uint32_t row, wl_evaluation_control_t *control)
{
    if (!authority->delta) return 0;
    for (uint32_t col = 0; col < authority->source->ncols; col++) {
        int rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        scratch->row.ptr[col] = columns[col][row];
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    bool committed = false;
    return wl_columnar_relation_incremental_append_row(authority, 1,
               scratch->row.ptr, &committed, control);
}

/* Production adaptive algorithm; the caller owns its original frame and bulk.
* Automatic variables below are scalar cursors, never lifetime authorities. */
int
wl_columnar_merge_incremental_controlled(
    wl_columnar_merge_incremental_authority_t *authority,
    wl_columnar_merge_incremental_scratch_t *scratch,
    wl_columnar_relation_bulk_append_t *bulk, col_rel_t *rel,
    uint32_t old_nrows, col_rel_t *delta, int *out_fast,
    wl_evaluation_control_t *control)
{
    if (!authority || !scratch || !bulk || !rel || !control || rel == delta)
        return EINVAL;
    int rc = wl_columnar_relation_incremental_acquire(authority, rel, delta,
            control);
    if (rc) {
        return rc;
    }
    rc = wl_evaluation_control_charge(control, 128); if (rc) goto done;
    if (!scratch->ready || scratch->lane || bulk->active
        || rel->run_count > COL_MAX_RUNS || rel->nrows > rel->capacity
        || (delta &&
        (delta->ncols != rel->ncols || delta->nrows > delta->capacity))){
        rc = EINVAL; goto done;
    }
    authority->old_nrows = old_nrows;
    authority->nrows = rel->nrows;
    authority->delta_initial_nrows = delta ? delta->nrows : 0;
    authority->delta_rolled_back = false; authority->consumed = false;
    authority->phase = 2u;
    rel->memory_budget_denial_pending = false;
    if (delta) delta->memory_budget_denial_pending = false;
    rc = col_op_consolidate_incremental_delta_generation_preflight(rel,
            old_nrows);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    for (uint32_t target = 0; target < (delta ? 2u : 1u); target++) {
        col_rel_t *r = target ? delta : rel;
        for (uint32_t col = 0; col < r->ncols; col++) {
            rc = wl_evaluation_control_charge(control, 8); if (rc) goto done;
            if (r->nrows && (!r->columns || !r->columns[col])) {
                rc = EINVAL; goto done;
            }
            bool floating = r->column_types &&
                r->column_types[col] == WIRELOG_TYPE_FLOAT;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
            if (!floating) continue;
            for (uint32_t row = 0; row < r->nrows; row++) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) goto done;
                if (!wl_columnar_float_bits_valid(r->columns[col][row])) {
                    rc = EINVAL; goto done;
                }
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
        }
    }
    if (!rel->nrows || old_nrows >= rel->nrows) {
        if (out_fast) *out_fast = 1;
        authority->consumed = true; goto done;
    }
    uint32_t count = rel->nrows - old_nrows;
    if (delta) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
        if (delta->view_generation >=
            WL_COLUMNAR_REL_GENERATION_INVALID - 1u - count){
            rc = EOVERFLOW; goto done;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        rc = wl_columnar_relation_incremental_reserve_rows(bulk, delta,
                authority, 1, count, false, control); if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 96); if (rc) goto done;
    scratch->lane = 1u;
    wl_columnar_memory_reservation_init(&scratch->sort.workspace.admission);
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    rc = wl_columnar_relation_incremental_radix_prepare(rel, old_nrows, count,
            &scratch->sort.workspace, &authority->leases[0], control);
    if (rc) goto done;
    rc = wl_columnar_relation_incremental_radix_execute(rel, old_nrows, count,
            &scratch->sort.workspace, authority, 0, bulk, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 64); if (rc) goto done;
    authority->set.published = true;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    uint32_t unique = 1;
    for (uint32_t row = 1; row < count; row++) {
        int cmp;
        rc = wl_columnar_merge_incremental_compare(rel, old_nrows + row - 1u,
                old_nrows + row, NULL, &cmp, control); if (rc) goto done;
        if (cmp) {
            rc = wl_columnar_merge_incremental_move(rel, old_nrows + unique,
                    old_nrows + row, control); if (rc) goto done;
            unique++;
        }
        rc = wl_evaluation_control_charge(control, 4); if (rc) goto done;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    }
    authority->unique_rows = unique;
    rc = wl_evaluation_control_charge(control, 32); if (rc) goto done;
    if (!rel->run_count || rel->run_ends[rel->run_count - 1u] != old_nrows) {
        rel->run_count = old_nrows ? 1u : 0u;
        if (old_nrows) rel->run_ends[0] = old_nrows;
    }
    bool fast = !old_nrows;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    if (old_nrows) {
        fast = true;
        for (uint32_t run = 0; run < rel->run_count && fast; run++) {
            rc = wl_evaluation_control_charge(control, 8); if (rc) goto done;
            uint32_t end = rel->run_ends[run];
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
            if (end) {
                int cmp;
                rc = wl_columnar_merge_incremental_compare(rel, end - 1u,
                        old_nrows, NULL, &cmp, control); if (rc) goto done;
                fast = cmp < 0;
            }
        }
    }
    bool small = !fast && unique <= old_nrows / 16u && rel->run_count;
    if (rel->col_shared && (!fast || rel->run_count >= COL_MAX_RUNS)) {
        rc = wl_columnar_relation_incremental_reserve_rows(bulk, rel,
                authority, 0, 0, true, control); if (rc) goto done;
    }
    if (fast || small) {
        rc = wl_columnar_merge_incremental_scratch_rows(scratch, rel->ncols,
                delta != NULL, control); if (rc) goto done;
        uint32_t novel = 0;
        for (uint32_t row = 0; row < unique; row++) {
            bool found = false;
            if (small) {
                for (uint32_t run = 0; run < rel->run_count && !found; run++) {
                    rc = wl_evaluation_control_charge(control, 16);
                    if (rc) goto done;
                    uint32_t lo = run ? rel->run_ends[run - 1u] : 0u;
                    uint32_t hi = rel->run_ends[run];
                    rc = wl_evaluation_control_charge(control, 0);
                    if (rc) goto done;
                    while (lo < hi) {
                        uint32_t mid = lo + (hi - lo) / 2u;
                        int cmp;
                        rc = wl_columnar_merge_incremental_compare(rel, mid,
                                old_nrows + row, NULL, &cmp, control);
                        if (rc) goto done;
                        if (!cmp) {
                            found = true; break;
                        }
                        rc = wl_evaluation_control_charge(control, 8);
                        if (rc) goto done;
                        if (cmp < 0) lo = mid + 1u; else hi = mid;
                        rc = wl_evaluation_control_charge(control, 0);
                        if (rc) goto done;
                    }
                }
            }
            if (!found) {
                rc = wl_columnar_merge_incremental_move(rel, old_nrows + novel,
                        old_nrows + row, control); if (rc) goto done;
                rc = wl_columnar_merge_incremental_emit(authority, scratch,
                        rel->columns, old_nrows + novel, control);
                if (rc) goto done;
                novel++;
            }
            rc = wl_evaluation_control_charge(control, 4); if (rc) goto done;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        }
        /* Raw row scratch retires before compaction scratch admission. */
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
        col_row_buf_release(&scratch->row);
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        bool compact = novel && rel->run_count >= COL_MAX_RUNS;
        if (compact) {
            rc = wl_columnar_merge_incremental_compact_prepare(rel,
                    old_nrows + novel, scratch, control); if (rc) goto done;
        }
        rc = wl_evaluation_control_charge(control, 32); if (rc) goto done;
        rel->nrows = old_nrows + novel; wl_columnar_relation_touch_view(rel);
        if (novel && !compact) rel->run_ends[rel->run_count++] = rel->nrows;
        if (novel && compact &&
            fast) rel->run_ends[rel->run_count - 1u] = rel->nrows;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        if (compact) {
            rc = wl_columnar_merge_incremental_compact(rel, scratch, control);
            if (rc) goto done;
            if (small) {
                uint32_t prefix = rel->nrows;
                for (uint32_t row = 0; row < novel; row++) {
                    rc = wl_columnar_merge_incremental_move(rel, prefix + row,
                            old_nrows + row, control); if (rc) goto done;
                }
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto done;
                rel->nrows = prefix + novel;
                wl_columnar_relation_touch_view(rel);
                rel->run_ends[rel->run_count++] = rel->nrows;
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
        }
        rel->sorted_nrows = rel->nrows;
    } else {
        uint32_t max_rows = old_nrows + unique;
        if (rel->merge_buf_cap < max_rows) {
            uint32_t cap = max_rows > rel->merge_buf_cap * 2u
                ? max_rows : rel->merge_buf_cap * 2u;
            if (cap < max_rows) cap = max_rows;
            rc = wl_columnar_relation_incremental_reserve_grid(bulk, rel,
                    authority, 0, cap, control); if (rc) goto done;
        }
        rc = wl_columnar_merge_incremental_scratch_rows(scratch, rel->ncols,
                true, control); if (rc) goto done;
        if (rel->run_count > 1u) {
            rc = wl_columnar_merge_incremental_compact_prepare(rel, rel->nrows,
                    scratch, control); if (rc) goto done;
            rc = wl_columnar_merge_incremental_compact(rel, scratch, control);
            if (rc) goto done;
            uint32_t prefix = rel->nrows;
            for (uint32_t row = 0; row < unique; row++) {
                rc = wl_columnar_merge_incremental_move(rel, prefix + row,
                        old_nrows + row, control); if (rc) goto done;
            }
            old_nrows = prefix;
            rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
            rel->nrows = prefix + unique; wl_columnar_relation_touch_view(rel);
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        }
        uint32_t oi = 0, di = 0, out = 0;
        while (oi < old_nrows || di < unique) {
            int cmp = oi >= old_nrows ? 1 : di >= unique ? -1 : 0;
            if (oi < old_nrows && di < unique) {
                rc = wl_columnar_merge_incremental_compare(rel, oi,
                        old_nrows + di, NULL, &cmp, control); if (rc) goto done;
            }
            uint32_t from = cmp <= 0 ? oi : old_nrows + di;
            for (uint32_t col = 0; col < rel->ncols; col++) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) goto done;
                rel->merge_columns[col][out] = rel->columns[col][from];
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto done;
            }
            if (cmp > 0) {
                rc = wl_columnar_merge_incremental_emit(authority, scratch,
                        rel->merge_columns, out, control); if (rc) goto done;
            }
            rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
            if (cmp <= 0) oi++;
            if (cmp >= 0) di++;
            out++;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
        }
        rc = wl_columnar_merge_incremental_complete_storage(authority,
                scratch, bulk, true, control); if (rc) goto done;
        rc = wl_evaluation_control_charge(control, 32); if (rc) goto done;
        rel->nrows = out; wl_columnar_relation_touch_view(rel);
        rel->sorted_nrows = out; rel->run_count = 1u; rel->run_ends[0] = out;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    }
    if (fast || small) {
        rc = wl_columnar_merge_incremental_complete_storage(authority,
                scratch, bulk, false, control); if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
    if (out_fast) *out_fast = fast ? 1 : 0;
    authority->consumed = true;
    rc = wl_evaluation_control_charge(control, 0);
done:
    rc = wl_columnar_relation_bulk_append_retire(bulk, rc);
    if (bulk->active) return rc > 0 ? rc : EINVAL;
    return wl_columnar_merge_incremental_retire(authority, scratch, rc);
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
