/*
 * columnar/internal.h - wirelog Columnar Backend Internal Types
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Private type definitions and constants for the columnar nanoarrow backend.
 * Extracted from backend/columnar_nanoarrow.c to enable modular splitting.
 */

#ifndef WL_COLUMNAR_INTERNAL_H
#define WL_COLUMNAR_INTERNAL_H

#define _GNU_SOURCE

#include "columnar/columnar_nanoarrow.h"
#include "columnar/delta_pool.h"
#include "session.h"
#include "workqueue.h"
#include "arena/arena.h"

#include "nanoarrow/nanoarrow.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#ifdef _MSC_VER
#include <windows.h> /* For GetTickCount64() in now_ns() */
#include <intrin.h>  /* For _BitScanForward64 */
#endif

/* ======================================================================== */
/* Platform Shims                                                           */
/* ======================================================================== */

/* GCC/Clang extension not supported on MSVC */
#ifdef _MSC_VER
#define UNUSED
/* MSVC: __builtin_ctzll not available; use _BitScanForward64 */
static inline int
ctzll(unsigned long long x)
{
    unsigned long index;
    if (_BitScanForward64(&index, x)) {
        return (int)index;
    }
    return 64; /* undefined for x=0, but we don't call it with x=0 */
}
#define __builtin_ctzll(x) ctzll(x)
#else
#define UNUSED __attribute__((unused))
#endif

/* ======================================================================== */
/* Profiling Helper                                                         */
/* ======================================================================== */

/*
 * now_ns: return monotonic time in nanoseconds.
 * Uses clock_gettime(CLOCK_MONOTONIC) on POSIX systems,
 * GetTickCount64() on Windows MSVC.
 * Returns 0 on platforms where time function is unavailable (non-fatal).
 */
static inline uint64_t
now_ns(void)
{
#ifdef _MSC_VER
    /* Windows MSVC: use GetTickCount64() in milliseconds, convert to nanoseconds.
     * Returns milliseconds since system startup (monotonic). */
    return GetTickCount64() * 1000000ULL;
#else
    /* POSIX: use clock_gettime with CLOCK_MONOTONIC. */
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return 0;
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

/* ======================================================================== */
/* Constants                                                                */
/* ======================================================================== */

#define COL_REL_INIT_CAP 64u
#define COL_STACK_MAX 32u
#define COL_FILTER_STACK 64u
#define MAX_ITERATIONS 4096u
#define MAX_STRATA 32
#define MAX_RULES 64
#define COL_MAT_CACHE_MAX 64u
#define COL_MAT_CACHE_LIMIT_BYTES (100ULL * 1024ULL * 1024ULL)

/* ======================================================================== */
/* Relation Storage                                                         */
/* ======================================================================== */

/*
 * col_rel_t: in-memory columnar relation.
 *
 * Tuples are stored row-major: data[row * ncols + col].
 * Column names enable JOIN key resolution (variable name -> position).
 * The ArrowSchema provides Arrow-compatible type metadata.
 */
typedef struct {
    char *name;                /* owned, null-terminated                */
    uint32_t ncols;            /* columns per tuple (0 = unset)         */
    int64_t *data;             /* owned, row-major int64 buffer         */
    uint32_t nrows;            /* current row count                     */
    uint32_t capacity;         /* allocated row capacity                */
    char **col_names;          /* owned array of ncols owned strings    */
    struct ArrowSchema schema; /* owned Arrow schema (lazy-inited)      */
    bool schema_ok;            /* true after schema is initialised      */
    /* Sorted-prefix tracking for incremental consolidation (issue #94).
     * After consolidation, sorted_nrows == nrows (all rows sorted+unique).
     * Appending new rows leaves sorted_nrows unchanged: data[0..sorted_nrows)
     * is sorted, data[sorted_nrows..nrows) is an unsorted suffix.
     * Enables O(D log D + N) merge instead of O(N log N) full sort. */
    uint32_t sorted_nrows;
    /* Persistent merge buffer for consolidation (issue #94).
     * Reused across iterations to avoid per-consolidation malloc/free.
     * Grows via realloc as needed; freed only on relation destruction. */
    int64_t *merge_buf;
    uint32_t merge_buf_cap; /* capacity in rows */
    /* Base row count for delta propagation (issue #83).
     * After initial eval convergence, base_nrows == nrows. On incremental
     * insert, nrows grows but base_nrows stays fixed. Rows[base_nrows..nrows)
     * are the delta (new EDB facts not yet propagated). */
    uint32_t base_nrows;
    /* Timestamp tracking (optional): NULL when disabled.
     * When non-NULL, timestamps[i] records the provenance of data row i.
     * Capacity tracks with the data array (capacity entries allocated). */
    col_delta_timestamp_t *timestamps;
    /* Pool ownership flag (issue #123).
     * true  = struct was allocated from delta_pool; do not free() the struct.
     * false = struct was heap-allocated via calloc(); free() on destroy. */
    bool pool_owned;
} col_rel_t;

/**
 * col_materialized_join_t - Cached intermediate join result for CSE
 *
 * Stores the output of a multi-way join operation for reuse across
 * multiple semi-naive iterations. Used by Option 2 + Materialization
 * to avoid redundant computation of shared join prefixes.
 */
typedef struct {
    int64_t *data;         /* owned, row-major int64 buffer         */
    uint32_t nrows;        /* current row count                     */
    uint32_t ncols;        /* columns per tuple                     */
    uint32_t capacity;     /* allocated row capacity (for appending)*/
    uint32_t memory_limit; /* max bytes before eviction             */
    bool is_valid;         /* true if data is current for this iter */
} col_materialized_join_t;

/* ======================================================================== */
/* Materialization Cache (US-006)                                           */
/* ======================================================================== */

/*
 * col_mat_entry_t: one entry in the materialization cache.
 * Keyed by content hashes of left and right input relations.
 * The cache owns result; freed when the entry is evicted or cache cleared.
 */
typedef struct {
    uint64_t left_hash;  /* content hash of left input relation  */
    uint64_t right_hash; /* content hash of right input relation */
    col_rel_t *result;   /* owned cached join result             */
    size_t mem_bytes;    /* bytes used by result->data           */
    uint64_t lru_clock;  /* logical time of last access          */
} col_mat_entry_t;

typedef struct {
    col_mat_entry_t entries[COL_MAT_CACHE_MAX];
    uint32_t count;
    size_t total_bytes;
    uint64_t clock;
    uint64_t hits;   /* cache hit counter  */
    uint64_t misses; /* cache miss counter */
} col_mat_cache_t;

/* ======================================================================== */
/* Operator Profiling (WL_PROFILE)                                         */
/* ======================================================================== */

#ifdef WL_PROFILE
typedef struct {
    uint64_t concat_calls;
    uint64_t concat_empty_out;
    uint64_t concat_ns;
    uint64_t join_calls;
    uint64_t join_unary; /* joins where left or right ncols == 1    */
    uint64_t join_empty_out;
    uint64_t join_cache_hit_ns;
    uint64_t join_compute_ns;
} wl_profile_stats_t;
#endif

/* ======================================================================== */
/* Arrangement Registry                                                     */
/* ======================================================================== */

/*
 * col_arr_entry_t: one (rel_name, key_cols) -> arrangement mapping.
 * Stored in the session's flat arrangement registry.
 */
typedef struct {
    char *rel_name;     /* owned copy of relation name    */
    uint32_t *key_cols; /* owned copy of key column array */
    uint32_t key_count;
    col_arrangement_t arr; /* embedded arrangement           */
} col_arr_entry_t;

/*
 * col_sorted_arr_t: cached sorted copy of a relation by a single key column.
 *
 * Used by WL_PLAN_OP_LFTJ (Issue #195) to avoid re-sorting EDB relations
 * on every semi-naive iteration.  sorted[] is rebuilt when indexed_rows
 * differs from the source relation's nrows.
 */
typedef struct {
    int64_t *sorted;       /* owned row-major sorted copy (nrows * ncols)  */
    uint32_t nrows;        /* rows in sorted copy                          */
    uint32_t ncols;        /* columns per row                              */
    uint32_t key_col;      /* sort key column index                        */
    uint32_t indexed_rows; /* source nrows at last build                  */
} col_sorted_arr_t;

/*
 * col_sorted_arr_entry_t: one (rel_name, key_col) -> sorted arrangement entry.
 * Stored in the session's flat sorted-arrangement registry.
 */
typedef struct {
    char *rel_name; /* owned */
    uint32_t key_col;
    col_sorted_arr_t sarr;
} col_sorted_arr_entry_t;

/* ======================================================================== */
/* Session                                                                  */
/* ======================================================================== */

/*
 * wl_col_session_t: Columnar backend session state
 *
 * Memory layout (C11 6.7.2.1 P15 - pointer compatibility):
 *   Offset 0: wl_session_t base
 *     -- Contains: backend pointer (vtable dispatch, set by session.c:38)
 *   Offset sizeof(wl_session_t): columnar-specific fields
 *     -- const wl_plan_t *plan
 *     -- col_rel_t **rels
 *     -- uint32_t nrels
 *     -- uint32_t rel_cap
 *     -- ...
 *
 * The `base` field MUST be the first member so that casting a
 * wl_session_t* to wl_col_session_t* is valid per C11 pointer
 * compatibility (address of struct == address of first member).
 */
typedef struct {
    wl_session_t base;         /* MUST be first field (vtable dispatch)  */
    const wl_plan_t *plan;     /* borrowed, lifetime: caller             */
    col_rel_t **rels;          /* owned array of owned col_rel_t*        */
    uint32_t nrels;            /* current number of registered relations */
    uint32_t rel_cap;          /* allocated capacity of rels[]           */
    wl_on_delta_fn delta_cb;   /* delta callback (NULL = disabled)       */
    void *delta_data;          /* opaque user context for delta_cb       */
    wl_arena_t *eval_arena;    /* arena for per-iteration temporaries    */
    col_mat_cache_t mat_cache; /* materialization cache (US-006)        */
    uint32_t total_iterations; /* fixed-point iterations in last eval   */
    /* Issue #176: Per-iteration cache eviction for recursive strata.
     * cache_evict_threshold: target cache size (bytes) for LRU eviction
     * in recursive stratum iteration loop. When cache exceeds this threshold,
     * LRU entries are evicted until size drops below it.
     * 0 = disabled (cache cleared each iteration), default = 80% of limit */
    size_t cache_evict_threshold;
    wl_work_queue_t *wq; /* reusable thread pool for K-fusion     */
    /* Profiling counters (3B-003): accumulated across all strata/iters  */
    uint64_t
        consolidation_ns; /* time in col_op_consolidate_incremental_delta */
    uint64_t kfusion_ns;  /* time in col_op_k_fusion                */
    /* Arrangement registry (Phase 3C): persistent hash indices           */
    col_arr_entry_t *arr_entries; /* owned flat array of arrangements     */
    uint32_t arr_count;           /* number of active arrangements        */
    uint32_t arr_cap;             /* allocated capacity                   */
    /* Delta arrangement cache (Phase 3C-001-Ext): per-iteration hash
     * indices for delta-substituted right relations.  Unlike arr_entries
     * (cross-iteration, session-global), these are:
     *   - Per-worker: zeroed in each K-fusion worker's session copy
     *   - Per-iteration: freed at start of each col_eval_stratum iter
     * This avoids races: workers write to their own copy, never shared. */
    col_arr_entry_t *darr_entries; /* owned flat array of delta arrs      */
    uint32_t darr_count;           /* number of active delta arrangements */
    uint32_t darr_cap;             /* allocated capacity                  */
    /* 2D Frontier Epoch Tracking (Issue #103): Incremental insertion epoch counter.
     * Incremented before each EDB insertion and stratum evaluation to distinguish
     * between different insertion epochs. Used with frontiers[] to track
     * (outer_epoch, iteration) pairs: an iteration is skipped only if it was
     * already processed in the SAME outer_epoch. This prevents incorrect skipping
     * when comparing across different insertion epochs. */
    uint32_t outer_epoch;
    /* Frontier tracking (Phase 4 / Issue #104): per-stratum 2D frontier array
     * for epoch-aware incremental re-evaluation. Each frontiers[i] tracks the
     * (outer_epoch, iteration) pair at which stratum i last converged.
     * An iteration is skipped only when it was already processed in the SAME
     * outer_epoch, preventing incorrect skipping across insertion epochs.
     * This enables skipping redundant iterations when frontier persists across
     * session_step calls (incremental evaluation). */
    col_frontier_2d_t
        frontiers[MAX_STRATA]; /* per-stratum 2D frontier tracking */
    /* Frontier tracking (Phase 4, US-4-002): per-rule frontier array for
     * rule-level incremental re-evaluation. Each rule_frontiers[i] tracks the
     * minimum (iteration, stratum) boundary fully processed for rule i.
     * Initialized to (0, 0) via calloc. Reset to UINT32_MAX for affected
     * rules on incremental insert (enables selective rule skipping in
     * US-4-004). MAX_RULES=64 matches uint64_t bitmask width. */
    col_frontier_2d_t
        rule_frontiers[MAX_RULES]; /* per-rule frontier tracking with
                                                     (outer_epoch, iteration) pairs */
    /* Per-phase K-fusion profiling (Phase 3A): breakdown of kfusion_ns into
     * four sub-phases accumulated across all col_op_k_fusion calls per eval.
     *   alloc_ns:    calloc of results/workers/worker_sess arrays
     *   dispatch_ns: submit loop + wl_workqueue_wait_all barrier
     *   merge_ns:    col_rel_merge_k + eval_stack_push
     *   cleanup_ns:  result/worker mat_cache/arr/darr free loops + free() */
    uint64_t kfusion_alloc_ns;
    uint64_t kfusion_dispatch_ns;
    uint64_t kfusion_merge_ns;
    uint64_t kfusion_cleanup_ns;
    /* Phase 4: tracks which relation was just inserted via
     * col_session_insert_incremental, enables affected-stratum skip
     * optimization. Borrowed pointer; lifetime: until next session_step.
     * NULL when no incremental insert preceded the current step (all strata
     * evaluated normally via affected_mask = UINT64_MAX). */
    const char *last_inserted_relation;
    /* Current fixed-point iteration counter within col_eval_stratum.
     * Set at the start of each iteration; operators use this to distinguish
     * iteration 0 (base case: FORCE_DELTA falls back to full relation) from
     * iteration > 0 (FORCE_DELTA with absent delta -> empty short-circuit).
     * Only valid during col_eval_stratum execution. */
    uint32_t current_iteration;
    /* Delta-seeded incremental evaluation (issue #83).
     * When true, EDB delta relations have been pre-seeded into the session
     * before re-evaluation. FORCE_DELTA at iteration 0 pushes empty (not full)
     * for relations without a pre-seeded delta, enabling delta-only propagation
     * instead of full re-derivation. Cleared after eval completes. */
    bool delta_seeded;
    /* Multi-worker support (issue #99).
     * Stored from col_session_create num_workers parameter.
     * When > 1, sess->wq is created at session init for parallel K-fusion.
     * When == 1, K-fusion evaluates copies sequentially (no thread overhead). */
    uint32_t num_workers;
    /* Monotone property tracking (issue #105).
     * stratum_is_monotone[si] = true if stratum si only derives facts
     * (no deletion via negation/antijoin/subtraction). Used for DRedL-style
     * deletion phase skip optimization. Populated from plan->strata[si].is_monotone
     * during session_create. Conservative default: all false (no optimization). */
    bool stratum_is_monotone[MAX_STRATA];
    /* Retraction delta tracking (Issue #158): tracks which relation was just
     * removed via col_session_remove_incremental, enables delta retraction path.
     * Borrowed pointer; lifetime: until next session_step.
     * NULL when no incremental remove preceded the current step. */
    const char *last_removed_relation;
    /* Retraction-seeded incremental evaluation (Issue #158).
     * When true, EDB retraction delta relations have been pre-seeded into
     * the session before re-evaluation. Similar to delta_seeded but for
     * $r$<name> relations. Cleared after eval completes. */
    bool retraction_seeded;
    delta_pool_t *delta_pool; /* Pool allocator for operator temporaries */
    /* Sorted arrangement cache (Issue #195): per-key-column sorted copies of
     * EDB relations for leapfrog triejoin (WL_PLAN_OP_LFTJ).  Unlike hash
     * arrangements, these are sorted by a single key column and reused across
     * semi-naive iterations (EDB relations are stable between iterations).
     * Freed only on session destroy. */
    col_sorted_arr_entry_t
        *sarr_entries; /* flat array of sorted arrangements */
    uint32_t sarr_count;
    uint32_t sarr_cap;
#ifdef WL_PROFILE
    wl_profile_stats_t profile; /* operator profiling counters */
#endif
} wl_col_session_t;

/*
 * COL_SESSION: Cast wl_session_t* to wl_col_session_t*
 *
 * Safe because wl_session_t base is the first member of wl_col_session_t
 * (C11 6.7.2.1 P15 guarantees address equality of struct and first member).
 */
#define COL_SESSION(s) ((wl_col_session_t *)(s))

/* ======================================================================== */
/* Evaluation Stack                                                         */
/* ======================================================================== */

/*
 * eval_entry_t: one entry on the operator evaluation stack.
 *
 * @rel:   heap-allocated result relation (owned; freed on pop)
 * @owned: true if this entry owns @rel (must free on pop)
 */
typedef struct {
    col_rel_t *rel;
    bool owned;
    bool is_delta; /* true when rel is a delta (DR) relation, not the full */
    uint32_t
        *seg_boundaries; /* Array of K+1 boundary row indices (K-way merge) */
    uint32_t seg_count;  /* Number of segments (0 = no segmentation) */
} eval_entry_t;

typedef struct {
    eval_entry_t items[COL_STACK_MAX];
    uint32_t top;
} eval_stack_t;

/* ======================================================================== */
/* Relation Storage (columnar/relation.c)                                   */
/* ======================================================================== */

void
col_rel_free_contents(col_rel_t *r);
void
col_rel_destroy(col_rel_t *r);
int
col_rel_set_schema(col_rel_t *r, uint32_t ncols, const char *const *col_names);
int
col_rel_alloc(col_rel_t **out, const char *name);
int
col_rel_append_row(col_rel_t *r, const int64_t *row);
int
col_rel_append_all(col_rel_t *dst, const col_rel_t *src);
int
col_rel_col_idx(const col_rel_t *r, const char *name);
col_rel_t *
col_rel_new_auto(const char *name, uint32_t ncols);
col_rel_t *
col_rel_new_like(const char *name, const col_rel_t *src);
col_rel_t *
col_rel_pool_new_like(delta_pool_t *pool, const char *name,
                      const col_rel_t *like);
col_rel_t *
col_rel_pool_new_auto(delta_pool_t *pool, const char *name, uint32_t ncols);

/* ======================================================================== */
/* Cache & Materialized Join (columnar/cache.c)                             */
/* ======================================================================== */

col_materialized_join_t *
col_materialized_join_create(uint32_t ncols, uint32_t memory_limit);
int
col_materialized_join_append(col_materialized_join_t *mj, const int64_t *row);
void
col_materialized_join_free(col_materialized_join_t *mj);
void
col_materialized_join_invalidate(col_materialized_join_t *mj);
uint64_t
col_mat_cache_key_content(const col_rel_t *rel);
void
col_mat_cache_clear(col_mat_cache_t *cache);
void
col_mat_cache_evict_until(col_mat_cache_t *cache, size_t target_bytes);
col_rel_t *
col_mat_cache_lookup(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right);
void
col_mat_cache_insert(col_mat_cache_t *cache, const col_rel_t *left,
                     const col_rel_t *right, col_rel_t *result);

/* ======================================================================== */
/* qsort_r Compatibility                                                    */
/* ======================================================================== */

/* Comparison for qsort_r: lexicographic int64 row order.
 * Note: BSD qsort_r has signature: qsort_r(base, nmemb, size, ctx, comparator)
 *       GNU qsort_r has signature: qsort_r(base, nmemb, size, comparator, arg)
 */
#ifdef __GLIBC__
/* GNU glibc qsort_r: comparator first, context last */
static inline int
row_cmp_fn(const void *a, const void *b, void *ctx)
{
    const uint32_t ncols = *(const uint32_t *)ctx;
    const int64_t *ra = (const int64_t *)a;
    const int64_t *rb = (const int64_t *)b;
    for (uint32_t c = 0; c < ncols; c++) {
        if (ra[c] < rb[c])
            return -1;
        if (ra[c] > rb[c])
            return 1;
    }
    return 0;
}
#define QSORT_R_CALL(base, nmemb, size, ctx, fn) \
    qsort_r(base, nmemb, size, fn, ctx)
#elif defined(_MSC_VER)
/* MSVC qsort_s: context first, comparator last */
static inline int __cdecl row_cmp_fn(void *ctx, const void *a, const void *b)
{
    const uint32_t ncols = *(const uint32_t *)ctx;
    const int64_t *ra = (const int64_t *)a;
    const int64_t *rb = (const int64_t *)b;
    for (uint32_t c = 0; c < ncols; c++) {
        if (ra[c] < rb[c])
            return -1;
        if (ra[c] > rb[c])
            return 1;
    }
    return 0;
}
#define QSORT_R_CALL(base, nmemb, size, ctx, fn) \
    qsort_s(base, nmemb, size, fn, ctx)
#else
/* BSD qsort_r: context first, comparator last */
static inline int
row_cmp_fn(void *ctx, const void *a, const void *b)
{
    const uint32_t ncols = *(const uint32_t *)ctx;
    const int64_t *ra = (const int64_t *)a;
    const int64_t *rb = (const int64_t *)b;
    for (uint32_t c = 0; c < ncols; c++) {
        if (ra[c] < rb[c])
            return -1;
        if (ra[c] > rb[c])
            return 1;
    }
    return 0;
}
#define QSORT_R_CALL(base, nmemb, size, ctx, fn) \
    qsort_r(base, nmemb, size, ctx, fn)
#endif

/* ======================================================================== */
/* Session Helpers (backend/columnar_nanoarrow.c)                           */
/* ======================================================================== */

col_rel_t *
session_find_rel(wl_col_session_t *sess, const char *name);
int
session_add_rel(wl_col_session_t *sess, col_rel_t *r);
void
session_remove_rel(wl_col_session_t *sess, const char *name);

/* ======================================================================== */
/* Arrangement Layer (columnar/arrangement.c)                               */
/* ======================================================================== */

void
arr_free_contents(col_arrangement_t *arr);
col_arrangement_t *
col_session_get_delta_arrangement(wl_col_session_t *cs, const char *rel_name,
                                  const col_rel_t *delta_rel,
                                  const uint32_t *key_cols, uint32_t key_count);
void
col_session_free_delta_arrangements(wl_col_session_t *cs);
col_sorted_arr_t *
col_session_get_sorted_arrangement(wl_col_session_t *cs, const char *rel_name,
                                   uint32_t key_col);
void
col_session_free_sorted_arrangements(wl_col_session_t *cs);

/* ======================================================================== */
/* Frontier & Affected Strata (columnar/frontier.c)                         */
/* ======================================================================== */

uint64_t
col_compute_affected_strata(wl_session_t *session,
                            const char *inserted_relation);
uint64_t
col_compute_affected_rules(wl_session_t *session,
                           const char *inserted_relation);

/* ======================================================================== */
/* Mobius / Z-set (columnar/mobius.c)                                        */
/* ======================================================================== */

int
col_compute_delta_mobius(const col_rel_t *prev_collection,
                         const col_rel_t *curr_collection,
                         col_rel_t *out_delta);

/* ======================================================================== */
/* Eval Stack & Operators (columnar/ops.c)                                  */
/* ======================================================================== */

void
eval_stack_init(eval_stack_t *s);
int
eval_stack_push(eval_stack_t *s, col_rel_t *r, bool owned);
int
eval_stack_push_delta(eval_stack_t *s, col_rel_t *r, bool owned, bool is_delta);
eval_entry_t
eval_stack_pop(eval_stack_t *s);
void
eval_stack_drain(eval_stack_t *s);
int
col_op_consolidate(eval_stack_t *stack, wl_col_session_t *sess);
int
col_op_consolidate_kway_merge(col_rel_t *rel, const uint32_t *seg_boundaries,
                              uint32_t seg_count);
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
                                     col_rel_t *delta_out);
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst);
int
col_op_join_weighted(const col_rel_t *lhs, const col_rel_t *rhs,
                     uint32_t key_col, col_rel_t *dst);

/* Individual operator functions (columnar/ops.c, called by eval.c) */
int
col_op_variable(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess);
int
col_op_map(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess);
int
col_op_filter(const wl_plan_op_t *op, eval_stack_t *stack,
              wl_col_session_t *sess);
int
col_op_join(const wl_plan_op_t *op, eval_stack_t *stack,
            wl_col_session_t *sess);
int
col_op_antijoin(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess);
int
col_op_concat(eval_stack_t *stack, wl_col_session_t *sess);
int
col_op_semijoin(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess);
int
col_op_reduce(const wl_plan_op_t *op, eval_stack_t *stack,
              wl_col_session_t *sess);
int
col_op_k_fusion(const wl_plan_op_t *op, eval_stack_t *stack,
                wl_col_session_t *sess);
int
col_op_lftj(const wl_plan_op_t *op, eval_stack_t *stack,
            wl_col_session_t *sess);

/* ======================================================================== */
/* Evaluator (columnar/eval.c)                                              */
/* ======================================================================== */

int
col_eval_relation_plan(const wl_plan_relation_t *rplan, eval_stack_t *stack,
                       wl_col_session_t *sess);
int
retraction_rel_name(const char *rel, char *buf, size_t sz);
bool
has_empty_forced_delta(const wl_plan_relation_t *rp, wl_col_session_t *sess,
                       uint32_t iteration);
int
col_eval_stratum(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
                 uint32_t stratum_idx);
int
col_stratum_step_with_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
                            uint32_t stratum_idx);
bool
stratum_has_preseeded_delta(const wl_plan_stratum_t *sp,
                            wl_col_session_t *sess);
uint32_t
rule_index_to_stratum_index(const wl_plan_t *plan, uint32_t rule_id);

#endif /* WL_COLUMNAR_INTERNAL_H */
