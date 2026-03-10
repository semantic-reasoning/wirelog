/*
 * backend/columnar_nanoarrow.h - wirelog Nanoarrow Columnar Backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * ========================================================================
 * Overview
 * ========================================================================
 *
 * The nanoarrow columnar backend stores relations in row-major int64_t
 * buffers and uses Apache Arrow schemas (via nanoarrow) for type metadata.
 * Evaluation uses a stack-based relational algebra interpreter executing
 * the wl_plan_t operator sequence with semi-naive fixed-point
 * iteration for recursive strata.
 *
 * ========================================================================
 * Evaluation Model
 * ========================================================================
 *
 * Plan operators are emitted in post-order (left child first), forming a
 * stack machine:
 *
 *   VARIABLE(rel)   -> push named relation onto eval stack
 *   MAP(indices)    -> pop, project columns, push result
 *   FILTER(expr)    -> pop, apply predicate, push filtered result
 *   JOIN(right,keys)-> pop left, join with named right, push output
 *   ANTIJOIN(...)   -> pop left, remove rows matching right, push
 *   CONCAT          -> pop top two, concatenate, push union
 *   CONSOLIDATE     -> pop, sort+deduplicate, push
 *   REDUCE(agg)     -> pop, group-by + aggregate, push
 *   SEMIJOIN(...)   -> pop left, semijoin with right, push left cols only
 *
 * Column name tracking: each stack entry carries column names for
 * variable→position resolution in JOIN conditions.
 */

#ifndef WL_BACKEND_COLUMNAR_NANOARROW_H
#define WL_BACKEND_COLUMNAR_NANOARROW_H

#include "../backend.h"
#include "../exec_plan.h"

/* ======================================================================== */
/* K-Fusion Metadata                                                        */
/* ======================================================================== */

/**
 * wl_plan_op_k_fusion_t:
 *
 * Backend-specific metadata for a WL_PLAN_OP_K_FUSION operator.
 * Stored in wl_plan_op_t.opaque_data and owned by the plan.
 *
 * A K_FUSION operator encapsulates K independent operator sequences
 * (one per semi-naive delta copy) for parallel workqueue execution.
 * Each sequence in k_ops[d] is annotated with appropriate delta_mode
 * values: position d uses FORCE_DELTA; all other IDB positions use
 * FORCE_FULL.
 *
 * @k:          Number of delta copies (>= 2).
 * @k_ops:      Array of K operator sequence pointers (each owned here).
 * @k_op_counts: Number of operators in each sequence k_ops[d].
 */
typedef struct {
    uint32_t k;
    wl_plan_op_t **k_ops;
    uint32_t *k_op_counts;
} wl_plan_op_k_fusion_t;

/*
 * NOTE: wl_col_session_t and COL_SESSION() are defined in columnar_nanoarrow.c
 * because col_rel_t (a private implementation type) cannot be declared in this
 * header. See columnar_nanoarrow.c for the full memory layout documentation.
 *
 * Summary of the embedding contract:
 *   - wl_col_session_t embeds wl_session_t as its first field (base)
 *   - (wl_col_session_t *)session is safe per C11 §6.7.2.1 ¶15
 *   - session.c:38 sets (*out)->backend after col_session_create returns
 *   - All col_session_* vtable functions cast via COL_SESSION() internally
 *
 * @see backend_dd.c:35-44 for the embedding pattern reference
 * @see session.h:38-40 for canonical wl_session_t definition
 */

/**
 * col_delta_timestamp_t - Per-row provenance record for delta tracking.
 *
 * Attached to rows in delta relations produced by col_eval_stratum().
 * Records when and where each row was first derived during semi-naive
 * fixed-point evaluation.  Used for debugging row lineage and by Phase 3C
 * frontier tracking.
 *
 * Fields:
 *   iteration    Fixed-point iteration (0-based) that first produced this row.
 *   stratum      Stratum index within the evaluation plan (0-based).
 *   worker       K-fusion worker index (0 = sequential / non-parallel path).
 *   _reserved    Must be zero (reserved for future use).
 *   multiplicity Signed weight in Z-set semantics (1 = insert, -1 = retract,
 *                >1 = bulk insert, <-1 = bulk delete).  All newly derived
 *                rows are stamped with multiplicity=1.
 */
typedef struct {
    uint32_t iteration;
    uint32_t stratum;
    uint32_t worker;
    uint32_t _reserved;
    int64_t multiplicity;
} col_delta_timestamp_t;

/* ======================================================================== */
/* Frontier Tracking (Phase 3B)                                             */
/* ======================================================================== */

/**
 * col_frontier_t - Minimum processed (iteration, stratum) boundary.
 *
 * Frontier tracks the lowest (iteration, stratum) pair that has been fully
 * processed. Data with (iter, strat) <= frontier can be skipped in evaluation.
 *
 * Used to:
 *   - Skip redundant recalculation of old iterations
 *   - Clean up old relation snapshots and arrangement entries
 *   - Enable streaming consolidation (only process new deltas)
 *
 * Ordering: (iter1, strat1) <= (iter2, strat2) iff
 *   iter1 < iter2 OR (iter1 == iter2 AND strat1 <= strat2)
 */
typedef struct {
    uint32_t iteration;
    uint32_t stratum;
} col_frontier_t;

/**
 * col_frontier_2d_t - 2D frontier for epoch-aware incremental evaluation.
 *
 * Issue #103: Tracks both insertion epoch and iteration for convergence.
 * Enables proper skip condition across multiple insertion epochs:
 * an iteration is skipped only if it was ALREADY processed in the
 * SAME outer_epoch.
 *
 * @outer_epoch: Insertion epoch at which convergence occurred.
 *               Incremented by col_session_insert_incremental().
 * @iteration:   Iteration within outer_epoch at which stratum converged
 *               (UINT32_MAX if not yet converged in this epoch).
 */
typedef struct {
    uint32_t outer_epoch;
    uint32_t iteration;
} col_frontier_2d_t;

/* ======================================================================== */
/* Arrangement Layer (Phase 3C)                                             */
/* ======================================================================== */

/**
 * col_arrangement_t - Persistent hash index over a subset of columns.
 *
 * Indexes a col_rel_t on `key_count` columns specified by `key_cols[]`.
 * Rows are hashed into `nbuckets` buckets using FNV-1a over the key values.
 *
 * The hash table uses chaining (separate lists per bucket):
 *   ht_head[bucket]  UINT32_MAX = empty; else = first row index in chain
 *   ht_next[row]     UINT32_MAX = end of chain; else = next row in chain
 *
 * `indexed_rows` tracks how many rows from the relation are currently
 * indexed.  When indexed_rows < rel->nrows an incremental update is needed.
 *
 * `content_hash` stores a fingerprint of the relation when last rebuilt
 * (reserved for future staleness detection; currently unused).
 */
typedef struct {
    uint32_t *key_cols;    /* owned, column positions in the relation     */
    uint32_t key_count;    /* number of key columns                       */
    uint32_t nbuckets;     /* hash table size (power of 2)                */
    uint32_t *ht_head;     /* owned, size=nbuckets; UINT32_MAX=empty      */
    uint32_t *ht_next;     /* owned, size=capacity; UINT32_MAX=end        */
    uint32_t ht_cap;       /* allocated size of ht_next[]                 */
    uint32_t indexed_rows; /* rows indexed so far                         */
    uint64_t content_hash; /* fingerprint at last full rebuild (reserved) */
} col_arrangement_t;

/**
 * col_session_get_arrangement:
 *
 * Return (or lazily create) an arrangement for `rel_name` keyed on
 * `key_cols[0..key_count)`.  If the arrangement exists but is stale
 * (rel->nrows > indexed_rows), it is updated incrementally before return.
 *
 * Returns NULL on allocation failure or if the relation is not found.
 *
 * @param sess       A wl_session_t* backed by the columnar backend.
 * @param rel_name   Relation to index.
 * @param key_cols   Column positions to hash on.
 * @param key_count  Number of key columns.
 */
col_arrangement_t *
col_session_get_arrangement(wl_session_t *sess, const char *rel_name,
                            const uint32_t *key_cols, uint32_t key_count);

/**
 * col_arrangement_find_first:
 *
 * Return the first row index in `rel` matching the key columns of `key_row`
 * under `arr`.  Returns UINT32_MAX if no match.
 */
uint32_t
col_arrangement_find_first(const col_arrangement_t *arr,
                           const int64_t *rel_data, uint32_t rel_ncols,
                           const int64_t *key_row);

/**
 * col_arrangement_find_next:
 *
 * Given a row index previously returned by col_arrangement_find_first or
 * col_arrangement_find_next, return the next matching row index in the
 * same chain, or UINT32_MAX if there are no more matches.
 *
 * Caller must verify the returned row actually matches the key (there may
 * be false positives from hash collisions).
 */
uint32_t
col_arrangement_find_next(const col_arrangement_t *arr, uint32_t row_idx);

/**
 * col_session_invalidate_arrangements:
 *
 * Mark all arrangements for `rel_name` as stale (indexed_rows = 0).
 * Called after consolidation modifies relation data.
 * Next call to col_session_get_arrangement will trigger a full rebuild.
 */
void
col_session_invalidate_arrangements(wl_session_t *sess, const char *rel_name);

/**
 * col_session_get_iteration_count:
 *
 * Return the number of fixed-point iterations performed during the last
 * evaluation.  Returns 0 if no evaluation has occurred yet.
 *
 * @param sess  A wl_session_t* backed by the columnar backend.
 */
uint32_t
col_session_get_iteration_count(wl_session_t *sess);

/**
 * col_session_get_perf_stats:
 *
 * Return accumulated profiling counters (nanoseconds) from the last
 * wl_session_snapshot() call.  Counters reset at evaluation start.
 * All out-parameters are NULL-safe.
 *
 * @param sess                    A wl_session_t* backed by the columnar backend.
 * @param out_consolidation_ns    Time in col_op_consolidate_incremental_delta.
 * @param out_kfusion_ns          Total time in col_op_k_fusion.
 * @param out_kfusion_alloc_ns    Phase: calloc of results/workers/worker_sess.
 * @param out_kfusion_dispatch_ns Phase: submit loop + wl_workqueue_wait_all.
 * @param out_kfusion_merge_ns    Phase: col_rel_merge_k + eval_stack_push.
 * @param out_kfusion_cleanup_ns  Phase: result/mat_cache/arr/darr free loops.
 */
void
col_session_get_perf_stats(wl_session_t *sess, uint64_t *out_consolidation_ns,
                           uint64_t *out_kfusion_ns,
                           uint64_t *out_kfusion_alloc_ns,
                           uint64_t *out_kfusion_dispatch_ns,
                           uint64_t *out_kfusion_merge_ns,
                           uint64_t *out_kfusion_cleanup_ns);

/**
 * col_session_get_darr_count:
 *
 * Return the number of delta arrangement cache entries currently held by
 * the session.  On the main session this is expected to be 0 after
 * K-fusion evaluation (delta caches are per-worker and freed on dispatch
 * completion).  Intended for testing the per-worker isolation invariant.
 *
 * @param sess  A wl_session_t* backed by the columnar backend.
 */
uint32_t
col_session_get_darr_count(wl_session_t *sess);

/**
 * col_compute_affected_strata:
 *
 * Identify which strata need re-evaluation after new facts are inserted into
 * `inserted_relation`.  Returns a uint64_t bitmask where bit i is set if
 * stratum i directly or transitively depends on that relation.
 *
 * Uses SIMD (ARM NEON or SSE2) to accelerate bitmask union operations when
 * more than 8 strata are present.  Supports up to 64 strata.
 *
 * @param session           Active session backed by the columnar backend.
 * @param inserted_relation Name of the EDB relation receiving new facts.
 * @return Bitmask of affected stratum indices; 0 on invalid input.
 */
uint64_t
col_compute_affected_strata(wl_session_t *session,
                            const char *inserted_relation);

/**
 * col_session_get_frontier:
 *
 * Copy the per-stratum frontier value for stratum index @stratum_idx into
 * @out_frontier.  Returns 0 on success, EINVAL if @session or @out_frontier
 * is NULL, or if @stratum_idx is out of range.
 *
 * This accessor is provided for testing: production code accesses frontiers
 * via the COL_SESSION() cast which is private to columnar_nanoarrow.c.
 *
 * @param session       Active session backed by the columnar backend.
 * @param stratum_idx   Zero-based stratum index.
 * @param out_frontier  Receives a copy of frontiers[stratum_idx].
 * @return 0 on success, EINVAL on bad arguments or out-of-range index.
 */
int
col_session_get_frontier(wl_session_t *session, uint32_t stratum_idx,
                         col_frontier_2d_t *out_frontier);

/**
 * col_compute_affected_rules:
 *
 * Identify which rules need re-evaluation after new facts are inserted into
 * `inserted_relation`.  Returns a uint64_t bitmask where bit i is set if
 * rule i directly or transitively depends on that relation.
 *
 * Rules are enumerated globally across all strata in order: stratum 0
 * relations first (in relation order), then stratum 1, etc.  Rule index i
 * corresponds to the i-th relation encountered in this traversal.
 *
 * Uses SIMD (ARM NEON or SSE2) to accelerate bitmask union operations when
 * more than 8 rules are present.  Supports up to 64 rules.
 *
 * @param session           Active session backed by the columnar backend.
 * @param inserted_relation Name of the EDB relation receiving new facts.
 * @return Bitmask of affected rule indices; 0 on invalid input.
 */
uint64_t
col_compute_affected_rules(wl_session_t *session,
                           const char *inserted_relation);

/**
 * col_session_insert:
 *
 * Append facts to an EDB relation and enable incremental re-evaluation mode.
 *
 * This is the primary insert function for the columnar backend. It appends
 * facts and sets last_inserted_relation to activate affected-stratum skip
 * optimization in col_session_snapshot, enabling frontier persistence benefits.
 *
 * Phase 4 incremental frontier integration: Setting last_inserted_relation
 * allows affected-stratum detection to skip strata that don't depend on the
 * inserted relation, achieving iteration reduction (6->5 on CSPA, ~15% speedup).
 *
 * Facts are appended to the existing relation; existing rows are kept.
 * Schema is lazily initialised on the first call.
 *
 * @param session   Active session created by col_session_create.
 * @param relation  Name of the EDB relation to append to.
 * @param data      Row-major int64_t array, num_rows * num_cols elements.
 * @param num_rows  Number of rows to append.
 * @param num_cols  Number of columns per row.
 * @return 0 on success, EINVAL on bad arguments or column-count mismatch,
 *         ENOENT if the relation is not registered, ENOMEM on allocation failure.
 */
int
col_session_insert(wl_session_t *session, const char *relation,
                   const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * col_session_insert_incremental:
 *
 * Append facts to an EDB relation WITHOUT resetting per-stratum frontiers.
 *
 * Unlike col_session_insert(), this function preserves the frontiers[] array
 * across calls so that a subsequent col_session_snapshot() call performs
 * incremental re-evaluation: strata whose frontier has already converged past
 * the current iteration are skipped.
 *
 * Facts are appended to the existing relation; existing rows are kept.
 * Schema is lazily initialised on the first call (same rule as col_session_insert).
 * An empty insertion (num_rows == 0) is a safe no-op that returns 0.
 *
 * @param session   Active session created by col_session_create.
 * @param relation  Name of the EDB relation to append to.
 * @param data      Row-major int64_t array, num_rows * num_cols elements.
 * @param num_rows  Number of rows to append (0 is a safe no-op).
 * @param num_cols  Number of columns per row.
 * @return 0 on success, EINVAL on bad arguments or column-count mismatch,
 *         ENOENT if the relation is not registered, ENOMEM on allocation failure.
 */
int
col_session_insert_incremental(wl_session_t *session, const char *relation,
                               const int64_t *data, uint32_t num_rows,
                               uint32_t num_cols);

#endif /* WL_BACKEND_COLUMNAR_NANOARROW_H */
