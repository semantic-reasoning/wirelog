/*
 * Direct generation-token regression coverage for cache consumers (#1438).
 */

#include "wirelog/columnar/columnar_nanoarrow.h"
#include "wirelog/columnar/internal.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/passes/fusion.h"
#include "wirelog/passes/jpp.h"
#include "wirelog/passes/sip.h"
#include "wirelog/session.h"
#include "wirelog/session_facts.h"
#include "wirelog/wirelog.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

#define CHECK(condition, message) \
        do { \
            if (!(condition)) { \
                fprintf(stderr, "FAIL: %s\n", (message)); \
                failures++; \
                goto cleanup; \
            } \
        } while (0)

static void
noop_cb(const char *relation, const int64_t *row, uint32_t ncols, void *data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)data;
}

static col_rel_t *
find_relation(wl_session_t *session, const char *name)
{
    wl_col_session_t *cs = COL_SESSION(session);
    for (uint32_t i = 0; i < cs->nrels; i++) {
        if (cs->rels[i] && cs->rels[i]->name
            && strcmp(cs->rels[i]->name, name) == 0)
            return cs->rels[i];
    }
    return NULL;
}

static int
make_session(wl_session_t **session_out, wl_plan_t **plan_out,
    wirelog_program_t **program_out)
{
    /* Keep the mutation at row 100 so a prefix-only content check cannot
     * accidentally validate the cache. */
    size_t cap = 4096;
    char *source = (char *)malloc(cap);
    if (!source)
        return -1;
    size_t used = (size_t)snprintf(source, cap,
            ".decl edge(a: int32, b: int32)\n");
    for (int i = 0; i <= 100; i++) {
        int n = snprintf(source + used, cap - used, "edge(%d, %d).\n", i, i);
        if (n < 0 || (size_t)n >= cap - used) {
            free(source);
            return -1;
        }
        used += (size_t)n;
    }

    wirelog_error_t error = WIRELOG_OK;
    wirelog_program_t *program = wirelog_parse_string(source, &error);
    free(source);
    if (!program)
        return -1;
    wl_fusion_apply(program, NULL);
    wl_jpp_apply(program, NULL);
    wl_sip_apply(program, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(program, &plan) != 0) {
        wirelog_program_free(program);
        return -1;
    }
    wl_session_t *session = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &session) != 0
        || wl_session_load_facts(session, program) != 0
        || wl_session_snapshot(session, noop_cb, NULL) != 0) {
        if (session)
            wl_session_destroy(session);
        wl_plan_free(plan);
        wirelog_program_free(program);
        return -1;
    }
    *session_out = session;
    *plan_out = plan;
    *program_out = program;
    return 0;
}

static void
destroy_session(wl_session_t *session, wl_plan_t *plan,
    wirelog_program_t *program)
{
    wl_session_destroy(session);
    wl_plan_free(plan);
    wirelog_program_free(program);
}

static wl_plan_expr_buffer_t
make_b_gt_100(void)
{
    /* VAR("col1") CONST_INT(100) CMP_GT. */
    static const uint8_t expression[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
        WL_PLAN_EXPR_CONST_INT,
        100, 0, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_GT
    };
    wl_plan_expr_buffer_t result;
    result.data = (uint8_t *)malloc(sizeof(expression));
    result.size = sizeof(expression);
    if (result.data)
        memcpy(result.data, expression, sizeof(expression));
    return result;
}

static int
index_diff_arrangement(col_diff_arrangement_t *arr, const col_rel_t *rel,
    const uint32_t *key_cols, uint32_t key_count)
{
    if (col_diff_arrangement_ensure_ht_capacity(arr, rel->nrows) != 0)
        return -1;
    for (uint32_t row = 0; row < rel->nrows; row++) {
        uint32_t hash = 2166136261u;
        for (uint32_t key = 0; key < key_count; key++)
            hash = wl_columnar_hash_value(hash, rel, key_cols[key],
                    rel->columns[key_cols[key]][row]);
        uint32_t bucket = hash & (arr->nbuckets - 1);
        arr->ht_next[row] = arr->ht_head[bucket];
        arr->ht_head[bucket] = row + 1;
    }
    arr->indexed_rows = rel->nrows;
    arr->current_nrows = rel->nrows;
    return 0;
}

static int
diff_contains_key(const col_diff_arrangement_t *arr, const col_rel_t *rel,
    uint32_t key_col, int64_t value)
{
    uint32_t hash = wl_columnar_hash_value(2166136261u, rel, key_col,
            value);
    uint32_t cursor = arr->ht_head[hash & (arr->nbuckets - 1)];
    while (cursor != 0) {
        uint32_t row = cursor - 1;
        if (row < rel->nrows && rel->columns[key_col][row] == value)
            return 1;
        cursor = arr->ht_next[row];
    }
    return 0;
}

static void
free_arrangement_clones(col_arr_entry_t *entries, uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        arr_free_contents(&entries[i].arr);
        free(entries[i].key_cols);
        free(entries[i].rel_name);
    }
    free(entries);
}

static void
free_diff_clones(col_diff_arr_entry_t *entries, uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        col_diff_arrangement_destroy(entries[i].diff_arr);
        free(entries[i].key_cols);
        free(entries[i].rel_name);
    }
    free(entries);
}

static int
test_generation_consumers(void)
{
    wl_session_t *session = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *program = NULL;
    col_rel_t *rel = NULL;
    wl_plan_expr_buffer_t filter = { 0 };
    col_arr_entry_t *arr_clone = NULL;
    uint32_t arr_clone_cap = 0;
    col_diff_arr_entry_t *diff_clone = NULL;
    uint32_t diff_clone_cap = 0;
    col_rel_t *worker_rel = NULL;
    wl_mem_ledger_t worker_ledger;
    wl_mem_ledger_init(&worker_ledger, 0);
    wl_columnar_arrangement_diff_txn_t worker_txn = { 0 };
    int result = 0;
    CHECK(make_session(&session, &plan, &program) == 0,
        "session creation failed");
    rel = find_relation(session, "edge");
    CHECK(rel != NULL && rel->nrows == 101, "source relation missing");

    wl_col_session_t *cs = COL_SESSION(session);
    filter = make_b_gt_100();
    CHECK(filter.data != NULL, "filter allocation failed");
    uint64_t filter_hash = wl_columnar_filter_fnv1a_hash(filter.data,
            filter.size);
    uint32_t key_col = 0;

    col_rel_t *filtered = wl_columnar_filter_apply_right_filter_cached(
        cs, &filter, "edge", rel);
    CHECK(filtered != NULL && filtered->nrows == 0,
        "initial filtered cache result must be empty");

    /* Mutation is at row 100, outside the first 100 rows, and nrows stays 101. */
    CHECK(col_rel_set(rel, 100, 1, 101) == 0,
        "same-nrows source mutation failed");
    filtered = wl_columnar_filter_apply_right_filter_cached(
        cs, &filter, "edge", rel);
    CHECK(filtered != NULL && filtered->nrows == 1,
        "filtered cache did not rebuild after same-nrows mutation");
    CHECK(filtered->columns[0][0] == 100 && filtered->columns[1][0] == 101,
        "filtered cache did not expose the new row");

    col_arrangement_t *filtered_arr = col_session_get_filt_arrangement(cs,
            "edge", filter_hash, filtered, &key_col, 1);
    CHECK(filtered_arr != NULL, "filtered arrangement creation failed");
    int64_t key = 100;
    CHECK(col_arrangement_find_first_typed(filtered_arr, filtered, &key)
        != UINT32_MAX,
        "filtered arrangement missed initial filtered key");

    CHECK(col_rel_set(rel, 100, 0, 202) == 0,
        "second same-nrows source mutation failed");
    filtered = wl_columnar_filter_apply_right_filter_cached(
        cs, &filter, "edge", rel);
    CHECK(filtered != NULL && filtered->nrows == 1
        && filtered->columns[0][0] == 202,
        "filtered cache did not regenerate after key mutation");
    filtered_arr = col_session_get_filt_arrangement(cs, "edge", filter_hash,
            filtered, &key_col, 1);
    CHECK(filtered_arr != NULL, "filtered arrangement rebuild failed");
    key = 202;
    CHECK(col_arrangement_find_first_typed(filtered_arr, filtered, &key)
        != UINT32_MAX,
        "filtered arrangement missed new key");
    key = 100;
    CHECK(col_arrangement_find_first_typed(filtered_arr, filtered, &key)
        == UINT32_MAX,
        "filtered arrangement retained stale key");

    col_sorted_arr_t *sorted = col_session_get_sorted_arrangement(cs, "edge",
            1);
    CHECK(sorted != NULL && sorted->nrows == 101,
        "sorted arrangement creation failed");
    CHECK(col_rel_set(rel, 0, 1, 1000) == 0,
        "sorted same-nrows mutation failed");
    sorted = col_session_get_sorted_arrangement(cs, "edge", 1);
    CHECK(sorted != NULL && sorted->sorted[1] == 1
        && sorted->sorted[(size_t)(sorted->nrows - 1) * sorted->ncols + 1]
        == 1000,
        "sorted arrangement did not rebuild order");

    col_diff_arrangement_t *diff = col_session_get_diff_arrangement(cs,
            "edge", rel, &key_col, 1);
    CHECK(diff != NULL, "differential arrangement creation failed");
    CHECK(index_diff_arrangement(diff, rel, &key_col, 1) == 0,
        "differential arrangement initial index failed");
    CHECK(diff->indexed_rows == rel->nrows,
        "initial differential index incomplete");
    CHECK(col_rel_set(rel, 100, 0, 303) == 0,
        "differential same-nrows mutation failed");
    diff = col_session_get_diff_arrangement(cs, "edge", rel, &key_col, 1);
    CHECK(diff != NULL && diff->indexed_rows == 0,
        "differential arrangement did not discard stale index");
    CHECK(index_diff_arrangement(diff, rel, &key_col, 1) == 0,
        "differential arrangement rebuild failed");
    CHECK(diff_contains_key(diff, rel, 0, 303),
        "differential arrangement missed new probe key");
    CHECK(!diff_contains_key(diff, rel, 0, 202),
        "differential arrangement retained stale bucket");

    col_arrangement_t *primary = col_session_get_arrangement(session, "edge",
            &key_col, 1);
    CHECK(primary != NULL && primary->indexed_rows == rel->nrows,
        "primary arrangement creation failed");
    CHECK(col_arr_entries_clone(cs->arr_entries, cs->arr_count,
        &arr_clone, &arr_clone_cap, NULL) == 0 && arr_clone_cap > 0,
        "primary arrangement worker clone failed");
    CHECK(arr_clone[0].source_snapshot.relation_identity == 0
        && arr_clone[0].arr.indexed_rows == 0
        && arr_clone[0].arr.ht_head == NULL
        && arr_clone[0].arr.ht_next == NULL
        && arr_clone[0].mem_bytes == 0
        && arr_clone[0].arr.reserved_bytes == 0,
        "worker clone reused coordinator primary snapshot");

    CHECK(col_diff_arr_entries_clone(cs->diff_arr_entries, cs->diff_arr_count,
        &diff_clone, &diff_clone_cap) == 0 && diff_clone_cap > 0,
        "differential arrangement worker clone failed");
    CHECK(diff_clone[0].diff_arr->source_snapshot.relation_identity == 0
        && diff_clone[0].diff_arr->indexed_rows == 0
        && diff_clone[0].diff_arr->ht_head == NULL
        && diff_clone[0].diff_arr->ht_next == NULL
        && diff_clone[0].diff_arr->ht_cap == 0
        && diff_clone[0].diff_arr->nbuckets == 0
        && diff_clone[0].diff_arr->ledger == NULL,
        "worker clone reused coordinator differential snapshot");
    CHECK(col_rel_deep_copy(rel, &worker_rel, NULL) == 0,
        "worker relation copy failed");
    CHECK(col_rel_set(worker_rel, 0, key_col, 123456) == 0,
        "worker relation mutation failed");
    wl_col_session_t worker = { 0 };
    worker.diff_arr_entries = diff_clone;
    worker.diff_arr_count = cs->diff_arr_count;
    worker.diff_arr_cap = diff_clone_cap;
    col_diff_arrangement_attach_ledger(diff_clone[0].diff_arr, &worker_ledger);
    CHECK(atomic_load_explicit(&worker_ledger.current_bytes,
        memory_order_relaxed)
        == sizeof(col_diff_arrangement_t) + sizeof(uint32_t),
        "cold differential clone charged copied hash storage");
    col_diff_arrangement_t *worker_diff = col_session_get_diff_arrangement(
        &worker, "edge", worker_rel, &key_col, 1);
    CHECK(worker_diff && wl_columnar_arrangement_diff_txn_begin(&worker,
        "edge", worker_rel, &key_col, 1, &worker_txn) == 0,
        "cold differential transaction failed");
    worker_diff = worker_txn.working;
    CHECK(index_diff_arrangement(worker_diff, worker_rel,
        &key_col, 1) == 0, "cold differential first access failed");
    worker_diff->source_snapshot = wl_columnar_relation_snapshot(worker_rel);
    wl_columnar_arrangement_diff_txn_commit(&worker_txn);
    CHECK(worker_diff->ledger == &worker_ledger
        && atomic_load_explicit(&worker_ledger.current_bytes,
        memory_order_relaxed)
        == col_diff_arrangement_bytes(worker_diff),
        "private differential rebuild ledger mismatch");
    CHECK(worker_diff->ht_head != diff->ht_head
        && worker_diff->ht_next != diff->ht_next
        && diff_contains_key(worker_diff, worker_rel, key_col, 123456)
        && !diff_contains_key(diff, rel, key_col, 123456)
        && diff->indexed_rows == rel->nrows,
        "differential worker rebuild changed coordinator");
    arr_clone[0].arr.indexed_rows = 7;
    diff_clone[0].diff_arr->indexed_rows = 7;
    CHECK(cs->arr_entries[0].arr.indexed_rows != 7
        && cs->diff_arr_entries[0].diff_arr->indexed_rows != 7,
        "worker cache mutation leaked into coordinator");

    result = 1;

cleanup:
    wl_columnar_arrangement_diff_txn_abort(&worker_txn);
    if (worker_rel)
        col_rel_destroy(worker_rel);
    free_arrangement_clones(arr_clone, arr_clone_cap);
    free_diff_clones(diff_clone, diff_clone_cap);
    if (atomic_load_explicit(&worker_ledger.current_bytes,
        memory_order_relaxed) != 0) {
        fprintf(stderr,
            "FAIL: differential clone cleanup leaked ledger bytes\n");
        failures++;
        result = 0;
    }
    free(filter.data);
    if (session)
        destroy_session(session, plan, program);
    return result;
}

int
main(void)
{
    printf("generation cache consumer regressions\n");
    (void)test_generation_consumers();
    return failures == 0 ? 0 : 1;
}
