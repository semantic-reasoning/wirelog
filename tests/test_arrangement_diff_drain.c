/* Real private differential images: physical retirement precedes attribution
* and reservation return. No transaction phase or ownership is fabricated. */
#include "../wirelog/columnar/internal.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #e); abort(); \
                      } } while (0)

static int owner;
static wl_evaluation_control_t *
control_new(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    return c;
}

static void
control_close(wl_evaluation_control_t *c, int result, uint64_t consumed)
{
    CHECK(wl_evaluation_control_finish(c, &owner, result, 0) == result);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report));
    CHECK(report.result == result && report.consumed == consumed);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}

static uint64_t
reserved(wl_col_session_t *s)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(s->memory_governor));
}

static uint64_t
accounted(wl_col_session_t *s)
{
    wl_mem_ledger_snapshot_t view;
    wl_mem_ledger_snapshot(&s->mem_ledger, &view);
    return view.subsys_bytes[WL_MEM_SUBSYS_ARRANGEMENT];
}

static int
turn(wl_columnar_arrangement_diff_attempt_t *tx, wl_col_session_t *s,
    unsigned refusal)
{
    wl_evaluation_control_t *c = control_new(refusal == 1 ? 127 : 128);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    if (refusal == 2) wl_evaluation_control_request_cancel(c);
    int rc = wl_columnar_arrangement_diff_attempt_drain(tx, s, c, id);
    control_close(c, rc, refusal ? 0 : 128);
    return rc;
}

static void
published(col_diff_arr_entry_t *entries, col_diff_arrangement_t *arr,
    wl_col_session_t *s, uint32_t rows, col_relation_snapshot_t snapshot,
    const uint32_t *heads,
    const uint32_t *next)
{
    CHECK(s->diff_arr_entries == entries && s->diff_arr_count == 1
        && entries[0].diff_arr == arr);
    CHECK(arr->indexed_rows == rows
        && arr->current_nrows == rows
        && wl_columnar_relation_snapshot_equal(arr->source_snapshot,
        snapshot));
    CHECK(!memcmp(arr->ht_head, heads, arr->nbuckets*sizeof(*heads))
        && !memcmp(arr->ht_next, next, rows*sizeof(*next)));
}

static void
drain(wl_columnar_arrangement_diff_attempt_t *tx, wl_col_session_t *s,
    col_diff_arr_entry_t *public_entries, col_diff_arrangement_t *public_arr,
    const col_rel_t *source)
{
    (void)source;
    uint32_t rows = public_arr->indexed_rows;
    col_relation_snapshot_t snapshot = public_arr->source_snapshot;
    uint32_t *heads = malloc(public_arr->nbuckets*sizeof(*heads));
    uint32_t *next = malloc(rows*sizeof(*next));
    CHECK(heads && next);
    memcpy(heads, public_arr->ht_head, public_arr->nbuckets*sizeof(*heads));
    memcpy(next, public_arr->ht_next, rows*sizeof(*next));
    bool saw_retire[9] = {false}, saw_tail[7] = {false};
    for (unsigned calls = 0; !tx->drained; ++calls) {
        CHECK(calls < 1000);
        for (unsigned refused = 1; refused <= 2; ++refused) {
            wl_columnar_arrangement_diff_attempt_t image;
            memcpy(&image, tx, sizeof(image));
            uint64_t bytes = accounted(s), credit = reserved(s);
            unsigned count = s->diff_txn_count;
            CHECK(turn(tx, s, refused) == (refused == 1
                ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(!memcmp(&image, tx, sizeof(image)) && accounted(s) == bytes
                && reserved(s) == credit && s->diff_txn_count == count);
            published(public_entries, public_arr, s, rows, snapshot, heads,
                next);
        }
        unsigned stage = tx->retire_phase, cleanup = tx->cleanup_phase;
        bool retiring = tx->building || (tx->cleanup_index <
            (tx->committed ? tx->old_count : tx->count) && cleanup == 1);
        bool tail = !tx->building && tx->cleanup_index ==
            (tx->committed ? tx->old_count : tx->count);
        col_diff_arrangement_t *old = tx->retiring;
        uint64_t ledger = accounted(s), credit = reserved(s);
        uint64_t retire_bytes = tx->retire_bytes;
        uint64_t tail_bytes = tx->committed ? tx->old_bytes
            : tx->array_bytes + tx->metadata_bytes;
        int rc = turn(tx, s, 0);
        CHECK(!rc || rc == EAGAIN);
        published(public_entries, public_arr, s, rows, snapshot, heads, next);
        if (retiring) {
            CHECK(stage < 9); saw_retire[stage] = true;
            if (stage < 5) {
                CHECK(accounted(s) == ledger && reserved(s) == credit);
                if (stage == 1) CHECK(old && !old->ht_head);
                if (stage == 2) CHECK(old && !old->ht_next);
                if (stage == 3) CHECK(old && !old->key_cols);
                if (stage == 4) CHECK(!tx->retiring && tx->retire_phase == 5);
            } else if (stage == 5) {
                CHECK(!tx->retiring && accounted(s) == ledger-retire_bytes
                    && reserved(s) == credit && !tx->retire_ledger);
            } else if (stage == 6) {
                CHECK(!tx->retiring && !tx->retire_ledger
                    && accounted(s) == ledger && reserved(s) <= credit);
            } else {
                CHECK(accounted(s) == ledger && reserved(s) == credit);
                if (stage == 7) CHECK(!tx->retire_governor);
            }
        }
        if (tail) {
            CHECK(cleanup < 7); saw_tail[cleanup] = true;
            if (cleanup == 0) CHECK((tx->committed ? !tx->original
                : !tx->image) && accounted(s) == ledger &&
                    reserved(s) == credit);
            if (cleanup == 1) CHECK(accounted(s) == ledger-tail_bytes
                    && reserved(s) == credit);
            if (cleanup == 2) CHECK(accounted(s) == ledger
                    && reserved(s) <= credit && (tx->committed ? !tx->original
                    : !tx->image));
            if (cleanup == 3) CHECK(!tx->slots && accounted(s) == ledger
                    && reserved(s) == credit);
            if (cleanup == 4) CHECK(!tx->slots
                    && accounted(s) == ledger-tx->slots_bytes
                    && reserved(s) == credit);
            if (cleanup == 5) CHECK(!tx->slots && accounted(s) == ledger
                    && reserved(s) <= credit);
            if (cleanup == 6) CHECK(tx->drained && !s->diff_txn_count
                    && accounted(s) == ledger && reserved(s) == credit);
        }
    }
    for (unsigned i = 0; i < 9; ++i) CHECK(saw_retire[i]);
    for (unsigned i = 0; i < 7; ++i) CHECK(saw_tail[i]);
    free(heads); free(next);
}

/* The index API returns writable private hash storage to its actual caller.
 * Populate it with the same public hash primitive used by the JOIN producer;
 * no lifecycle/credit/ownership field is manufactured. */
static col_diff_arrangement_t *
index_image(wl_columnar_arrangement_diff_attempt_t *tx, wl_col_session_t *s,
    col_rel_t *source, wl_evaluation_control_t *c)
{
    uint32_t key = 0;
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .right_relation = "right",
                       .key_count = 1};
    col_diff_arrangement_t *arr = NULL; bool build = false;
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(!wl_columnar_arrangement_diff_attempt_index(tx, s, &op, 1, source,
        "right", &key, 1, c, id, &arr, &build) && arr && build);
    for (uint32_t row = 0; row < source->nrows; ++row) {
        uint32_t hash = wl_columnar_hash_value(2166136261u, source, 0,
                source->columns[0][row]) & (arr->nbuckets-1);
        arr->ht_next[row] = arr->ht_head[hash]; arr->ht_head[hash] = row+1;
    }
    arr->indexed_rows = arr->current_nrows = source->nrows;
    arr->source_snapshot = wl_columnar_relation_snapshot(source);
    return arr;
}

static void
prepare(wl_columnar_arrangement_diff_attempt_t *tx, wl_col_session_t *s,
    wl_evaluation_control_t *c)
{
    for (unsigned n = 0; ; ++n) {
        CHECK(n < 100);
        int rc = wl_columnar_arrangement_diff_attempt_prepare(tx, s, c,
                wl_evaluation_control_active_attempt_id(c));
        CHECK(!rc || rc == EAGAIN);
        if (!rc) break;
    }
}

static void
test(bool commit)
{
    wl_col_session_t *s = calloc(1, sizeof(*s)); CHECK(s);
    wl_mem_ledger_init(&s->mem_ledger, 0);
    wl_columnar_memory_reservation_init(&s->diff_arr_registry_reservation);
    wl_columnar_memory_resolution_t resolution = {.usable_bytes = UINT64_MAX,
                                                  .budget_bytes = UINT64_MAX,
                                                  .mode =
                                                      WL_COLUMNAR_MEMORY_MODE_ENFORCING};
    s->memory_governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(s->memory_governor);
    col_rel_t *source = col_rel_new_auto("right", 1); CHECK(source);
    const int64_t first[] = {11}, second[] = {22};
    CHECK(!col_rel_append_row(source, first));
    wl_evaluation_control_t *c = control_new(UINT64_C(1000000000));
    wl_columnar_arrangement_diff_attempt_t initial = {0};
    col_diff_arrangement_t *original = index_image(&initial, s, source, c);
    prepare(&initial, s, c);
    wl_columnar_arrangement_diff_attempt_commit(&initial, s);
    /* Initial publication has no old arrangement; exercise its normal drain
     * without requiring the replacement-only retire-stage witness. */
    while (!initial.drained) {
        int rc = turn(&initial, s, 0); CHECK(!rc || rc == EAGAIN);
    }
    uint64_t baseline_credit = reserved(s), baseline_ledger = accounted(s);
    col_diff_arr_entry_t *original_entries = s->diff_arr_entries;
    CHECK(!col_rel_append_row(source, second));
    wl_columnar_arrangement_diff_attempt_t tx = {0};
    col_diff_arrangement_t *replacement = index_image(&tx, s, source, c);
    CHECK(replacement != original);
    if (commit) {
        prepare(&tx, s, c);
        wl_columnar_arrangement_diff_attempt_commit(&tx, s);
    }
    /* Abort preserves the old published immutable index even though its source
     * snapshot is stale; commit preserves the replacement two-row image. */
    drain(&tx, s, commit ? s->diff_arr_entries : original_entries,
        commit ? replacement : original, source);
    CHECK(!s->diff_txn_count && !tx.retire_governor);
    if (!commit) CHECK(reserved(s) == baseline_credit
            && accounted(s) == baseline_ledger);
    uint64_t used;
    CHECK(!wl_evaluation_control_test_active_consumed(c,
        wl_evaluation_control_active_attempt_id(c), &used));
    control_close(c, 0, used);
    col_session_free_diff_arrangements(s);
    CHECK(!reserved(s) && !accounted(s));
    col_rel_destroy(source);
    wl_columnar_memory_governor_ref_release(s->memory_governor); free(s);
}

int main(void)
{
    test(false); test(true);
    puts("differential drain: physical-before-ledger-before-credit passed");
    return 0;
}
