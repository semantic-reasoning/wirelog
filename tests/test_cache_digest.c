/* #2297: pin-free exact cache recipes under one original allowance. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,#x); \
                          abort(); \
                      } } while (0)

static wl_evaluation_control_t *
control_new(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, owner));
    CHECK(!wl_evaluation_control_begin(c, owner));
    return c;
}

static void
control_close(wl_evaluation_control_t *c, const void *owner, int semantic)
{
    if (wl_evaluation_control_active_attempt_id(c))
        CHECK(wl_evaluation_control_finish(c, owner, semantic, 0) == semantic);
    CHECK(!wl_evaluation_control_detach(c, owner));
    wl_evaluation_control_release(c);
}

static col_rel_t *
relation_new(uint32_t width, uint32_t rows)
{
    col_rel_t *r = col_rel_new_auto("digest-source", width);
    CHECK(r);
    int64_t *row = malloc((width ? width : 1u)*sizeof(*row)); CHECK(row);
    for (uint32_t i = 0; i < rows; ++i) {
        for (uint32_t c = 0; c < width; ++c)
            row[c] = (int64_t)((uint64_t)(i+1)*UINT64_C(0x0102030405060708)+c);
        CHECK(!col_rel_append_row(r, row));
    }
    free(row); return r;
}

static uint64_t
literal_content(const col_rel_t *r)
{
    if (!r || !r->nrows) return 0;
    uint64_t h = UINT64_C(14695981039346656037);
    for (uint32_t row = 0; row < r->nrows && row < 100; ++row)
        for (uint32_t col = 0; col < r->ncols; ++col)
            h = (h ^ (uint64_t)r->columns[col][row])*UINT64_C(1099511628211);
    h = (h ^ r->nrows)*UINT64_C(1099511628211);
    return (h ^ r->ncols)*UINT64_C(1099511628211);
}

static uint64_t
literal_bytes(uint64_t h, const void *data, size_t size)
{
    const unsigned char *bytes = data;
    for (size_t i = 0; i < size; ++i)
        h = (h ^ bytes[i])*UINT64_C(1099511628211);
    return h;
}

static uint64_t
literal_signature(const wl_plan_op_t *op, bool diff)
{
    if (!op) return 1;
    uint64_t h = UINT64_C(14695981039346656037);
    uint32_t kind = (uint32_t)op->op;
    unsigned char path = diff ? 1 : 0, separator = 0xff;
    h = literal_bytes(h, &kind, sizeof(kind));
    h = literal_bytes(h, &path, sizeof(path));
    h = literal_bytes(h, &op->key_count, sizeof(op->key_count));
    for (uint32_t k = 0; k < op->key_count; ++k) {
        const char *keys[] = {op->left_keys ? op->left_keys[k] : NULL,
                              op->right_keys ? op->right_keys[k] : NULL};
        for (unsigned side = 0; side < 2; ++side) {
            if (keys[side]) h = literal_bytes(h, keys[side],
                        strlen(keys[side]));
            h = literal_bytes(h, &separator, 1);
        }
    }
    h = literal_bytes(h, &op->right_filter_expr.size,
            sizeof(op->right_filter_expr.size));
    if (op->right_filter_expr.data)
        h = literal_bytes(h, op->right_filter_expr.data,
                op->right_filter_expr.size);
    return h ? h : 1;
}

static void
until_ready(wl_columnar_cache_digest_attempt_t *a)
{
    bool ready = false;
    for (unsigned calls = 0; !ready; ++calls) {
        CHECK(calls < 1000000);
        CHECK(!wl_columnar_cache_digest_step(a, &ready));
    }
    CHECK(a->phase == WL_COLUMNAR_CACHE_DIGEST_READY);
}

/* Every call gets a real fresh grant; the leaf retains no funding reference. */
static int
end_turn(wl_columnar_cache_digest_attempt_t *a, uint64_t limit,
    bool aborting, int initiating, bool *done, uint64_t *consumed)
{
    int owner = 0;
    wl_evaluation_control_t *fund = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(fund);
    int rc = aborting ? wl_columnar_cache_digest_abort(a, initiating, fund, id,
            done)
        : wl_columnar_cache_digest_finish(a, fund, id, done);
    CHECK(wl_evaluation_control_finish(fund, &owner, rc, 0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(fund, &report));
    if (consumed) *consumed = report.consumed;
    control_close(fund, &owner, 0); return rc;
}

static void
drain(wl_columnar_cache_digest_attempt_t *a)
{
    bool done = false;
    for (unsigned calls = 0; !done; ++calls) {
        CHECK(calls < 16);
        uint64_t consumed = 0;
        CHECK(!end_turn(a, 128, false, 0, &done, &consumed) && consumed == 128);
    }
}

static int
ack_turn(wl_columnar_cache_digest_attempt_t *a, uint64_t limit,
    uint64_t *consumed)
{
    int owner = 0;
    wl_evaluation_control_t *fund = control_new(limit, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(fund);
    int rc = wl_columnar_cache_digest_ack(a, fund, id);
    CHECK(wl_evaluation_control_finish(fund, &owner, rc, 0) == rc);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(fund, &report));
    if (consumed) *consumed = report.consumed;
    control_close(fund, &owner, 0); return rc;
}

static wl_columnar_cache_digest_result_t
digest(const col_rel_t *left, const col_rel_t *right,
    const wl_plan_op_t *op, bool diff)
{
    wl_columnar_cache_digest_attempt_t a = {0};
    int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(!wl_columnar_cache_digest_begin(&a, left, right, op, diff, c, id));
    until_ready(&a); drain(&a);
    wl_columnar_cache_digest_result_t result; int semantic = -1;
    CHECK(!wl_columnar_cache_digest_query(&a, &result, &semantic) && !semantic);
    CHECK(result.left_hash == col_mat_cache_key_content(left)
        && result.right_hash == col_mat_cache_key_content(right)
        && result.signature == col_mat_cache_join_signature(op, diff));
    CHECK(result.left_hash == literal_content(left)
        && result.right_hash == literal_content(right)
        && result.signature == literal_signature(op, diff));
    CHECK(result.control_identity == wl_evaluation_control_identity(c) &&
        result.attempt_id == id);
    CHECK(!ack_turn(&a, 128, NULL));
    CHECK(!a.identity && !a.control && !a.control_ref);
    control_close(c, &owner, 0); return result;
}

static void
test_domains(void)
{
    wl_columnar_cache_digest_result_t empty = digest(NULL, NULL, NULL, false);
    CHECK(!empty.left_hash && !empty.right_hash && empty.signature == 1);
    const uint32_t widths[] = {0, 1, 65, 257},
        counts[] = {0, 1, 99, 100, 101, 200};
    const char *left_keys[] = {"col0", NULL, ""};
    const char *right_keys[] = {"other", "", "col0"};
    uint8_t filter[] = {0, 0xff, 1, 128, 42};
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .key_count = 3,
                       .left_keys = left_keys, .right_keys = right_keys,
                       .right_filter_expr = {filter, sizeof(filter)}};
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned n = 0; n < 6; ++n) {
            col_rel_t *r = relation_new(widths[w], counts[n]);
            wl_columnar_cache_digest_result_t result = digest(r, r, &op, n&1u);
            CHECK(result.left_hash == result.right_hash);
            if (widths[w] == 1 && counts[n] == 1)
                CHECK(result.left_hash == UINT64_C(0xba07dd4577a0ee99));
            CHECK(wl_columnar_relation_snapshot_equal(result.left_snapshot,
                wl_columnar_relation_snapshot(r)));
            CHECK(!col_rel_destroy_checked(r));
        }
    col_rel_t *bare = NULL; CHECK(!col_rel_alloc(&bare, "bare"));
    CHECK(!bare->schema_ok && !bare->ncols && !bare->nrows);
    bare->has_graph_column = true; bare->graph_col_idx = 7;
    bare->declared_ncols = 9;
    wl_columnar_relation_touch_view(bare);
    CHECK(!digest(bare, NULL, &op, true).left_hash);
    CHECK(!col_rel_destroy_checked(bare));

    col_rel_t *r = relation_new(1, 101), *alias = relation_new(1, 0);
    CHECK(!col_rel_install_shared_view(alias, r));
    CHECK(alias->storage_owner == r && alias != r);
    wl_columnar_cache_digest_result_t aliased = digest(alias, r, &op, false);
    CHECK(aliased.left_hash == aliased.right_hash
        && aliased.left_snapshot.relation_identity !=
        aliased.right_snapshot.relation_identity);
    col_rel_destroy(alias);
    wl_columnar_cache_digest_result_t before = digest(r, NULL, &op, false);
    CHECK(!col_rel_set(r, 100, 0, 123));
    wl_columnar_cache_digest_result_t after = digest(r, NULL, &op, false);
    CHECK(before.left_hash == after.left_hash
        && !wl_columnar_relation_snapshot_equal(before.left_snapshot,
        after.left_snapshot));
    col_rel_destroy(r);

    r = relation_new(1, 2);
    uint64_t plain = col_mat_cache_key_content(r);
    CHECK(!col_rel_enable_timestamps(r));
    r->has_graph_column = true; r->graph_col_idx = 0;
    wl_columnar_relation_touch_view(r);
    CHECK(digest(r, r, &op, false).left_hash == plain);
    col_rel_destroy(r);

    char *long_key = malloc(8193); CHECK(long_key);
    memset(long_key, 'z', 8192); long_key[8192] = 0;
    const char *long_keys[] = {long_key};
    uint8_t *long_filter = malloc(10003); CHECK(long_filter);
    for (unsigned i = 0; i < 10003; ++i)long_filter[i] = (uint8_t)i;
    op.key_count = 1; op.left_keys = long_keys; op.right_keys = NULL;
    op.right_filter_expr = (wl_plan_expr_buffer_t){long_filter, 10003};
    digest(NULL, NULL, &op, true);
    op.right_filter_expr.data = NULL; digest(NULL, NULL, &op, true);
    op.left_keys = NULL; digest(NULL, NULL, &op, false);
    free(long_filter); free(long_key);

    const wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT,
                                           WIRELOG_TYPE_STRING};
    for (unsigned i = 0; i < 2; ++i) {
        r = relation_new(1, 0);
        CHECK(!col_rel_set_column_types(r, &types[i], 1));
        const int64_t values[] = {
            i ? 11 : wl_columnar_float_to_bits(-3.5),
            i ? 12 : wl_columnar_float_to_bits(1.25), i ? 11 : 0
        };
        for (unsigned row = 0; row < 3;
            ++row) CHECK(!col_rel_append_row(r, &values[row]));
        digest(r, NULL, NULL, false);
        col_rel_destroy(r);
    }
}

static void
test_begin_and_terminal_funds(void)
{
    col_rel_t *r = relation_new(1, 2);
    for (unsigned allowance = 255; allowance <= 256; ++allowance) {
        wl_columnar_cache_digest_attempt_t a = {0};
        int owner = 0;
        wl_evaluation_control_t *c = control_new(allowance, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        int rc = wl_columnar_cache_digest_begin(&a, r, NULL, NULL, false, c,
                id);
        CHECK(rc == (allowance == 255?WL_EVALUATION_CONTROL_WORK_LIMIT:0));
        if (allowance == 255)CHECK(!a.identity && !a.control_ref &&
                !a.control &&
                !a.busy);
        else {
            bool done = false;
            CHECK(!end_turn(&a, 128, true, ECANCELED, &done, NULL));
            drain(&a); int semantic = 0;
            CHECK(!wl_columnar_cache_digest_query(&a, NULL,
                &semantic) && semantic == ECANCELED);
            CHECK(!ack_turn(&a, 128, NULL));
        }
        wl_evaluation_control_report_t report;
        int semantic = allowance ==
            255 ? WL_EVALUATION_CONTROL_WORK_LIMIT : ECANCELED;
        CHECK(wl_evaluation_control_finish(c, &owner, semantic, 0) == semantic);
        CHECK(!wl_evaluation_control_report(c,
            &report) && report.consumed == (allowance == 255?0u:256u));
        control_close(c, &owner, 0);
    }

    wl_columnar_cache_digest_attempt_t a = {0}; int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(!wl_columnar_cache_digest_begin(&a, r, r, NULL, false, c, id));
    until_ready(&a);
    wl_columnar_cache_digest_attempt_t saved = a; bool done = false;
    uint64_t work = 1;
    CHECK(end_turn(&a, 127, false, 0, &done,
        &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work);
    CHECK(!memcmp(&saved, &a, sizeof(a)));
    CHECK(wl_columnar_cache_digest_finish(&a, c, id, &done) == EINVAL);
    CHECK(!memcmp(&saved, &a, sizeof(a)));
    drain(&a);
    wl_columnar_cache_digest_result_t result; int semantic = -1;
    CHECK(!wl_columnar_cache_digest_query(&a, &result, &semantic) && !semantic);
    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(c, &before));
    CHECK(!wl_evaluation_control_reset(c));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    CHECK(wl_evaluation_control_active_attempt_id(c) != id);
    saved = a;
    CHECK(ack_turn(&a, 127,
        &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work);
    CHECK(!memcmp(&saved, &a, sizeof(a)));
    CHECK(!ack_turn(&a, 128, &work) && work == 128);
    CHECK(ack_turn(&a, 128, &work) == EINVAL && !work);
    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    CHECK(!wl_evaluation_control_report(c,
        &after) && after.attempt_id != before.attempt_id && !after.consumed);
    CHECK(!col_rel_append_row(r, (int64_t[]){7}));
    CHECK(!wl_evaluation_control_reset(c) && !wl_evaluation_control_begin(c,
        &owner));
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN};
    CHECK(!wl_columnar_cache_digest_begin(&a, r, NULL, &op, true, c,
        wl_evaluation_control_active_attempt_id(c)));
    until_ready(&a); drain(&a);
    wl_columnar_cache_digest_result_t reused;
    CHECK(!wl_columnar_cache_digest_query(&a, &reused, &semantic) && !semantic
        && reused.attempt_id != result.attempt_id
        && reused.left_hash == col_mat_cache_key_content(r)
        && reused.signature == col_mat_cache_join_signature(&op, true));
    CHECK(!ack_turn(&a, 128, NULL));
    control_close(c, &owner, 0); col_rel_destroy(r);
}

static void
test_partial_stops(void)
{
    const wl_columnar_cache_digest_phase_t phases[] = {
        WL_COLUMNAR_CACHE_DIGEST_ACQUIRE, WL_COLUMNAR_CACHE_DIGEST_VALIDATE,
        WL_COLUMNAR_CACHE_DIGEST_CELL, WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_FIELD,
        WL_COLUMNAR_CACHE_DIGEST_SIGNATURE_BYTE,
        WL_COLUMNAR_CACHE_DIGEST_SEPARATOR,
        WL_COLUMNAR_CACHE_DIGEST_KEY, WL_COLUMNAR_CACHE_DIGEST_FILTER,
        WL_COLUMNAR_CACHE_DIGEST_FINAL_LEFT,
        WL_COLUMNAR_CACHE_DIGEST_FINAL_RIGHT,
        WL_COLUMNAR_CACHE_DIGEST_LATCH
    };
    const char *keys[] = {"longer-key"};
    uint8_t bytes[] = {1, 2, 3};
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .key_count = 1, .left_keys = keys,
                       .right_keys = keys,
                       .right_filter_expr = {bytes, sizeof(bytes)}};
    for (unsigned p = 0; p < sizeof(phases)/sizeof(phases[0]); ++p) {
        col_rel_t *r = relation_new(65, 2); int owner = 0;
        wl_evaluation_control_t *c = control_new(10000000, &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_columnar_cache_digest_attempt_t a = {0};
        CHECK(!wl_columnar_cache_digest_begin(&a, r, r, &op, false, c, id));
        for (unsigned turn = 0;; ++turn) {
            CHECK(turn < 10000);
            if (a.phase == phases[p] &&
                (p || a.source[0].reader.acquire_stage == 1)) break;
            bool ready = false;
            CHECK(!wl_columnar_cache_digest_step(&a, &ready) && !ready);
        }
        if (!p)CHECK(a.source[0].reader.owner == &r->descriptor_access &&
                !a.source[0].held);
        else CHECK(a.source[0].held);
        col_relation_snapshot_t image = wl_columnar_relation_snapshot(r);
        CHECK(col_rel_destroy_checked(r) == EBUSY);
        wl_evaluation_control_request_cancel(c);
        bool ready = false;
        CHECK(wl_columnar_cache_digest_step(&a,
            &ready) == WL_EVALUATION_CONTROL_CANCELLED && !ready);
        CHECK(wl_evaluation_control_finish(c, &owner,
            WL_EVALUATION_CONTROL_CANCELLED, 0)
            == WL_EVALUATION_CONTROL_CANCELLED);
        wl_evaluation_control_report_t before, after;
        CHECK(!wl_evaluation_control_report(c, &before));
        bool done = false; uint64_t work = 1;
        wl_columnar_cache_digest_attempt_t saved = a;
        CHECK(end_turn(&a, 127, false, 0, &done,
            &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work);
        CHECK(!memcmp(&saved, &a, sizeof(a)));
        for (unsigned turn = 0; !done; ++turn) {
            CHECK(turn < 16);
            CHECK(!end_turn(&a, 128, false, 0, &done, &work) && work == 128);
        }
        wl_columnar_cache_digest_result_t result, bits;
        memset(&result, 0xa5, sizeof(result)); bits = result; int semantic = 0;
        CHECK(!wl_columnar_cache_digest_query(&a, &result, &semantic)
            && semantic == WL_EVALUATION_CONTROL_CANCELLED && !memcmp(&result,
            &bits, sizeof(result)));
        CHECK(!wl_evaluation_control_report(c, &after) && !memcmp(&before,
            &after, sizeof(before)));
        CHECK(wl_columnar_relation_snapshot_equal(image,
            wl_columnar_relation_snapshot(r)));
        CHECK(!atomic_load_explicit(&r->descriptor_access.state,
            memory_order_acquire)
            && !atomic_load_explicit(&r->source_access.state,
            memory_order_acquire));
        CHECK(!ack_turn(&a, 128, NULL));
        int64_t row[65] = {0}; CHECK(!col_rel_append_row(r, row));
        control_close(c, &owner, 0); col_rel_destroy(r);
    }
}

static unsigned hook_calls, reentries;

/* Literal primitive fees, not an empirical whole-host budget: BEGIN256,
 * descriptor128+storage128+validate192, then257 distinct CELL128 actions. */
static void
test_cell_budget(void)
{
    const uint64_t prefix = 256+128+128+192, total = 256+128+128+192+257*128;
    for (unsigned exact = 0; exact < 2; ++exact) {
        col_rel_t *r = relation_new(257, 1); int owner = 0;
        wl_evaluation_control_t *c = control_new(total-(exact?0:1), &owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        wl_columnar_cache_digest_attempt_t a = {0};
        CHECK(!wl_columnar_cache_digest_begin(&a, r, NULL, NULL, false, c, id));
        for (unsigned turn = 0; a.phase != WL_COLUMNAR_CACHE_DIGEST_CELL;
            ++turn) {
            CHECK(turn < 4); bool ready = false;
            CHECK(!wl_columnar_cache_digest_step(&a, &ready) && !ready);
        }
        for (unsigned col = 0; col < 256; ++col) {
            bool ready = false;
            CHECK(!wl_columnar_cache_digest_step(&a, &ready) && !ready);
        }
        CHECK(a.column == 256 && a.row == 0);
        uint64_t hash = a.hash[0]; bool ready = false;
        int rc = wl_columnar_cache_digest_step(&a, &ready);
        CHECK(rc == (exact?0:WL_EVALUATION_CONTROL_WORK_LIMIT) && !ready);
        CHECK(exact ? a.phase == WL_COLUMNAR_CACHE_DIGEST_ROWS : a.hash[0] ==
            hash);
        bool done = false;
        CHECK(!end_turn(&a, 128, true, EINVAL, &done, NULL)); drain(&a);
        int semantic = 0;
        CHECK(!wl_columnar_cache_digest_query(&a, NULL, &semantic)
            && semantic == (exact?EINVAL:WL_EVALUATION_CONTROL_WORK_LIMIT));
        CHECK(wl_evaluation_control_finish(c, &owner, semantic, 0) == semantic);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(c, &report));
        CHECK(report.consumed == (exact?total:prefix+256*128));
        CHECK(!ack_turn(&a, 128, NULL)); control_close(c, &owner, 0);
        col_rel_destroy(r);
    }
}

static void
test_success_result_is_saved(void)
{
    col_rel_t *r = relation_new(1, 1); int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    wl_columnar_cache_digest_attempt_t a = {0};
    CHECK(!wl_columnar_cache_digest_begin(&a, r, NULL, NULL, false, c,
        wl_evaluation_control_active_attempt_id(c)));
    until_ready(&a); bool done = false;
    CHECK(!end_turn(&a, 128, false, 0, &done,
        NULL) && a.result_saved && !a.semantic_result);
    CHECK(!end_turn(&a, 128, true, EIO, &done, NULL)); drain(&a);
    wl_columnar_cache_digest_result_t result; int semantic = -1;
    CHECK(!wl_columnar_cache_digest_query(&a, &result, &semantic) && !semantic
        && result.left_hash == col_mat_cache_key_content(r));
    CHECK(!ack_turn(&a, 128, NULL)); control_close(c, &owner, 0);
    col_rel_destroy(r);
}

static void
bad_funding(wl_columnar_cache_digest_attempt_t *a, bool ack)
{
    int owner = 0; wl_evaluation_control_t *fund = control_new(0, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(fund);
    wl_columnar_cache_digest_attempt_t saved = *a; bool done = true;
    int rc = ack?wl_columnar_cache_digest_ack(a, fund, id+1)
        :wl_columnar_cache_digest_finish(a, fund, id+1, &done);
    CHECK(rc == EINVAL && !memcmp(&saved, a, sizeof(saved)));
    CHECK(!wl_evaluation_control_finish(fund, &owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(fund, &report) && !report.consumed);
    control_close(fund, &owner, 0);
}

static void
test_clear_grants(void)
{
    col_rel_t *r = relation_new(1, 2); int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    wl_columnar_cache_digest_attempt_t a = {0};
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    CHECK(!wl_columnar_cache_digest_begin(&a, r, r, NULL, false, c, id));
    until_ready(&a); bool done = false; uint64_t work = 0;
    for (unsigned turn = 0; a.phase != WL_COLUMNAR_CACHE_DIGEST_END_CLEAR;
        ++turn) {
        CHECK(turn < 10);
        CHECK(!end_turn(&a, 128, false, 0, &done,
            &work) && work == 128 && !done);
    }
    wl_columnar_cache_digest_attempt_t saved = a;
    bad_funding(&a, false);
    CHECK(end_turn(&a, 127, false, 0, &done,
        &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work
        && !memcmp(&saved, &a, sizeof(a)));
    CHECK(wl_columnar_cache_digest_finish(&a, c, id, &done) == EINVAL
        && !memcmp(&saved, &a, sizeof(a)));
    CHECK(!end_turn(&a, 128, false, 0, &done, &work) && work == 128 && !done
        && a.phase == WL_COLUMNAR_CACHE_DIGEST_END_RESET
        && !a.source[0].relation && !a.source[1].relation
        && a.control_ref && a.control == c &&
        a.control_identity == saved.control_identity
        && !memcmp(&a.result, &saved.result, sizeof(a.result)));
    wl_columnar_cache_digest_result_t result;
    memset(&result, 0xa5, sizeof(result));
    wl_columnar_cache_digest_result_t sentinel = result; int semantic = 123;
    CHECK(wl_columnar_cache_digest_query(&a, &result,
        &semantic) == EBUSY && semantic == 123
        && !memcmp(&result, &sentinel, sizeof(result)));
    saved = a;
    bad_funding(&a, false);
    CHECK(end_turn(&a, 127, true, EIO, &done,
        &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work
        && !memcmp(&saved, &a, sizeof(a)));
    CHECK(!end_turn(&a, 128, true, EIO, &done, &work) && done && work == 128
        && a.phase == WL_COLUMNAR_CACHE_DIGEST_DONE && a.result_saved &&
        !a.semantic_result
        && a.control_ref && a.control_identity == saved.control_identity
        && !memcmp(&a.result, &saved.result, sizeof(a.result)));
    CHECK(!wl_columnar_cache_digest_query(&a, &result, &semantic) && !semantic);
    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    wl_evaluation_control_report_t before, after;
    CHECK(!wl_evaluation_control_report(c, &before));
    saved = a;
    bad_funding(&a, true);
    CHECK(wl_columnar_cache_digest_ack(&a, c, id) == EINVAL && !memcmp(&saved,
        &a, sizeof(a)));
    CHECK(ack_turn(&a, 127, &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work
        && !memcmp(&saved, &a, sizeof(a)));
    CHECK(!ack_turn(&a, 128, &work) && work == 128);
    CHECK(!wl_evaluation_control_report(c, &after) && !memcmp(&before, &after,
        sizeof(before)));
    control_close(c, &owner, 0); col_rel_destroy(r);
}

static void
test_reader_contention(void)
{
    col_rel_t *owner = relation_new(1, 2), *alias = relation_new(1, 0);
    CHECK(!col_rel_install_shared_view(alias, owner));
    int key = 0; wl_evaluation_control_t *c = control_new(0, &key);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_columnar_cache_digest_attempt_t a = {0};
    CHECK(!wl_columnar_cache_digest_begin(&a, alias, owner, NULL, false, c,
        id));
    wl_columnar_source_access_writer_t descriptor = {0}, storage = {0};
    CHECK(!wl_columnar_source_access_writer_acquire(&alias->descriptor_access,
        &descriptor));
    bool ready = false;
    CHECK(!wl_columnar_cache_digest_step(&a,
        &ready) && !ready && !a.source[0].reader.owner);
    CHECK(!wl_columnar_source_access_writer_release(&descriptor));
    CHECK(!wl_columnar_cache_digest_step(&a, &ready) && !ready
        && a.source[0].reader.acquire_stage == 1
        && a.source[0].reader.owner == &alias->descriptor_access &&
        !a.source[0].held);
    CHECK(!wl_columnar_source_access_writer_acquire(&owner->source_access,
        &storage));
    for (unsigned i = 0; i < 2; ++i)
        CHECK(!wl_columnar_cache_digest_step(&a, &ready) && !ready
            && a.source[0].reader.acquire_stage == 1 && !a.source[0].held);
    CHECK(!wl_columnar_source_access_writer_release(&storage));
    CHECK(!wl_columnar_cache_digest_step(&a, &ready) && a.source[0].held
        && a.source[0].reader.owner == &owner->source_access
        && a.source[0].reader.secondary_owner == &alias->descriptor_access);
    CHECK(!atomic_load_explicit(&alias->source_access.state,
        memory_order_acquire));
    CHECK(col_rel_append_row(owner, (int64_t[]){77}) == EBUSY
        && col_rel_destroy_checked(alias) == EBUSY);
    until_ready(&a);
    CHECK(atomic_load_explicit(&owner->source_access.state,
        memory_order_acquire) == 2);
    bool done = false; uint64_t work = 0;
    CHECK(!end_turn(&a, 128, false, 0, &done, &work) && work == 128 && !done);
    CHECK(!end_turn(&a, 128, false, 0, &done, &work) && work == 128 && !done
        && a.source[0].reader.release_stage == 1 && a.source[0].held);
    wl_columnar_cache_digest_attempt_t saved = a;
    CHECK(end_turn(&a, 127, false, 0, &done,
        &work) == WL_EVALUATION_CONTROL_WORK_LIMIT && !work
        && !memcmp(&saved, &a, sizeof(a)));
    CHECK(!end_turn(&a, 128, false, 0, &done,
        &work) && !done && !a.source[0].held
        && a.release_side == 1);
    CHECK(col_rel_append_row(owner, (int64_t[]){77}) == EBUSY);
    drain(&a);
    wl_columnar_cache_digest_result_t result; int semantic = -1;
    CHECK(!wl_columnar_cache_digest_query(&a, &result, &semantic) && !semantic
        && result.left_hash == result.right_hash
        && result.left_snapshot.relation_identity !=
        result.right_snapshot.relation_identity);
    /* Caller releases its original control ownership before ACK: the record's
     * real reference alone must keep it alive until independently funded ACK. */
    control_close(c, &key, 0);
    CHECK(!ack_turn(&a, 128, NULL));
    col_rel_destroy(alias); col_rel_destroy(owner);
}

typedef struct {
    col_rel_t *relation;
    wl_mutex_t lock;
    wl_cond_t wake;
    unsigned stage;
    int blocked, after;
} writer_barrier_t;

static void *
writer_barrier(void *arg)
{
    writer_barrier_t *b = arg; int64_t row = 93;
    b->blocked = col_rel_append_row(b->relation, &row);
    CHECK(!wl_mutex_lock(&b->lock)); b->stage = 1;
    CHECK(!wl_cond_signal(&b->wake));
    while (b->stage != 2)CHECK(!wl_cond_wait(&b->wake, &b->lock));
    CHECK(!wl_mutex_unlock(&b->lock));
    b->after = col_rel_append_row(b->relation, &row);
    return NULL;
}

static void
test_writer_barrier(void)
{
    col_rel_t *r = relation_new(1, 2); int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    wl_columnar_cache_digest_attempt_t a = {0};
    CHECK(!wl_columnar_cache_digest_begin(&a, r, r, NULL, false, c,
        wl_evaluation_control_active_attempt_id(c)));
    until_ready(&a);
    writer_barrier_t b = {.relation = r}; wl_thread_t thread;
    CHECK(!wl_mutex_init(&b.lock) && !wl_cond_init(&b.wake));
    CHECK(!wl_thread_create(&thread, writer_barrier, &b));
    CHECK(!wl_mutex_lock(&b.lock));
    while (!b.stage)CHECK(!wl_cond_wait(&b.wake, &b.lock));
    CHECK(!wl_mutex_unlock(&b.lock));
    CHECK(b.blocked == EBUSY);
    drain(&a);
    wl_columnar_cache_digest_result_t result; int semantic = -1;
    CHECK(!wl_columnar_cache_digest_query(&a, &result, &semantic) && !semantic);
    CHECK(!wl_mutex_lock(&b.lock)); b.stage = 2;
    CHECK(!wl_cond_signal(&b.wake));
    CHECK(!wl_mutex_unlock(&b.lock) && !wl_thread_join(&thread));
    CHECK(!b.after && r->nrows == 3
        && !wl_columnar_relation_snapshot_equal(result.left_snapshot,
        wl_columnar_relation_snapshot(r)));
    CHECK(!ack_turn(&a, 128, NULL));
    wl_cond_destroy(&b.wake); wl_mutex_destroy(&b.lock);
    control_close(c, &owner, 0); col_rel_destroy(r);
}
static void
reenter(wl_columnar_cache_digest_attempt_t *a, wl_evaluation_control_t *control,
    unsigned api)
{
    (void)api; unsigned before = ++hook_calls;
    wl_columnar_cache_digest_attempt_t snapshot = *a;
    bool ready = true, done = true; int semantic = 123;
    wl_columnar_cache_digest_result_t result;
    memset(&result, 0xa5, sizeof(result));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(wl_columnar_cache_digest_begin(a, NULL, NULL, NULL, false, control,
        id) == EBUSY);
    CHECK(wl_columnar_cache_digest_step(a, &ready) == EBUSY && ready);
    CHECK(wl_columnar_cache_digest_finish(a, control, id,
        &done) == EBUSY && done);
    CHECK(wl_columnar_cache_digest_abort(a, ECANCELED, control, id,
        &done) == EBUSY && done);
    CHECK(wl_columnar_cache_digest_ack(a, control, id) == EBUSY);
    CHECK(wl_columnar_cache_digest_query(a, &result,
        &semantic) == EBUSY && semantic == 123);
    CHECK(wl_columnar_cache_digest_step(a, &a->source[0].held) == EBUSY);
    CHECK(wl_columnar_cache_digest_finish(a, control, id, &a->busy) == EBUSY);
    CHECK(wl_columnar_cache_digest_query(a, &a->result,
        &a->semantic_result) == EBUSY);
    CHECK(hook_calls == before && !memcmp(&snapshot, a, sizeof(snapshot)));
    ++reentries;
}

static uint64_t
reentry_case(bool enabled)
{
    col_rel_t *r = relation_new(3, 2); int owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(c);
    wl_columnar_cache_digest_attempt_t a = {0};
    hook_calls = reentries = 0;
    wl_columnar_cache_digest_test_checkpoint = enabled?reenter:NULL;
    const char *keys[] = {"col0"}; uint8_t bytes[] = {4, 3, 2, 1};
    wl_plan_op_t op = {.op = WL_PLAN_OP_JOIN, .key_count = 1, .left_keys = keys,
                       .right_keys = keys,
                       .right_filter_expr = {bytes, sizeof(bytes)}};
    CHECK(!wl_columnar_cache_digest_begin(&a, r, r, &op, true, c, id));
    until_ready(&a); drain(&a); CHECK(!ack_turn(&a, 128, NULL));
    wl_columnar_cache_digest_test_checkpoint = NULL;
    if (enabled)CHECK(reentries == hook_calls && hook_calls > 40);
    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(c, &report));
    control_close(c, &owner, 0); col_rel_destroy(r); return report.consumed;
}

typedef struct {
    wl_columnar_cache_digest_attempt_t *a;
    wl_evaluation_control_t *funding;
    uint64_t id;
} foreign_t;

static void *
foreign_call(void *arg)
{
    foreign_t *f = arg; bool ready = true, done = true; int semantic = 123;
    wl_columnar_cache_digest_result_t out;
    CHECK(wl_columnar_cache_digest_step(f->a, &ready) == EINVAL && ready);
    CHECK(wl_columnar_cache_digest_finish(f->a, f->funding, f->id,
        &done) == EINVAL && done);
    CHECK(wl_columnar_cache_digest_ack(f->a, f->funding, f->id) == EINVAL);
    CHECK(wl_columnar_cache_digest_query(f->a, &out,
        &semantic) == EINVAL && semantic == 123);
    return NULL;
}

static void
test_authority(void)
{
    CHECK(reentry_case(false) == reentry_case(true));
    col_rel_t *r = relation_new(1, 2);
    int owner = 0, fund_owner = 0;
    wl_evaluation_control_t *c = control_new(0, &owner),
        *fund = control_new(0, &fund_owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(c),
        fid = wl_evaluation_control_active_attempt_id(fund);
    wl_columnar_cache_digest_attempt_t a = {0};
    CHECK(!wl_columnar_cache_digest_begin(&a, r, NULL, NULL, false, c, id));
    wl_columnar_cache_digest_attempt_t saved = a, copy = a;
    bool ready = true, done = true;
    CHECK(wl_columnar_cache_digest_begin(&a, r, NULL, NULL, false, c,
        id) == EBUSY);
    CHECK(!memcmp(&a, &saved, sizeof(a)));
    CHECK(wl_columnar_cache_digest_step(&copy, &ready) == EINVAL && ready);
    CHECK(wl_columnar_cache_digest_abort(&copy, ECANCELED, fund, fid,
        &done) == EINVAL && done);
    foreign_t foreign = {&a, fund, fid}; wl_thread_t thread;
    CHECK(!wl_thread_create(&thread, foreign_call, &foreign));
    CHECK(!wl_thread_join(&thread));
    CHECK(!memcmp(&a, &saved, sizeof(a)));
    CHECK(wl_columnar_cache_digest_abort(&a, ECANCELED, fund, fid+1,
        &done) == EINVAL);
    CHECK(!memcmp(&a, &saved, sizeof(a)));
    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(c) && !wl_evaluation_control_begin(c,
        &owner));
    ready = true;
    CHECK(wl_columnar_cache_digest_step(&a, &ready) == EINVAL && ready);
    CHECK(!memcmp(&a, &saved, sizeof(a)));
    CHECK(!wl_columnar_cache_digest_abort(&a, ECANCELED, fund, fid, &done));
    drain(&a);
    wl_columnar_cache_digest_result_t result; int semantic = 0;
    CHECK(wl_columnar_cache_digest_query(&a, &a.result, &semantic) == EINVAL);
    CHECK(wl_columnar_cache_digest_query(&a, &result,
        &a.semantic_result) == EINVAL);
    CHECK(!wl_columnar_cache_digest_query(&a, &result,
        &semantic) && semantic == ECANCELED);
    CHECK(!ack_turn(&a, 128, NULL));
    control_close(c, &owner, 0); control_close(fund, &fund_owner, 0);
    col_rel_destroy(r);
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2);
    if (!strcmp(argv[1], "domains"))test_domains();
    else if (!strcmp(argv[1], "failures")) {
        test_begin_and_terminal_funds(); test_partial_stops();
        test_reader_contention();
        test_cell_budget(); test_success_result_is_saved();
        test_clear_grants();
    } else if (!strcmp(argv[1], "authority")) {
        test_authority(); test_writer_barrier();
    }else CHECK(false);
    puts(
        "cache_digest: exact recipes and real retained-reader ownership passed");
    return 0;
}
