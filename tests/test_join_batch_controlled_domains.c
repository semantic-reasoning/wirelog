/* General producer contracts. Session hosting is qualified separately. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/session_options.h"
#include "../wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <math.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)

typedef struct {
    wirelog_program_t *program;
    wl_plan_t *plan;
    wl_session_t *base;
    wl_col_session_t *session;
    wl_columnar_memory_governor_ref_t *governor;
    col_rel_t *left, *right;
    const col_arrangement_t *arrangement;
    wl_plan_op_t op;
    uint32_t lk[2], rk[2], projection[3];
    const char *names[2];
    unsigned left_width, right_width, width, keys, left_rows, right_rows;
    bool floating, mismatch, timestamps;
} fixture_t;

static void
text_add(char *out, size_t size, size_t *used, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    int n = vsnprintf(out + *used, size - *used, format, args);
    va_end(args);
    CHECK(n >= 0 && (size_t)n < size - *used);
    *used += (size_t)n;
}

static int64_t
float_bits(double value)
{
    int64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return bits;
}

static uint32_t
head_at(const col_arrangement_t *arr, uint32_t bucket)
{
    if (!arr->ht_head || !arr->nbuckets) return UINT32_MAX;
    uint64_t head = arr->ht_head[bucket];
    return (uint32_t)(head >> 32) == arr->generation
        ? (uint32_t)head : UINT32_MAX;
}

static void
fixture_init(fixture_t *f, unsigned kind, bool timestamps)
{
    memset(f, 0, sizeof(*f));
    f->floating = kind == 1 || kind >= 5;
    f->mismatch = kind == 5;
    f->timestamps = timestamps;
    f->keys = kind == 0 || kind == 4 ? 2 : 1;
    f->left_width = kind == 2 ? 32 : kind == 3 ? 128 : f->keys + 1;
    f->right_width = kind == 2 ? 33 : kind == 3 ? 129 : f->keys + 1;
    f->left_rows = f->keys == 2 ? 4 : 3;
    f->right_rows = f->keys == 2 ? 128 : 3;
    f->width = kind == 4 ? 3 : f->left_width + f->right_width;
    f->lk[0] = f->rk[0] = 0; f->lk[1] = f->rk[1] = 1;
    f->names[0] = "col0"; f->names[1] = "col1";
    f->op = (wl_plan_op_t){.op = WL_PLAN_OP_JOIN, .right_relation = "right",
                           .key_count = f->keys, .left_keys = f->names,
                           .right_keys = f->names,
                           .delta_mode = WL_DELTA_FORCE_FULL};
    if (kind == 4) {
        f->projection[0] = f->left_width + f->right_width - 1;
        f->projection[1] = 0;
        f->projection[2] = UINT32_MAX;
        f->op.project_count = 3; f->op.project_indices = f->projection;
    }
    char text[16384] = {0}; size_t used = 0;
    for (unsigned side = 0; side < 2; ++side) {
        unsigned width = side ? f->right_width : f->left_width;
        text_add(text, sizeof(text), &used, ".decl %s(",
            side ? "right" : "left");
        for (unsigned c = 0; c < width; ++c)
            text_add(text, sizeof(text), &used, "%sp%u:%s", c ? "," : "", c,
                !c && f->floating ? "float" : "int64");
        text_add(text, sizeof(text), &used, ")\n");
    }
    text_add(text, sizeof(text), &used, ".decl output(k:%s)\noutput(k) :- ",
        f->floating ? "float" : "int64");
    for (unsigned side = 0; side < 2; ++side) {
        unsigned width = side ? f->right_width : f->left_width;
        text_add(text, sizeof(text), &used, "%s%s(", side ? "," : "",
            side ? "right" : "left");
        for (unsigned c = 0; c < width; ++c)
            text_add(text, sizeof(text), &used, "%s%s", c ? "," : "",
                c ? "_" : "k");
        text_add(text, sizeof(text), &used, ")");
    }
    text_add(text, sizeof(text), &used, ".\n");
    wirelog_error_t error;
    f->program = wirelog_parse_string(text, &error);
    CHECK(f->program && !wl_plan_from_program(f->program, &f->plan));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(512) << 20,
            .usable_bytes = UINT64_C(512) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    f->governor = wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(f->governor);
    wl_session_options_t options;
    wl_session_options_init(&options); options.memory_governor = f->governor;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), f->plan, 1,
        &options, &f->base));
    f->session = (wl_col_session_t *)f->base;
    int64_t row[129];
    for (unsigned r = 0; r < f->right_rows; ++r) {
        for (unsigned c = 0; c < f->right_width; ++c) row[c] = 1000 + 10*r + c;
        row[0] = f->floating ? float_bits(r < 2 ? (r ? -0.0 : 0.0) : 1.0)
            : f->keys == 2 ? 1 : r < 2 ? 1 : 2;
        if (f->keys ==
            2) row[1] = (int64_t)((uint64_t)(r % 64) * UINT64_C(2654435761));
        CHECK(!wl_session_insert(f->base, "right", row, 1, f->right_width));
    }
    f->right = session_find_rel(f->session, "right");
    if (f->floating) {
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(!col_rel_source_writer_acquire(f->right, &writer));
        /* Public writes canonicalize zero. Exercise valid physical -0 and
         * deliberately invalid nonfinite keys through the primitive too. */
        f->right->columns[0][1] = float_bits(-0.0);
        if (kind >= 6) f->right->columns[0][2] = kind == 6
            ? (int64_t)UINT64_C(0x7ff8000000000001)
            : (int64_t)UINT64_C(0x7ff0000000000000);
        wl_columnar_relation_touch_view(f->right);
        CHECK(!wl_columnar_source_access_writer_release(&writer));
    }
    f->arrangement = col_session_get_arrangement(f->base, "right", f->rk,
            f->keys);
    CHECK(f->arrangement);
    int64_t matched_key = 0;
    if (f->keys == 2) {
        bool collision = false;
        for (uint32_t bucket = 0; bucket < f->arrangement->nbuckets; ++bucket) {
            uint32_t first = head_at(f->arrangement, bucket);
            if (first == UINT32_MAX) continue;
            uint32_t next = f->arrangement->ht_next[first];
            while (next != UINT32_MAX) {
                if (f->right->columns[1][next] != f->right->columns[1][first]) {
                    collision = true; matched_key = f->right->columns[1][first];
                    break;
                }
                next = f->arrangement->ht_next[next];
            }
            if (collision) break;
        }
        CHECK(collision);
    }
    for (unsigned r = 0; r < f->left_rows; ++r) {
        for (unsigned c = 0; c < f->left_width; ++c) row[c] = 2000 + 10*r + c;
        row[0] = f->floating ? float_bits(r < 2 ? (r ? 0.0 : -0.0) : 1.0)
            : f->keys == 2 ? 1 : r < 2 ? 1 : 2;
        if (f->keys == 2) row[1] = r == 3 ? -1 : matched_key;
        CHECK(!wl_session_insert(f->base, "left", row, 1, f->left_width));
    }
    f->left = session_find_rel(f->session, "left");
    if (f->floating) {
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(!col_rel_source_writer_acquire(f->left, &writer));
        f->left->columns[0][0] = float_bits(-0.0);
        if (kind >= 6) f->left->columns[0][2] = kind == 6
            ? (int64_t)UINT64_C(0x7ff8000000000001)
            : (int64_t)UINT64_C(0x7ff0000000000000);
        wl_columnar_relation_touch_view(f->left);
        CHECK(!wl_columnar_source_access_writer_release(&writer));
    }
    if (f->mismatch) {
        wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                         WIRELOG_TYPE_INT64};
        CHECK(!col_rel_set_column_types(f->left, types, 2));
    }
    if (timestamps) {
        CHECK(!col_rel_enable_timestamps(f->left));
        for (unsigned r = 0; r < f->left_rows; ++r)
            f->left->timestamps[r] = (col_delta_timestamp_t){.iteration = r+10,
                                                             .stratum = 2,
                                                             .worker = r,
                                                             .multiplicity =
                                                                 -3};
        wl_columnar_relation_touch_storage(f->left);
    }
}

static void
fixture_destroy(fixture_t *f)
{
    CHECK(!wl_session_try_destroy(&f->base));
    wl_plan_free(f->plan); wirelog_program_free(f->program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            f->governor)));
    wl_columnar_memory_governor_ref_release(f->governor);
}

/* Literal type-aware oracle, independent of the controlled matcher. */
static bool
matches(const fixture_t *f, unsigned lr, unsigned rr, unsigned *checked)
{
    for (unsigned k = 0; k < f->keys; ++k) {
        ++*checked;
        if (f->mismatch) return false;
        int64_t a = f->left->columns[f->lk[k]][lr];
        int64_t b = f->right->columns[f->rk[k]][rr];
        if (f->floating) {
            double x, y; memcpy(&x, &a, sizeof(x)); memcpy(&y, &b, sizeof(y));
            if (!isfinite(x) || !isfinite(y) || x != y) return false;
        } else if (a != b) return false;
    }
    return true;
}

static unsigned sort_width;
static int
row_order(const void *a, const void *b)
{
    const int64_t *x = a, *y = b;
    for (unsigned c = 0; c < sort_width; ++c)
        if (x[c] != y[c]) return (x[c] > y[c]) - (x[c] < y[c]);
    return 0;
}

static unsigned
oracle(const fixture_t *f, int64_t *rows)
{
    unsigned count = 0;
    for (unsigned l = 0; l < f->left_rows; ++l)
        for (unsigned r = 0; r < f->right_rows; ++r) {
            unsigned checked = 0;
            if (!matches(f, l, r, &checked)) continue;
            for (unsigned c = 0; c < f->width; ++c) {
                uint32_t src = f->op.project_count ? f->projection[c] : c;
                int64_t value = src < f->left_width
                    ? f->left->columns[src][l] : src - f->left_width <
                    f->right_width
                    ? f->right->columns[src - f->left_width][r] : 0;
                bool floating = f->floating &&
                    (src == 0 || src == f->left_width);
                if (floating) {
                    double number; memcpy(&number, &value, sizeof(number));
                    if (number == 0.0) value = 0;
                }
                rows[count * f->width + c] = value;
            }
            ++count;
        }
    return count;
}

/* Independent literal action inventory: key/schema validation, scan, nine
 * hash actions per key, bucket, compared keys, emitted cells/stamp and advance.
 * The actual primary chain is input data, not a measured control report. */
static uint64_t
expected_work(const fixture_t *f)
{
    uint64_t work = f->keys + 1u + f->width + 1u;
    int64_t *key = calloc(f->right_width, sizeof(*key)); CHECK(key);
    for (unsigned l = 0; l < f->left_rows; ++l) {
        work += 1u + 9u * f->keys + 1u;
        for (unsigned k = 0; k < f->keys; ++k)
            key[f->rk[k]] = f->left->columns[f->lk[k]][l];
        uint32_t r = col_arrangement_bucket_head_typed(f->arrangement, f->right,
                key);
        if (r == UINT32_MAX) ++work;
        for (; r != UINT32_MAX; r = f->arrangement->ht_next[r]) {
            unsigned checked = 0;
            bool matched = matches(f, l, r, &checked);
            work += checked + 1u;
            if (matched) work += f->width + 1u
                    + (f->timestamps ? sizeof(col_delta_timestamp_t) : 0);
        }
    }
    free(key);
    return work;
}

static void
trial(fixture_t *f, uint64_t limit, unsigned turn, bool short_one)
{
    col_relation_snapshot_t left_before =
        wl_columnar_relation_snapshot(f->left);
    col_relation_snapshot_t right_before =
        wl_columnar_relation_snapshot(f->right);
    col_delta_timestamp_t stamps[4];
    CHECK(f->left_rows <= 4);
    if (f->timestamps) memcpy(stamps, f->left->timestamps,
            f->left_rows * sizeof(*stamps));
    wl_columnar_continuation_t *producer = NULL;
    uint64_t row_bytes = (uint64_t)f->width * sizeof(int64_t)
        + (f->timestamps ? sizeof(col_delta_timestamp_t) : 0);
    CHECK(!col_join_batch_producer_create(f->session, &f->op, f->left, false,
        f->lk, f->rk, f->keys, row_bytes, &producer));
    wl_evaluation_control_t *control = NULL; int owner = 0;
    CHECK(!wl_evaluation_control_create(limit, &control));
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(!col_join_batch_control_bind(producer, control,
        wl_evaluation_control_active_attempt_id(control)));
    size_t cells = (size_t)f->left_rows * f->right_rows * f->width;
    int64_t *actual = calloc(cells ? cells : 1u, sizeof(*actual));
    int64_t *expected = calloc(cells ? cells : 1u, sizeof(*expected));
    CHECK(actual && expected);
    unsigned count = 0, batches = 0; bool done = false;
    uint64_t last_work = 0;
    col_join_batch_control_result_t result;
    for (unsigned calls = 0; calls < 1000000; ++calls) {
        col_join_batch_control_status_t status = col_join_batch_control_step(
            producer, turn, &result);
        CHECK(result.work_ordinal >= last_work
            && result.work_ordinal - last_work <= turn);
        last_work = result.work_ordinal;
        if (status == COL_JOIN_BATCH_CONTROL_PROGRESS) continue;
        if (status == COL_JOIN_BATCH_CONTROL_DONE) {
            done = true; break;
        }
        if (short_one && status == COL_JOIN_BATCH_CONTROL_WORK_LIMIT) break;
        CHECK(status == COL_JOIN_BATCH_CONTROL_BATCH_READY);
        const col_rel_t *batch = NULL; uint64_t generation = 0;
        CHECK(!col_join_batch_control_peek(producer, &batch, &generation));
        CHECK(batch && batch->nrows == 1 && batch->ncols == f->width);
        for (unsigned c = 0; c < f->width; ++c)
            actual[count * f->width + c] = batch->columns[c][0];
        if (f->timestamps) CHECK(batch->timestamps
                && !batch->timestamps[0].iteration &&
                !batch->timestamps[0].stratum
                && !batch->timestamps[0].worker &&
                !batch->timestamps[0]._reserved
                && !batch->timestamps[0].multiplicity);
        ++count; ++batches;
        CHECK(!col_join_batch_control_precommit(producer, generation));
        CHECK(!col_join_batch_control_ack(producer, generation));
        col_join_batch_cursor_t cursor;
        CHECK(col_join_batch_cursor_get(producer, &cursor));
        CHECK(cursor.sequence == batches);
    }
    uint64_t cost = expected_work(f);
    CHECK(last_work == cost - (short_one ? 1u : 0u));
    CHECK(done != short_one);
    if (!short_one) {
        unsigned expected_count = oracle(f, expected);
        CHECK(count == expected_count);
        sort_width = f->width;
        qsort(actual, count, f->width * sizeof(int64_t), row_order);
        qsort(expected, count, f->width * sizeof(int64_t), row_order);
        CHECK(!memcmp(actual, expected, count * f->width * sizeof(int64_t)));
        if (expected_count) CHECK(batches >= 3);
    }
    CHECK(!col_join_batch_control_discard(&producer) && !producer);
    int status = short_one ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
    CHECK(wl_evaluation_control_finish(control, &owner, status, 0) == status);
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(control, &report)
        && report.consumed == last_work && report.result == status);
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
    CHECK(wl_columnar_relation_snapshot_equal(left_before,
        wl_columnar_relation_snapshot(f->left)));
    CHECK(wl_columnar_relation_snapshot_equal(right_before,
        wl_columnar_relation_snapshot(f->right)));
    if (f->timestamps) CHECK(!memcmp(stamps, f->left->timestamps,
            f->left_rows * sizeof(*stamps)));
    free(actual); free(expected);
}

static void
raw_reference(fixture_t *f)
{
    wl_columnar_continuation_t *producer = NULL;
    uint64_t bytes = (uint64_t)f->width * sizeof(int64_t)
        + (f->timestamps ? sizeof(col_delta_timestamp_t) : 0);
    CHECK(!col_join_batch_producer_create(f->session, &f->op, f->left, false,
        f->lk, f->rk, f->keys, bytes, &producer));
    col_rel_t *out = col_rel_new_auto("$raw", f->width);
    CHECK(out && !col_join_set_output_types(out, f->left, f->right, &f->op));
    if (f->timestamps) CHECK(!col_rel_enable_timestamps(out));
    CHECK(!col_join_batch_run_to_relation(producer, f->session, out));
    wl_columnar_continuation_destroy(producer);
    size_t cells = (size_t)f->left_rows * f->right_rows * f->width;
    int64_t *actual = calloc(cells ? cells : 1u, sizeof(*actual));
    int64_t *expected = calloc(cells ? cells : 1u, sizeof(*expected));
    CHECK(actual && expected);
    unsigned count = oracle(f, expected);
    CHECK(out->nrows == count);
    for (unsigned row = 0; row < count; ++row)
        for (unsigned col = 0; col < f->width; ++col)
            actual[row * f->width + col] = out->columns[col][row];
    sort_width = f->width;
    qsort(actual, count, f->width * sizeof(int64_t), row_order);
    qsort(expected, count, f->width * sizeof(int64_t), row_order);
    CHECK(!memcmp(actual, expected, count * f->width * sizeof(int64_t)));
    col_rel_destroy(out); free(actual); free(expected);
}

static unsigned stop_phase, stop_column;
static bool stop_hit, stop_work;

static void
stop_cell(wl_evaluation_control_t *control, unsigned phase, uint64_t progress,
    const col_rel_t *batch)
{
    if (stop_hit || phase != stop_phase || progress != stop_column) return;
    CHECK(batch && !batch->nrows && batch->ncols >= 65);
    stop_hit = true;
    if (stop_work) CHECK(wl_evaluation_control_charge(control, UINT64_MAX)
            == WL_EVALUATION_CONTROL_WORK_LIMIT);
    else wl_evaluation_control_request_cancel(control);
}

static void
test_partial_rows(void)
{
    fixture_t f;
    fixture_init(&f, 3, true);
    const unsigned columns[] = {1, 64, 256, 0};
    uint64_t baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(f.governor));
    for (unsigned stage = 0; stage < 4; ++stage)
        for (unsigned work = 0; work < 2; ++work) {
            wl_columnar_continuation_t *producer = NULL;
            uint64_t row_bytes = f.width * sizeof(int64_t) +
                sizeof(col_delta_timestamp_t);
            CHECK(!col_join_batch_producer_create(f.session, &f.op, f.left,
                false,
                f.lk, f.rk, f.keys, row_bytes, &producer));
            wl_evaluation_control_t *control = NULL; int owner = 0;
            CHECK(!wl_evaluation_control_create(UINT64_C(1000000000),
                &control));
            CHECK(!wl_evaluation_control_attach(control, &owner));
            CHECK(!wl_evaluation_control_begin(control, &owner));
            CHECK(!col_join_batch_control_bind(producer, control,
                wl_evaluation_control_active_attempt_id(control)));
            stop_column = columns[stage]; stop_phase = stage == 3 ? 2 : 1;
            stop_hit = false; stop_work = work != 0;
            wl_columnar_join_batch_test_control_checkpoint = stop_cell;
            col_join_batch_control_result_t result;
            col_join_batch_control_status_t status =
                COL_JOIN_BATCH_CONTROL_PROGRESS;
            for (unsigned turn = 0; turn < 10000 && !stop_hit; ++turn)
                status = col_join_batch_control_step(producer, 7, &result);
            wl_columnar_join_batch_test_control_checkpoint = NULL;
            CHECK(stop_hit &&
                status == (work ? COL_JOIN_BATCH_CONTROL_WORK_LIMIT
                : COL_JOIN_BATCH_CONTROL_CANCELLED));
            CHECK(result.work_ordinal > stop_column);
            uint64_t consumed = result.work_ordinal;
            CHECK(col_join_batch_control_step(producer, 256, &result) == status
                && result.work_ordinal == consumed);
            const col_rel_t *batch = NULL; uint64_t generation = 0;
            CHECK(col_join_batch_control_peek(producer, &batch, &generation)
                && !batch && !generation);
            col_join_batch_cursor_t cursor;
            CHECK(col_join_batch_cursor_get(producer, &cursor)
                && !cursor.sequence && !cursor.next.lr &&
                cursor.next.rr == UINT32_MAX);
            CHECK(!col_join_batch_control_discard(&producer));
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(f.governor)) ==
                baseline);
            int rc =
                work ? WL_EVALUATION_CONTROL_WORK_LIMIT :
                WL_EVALUATION_CONTROL_CANCELLED;
            CHECK(wl_evaluation_control_finish(control, &owner, rc, 0) == rc);
            CHECK(!wl_evaluation_control_detach(control, &owner));
            wl_evaluation_control_release(control);
            /* A fresh attempt/producer recovers all values, not the partial row. */
            trial(&f, expected_work(&f), 1, false);
        }
    fixture_destroy(&f);
}

static void
test_stale_and_zero_work(void)
{
    fixture_t f; fixture_init(&f, 1, false);
    for (unsigned zero = 0; zero < 2; ++zero) {
        wl_columnar_continuation_t *producer = NULL;
        CHECK(!col_join_batch_producer_create(f.session, &f.op, f.left, false,
            f.lk, f.rk, f.keys, 32, &producer));
        wl_evaluation_control_t *control = NULL; int owner = 0;
        CHECK(!wl_evaluation_control_create(0, &control));
        CHECK(!wl_evaluation_control_attach(control, &owner));
        CHECK(!wl_evaluation_control_begin(control, &owner));
        CHECK(!col_join_batch_control_bind(producer, control,
            wl_evaluation_control_active_attempt_id(control)));
        col_join_batch_control_result_t result;
        if (zero) {
            CHECK(col_join_batch_control_step(producer, 0, &result)
                == COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR);
            CHECK(result.execution_status == EINVAL && !result.work_ordinal);
            CHECK(col_join_batch_control_step(producer, 1, &result)
                == COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR);
        } else {
            CHECK(col_join_batch_control_step(producer, 1, &result)
                == COL_JOIN_BATCH_CONTROL_PROGRESS && result.work_ordinal == 1);
            CHECK(!col_rel_set(f.left, 0, 0, float_bits(7.5)));
            CHECK(col_join_batch_control_step(producer, 1, &result)
                == COL_JOIN_BATCH_CONTROL_STALE_INPUT &&
                result.work_ordinal == 1);
        }
        CHECK(!col_join_batch_control_discard(&producer));
        CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
        CHECK(!wl_evaluation_control_detach(control, &owner));
        wl_evaluation_control_release(control);
    }
    fixture_destroy(&f);
}

static void
test_empty_inputs(void)
{
    for (unsigned empty_left = 0; empty_left < 2; ++empty_left) {
        fixture_t f; fixture_init(&f, 3, false);
        col_rel_t *source = empty_left ? f.left : f.right;
        wl_columnar_source_access_writer_t writer = {0};
        CHECK(!col_rel_source_writer_acquire(source, &writer));
        source->nrows = 0;
        wl_columnar_relation_touch_view(source);
        CHECK(!wl_columnar_source_access_writer_release(&writer));
        if (empty_left) f.left_rows = 0;
        else {
            f.right_rows = 0;
            f.arrangement = col_session_get_arrangement(f.base, "right", f.rk,
                    f.keys);
            CHECK(f.arrangement);
        }
        trial(&f, expected_work(&f), 1, false);
        fixture_destroy(&f);
    }
}

static void
test_invalid_last_cell(void)
{
    fixture_t f; fixture_init(&f, 3, false);
    wirelog_column_type_t types[129];
    for (unsigned c = 0; c < 129; ++c) types[c] = WIRELOG_TYPE_INT64;
    CHECK(!col_rel_set_column_types(f.right, types, 129));
    types[128] = WIRELOG_TYPE_FLOAT;
    CHECK(!col_rel_set_column_types(f.right, types, 129));
    wl_columnar_source_access_writer_t writer = {0};
    CHECK(!col_rel_source_writer_acquire(f.right, &writer));
    f.right->columns[128][1] = (int64_t)UINT64_C(0x7ff8000000000001);
    wl_columnar_relation_touch_view(f.right);
    CHECK(!wl_columnar_source_access_writer_release(&writer));
    f.arrangement = col_session_get_arrangement(f.base, "right", f.rk, 1);
    CHECK(f.arrangement);
    wl_columnar_continuation_t *producer = NULL;
    uint64_t bytes = f.width * sizeof(int64_t);
    CHECK(!col_join_batch_producer_create(f.session, &f.op, f.left, false,
        f.lk, f.rk, 1, bytes, &producer));
    col_rel_t *raw = col_rel_new_auto("$invalid_raw", f.width);
    CHECK(raw && !col_join_set_output_types(raw, f.left, f.right, &f.op));
    CHECK(col_join_batch_run_to_relation(producer, f.session, raw) == EINVAL);
    CHECK(!raw->nrows);
    wl_columnar_continuation_destroy(producer); producer = NULL;
    col_rel_destroy(raw);
    CHECK(!col_join_batch_producer_create(f.session, &f.op, f.left, false,
        f.lk, f.rk, 1, bytes, &producer));
    wl_evaluation_control_t *control = NULL; int owner = 0;
    CHECK(!wl_evaluation_control_create(0, &control));
    CHECK(!wl_evaluation_control_attach(control, &owner));
    CHECK(!wl_evaluation_control_begin(control, &owner));
    CHECK(!col_join_batch_control_bind(producer, control,
        wl_evaluation_control_active_attempt_id(control)));
    col_join_batch_control_result_t result;
    col_join_batch_control_status_t status = COL_JOIN_BATCH_CONTROL_PROGRESS;
    for (unsigned turn = 0;
        status == COL_JOIN_BATCH_CONTROL_PROGRESS && turn < 10000; ++turn)
        status = col_join_batch_control_step(producer, 7, &result);
    CHECK(status == COL_JOIN_BATCH_CONTROL_EXECUTION_ERROR
        && result.execution_status == EINVAL && !result.batch_generation);
    const col_rel_t *batch = NULL; uint64_t generation = 0;
    CHECK(col_join_batch_control_peek(producer, &batch, &generation) == EINVAL
        && !batch && !generation);
    CHECK(!col_join_batch_control_discard(&producer));
    CHECK(wl_evaluation_control_finish(control, &owner, EINVAL, 0) == EINVAL);
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
    fixture_destroy(&f);
}

static void
test_domains(void)
{
    for (unsigned kind = 0; kind < 8; ++kind)
        for (unsigned timestamps = 0; timestamps < 2; ++timestamps) {
            fixture_t f; fixture_init(&f, kind, timestamps);
            raw_reference(&f);
            uint64_t cost = expected_work(&f);
            printf("general producer kind=%u TS=%u work=%llu\n", kind,
                timestamps,
                (unsigned long long)cost); fflush(stdout);
            trial(&f, 0, 1, false);
            trial(&f, cost, 256, false);
            trial(&f, cost - 1u, 7, true);
            fixture_destroy(&f);
        }
}

int
main(void)
{
    test_domains();
    test_partial_rows();
    test_stale_and_zero_work();
    test_empty_inputs();
    test_invalid_last_cell();
    puts("general controlled producer tests passed");
    return 0;
}
