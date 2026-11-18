/* Natural materialized K-fusion input, bounded preserved-body destination. */
#include "wirelog/columnar/internal.h"
#include "wirelog/columnar/join_batch.h"
#include "wirelog/columnar/join_pipeline.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(e) do { if (!(e)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #e); abort(); \
                      } } while (0)

typedef struct { int64_t x, y; } pair_t;
typedef struct { int diff[2][8][8]; unsigned calls[2][8][8]; } events_t;
static const pair_t initial[] = {
    {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {1, 4}, {2, 5}, {3, 6}, {1, 6}
};
static const pair_t inserted[] = {
    {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {1, 4}, {2, 5}, {3, 6}, {1, 6},
    {6, 7}, {4, 7}, {2, 7}
};
static const pair_t deleted[] = {{1, 2}, {2, 3}, {4, 5}, {5, 6}, {6, 7},
                                 {4, 7}};
static const pair_t edges0[] = {{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}};
static const pair_t edges1[] = {{1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}};
static const pair_t edges2[] = {{1, 2}, {2, 3}, {4, 5}, {5, 6}, {6, 7}};
static const pair_t inline_initial[] = {
    {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {1, 5}, {2, 6}
};
static const pair_t inline_inserted[] = {
    {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {1, 5}, {2, 6}, {3, 7}
};
static unsigned inline_position, output_route, history_stage;
static int64_t input_tag, input_float;
static events_t snapshot_images[2];
static unsigned inline_seen, inline_gathers, inline_hashes, inline_prepared,
    inline_produced, inline_acks, inline_positive_acks, inline_done,
    inline_live_prepared;
static const wl_plan_op_t *inline_batch_op;
static bool path_child, selected_body, whole_scc;
static unsigned owned_joins;
static const wl_plan_relation_t *selected_plan;
static const wl_columnar_eval_private_bindings_t *selected_bindings;
static bool cancel_body, canceled_body;
static unsigned interrupt_kind;
static uint64_t successor_id;
static wl_evaluation_control_report_t frozen_report;
static wl_session_t *controlled_session;
static bool producer_error_armed;
static bool memory_mode, memory_armed, memory_denied;
static wl_columnar_memory_reservation_t memory_peer;
static uint64_t memory_denials_before;

static void
end_original(wl_evaluation_control_t *control)
{
    CHECK(wl_evaluation_control_finish(control,
        controlled_session->operation_admission, 7, 0) == 7);
    CHECK(!wl_evaluation_control_report(control, &frozen_report));
    if (interrupt_kind == 4) {
        CHECK(!wl_evaluation_control_begin(control,
            controlled_session->operation_admission));
        successor_id = wl_evaluation_control_active_attempt_id(control);
        CHECK(successor_id && successor_id != frozen_report.attempt_id);
    }
}

static void
first_producer_error(wl_evaluation_control_t *control, int result)
{
    if (memory_armed && !memory_denied && memory_peer.governor) {
        CHECK(control == controlled_session->evaluation_control
            && result == ENOSPC && !memory_denied);
        CHECK(wl_columnar_memory_denials(memory_peer.governor)
            > memory_denials_before);
        memory_denied = true;
        /* Actual admission has refused. Return peer capacity before mandatory
         * cleanup, while preserving the selected error and pending input. */
        wl_columnar_memory_release(&memory_peer);
        return;
    }
    if (interrupt_kind != 5 || canceled_body) return;
    CHECK(producer_error_armed && result == ENOMEM
        && !wl_columnar_join_batch_test_prepare_fail_allocation);
    canceled_body = true;
    end_original(control);
}

extern void (*wl_columnar_session_test_observed_transaction_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t,
    const wl_columnar_eval_private_bindings_t *);

static void
environment(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(!_putenv_s(name, value ? value : ""));
#else
    CHECK(value ? !setenv(name, value, 1) : !unsetenv(name));
#endif
}

static void
capture(const char *name, const int64_t *row, uint32_t width, int32_t sign,
    void *payload)
{
    events_t *events = payload;
    CHECK(width == 2 && (sign == 1 || sign == -1)
        && row[0] > 0 && row[0] < 8 && row[1] > 0 && row[1] < 8);
    unsigned relation = !strcmp(name, "path") ? 0 : 1;
    CHECK(!strcmp(name, relation ? "plain" : "path"));
    CHECK(!events->calls[relation][row[0]][row[1]]++);
    events->diff[relation][row[0]][row[1]] += sign;
}

static void
observed(wl_evaluation_control_t *control, unsigned phase, uint32_t index,
    const wl_columnar_eval_private_bindings_t *bindings)
{
    (void)control; (void)index;
    if (phase == 18) whole_scc = true; /* Actual bottom-up SCC seed. */
    if (phase != 8) return;
    selected_plan = bindings ? bindings->plan : NULL;
    selected_bindings = bindings;
    path_child = bindings && bindings->plan
        && !strcmp(bindings->plan->name, "path");
    if (!path_child) return;
    unsigned joins = 0;
    for (uint32_t i = 0; i < bindings->plan->op_count; ++i) {
        const wl_plan_op_t *op = &bindings->plan->ops[i];
        CHECK(!op->opaque_data && !op->materialized
            && op->op != WL_PLAN_OP_K_FUSION && op->op != WL_PLAN_OP_LFTJ);
        joins += op->op == WL_PLAN_OP_JOIN;
    }
    CHECK(joins == (inline_position ? 3u : 2u));
    if (inline_position) {
        const wl_plan_op_t *edge_join = NULL, *first_join = NULL;
        for (uint32_t i = 0; i < bindings->plan->op_count; ++i) {
            const wl_plan_op_t *op = &bindings->plan->ops[i];
            if (op->op != WL_PLAN_OP_JOIN) continue;
            if (!first_join) first_join = op;
            if (op->right_relation && !strcmp(op->right_relation, "edge"))
                edge_join = op;
        }
        CHECK(inline_position == 1 ? first_join && first_join->key_count == 1
            && !strcmp(first_join->left_keys[0], "col3")
            : edge_join && edge_join->key_count == 1
            && !strcmp(edge_join->right_keys[0], "col2"));
    }
    selected_body = true;
}

/* Read-only observation inside genuine private-body callbacks. */
static bool
inline_relation(const col_rel_t *rel)
{
    if (!rel || rel->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        || !rel->nrows) return false;
    CHECK(rel->schema_ok && rel->ncols == 4 && rel->compound_count == 1
        && rel->compound_arity_len == 3 && !rel->inline_physical_offset
        && rel->compound_arity_map && rel->compound_arity_map[0] == 2
        && rel->compound_arity_map[1] == 1 && rel->compound_arity_map[2] == 1
        && rel->column_types && rel->column_types[0] == WIRELOG_TYPE_STRING
        && rel->column_types[1] == WIRELOG_TYPE_FLOAT
        && rel->column_types[2] == WIRELOG_TYPE_INT64
        && rel->column_types[3] == WIRELOG_TYPE_INT64
        && rel->columns[0][0] == input_tag
        && rel->columns[1][0] == input_float);
    return true;
}
static bool
inline_source(const wl_columnar_relation_scalar_source_t *source)
{
    if (source->phase != WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
        || !inline_relation(source->descriptor)) return false;
    const col_rel_t *r = source->descriptor;
    CHECK(source->identity == source && source->width == 4
        && source->columns == (const int64_t *const *)r->columns
        && source->descriptor_reader.identity ==
        (uintptr_t)&source->descriptor_reader
        && source->descriptor_reader.owner == &r->descriptor_access
        && source->owner_reader.identity == (uintptr_t)&source->owner_reader
        && source->owner_reader.owner == &r->storage_owner->source_access);
    return true;
}
static wl_columnar_eval_serial_framed_attempt_t *
actual_body(void)
{
    wl_col_session_t *s = COL_SESSION(controlled_session);
    wl_columnar_eval_serial_framed_attempt_t *r = NULL;
    CHECK(!wl_columnar_eval_stack_framed_context(s->cleanup_active, s, &r)
        && r->capture_only && r->private_bindings == selected_bindings
        && r->child_bound && r->child.plan == selected_plan);
    return r;
}
static void
batch_event(wl_evaluation_control_t *control, unsigned event,
    uint64_t generation, uint32_t extent, uint64_t work)
{
    if (!inline_position || !path_child || !inline_batch_op) return;
    CHECK(control == controlled_session->evaluation_control);
    wl_columnar_eval_serial_framed_attempt_t *r = actual_body();
    if (r->child.cursor >= selected_plan->op_count ||
        &selected_plan->ops[r->child.cursor] != inline_batch_op) return;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE) ++inline_produced;
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK && generation) {
        CHECK(work); ++inline_acks;
        if (extent) ++inline_positive_acks;
    }
    if (event == WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCER_DONE && work)
        ++inline_done;
}
static void
batch_prepare(const wl_columnar_join_batch_prepare_t *attempt)
{
    if (!path_child || !selected_plan) return;
    for (uint32_t i = 0; i < selected_plan->op_count; ++i) {
        if (attempt->op != &selected_plan->ops[i]) continue;
        CHECK(attempt->identity == attempt
            && attempt->op->op == WL_PLAN_OP_JOIN
            && !attempt->op->materialized && !attempt->op->opaque_data);
        if (inline_position && attempt->phase >= 11 && attempt->phase <= 13
            && (inline_relation(attempt->left) ||
            inline_relation(attempt->right))){
            CHECK(attempt->arrays_committed && attempt->keys && attempt->types
                && attempt->arrangement.ht_head && attempt->key_row
                && attempt->arrangement.indexed_rows == attempt->right->nrows
                && attempt->right->nrows
                && attempt->left_reader.identity ==
                (uintptr_t)&attempt->left_reader
                && attempt->right_reader.identity ==
                (uintptr_t)&attempt->right_reader
                && attempt->left_reader.secondary_owner ==
                &attempt->left->descriptor_access
                && attempt->right_reader.secondary_owner ==
                &attempt->right->descriptor_access
                && attempt->left_reader.owner ==
                &attempt->left->storage_owner->source_access
                && attempt->right_reader.owner ==
                &attempt->right->storage_owner->source_access);
            CHECK(inline_relation(attempt->right) && attempt->keys[1] == 2);
            inline_seen |= 2; ++inline_prepared; inline_batch_op = attempt->op;
            if (attempt->left->nrows) ++inline_live_prepared;
        }
        if (interrupt_kind == 5 && !producer_error_armed) {
            CHECK(!wl_columnar_join_batch_test_prepare_fail_allocation);
            producer_error_armed = true;
            wl_columnar_join_batch_test_prepare_fail_allocation = 1;
        }
        /* A later real preparation phase witnesses completed positive work,
         * rather than merely reaching the pre-admission checkpoint. */
        if (attempt->arrays_committed) {
            ++owned_joins;
            if (memory_armed && !memory_peer.governor && !memory_denied) {
                wl_columnar_memory_governor_t *governor =
                    wl_columnar_memory_governor_ref_get(attempt->governor);
                uint64_t available =
                    atomic_load_explicit(&governor->usable_bytes,
                        memory_order_relaxed) -
                    wl_columnar_memory_reserved(governor);
                memory_denials_before = wl_columnar_memory_denials(governor);
                CHECK(available > 1 && wl_columnar_memory_reserve(governor,
                    available - 1, &memory_peer));
            }
            const wl_col_session_t *session = attempt->session;
            uint64_t identity = (uint64_t)(uintptr_t)session->controlled_step;
            CHECK(wl_columnar_session_controlled_batch_authorized(session,
                attempt->control, attempt->attempt_id, identity,
                selected_plan, selected_bindings));
            wl_plan_relation_t copied_plan = *selected_plan;
            wl_columnar_eval_private_bindings_t copied_bindings =
                *selected_bindings;
            CHECK(!wl_columnar_session_controlled_batch_authorized(session,
                attempt->control, attempt->attempt_id, identity,
                &copied_plan, selected_bindings));
            CHECK(!wl_columnar_session_controlled_batch_authorized(session,
                attempt->control, attempt->attempt_id, identity,
                selected_plan, &copied_bindings));
            CHECK(!wl_columnar_session_controlled_batch_authorized(session,
                attempt->control, attempt->attempt_id + 1, identity,
                selected_plan, selected_bindings));
            CHECK(!wl_columnar_session_controlled_batch_authorized(session,
                attempt->control, attempt->attempt_id, identity + 1,
                selected_plan, selected_bindings));
            if (cancel_body && interrupt_kind != 5 && !canceled_body) {
                canceled_body = true;
                if (interrupt_kind == 1) {
                    wl_evaluation_control_request_cancel(attempt->control);
                } else if (interrupt_kind == 2) {
                    CHECK(wl_evaluation_control_charge_attempt(attempt->control,
                        attempt->attempt_id, UINT64_C(100000001))
                        == WL_EVALUATION_CONTROL_WORK_LIMIT);
                } else {
                    end_original(attempt->control);
                }
            }
        }
        return;
    }
    CHECK(false);
}

static void
join_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)control;
    if ((phase == 1 || phase == 50) && path_child) ++owned_joins;
    if (!inline_position || !path_child || (phase != 18 && phase != 27)) return;
    wl_columnar_eval_serial_framed_attempt_t *r = actual_body();
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context_owned(r->frame, &ctx,
        &r->child)
        && ctx->join_operation == &selected_plan->ops[r->child.cursor]);
    unsigned mask = (inline_source(&ctx->source) ? 1u : 0u)
        | (inline_source(&ctx->join_right) ? 2u : 0u);
    inline_seen |= mask;
    if (mask && phase == 18 && progress == 1) ++inline_gathers;
    if ((mask & 2u) && phase == 27) ++inline_hashes;
}

static void
tuple(const char *name, const int64_t *row, uint32_t width, void *payload)
{
    capture(name, row, width, 1, payload);
}
static int
finish(wl_session_t *session)
{
    events_t *image = &snapshot_images[session == controlled_session ? 0 : 1];
    memset(image, 0, sizeof(*image));
    for (unsigned call = 0; call < 1000000; ++call) {
        int rc = output_route == 2 ? wl_session_snapshot(session, tuple, image)
            : wl_session_step(session);
        if (output_route == 2 && rc) {
            const events_t empty = {0};
            CHECK(!memcmp(image, &empty, sizeof(empty)));
        }
        if (rc != EAGAIN) return rc;
    }
    CHECK(false); return EIO;
}

static bool
contains(const pair_t *set, size_t count, unsigned x, unsigned y)
{
    for (size_t i = 0; i < count; ++i)
        if (set[i].x == (int64_t)x && set[i].y == (int64_t)y) return true;
    return false;
}

static void
content(wl_session_t *session, const char *name, const pair_t *set,
    size_t count)
{
    const col_rel_t *relation = session_find_rel(COL_SESSION(session), name);
    CHECK(relation && relation->ncols == 2 && relation->nrows == count);
    if (inline_position) CHECK(relation->compound_kind ==
            WIRELOG_COMPOUND_KIND_NONE
            && relation->column_types &&
            relation->column_types[0] == WIRELOG_TYPE_INT64
            && relation->column_types[1] == WIRELOG_TYPE_INT64);
    bool seen[8][8] = {{false}};
    for (uint32_t r = 0; r < relation->nrows; ++r) {
        int64_t x = col_rel_get(relation, r, 0),
            y = col_rel_get(relation, r, 1);
        CHECK(x > 0 && x < 8 && y > 0 && y < 8 && !seen[x][y]
            && contains(set, count, (unsigned)x, (unsigned)y));
        seen[x][y] = true;
    }
}

static void
events_match(const events_t *events, const pair_t *old, size_t old_count,
    const pair_t *now, size_t now_count, unsigned relation)
{
    for (unsigned x = 1; x < 8; ++x)
        for (unsigned y = 1; y < 8; ++y) {
            int diff = (int)contains(now, now_count, x, y)
                - (int)contains(old, old_count, x, y);
            CHECK(events->diff[relation][x][y] == diff
                && events->calls[relation][x][y] == (diff ? 1u : 0u));
        }
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2 || argc == 4);
    if (argc == 4) {
        inline_position = !strcmp(argv[2], "left") ? 1 : !strcmp(argv[2],
                "middle") ? 2 : 3;
        CHECK(inline_position != 3 || !strcmp(argv[2], "right"));
        output_route = !strcmp(argv[3], "observed") ? 0 : !strcmp(argv[3],
                "plain") ? 1 : 2;
        CHECK(output_route != 2 || !strcmp(argv[3], "snapshot"));
        CHECK(!strcmp(argv[1], "plain") || !strcmp(argv[1], "batch"));
    }
    wl_columnar_memory_reservation_init(&memory_peer);
    memory_mode = !strcmp(argv[1], "batch-memory-first")
        || !strcmp(argv[1], "batch-memory-successor");
    unsigned memory_stage = !strcmp(argv[1], "batch-memory-successor") ? 1 : 0;
    bool strict = !strcmp(argv[1], "strict");
    bool limited = !strcmp(argv[1], "limit");
    bool denied = !strcmp(argv[1], "limit-denied");
    interrupt_kind = !strcmp(argv[1], "batch-cancel") ? 1
        : !strcmp(argv[1], "batch-work") ? 2
        : !strcmp(argv[1], "batch-ended") ? 3
        : !strcmp(argv[1], "batch-successor") ? 4
        : !strcmp(argv[1], "batch-error-ended") ? 5 : 0;
    cancel_body = interrupt_kind != 0;
    bool batch = strcmp(argv[1], "plain") != 0;
    CHECK(!batch || !strcmp(argv[1], "batch") || strict || limited || denied
        || cancel_body || memory_mode);
    const int expected = strict ? ENOTSUP : denied ? EOVERFLOW : 0;
    environment("WIRELOG_JOIN_BATCH_BYTES", batch ? "256" : NULL);
    environment("WIRELOG_JOIN_BATCH_STRICT", strict ? "1" : NULL);
    environment("WIRELOG_JOIN_OUTPUT_LIMIT", denied ? "1" : limited ? "128"
        : NULL);
    wirelog_error_t error;
    const char *rules[] = {
        "path(x,z):-edge(pair(t,v),x,y),path(y,u),path(u,w),path(w,z).\n",
        "path(x,z):-path(x,y),edge(pair(t,v),y,u),path(u,w),path(w,z).\n",
        "path(x,z):-path(x,y),path(y,u),path(u,w),edge(pair(t,v),w,z).\n"
    };
    char text[1024];
    if (inline_position) snprintf(text, sizeof(text),
            ".decl edge(p:pair(symbol,float) inline,x:int64,y:int64)\n"
            ".decl path(x:int64,y:int64)\n.decl plain(x:int64,y:int64)\n"
            "plain(x,y):-edge(pair(t,v),x,y).\npath(x,y):-edge(pair(t,v),x,y).\n%s",
            rules[inline_position - 1]);
    else snprintf(text, sizeof(text),
            ".decl edge(x:int64,y:int64)\n.decl path(x:int64,y:int64)\n"
            ".decl plain(x:int64,y:int64)\nplain(x,y):-edge(x,y).\n"
            "path(x,y):-edge(x,y).\npath(x,z):-path(x,y),path(y,w),path(w,z).\n");
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    CHECK(program);
    wl_plan_t *plan = NULL;
    CHECK(!wl_plan_from_program(program, &plan));
    const wl_plan_body_t *body = NULL;
    for (uint32_t s = 0; s < plan->stratum_count; ++s)
        for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
            if (!strcmp(plan->strata[s].relations[r].name, "path"))
                body = &plan->strata[s].bodies[r];
    CHECK(body && body->lineage == WL_PLAN_BODY_LINEAGE_K_FUSION_EQUIVALENT
        && body->equivalent_k == 3 && body->equivalent_k_fusion_opaque);
    const void *identity = body->equivalent_k_fusion_opaque;
    if (inline_position) {
        const wl_plan_op_k_fusion_t *expanded = identity;
        unsigned hints = 0;
        CHECK(expanded->k == 3 && expanded->k_ops && expanded->k_op_counts);
        for (uint32_t k = 0; k < expanded->k; ++k)
            for (uint32_t op = 0; op < expanded->k_op_counts[k]; ++op)
                if (expanded->k_ops[k][op].materialized) {
                    CHECK(expanded->k_ops[k][op].op == WL_PLAN_OP_JOIN);
                    ++hints;
                }
        CHECK(hints);
        input_tag = wl_intern_put(plan->intern, "materialized-inline");
        input_float = wl_columnar_float_to_bits(1.5);
    }
    wl_evaluation_control_t *control = NULL;
    CHECK(!wl_evaluation_control_create(UINT64_C(100000000), &control));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(128) << 20,
            .usable_bytes = UINT64_C(128) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(governor);
    wl_session_options_t options; wl_session_options_init(&options);
    options.memory_governor = governor; options.evaluation_control = control;
    wl_session_t *sessions[2] = {NULL, NULL};
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &sessions[0]));
    controlled_session = sessions[0];
    options.evaluation_control = NULL;
    CHECK(!wl_session_create_with_options(wl_backend_columnar(), plan, 1,
        &options, &sessions[1]));
    environment("WIRELOG_JOIN_BATCH_BYTES", NULL);
    environment("WIRELOG_JOIN_BATCH_STRICT", NULL);
    environment("WIRELOG_JOIN_OUTPUT_LIMIT", NULL);
    events_t events[2] = {0};
    const int64_t rows[] = {1, 2, 2, 3, 3, 4, 4, 5, 5, 6};
    int64_t physical[20];
    for (unsigned i = 0; i < 5; ++i) {
        physical[4*i] = input_tag; physical[4*i+1] = input_float;
        physical[4*i+2] = edges0[i].x; physical[4*i+3] = edges0[i].y;
    }
    for (unsigned i = 0; i != 2; ++i) {
        if (!output_route) wl_session_set_delta_cb(sessions[i], capture,
                &events[i]);
        CHECK(!wl_session_insert(sessions[i], "edge",
            inline_position ? physical : rows,
            5, inline_position ? 4 : 2));
    }
    const pair_t *path[] = {inline_position ? inline_initial : initial,
                            inline_position ? inline_inserted : inserted,
                            inline_position ? edges2 : deleted};
    const size_t path_count[] = {inline_position ? 7u : 9u,
                                 inline_position ? 9u : 12u,
                                 inline_position ? 5u : 6u};
    const pair_t *edge[] = {edges0, edges1, edges2};
    const size_t edge_count[] = {5, 6, 5};
    for (unsigned stage = 0; stage != 3; ++stage) {
        int raw_rc = finish(sessions[1]);
        if (raw_rc != expected) fprintf(stderr, "raw stage=%u batch=%u rc=%d\n",
                stage, batch, raw_rc);
        CHECK(raw_rc == expected);
        history_stage = stage;
        inline_seen = inline_gathers = inline_hashes = inline_prepared =
            inline_produced = inline_acks = inline_positive_acks = inline_done =
            0;
        inline_batch_op = NULL; inline_live_prepared = 0;
        owned_joins = 0; path_child = selected_body = whole_scc = false;
        selected_plan = NULL;
        selected_bindings = NULL;
        wl_columnar_session_test_observed_transaction_checkpoint = observed;
        wl_columnar_join_test_attempt_checkpoint = join_checkpoint;
        wl_columnar_join_batch_test_prepare_checkpoint = batch_prepare;
        wl_columnar_join_pipeline_test_first_error = first_producer_error;
        wl_columnar_join_pipeline_test_numeric_checkpoint =
            inline_position ? batch_event : NULL;
        memory_armed = memory_mode && stage == memory_stage;
        int rc = finish(sessions[0]);
        if (memory_armed) {
            CHECK(memory_denied && rc == ENOSPC && selected_body && whole_scc
                && owned_joins && atomic_load_explicit(&memory_peer.state,
                memory_order_relaxed) == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
                && COL_SESSION(sessions[0])->memory_budget_denied
                && COL_SESSION(sessions[0])->pending_input_change);
            const events_t empty = {0};
            CHECK(!memcmp(&events[0], &empty, sizeof(empty)));
            if (stage) {
                content(sessions[0], "path", path[stage - 1],
                    path_count[stage - 1]);
                content(sessions[0], "plain", edge[stage - 1],
                    edge_count[stage - 1]);
            } else {
                const col_rel_t *failed =
                    session_find_rel(COL_SESSION(sessions[0]), "path");
                CHECK(!failed || !failed->nrows);
                failed = session_find_rel(COL_SESSION(sessions[0]), "plain");
                CHECK(!failed || !failed->nrows);
            }
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(control, &report)
                && report.result == ENOSPC && report.consumed
                && !report.cleanup_status);
            memory_armed = false;
            CHECK(!wl_evaluation_control_reset(control));
            rc = finish(sessions[0]);
        }
        if (cancel_body && !stage) {
            int stopped = interrupt_kind == 1 ? WL_EVALUATION_CONTROL_CANCELLED
                : interrupt_kind == 2 ? WL_EVALUATION_CONTROL_WORK_LIMIT
                : interrupt_kind == 5 ? ENOMEM : 7;
            if (rc != stopped) fprintf(stderr,
                    "body interruption=%u actual=%d expected=%d successor=%llu\n",
                    interrupt_kind, rc, stopped,
                    (unsigned long long)successor_id);
            CHECK(canceled_body && rc == stopped
                && selected_body && whole_scc
                && (interrupt_kind == 5 ? producer_error_armed : owned_joins));
            const events_t empty = {0};
            CHECK(!memcmp(&events[0], &empty, sizeof(empty)));
            const col_rel_t *failed = session_find_rel(COL_SESSION(sessions[0]),
                    "path");
            CHECK(!failed || !failed->nrows);
            failed = session_find_rel(COL_SESSION(sessions[0]), "plain");
            CHECK(!failed || !failed->nrows);
            if (interrupt_kind >= 3) {
                wl_evaluation_control_report_t report;
                CHECK(!wl_evaluation_control_report(control, &report));
                CHECK(report.attempt_id == frozen_report.attempt_id
                    && report.limit == frozen_report.limit
                    && report.consumed == frozen_report.consumed
                    && report.stop_reason == frozen_report.stop_reason
                    && report.execution_status == frozen_report.execution_status
                    && report.cleanup_status == frozen_report.cleanup_status
                    && report.result == frozen_report.result);
                if (successor_id) {
                    uint64_t used = UINT64_MAX;
                    CHECK(!wl_evaluation_control_test_active_consumed(control,
                        successor_id, &used) && !used);
                    CHECK(!wl_evaluation_control_finish(control,
                        sessions[0]->operation_admission, 0, 0));
                }
            }
            CHECK(!wl_evaluation_control_reset(control));
            rc = finish(sessions[0]);
        }
        if (rc != expected || !selected_body || !whole_scc
            || (!expected && !owned_joins))
            fprintf(stderr,
                "stage=%u batch=%u rc=%d body=%u scc=%u joins=%u c1=%u\n",
                stage, batch, rc, selected_body, whole_scc, owned_joins,
                sessions[0]->source_c1_admission);
        CHECK(rc == expected && selected_body && whole_scc
            && (expected || owned_joins));
        wl_columnar_session_test_observed_transaction_checkpoint = NULL;
        wl_columnar_join_test_attempt_checkpoint = NULL;
        wl_columnar_join_batch_test_prepare_checkpoint = NULL;
        wl_columnar_join_pipeline_test_first_error = NULL;
        wl_columnar_join_pipeline_test_numeric_checkpoint = NULL;
        if (inline_position && !expected) {
            CHECK(inline_seen == (inline_position == 1 ? 1u : 2u));
            if (inline_position == 3 && batch) {
                CHECK(inline_prepared && inline_live_prepared &&
                    inline_produced && inline_done);
                if (history_stage < 2) CHECK(inline_acks &&
                        inline_positive_acks);
            } else CHECK(history_stage < 2 ? inline_gathers : inline_gathers ||
                    inline_hashes);
        }
        CHECK(body->equivalent_k_fusion_opaque == identity);
        if (expected) {
            const events_t empty = {0};
            CHECK(!memcmp(&events[0], &empty, sizeof(empty)));
            const col_rel_t *failed = session_find_rel(COL_SESSION(sessions[0]),
                    "path");
            CHECK(!failed || !failed->nrows);
            failed = session_find_rel(COL_SESSION(sessions[0]), "plain");
            CHECK(!failed || !failed->nrows);
            break;
        }
        for (unsigned i = 0; i != 2; ++i) {
            content(sessions[i], "path", path[stage], path_count[stage]);
            content(sessions[i], "plain", edge[stage], edge_count[stage]);
            if (!output_route) {
                events_match(&events[i], stage ? path[stage-1] : NULL,
                    stage ? path_count[stage-1] : 0, path[stage],
                    path_count[stage], 0);
                events_match(&events[i], stage ? edge[stage-1] : NULL,
                    stage ? edge_count[stage-1] : 0, edge[stage],
                    edge_count[stage], 1);
            } else if (output_route == 2) {
                events_match(&snapshot_images[i], NULL, 0, path[stage],
                    path_count[stage], 0);
                events_match(&snapshot_images[i], NULL, 0, edge[stage],
                    edge_count[stage], 1);
            }
            memset(&events[i], 0, sizeof(events[i]));
        }
        /* A real unchanged public call after publication must retain the
         * complete image and deliver no duplicate signed event. */
        for (unsigned i = 0; i != 2; ++i) {
            CHECK(!finish(sessions[i]));
            content(sessions[i], "path", path[stage], path_count[stage]);
            content(sessions[i], "plain", edge[stage], edge_count[stage]);
            const events_t empty = {0};
            CHECK(!memcmp(&events[i], &empty, sizeof(empty)));
            if (output_route == 2) {
                events_match(&snapshot_images[i], NULL, 0, path[stage],
                    path_count[stage], 0);
                events_match(&snapshot_images[i], NULL, 0, edge[stage],
                    edge_count[stage], 1);
            }
            int64_t next[] = {input_tag, input_float, 6, 7},
                removed[] = {input_tag, input_float, 3, 4};
            if (!stage) CHECK(!wl_session_insert(sessions[i], "edge",
                    inline_position ? next : &next[2], 1,
                    inline_position ? 4 : 2));
            if (stage == 1) CHECK(!wl_session_remove(sessions[i], "edge",
                    inline_position ? removed : &removed[2], 1,
                    inline_position ? 4 : 2));
        }
    }
    CHECK(!memory_mode || memory_denied);
    CHECK(atomic_load_explicit(&memory_peer.state, memory_order_relaxed)
        == (memory_mode ? WL_COLUMNAR_MEMORY_RESERVATION_RELEASED
            : WL_COLUMNAR_MEMORY_RESERVATION_EMPTY));
    for (unsigned i = 0; i != 2;
        ++i) CHECK(!wl_session_try_destroy(&sessions[i]));
    wl_evaluation_control_release(control); wl_plan_free(plan);
    wirelog_program_free(program);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            governor)));
    wl_columnar_memory_governor_ref_release(governor);
    puts(expected ? "materialized preserved body: exact policy refusal passed"
        : inline_position ?
        "preserved K3 INLINE body: literal 7/9/5 history passed"
        : "materialized preserved body: literal 9/12/6 signed history passed");
    return 0;
}
