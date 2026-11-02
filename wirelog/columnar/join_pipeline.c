#include "columnar/join_pipeline.h"
#include "columnar/join_batch.h"

#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/* Row-local numeric bytecode: variables, literals and comparisons, a pure
 * function of the current row that reads no intern, extension or session
 * state.  MAP (Issue #1777) also admits integer and float arithmetic, whose
 * checked failures surface as the ordinary MAP's ERANGE; FILTER does not. */
/* Both the synchronous raw classifier and the controlled cursor use this
 * one-opcode semantic leaf. Callers prove a nonempty current byte position. */
static bool
wl_columnar_join_pipeline_expr_next(const wl_plan_expr_buffer_t *expr,
    bool allow_arithmetic, uint32_t *position, uint32_t *stack_depth)
{
    const uint8_t *data = expr->data;
    uint32_t pos = *position, depth = *stack_depth;
    uint8_t tag = data[pos++];
    uint32_t payload = 0;

    switch ((wl_plan_expr_tag_t)tag) {
    case WL_PLAN_EXPR_VAR:
    case WL_PLAN_EXPR_VAR_FLOAT:
        if (expr->size - pos < 2u)
            return false;
        uint16_t name_len;
        memcpy(&name_len, data + pos, sizeof(name_len));
        payload = 2u + (uint32_t)name_len;
        if (payload > expr->size - pos)
            return false;
        pos += payload;
        depth++;
        break;
    case WL_PLAN_EXPR_CONST_INT:
    case WL_PLAN_EXPR_CONST_FLOAT:
        if (expr->size - pos < 8u)
            return false;
        pos += 8u;
        depth++;
        break;
    case WL_PLAN_EXPR_BOOL:
        if (expr->size - pos < 1u)
            return false;
        pos++;
        depth++;
        break;
    case WL_PLAN_EXPR_CMP_EQ:
    case WL_PLAN_EXPR_CMP_NEQ:
    case WL_PLAN_EXPR_CMP_LT:
    case WL_PLAN_EXPR_CMP_GT:
    case WL_PLAN_EXPR_CMP_LTE:
    case WL_PLAN_EXPR_CMP_GTE:
    case WL_PLAN_EXPR_CMP_FLOAT_EQ:
    case WL_PLAN_EXPR_CMP_FLOAT_NEQ:
    case WL_PLAN_EXPR_CMP_FLOAT_LT:
    case WL_PLAN_EXPR_CMP_FLOAT_GT:
    case WL_PLAN_EXPR_CMP_FLOAT_LTE:
    case WL_PLAN_EXPR_CMP_FLOAT_GTE:
        if (depth < 2u)
            return false;
        depth--;
        break;
    case WL_PLAN_EXPR_ARITH_ADD:
    case WL_PLAN_EXPR_ARITH_SUB:
    case WL_PLAN_EXPR_ARITH_MUL:
    case WL_PLAN_EXPR_ARITH_DIV:
    case WL_PLAN_EXPR_ARITH_MOD:
    case WL_PLAN_EXPR_ARITH_BAND:
    case WL_PLAN_EXPR_ARITH_BOR:
    case WL_PLAN_EXPR_ARITH_BXOR:
    case WL_PLAN_EXPR_ARITH_SHL:
    case WL_PLAN_EXPR_ARITH_SHR:
    case WL_PLAN_EXPR_ARITH_FLOAT_ADD:
    case WL_PLAN_EXPR_ARITH_FLOAT_SUB:
    case WL_PLAN_EXPR_ARITH_FLOAT_MUL:
    case WL_PLAN_EXPR_ARITH_FLOAT_DIV:
        if (!allow_arithmetic || depth < 2u)
            return false;
        depth--;
        break;
    case WL_PLAN_EXPR_ARITH_BNOT:
        if (!allow_arithmetic || depth < 1u)
            return false;
        break;
    default:
        /* Strings, digests, UUIDs, aggregates, extensions and unknown
         * tags stay on the materialized path until their batch contract
         * is specified. */
        return false;
    }
    *position = pos; *stack_depth = depth;
    return true;
}

static bool
wl_columnar_join_pipeline_expr_is_row_local(
    const wl_plan_expr_buffer_t *expr, bool allow_arithmetic,
    wl_evaluation_control_t *control, uint64_t id, int *error)
{
    uint32_t pos = 0, depth = 0;
    if (!expr || !expr->data || !expr->size) return false;
    while (pos < expr->size) {
        if (control) {
            *error = wl_evaluation_control_charge_attempt(control, id, 1);
            if (*error) return false;
        }
        if (!wl_columnar_join_pipeline_expr_next(expr, allow_arithmetic,
            &pos, &depth)) return false;
    }
    return depth == 1u;
}

/* Whether @map is a row-local MAP the pipeline can evaluate per batch. */
static bool
wl_columnar_join_pipeline_map_is_consumable(const wl_plan_op_t *map,
    wl_evaluation_control_t *control, uint64_t id, int *error)
{
    if (map->project_count == 0 || map->materialized)
        return false;
    if (map->map_expr_count == 0)
        return map->map_exprs == NULL && map->project_indices != NULL;
    if (!map->map_exprs)
        return false;
    /* An empty slot projects its column, as in col_op_map(). */
    for (uint32_t c = 0; c < map->map_expr_count; c++) {
        if (control) {
            *error = wl_evaluation_control_charge_attempt(control, id, 1);
            if (*error) return false;
        }
        const wl_plan_expr_buffer_t *expr = &map->map_exprs[c];
        if ((expr->data && expr->size > 0)
            && !wl_columnar_join_pipeline_expr_is_row_local(expr, true, control,
            id, error))
            return false;
    }
    return true;
}

/* Whether @op is a SEMIJOIN/ANTIJOIN probe stage the pipeline can apply
 * per batch, judged from the plan alone: keyed, no right filter, not
 * materialized, and reading a named relation that is not an evaluator
 * temporary ("$" prefix: $d$, $r$ and the like). */
static bool
wl_columnar_join_pipeline_stage_is_consumable(const wl_plan_op_t *op)
{
    return op->key_count > 0 && op->right_filter_expr.size == 0
           && !op->materialized && op->right_relation
           && op->right_relation[0] != '$';
}

static wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight_header(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *sess)
{
    if (!plan || !sess || join_index >= plan->op_count)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
    if (!plan->ops)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
    if (sess->join_batch_bytes == 0)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF;
    if (sess->diff_operators_active)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION;

    const wl_plan_op_t *join = &plan->ops[join_index];
    if (join->op != WL_PLAN_OP_JOIN || join->key_count == 0
        || join->right_filter_expr.size != 0 || join->materialized)
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN;

    return WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
}

static wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight_internal(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *sess, uint32_t *tail_end,
    wl_evaluation_control_t *control, uint64_t id, int *error)
{
    uint32_t i;

    if (tail_end)
        *tail_end = UINT32_MAX;
    wl_columnar_join_pipeline_eligibility_t header =
        wl_columnar_join_pipeline_preflight_header(plan, join_index, sess);
    if (header != WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE) return header;

    /* The tail is the longest prefix of consumable operators: row-local
     * FILTERs, optionally ended by one row-local MAP. */
    i = join_index + 1u;
    while (i < plan->op_count) {
        if (control) {
            *error = wl_evaluation_control_charge_attempt(control, id, 1);
            if (*error) return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
        }
        const wl_plan_op_t *op = &plan->ops[i];
        if (op->op == WL_PLAN_OP_FILTER) {
            if (op->materialized
                || !wl_columnar_join_pipeline_expr_is_row_local(
                    &op->filter_expr, false, control, id, error))
                break;
            i++;
            continue;
        }
        if (op->op == WL_PLAN_OP_SEMIJOIN || op->op == WL_PLAN_OP_ANTIJOIN) {
            /* A probe stage resolves its operand by name, which a worker
             * run that resolves its operands by input slot need not hold
             * under that name, so there the tail ends before the stage
             * (#2201). */
            if (sess->tdd_input_run
                || !wl_columnar_join_pipeline_stage_is_consumable(op))
                break;
            i++;
            continue;
        }
        if (op->op == WL_PLAN_OP_MAP
            && wl_columnar_join_pipeline_map_is_consumable(op, control, id,
            error))
            i++;
        break;
    }
    if (i == join_index + 1u) {
        /* Empty prefix: report why the first operator is not consumed. */
        if (i < plan->op_count && plan->ops[i].op == WL_PLAN_OP_FILTER)
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER;
        if (i < plan->op_count && plan->ops[i].op == WL_PLAN_OP_MAP)
            return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP;
        return WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE;
    }
    if (tail_end)
        *tail_end = i - 1u;
    return WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
}

wl_columnar_join_pipeline_eligibility_t
wl_columnar_join_pipeline_preflight(const wl_plan_relation_t *plan,
    uint32_t join_index, const wl_col_session_t *session, uint32_t *tail_end)
{
    int error = 0;
    return wl_columnar_join_pipeline_preflight_internal(plan, join_index,
               session, tail_end, NULL, 0, &error);
}

const char *
wl_columnar_join_pipeline_eligibility_name(
    wl_columnar_join_pipeline_eligibility_t reason)
{
    switch (reason) {
    case WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE: return "eligible";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF: return "off";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION: return "session-dependent";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN: return "join-ineligible";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SHAPE: return "unsupported-shape";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_FILTER: return
            "filter-not-row-local";
    case WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_MAP: return "map-not-row-local";
    default: return "unknown";
    }
}

/* A SEMIJOIN/ANTIJOIN stage: its right relation as found at setup, the
 * key columns resolved against the stage input, and the probe set, built
 * on the first batch that reaches the stage. */
typedef struct {
    bool active;
    col_rel_t *right;
    uint32_t *lk;
    uint32_t *rk;
    uint64_t identity;
    uint64_t view_generation;
    uint64_t storage_generation;
    wl_columnar_join_probe_set_t set;
} wl_join_pipeline_stage_t;

static void
pipeline_stages_release(wl_join_pipeline_stage_t *stages, uint32_t count)
{
    if (!stages)
        return;
    for (uint32_t i = 0; i < count; i++) {
        wl_columnar_join_probe_set_release(&stages[i].set);
        free(stages[i].lk);
        free(stages[i].rk);
    }
    free(stages);
}

typedef struct {
    wl_col_session_t *sess;
    const wl_plan_relation_t *plan;
    uint32_t first_tail;
    uint32_t tail_end;
    wl_join_pipeline_stage_t *stages; /* indexed by op - first_tail */
    col_rel_t *out;
    uint32_t pending_begin;
    int error; /* errno of a failed FILTER/MAP, reported unchanged */
    bool begun;
} wl_join_pipeline_sink_t;

static wl_columnar_continuation_status_t
pipeline_sink_begin(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    if (!sink || !sink->out || !batch || !batch->payload || batch->rows == 0)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    sink->pending_begin = sink->out->nrows;
    sink->begun = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
pipeline_sink_reserve(void *context, uint64_t bytes, uint32_t rows)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;

    (void)bytes;
    wl_columnar_continuation_status_t status;
    uint64_t need;

    if (!sink || !sink->begun || !sink->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    need = (uint64_t)sink->out->nrows + rows;
    /* A representation limit, not a memory failure (#1382). */
    if (need > UINT32_MAX) {
        sink->error = EOVERFLOW;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    /* Same growth rule as the relation sink (Issue #2173): double first,
     * exact on a budget denial. */
    status = col_join_batch_sink_grow(sink->out, need);
    /* The publish loop destroys the output on failure, so a denied growth
     * is reported through the sink's error as a budget denial (#2185). */
    if (status == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED)
        sink->error = wl_columnar_join_budget_denial(sink->sess, NULL,
                ENOSPC);
    return status;
}

#ifdef WL_TEST_RELATION_RESIZE_HOOK
wl_columnar_continuation_status_t
wl_columnar_join_pipeline_test_reserve(col_rel_t *out, uint32_t rows,
    int *error_out)
{
    wl_join_pipeline_sink_t sink = { .out = out, .begun = true };
    wl_columnar_continuation_status_t status
        = pipeline_sink_reserve(&sink, 0, rows);
    if (error_out)
        *error_out = sink.error;
    return status;
}
#endif

/* Apply one probe stage to the batch on top of @batch_stack.  EAGAIN when
 * the stage's right relation changed since setup. */
static int
pipeline_apply_stage(wl_join_pipeline_sink_t *sink, const wl_plan_op_t *op,
    wl_join_pipeline_stage_t *stage, eval_stack_t *batch_stack)
{
    eval_entry_t input;
    col_rel_t *output = NULL;
    int rc;

    if (stage->right->relation_identity != stage->identity
        || stage->right->view_generation != stage->view_generation
        || stage->right->storage_generation != stage->storage_generation
        || stage->view_generation == UINT64_MAX
        || stage->storage_generation == UINT64_MAX)
        return EAGAIN;
    rc = eval_stack_pop_relation(batch_stack, &input);
    if (rc != 0)
        return rc;
    if (!stage->set.built) {
        rc = wl_columnar_join_probe_set_build(sink->sess,
                wl_columnar_join_output_governor(sink->sess, input.rel,
                stage->right), stage->right, stage->rk, op->key_count,
                op->op == WL_PLAN_OP_SEMIJOIN
                ? WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS
                : WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS_PLUS_ONE, &stage->set);
        if (rc != 0) {
            (void)eval_entry_dispose(&input);
            return rc;
        }
    }
    rc = op->op == WL_PLAN_OP_SEMIJOIN
        ? wl_columnar_semijoin_emit(op, sink->sess, input.rel, stage->right,
            stage->lk, stage->rk, &stage->set, &output)
        : wl_columnar_antijoin_emit(op, sink->sess, input.rel, stage->right,
            stage->lk, stage->rk, &stage->set, &output);
    (void)eval_entry_dispose(&input);
    if (rc != 0)
        return rc;
    rc = eval_stack_push(batch_stack, output, true);
    if (rc != 0)
        col_rel_destroy(output);
    return rc;
}

static wl_columnar_continuation_status_t
pipeline_sink_append(void *context,
    const wl_columnar_continuation_batch_t *batch)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    eval_stack_t batch_stack;
    eval_entry_t result;
    int rc;

    if (!sink || !sink->begun || !sink->out || !batch || !batch->payload)
        return WL_COLUMNAR_CONTINUATION_INVALID;

    eval_stack_init(&batch_stack);
    rc = eval_stack_push(&batch_stack, (col_rel_t *)batch->payload, false);
    if (rc != 0) {
        sink->error = rc;
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    for (uint32_t i = sink->first_tail; i <= sink->tail_end; i++) {
        const wl_plan_op_t *op = &sink->plan->ops[i];
        if (op->op == WL_PLAN_OP_SEMIJOIN || op->op == WL_PLAN_OP_ANTIJOIN)
            rc = pipeline_apply_stage(sink, op,
                    &sink->stages[i - sink->first_tail], &batch_stack);
        else
            rc = op->op == WL_PLAN_OP_MAP
                ? col_op_map(op, &batch_stack, sink->sess)
                : wl_columnar_filter_op(op, &batch_stack, sink->sess);
        if (rc != 0) {
            (void)eval_stack_drain(&batch_stack);
            sink->error = rc;
            return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
        }
    }
    rc = eval_stack_pop_relation(&batch_stack, &result);
    if (rc != 0 || !result.rel) {
        (void)eval_stack_drain(&batch_stack);
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    }
    if (result.rel->ncols != sink->out->ncols
        || result.rel->nrows > batch->rows
        || (uint64_t)sink->pending_begin + result.rel->nrows
        > sink->out->capacity) {
        (void)eval_entry_dispose(&result);
        return WL_COLUMNAR_CONTINUATION_INVALID;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        uint32_t dst = sink->pending_begin + row;
        for (uint32_t col = 0; col < result.rel->ncols; col++) {
            rc = col_rel_set_raw(sink->out, dst, col,
                    result.rel->columns[col][row]);
            if (rc != 0) {
                (void)eval_entry_dispose(&result);
                return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
            }
        }
    }
    sink->out->nrows = sink->pending_begin + result.rel->nrows;
    (void)eval_entry_dispose(&result);
    return WL_COLUMNAR_CONTINUATION_OK;
}

static wl_columnar_continuation_status_t
pipeline_sink_commit(void *context, bool *committed)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    if (committed)
        *committed = false;
    if (!sink || !sink->begun || !sink->out)
        return WL_COLUMNAR_CONTINUATION_INVALID;
    wl_columnar_relation_touch_view(sink->out);
    sink->begun = false;
    if (committed)
        *committed = true;
    return WL_COLUMNAR_CONTINUATION_OK;
}

static void
pipeline_sink_abort(void *context)
{
    wl_join_pipeline_sink_t *sink = (wl_join_pipeline_sink_t *)context;
    if (sink && sink->out && sink->begun)
        sink->out->nrows = sink->pending_begin;
    if (sink)
        sink->begun = false;
}

static int
pipeline_resolve_key(const col_rel_t *rel, const char *name, uint32_t *out)
{
    int index;
    if (!rel || !out || !name)
        return EINVAL;
    index = col_rel_col_idx(rel, name);
    /* The ordinary join operator preserves its historical column-0 fallback
     * for plans whose symbolic names were lowered without relation schemas.
     * Keep the pipeline on that same resolution contract. */
    *out = index >= 0 ? (uint32_t)index : 0u;
    return 0;
}

int
wl_columnar_join_pipeline_key_attempt(
    const wl_columnar_relation_scalar_source_t *source, const char *name,
    uint32_t *out, wl_evaluation_control_t *control, uint64_t id)
{
    if (!out) return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(control, id, 1);
    if (rc) return rc;
    *out = 0;
    rc = wl_columnar_relation_scalar_source_validate_attempt_controlled(source,
            control, id);
    if (rc) return rc;
    if (!name) return EINVAL;
    if (!source->names) return 0;
    for (uint32_t c = 0; c < source->width; ++c) {
        rc = wl_evaluation_control_charge_attempt(control, id, 1);
        if (rc) return rc;
        const char *actual = source->names[c];
        if (!actual) continue;
        for (size_t byte = 0; ; ++byte) {
            rc = wl_evaluation_control_charge_attempt(control, id, 1);
            if (rc) return rc;
            if (name[byte] != actual[byte]) break;
            if (!name[byte]) {
                /* Match the raw lookup's signed index and caller fallback. */
                *out = (int)c >= 0 ? (uint32_t)(int)c : 0;
                return 0;
            }
        }
    }
    for (unsigned byte = 0; byte < 3; ++byte) {
        rc = wl_evaluation_control_charge_attempt(control, id, 1);
        if (rc) return rc;
        if (name[byte] != "col"[byte]) return 0;
    }
    /* Match the host platform's strtol base10 saturation and the raw
     * lookup's uint32 range test before its int result conversion. This
     * includes unresolved overflowing/non-ASCII suffixes and LP64 wraps;
     * no library parser re-traverses an unbounded suffix between polls. */
    unsigned long magnitude = 0;
    bool initial = true, negative = false, overflow = false;
    for (size_t byte = 3; ; ++byte) {
        rc = wl_evaluation_control_charge_attempt(control, id, 1);
        if (rc) return rc;
        unsigned char ch = (unsigned char)name[byte];
        if (initial && isspace(ch)) continue;
        if (initial && (ch == '+' || ch == '-')) {
            initial = false; negative = ch == '-'; continue;
        }
        if (ch >= '0' && ch <= '9') {
            initial = false;
            unsigned digit = ch - '0';
            unsigned long limit = negative ? (unsigned long)LONG_MAX + 1ul
                : (unsigned long)LONG_MAX;
            if (!overflow) {
                if (magnitude > (limit - digit) / 10ul) overflow = true;
                else magnitude = magnitude * 10ul + digit;
            }
            continue;
        }
        if (!ch && (!negative || !magnitude)) {
            unsigned long value =
                overflow ? (unsigned long)LONG_MAX : magnitude;
            if ((uint32_t)value < source->width && (int)value >= 0)
                *out = (uint32_t)(int)value;
        }
        return 0;
    }
}

wl_columnar_join_successor_t
wl_columnar_join_pipeline_successor(const wl_plan_relation_t *plan,
    uint32_t first_unconsumed)
{
    /* Depth of the tracked entry below the top of the stack. */
    uint32_t depth = 0;

    if (!plan || (!plan->ops && plan->op_count > 0))
        return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
    for (uint32_t i = first_unconsumed; i < plan->op_count; i++) {
        const wl_plan_op_t *op = &plan->ops[i];
        switch (op->op) {
        case WL_PLAN_OP_VARIABLE:
        case WL_PLAN_OP_LFTJ:
        case WL_PLAN_OP_K_FUSION:
            /* Push only. */
            depth++;
            break;
        case WL_PLAN_OP_MAP:
        case WL_PLAN_OP_FILTER:
        case WL_PLAN_OP_JOIN:
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
        case WL_PLAN_OP_REDUCE:
            /* Pop one, push one: reads the tracked entry only at the top. */
            if (depth == 0)
                return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
            break;
        case WL_PLAN_OP_CONSOLIDATE:
            break;
        /* CONCAT pops two and pushes their union; with fewer than two
         * entries it passes the stack through, which leaves depth 0. */
        case WL_PLAN_OP_CONCAT:
            depth = depth <= 1u ? 0u : depth - 1u;
            break;
        case WL_PLAN_OP_EXCHANGE: {
            const wl_plan_op_exchange_t *meta
                = (const wl_plan_op_exchange_t *)op->opaque_data;
            if (!meta)
                return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
            if (meta->num_workers <= 1u)
                break;
            /* Pops the top into the exchange buffers and pushes nothing. */
            if (depth == 0)
                return WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK;
            depth--;
            break;
        }
        default:
            return WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING;
        }
    }
    return WL_COLUMNAR_JOIN_SUCCESSOR_RELATION_SINK;
}

/* Walk the statically consumable tail and set up its probe stages.  A
 * stage whose right relation is missing, or whose keys do not resolve in
 * range with compatible types against the stage input, ends the tail
 * before it (*@tail_end is lowered).  On success with at least one stage,
 * *@stages holds one slot per tail operator and *@final_template the
 * schema of the last stage's output; both are NULL when the tail has no
 * stage.  Errors are the output constructors' or ENOMEM. */
static int
pipeline_setup_stages(const wl_plan_relation_t *plan, uint32_t join_index,
    uint32_t *tail_end, wl_col_session_t *sess, const col_rel_t *left,
    const col_rel_t *right, const wl_plan_op_t *join,
    wl_join_pipeline_stage_t **stages_out, col_rel_t **final_template)
{
    wl_join_pipeline_stage_t *stages;
    col_rel_t *tpl = NULL;
    uint32_t count = *tail_end - join_index;
    bool any = false;
    int rc;

    *stages_out = NULL;
    *final_template = NULL;
    for (uint32_t i = join_index + 1u; i <= *tail_end; i++) {
        if (plan->ops[i].op == WL_PLAN_OP_SEMIJOIN
            || plan->ops[i].op == WL_PLAN_OP_ANTIJOIN)
            any = true;
    }
    if (!any)
        return 0;
    stages = (wl_join_pipeline_stage_t *)calloc(count, sizeof(*stages));
    if (!stages)
        return ENOMEM;
    tpl = col_rel_new_auto("$join_pipeline_shape",
            col_join_output_width(left, right, join));
    rc = tpl ? col_join_set_output_types(tpl, left, right, join) : ENOMEM;
    if (rc != 0) {
        col_rel_destroy(tpl);
        free(stages);
        return rc;
    }
    for (uint32_t i = join_index + 1u; i <= *tail_end; i++) {
        const wl_plan_op_t *op = &plan->ops[i];
        wl_join_pipeline_stage_t *stage = &stages[i - join_index - 1u];
        col_rel_t *stage_right;
        col_rel_t *next = NULL;
        bool usable = true;

        if (op->op != WL_PLAN_OP_SEMIJOIN && op->op != WL_PLAN_OP_ANTIJOIN)
            continue; /* FILTER keeps its input's schema; a MAP ends it */
        stage_right = session_find_rel(sess, op->right_relation);
        if (stage_right) {
            stage->lk = (uint32_t *)calloc(op->key_count, sizeof(uint32_t));
            stage->rk = (uint32_t *)calloc(op->key_count, sizeof(uint32_t));
            if (!stage->lk || !stage->rk) {
                rc = ENOMEM;
                break;
            }
            for (uint32_t k = 0; k < op->key_count && usable; k++) {
                if (pipeline_resolve_key(tpl,
                    op->left_keys ? op->left_keys[k] : NULL,
                    &stage->lk[k]) != 0
                    || pipeline_resolve_key(stage_right,
                    op->right_keys ? op->right_keys[k] : NULL,
                    &stage->rk[k]) != 0
                    || stage->lk[k] >= tpl->ncols
                    || stage->rk[k] >= stage_right->ncols)
                    usable = false;
            }
            if (usable && !col_join_key_types_compatible(tpl, stage->lk,
                stage_right, stage->rk, op->key_count))
                usable = false;
        } else {
            usable = false;
        }
        if (!usable) {
            /* The operator runs on its own after the tail, as today. */
            free(stage->lk);
            free(stage->rk);
            stage->lk = NULL;
            stage->rk = NULL;
            *tail_end = i - 1u;
            break;
        }
        rc = op->op == WL_PLAN_OP_SEMIJOIN
            ? wl_columnar_semijoin_new_output(op, sess, tpl, stage_right,
                &next)
            : wl_columnar_antijoin_new_output(op, sess, tpl, stage_right,
                &next);
        if (rc != 0)
            break;
        col_rel_destroy(tpl);
        tpl = next;
        stage->active = true;
        stage->right = stage_right;
        stage->identity = stage_right->relation_identity;
        stage->view_generation = stage_right->view_generation;
        stage->storage_generation = stage_right->storage_generation;
    }
    if (rc != 0) {
        col_rel_destroy(tpl);
        pipeline_stages_release(stages, count);
        return rc;
    }
    *stages_out = stages;
    *final_template = tpl;
    return 0;
}

/* Release the reader try_eval holds on the right operand while it drains
 * the producer.  The release is mandatory, as for the producer's own. */
static void
pipeline_release_right_reader(wl_columnar_source_access_reader_t *reader)
{
    if (reader->owner && col_rel_source_reader_release(reader) != 0)
        abort();
}

int
wl_columnar_join_pipeline_try_eval(const wl_plan_relation_t *plan,
    uint32_t join_index, eval_stack_t *stack, wl_col_session_t *sess,
    uint32_t *next_index, wl_columnar_join_pipeline_decline_t *decline)
{
    wl_columnar_join_pipeline_eligibility_t eligibility;
    const wl_plan_op_t *join;
    const wl_plan_op_t *last;
    eval_entry_t left_entry;
    col_rel_t *right;
    wl_columnar_join_selection_t selection = { 0 };
    col_rel_t *out = NULL;
    wl_columnar_continuation_t *continuation = NULL;
    wl_columnar_source_access_reader_t right_reader = { 0 };
    wl_join_pipeline_sink_t sink_ctx;
    wl_columnar_continuation_sink_t sink;
    uint32_t tail_end = UINT32_MAX;
    uint32_t *lk = NULL;
    uint32_t *rk = NULL;
    wl_join_pipeline_stage_t *stages = NULL;
    col_rel_t *stage_template = NULL;
    int rc;

    if (next_index)
        *next_index = join_index;
    if (decline)
        *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NOT_ELIGIBLE;
    eligibility = wl_columnar_join_pipeline_preflight(plan, join_index,
            sess, &tail_end);
    /* Session- and JOIN-level exclusions decline before the operand is
     * examined; a shape exclusion is decided after it, so that what the
     * JOIN reads takes precedence over what follows it. */
    if (eligibility == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_OFF
        || eligibility == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_SESSION
        || eligibility == WL_COLUMNAR_JOIN_PIPELINE_EXCLUDED_JOIN)
        return 0;
    /* The preflight also reports a missing plan or an index past its end
     * as a shape exclusion; there is no JOIN to examine then. */
    if (!plan || !plan->ops || join_index >= plan->op_count)
        return 0;
    if (decline)
        *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NONE;
    if (!stack || stack->top == 0 || !sess)
        return EINVAL;
    join = &plan->ops[join_index];
    if (stack->items[stack->top - 1].kind
        != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || !stack->items[stack->top - 1].rel)
        return EINVAL;
    /* A worker run that resolves its operands by input slot (a TDD bound
     * run or insert clone) takes the right operand from the slot, as the
     * ordinary JOIN does there (#2201).  A missing right relation joins to
     * an empty result in the ordinary JOIN.  ENOENT from
     * col_join_batch_producer_create below is a different case, an
     * unavailable arrangement or a right name that does not pin this
     * relation, and stays a fallback. */
    if (sess->tdd_input_run) {
        bool bound_delta = false;
        rc = wl_columnar_eval_tdd_plan_resolve_input(sess, join, true,
                &right, &bound_delta);
        if (rc != 0)
            return rc;
        if (right)
            selection = (wl_columnar_join_selection_t){
                right, bound_delta, bound_delta && right->nrows == 0
            };
    } else {
        right = session_find_rel(sess, join->right_relation);
        /* Consume only a JOIN that would read the persistent relation
         * itself.  A $d$/$r$ delta is declined below, after the key checks
         * the ordinary JOIN also runs against it. */
        if (right)
            selection = wl_columnar_join_select_right(join, sess, right,
                    stack->items[stack->top - 1].is_delta);
    }
    if (!right) {
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY;
        return 0;
    }
    /* A forced-empty right operand is left to the ordinary JOIN here.  So
     * is an empty right operand in a slot-resolved run, which the ordinary
     * JOIN joins in one pass without a producer, as it does on any session
     * (#2201). */
    if (selection.force_empty
        || (sess->tdd_input_run && selection.right->nrows == 0)) {
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY;
        return 0;
    }

    lk = (uint32_t *)calloc(join->key_count, sizeof(*lk));
    rk = (uint32_t *)calloc(join->key_count, sizeof(*rk));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        return ENOMEM;
    }
    for (uint32_t i = 0; i < join->key_count; i++) {
        rc = pipeline_resolve_key(stack->items[stack->top - 1].rel,
                join->left_keys ? join->left_keys[i] : NULL, &lk[i]);
        if (rc == 0)
            rc = pipeline_resolve_key(selection.right,
                    join->right_keys ? join->right_keys[i] : NULL, &rk[i]);
        if (rc != 0) {
            free(lk);
            free(rk);
            return ENOTSUP;
        }
    }
    /* A key that resolved out of range came from the column-0 fallback
     * above on a zero-column relation; the ordinary JOIN handles that
     * input, so decline it.  This check must precede the type check,
     * which indexes the column types by these keys. */
    for (uint32_t i = 0; i < join->key_count; i++) {
        if (lk[i] >= stack->items[stack->top - 1].rel->ncols
            || rk[i] >= selection.right->ncols) {
            free(lk);
            free(rk);
            if (decline)
                *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_ORDINARY;
            return 0;
        }
    }
    if (!col_join_key_types_compatible(stack->items[stack->top - 1].rel, lk,
        selection.right, rk, join->key_count)) {
        free(lk);
        free(rk);
        return EINVAL;
    }
    /* Today every substituted right operand also sets used_right_delta;
     * the pointer comparison also declines a future substitution that does
     * not set it.  A slot-resolved run reads a delta in the slot through
     * the producer's own index, so it does not decline (#2201). */
    if (!sess->tdd_input_run
        && (selection.used_right_delta || selection.right != right)) {
        free(lk);
        free(rk);
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_DELTA_RIGHT;
        return 0;
    }
    if (eligibility != WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE) {
        free(lk);
        free(rk);
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NOT_ELIGIBLE;
        return 0;
    }
    /* Probe stages: resolve each against the schema its input would have,
     * truncating the tail before a stage the pipeline cannot apply.  The
     * schemas come from zero-row templates built with the operators' own
     * output constructors; no operator runs. */
    rc = pipeline_setup_stages(plan, join_index, &tail_end, sess,
            stack->items[stack->top - 1].rel, right, join, &stages,
            &stage_template);
    if (rc != 0) {
        free(lk);
        free(rk);
        return rc;
    }
    if (tail_end == join_index) {
        free(lk);
        free(rk);
        pipeline_stages_release(stages, tail_end - join_index);
        col_rel_destroy(stage_template);
        if (decline)
            *decline = WL_COLUMNAR_JOIN_PIPELINE_DECLINE_NOT_ELIGIBLE;
        return 0;
    }
    last = &plan->ops[tail_end];
    /* Strict mode refuses a tail whose output another operator reads
     * before any producer exists; eval_plan records
     * pipeline-not-consumable once for the ENOTSUP. */
    if (sess->join_batch_strict
        && wl_columnar_join_pipeline_successor(plan, tail_end + 1u)
        == WL_COLUMNAR_JOIN_SUCCESSOR_MATERIALIZING) {
        free(lk);
        free(rk);
        pipeline_stages_release(stages, tail_end - join_index);
        col_rel_destroy(stage_template);
        return ENOTSUP;
    }

    /* A producer over the session's arrangement holds no reader on its
     * inputs.  On a worker the right operand can be a view of the
     * coordinator's storage, so hold a reader on it from before the
     * producer pins its arrangement until the producer is destroyed
     * (#2201), as the ordinary JOIN holds its probe's; a busy source fails
     * here before any batch is drawn.  In a slot-resolved run the producer
     * builds its own index over the slot's relation and holds its own
     * reader on it for as long. */
    if (sess->tdd_input_run) {
        rc = col_join_batch_producer_create_over(sess, join,
                stack->items[stack->top - 1].rel,
                stack->items[stack->top - 1].is_delta, right, lk, rk,
                join->key_count, sess->join_batch_bytes, &continuation);
    } else {
        rc = col_rel_source_reader_acquire(right, &right_reader);
        if (rc == 0)
            rc = col_join_batch_producer_create(sess, join,
                    stack->items[stack->top - 1].rel,
                    stack->items[stack->top - 1].is_delta, lk, rk,
                    join->key_count, sess->join_batch_bytes, &continuation);
    }
    free(lk);
    free(rk);
    if (rc != 0) {
        pipeline_release_right_reader(&right_reader);
        pipeline_stages_release(stages, tail_end - join_index);
        col_rel_destroy(stage_template);
        return rc;
    }

    /* The output has the schema the sequential tail would publish: a MAP's
     * has the column types col_op_map() gives the tail's input, resolved
     * without evaluating an expression; a FILTER's is its input's, which
     * is the JOIN output's for a FILTER-only tail. */
    if (last->op == WL_PLAN_OP_MAP) {
        /* The MAP's input is the last stage's schema, or the JOIN output's
         * (the producer's batch schema) when no stage precedes it. */
        col_rel_t *shape = stage_template;
        col_rel_t *join_shape = NULL;
        out = col_rel_new_auto("$join_pipeline", last->project_count);
        rc = out ? 0 : ENOMEM;
        if (rc == 0 && !shape) {
            join_shape = col_rel_new_auto("$join_pipeline_shape",
                    col_join_output_width(stack->items[stack->top - 1].rel,
                    right, join));
            rc = join_shape ? col_join_set_output_types(join_shape,
                    stack->items[stack->top - 1].rel, right, join) : ENOMEM;
            shape = join_shape;
        }
        if (rc == 0) {
            const col_rel_t *left_rel = stack->items[stack->top - 1].rel;
            rc = col_op_map_output_types(last, shape, sess,
                    sess->memory_governor ? sess->memory_governor
                    : left_rel->memory_governor ? left_rel->memory_governor
                    : right->memory_governor, out);
        }
        col_rel_destroy(join_shape);
    } else if (stage_template) {
        /* The last stage's schema; new_like copies no timestamps. */
        out = col_rel_new_like("$join_pipeline", stage_template);
        rc = out ? 0 : ENOMEM;
    } else {
        out = col_rel_new_auto("$join_pipeline",
                col_join_output_width(stack->items[stack->top - 1].rel,
                right, join));
        rc = out ? col_join_set_output_types(out,
                stack->items[stack->top - 1].rel, right, join) : ENOMEM;
    }
    col_rel_destroy(stage_template);
    stage_template = NULL;
    if (rc != 0) {
        col_rel_destroy(out);
        wl_columnar_continuation_cancel(continuation);
        wl_columnar_continuation_destroy(continuation);
        pipeline_release_right_reader(&right_reader);
        pipeline_stages_release(stages, tail_end - join_index);
        return rc;
    }
    if (sess->memory_governor) {
        rc = col_rel_attach_memory_governor(out, sess->memory_governor);
        if (rc == 0)
            rc = col_rel_reserve_capacity_admitted(out, out->capacity, NULL);
        if (rc != 0) {
            rc = wl_columnar_join_budget_denial(sess, out, rc);
            col_rel_destroy(out);
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            pipeline_release_right_reader(&right_reader);
            pipeline_stages_release(stages, tail_end - join_index);
            return rc;
        }
    }

    left_entry = eval_stack_pop(stack);
    memset(&sink_ctx, 0, sizeof(sink_ctx));
    sink_ctx.sess = sess;
    sink_ctx.plan = plan;
    sink_ctx.first_tail = join_index + 1u;
    sink_ctx.tail_end = tail_end;
    sink_ctx.stages = stages;
    sink_ctx.out = out;
    memset(&sink, 0, sizeof(sink));
    sink.context = &sink_ctx;
    sink.begin = pipeline_sink_begin;
    sink.reserve = pipeline_sink_reserve;
    sink.append = pipeline_sink_append;
    sink.commit = pipeline_sink_commit;
    sink.abort = pipeline_sink_abort;

    for (;;) {
        uint64_t before = out->nrows;
        wl_columnar_continuation_status_t status
            = wl_columnar_continuation_publish(continuation, &sink);
        if (status == WL_COLUMNAR_CONTINUATION_DONE)
            break;
        if (status != WL_COLUMNAR_CONTINUATION_OK) {
            pipeline_sink_abort(&sink_ctx);
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            pipeline_release_right_reader(&right_reader);
            col_rel_destroy(out);
            pipeline_stages_release(stages, tail_end - join_index);
            if (eval_stack_repush_entry(stack, &left_entry) != 0)
                return EFAULT;
            if (status == WL_COLUMNAR_CONTINUATION_STALE)
                return EAGAIN;
            /* A FILTER/MAP failure keeps the operator's own errno (for
             * MAP, ERANGE for an expression error and ENOSPC for a denied
             * admission) instead of collapsing into ENOMEM.  An errno the
             * caller treats as a fallback (EAGAIN, ENOENT, ENOTSUP) re-runs
             * the restored input on the one-shot path. */
            return sink_ctx.error != 0 ? sink_ctx.error : ENOMEM;
        }
        /* Preserve the pipeline's post-FILTER/MAP output cap semantics. */
        if (wl_columnar_join_output_limit_reached(sess, out,
            out->nrows - before)) {
            wl_columnar_continuation_cancel(continuation);
            wl_columnar_continuation_destroy(continuation);
            pipeline_release_right_reader(&right_reader);
            col_rel_destroy(out);
            pipeline_stages_release(stages, tail_end - join_index);
            if (eval_stack_repush_entry(stack, &left_entry) != 0)
                return EFAULT;
            return EOVERFLOW;
        }
    }
    wl_columnar_continuation_destroy(continuation);
    pipeline_release_right_reader(&right_reader);
    pipeline_stages_release(stages, tail_end - join_index);
    stages = NULL;
    rc = eval_entry_dispose(&left_entry);
    if (rc != 0) {
        col_rel_destroy(out);
        if (eval_stack_repush_entry(stack, &left_entry) != 0)
            return EFAULT;
        return rc;
    }
    rc = eval_stack_push(stack, out, true);
    if (rc != 0) {
        col_rel_destroy(out);
        return rc;
    }
    col_join_batch_record_run(sess, COL_JOIN_BATCH_RUN_PIPELINE);
    if (next_index)
        *next_index = tail_end;
    return 1;
}

/* Same-frame materializing destination. Streaming tail ownership is separate:
* this owner consumes producer batches into one private relation, then returns
* to the original composed FILTER/MAP path without changing session policy. */
enum {
    WL_COLUMNAR_JOIN_HOST_LEFT_INIT = 1,
    WL_COLUMNAR_JOIN_HOST_LEFT_HOLD,
    WL_COLUMNAR_JOIN_HOST_FULL,
    WL_COLUMNAR_JOIN_HOST_DELTA,
    WL_COLUMNAR_JOIN_HOST_SELECT,
    WL_COLUMNAR_JOIN_HOST_RIGHT_INIT,
    WL_COLUMNAR_JOIN_HOST_RIGHT_HOLD,
    WL_COLUMNAR_JOIN_HOST_KEYS_ALLOC,
    WL_COLUMNAR_JOIN_HOST_KEYS,
    WL_COLUMNAR_JOIN_HOST_UNARY,
    WL_COLUMNAR_JOIN_HOST_PREPARE_BEGIN,
    WL_COLUMNAR_JOIN_HOST_PREPARE,
    WL_COLUMNAR_JOIN_HOST_SELECTION_END,
    WL_COLUMNAR_JOIN_HOST_OUTPUT,
    WL_COLUMNAR_JOIN_HOST_PRODUCE,
    WL_COLUMNAR_JOIN_HOST_SOURCE_INIT,
    WL_COLUMNAR_JOIN_HOST_SOURCE_HOLD,
    WL_COLUMNAR_JOIN_HOST_GROW,
    WL_COLUMNAR_JOIN_HOST_WRITER,
    WL_COLUMNAR_JOIN_HOST_GATHER,
    WL_COLUMNAR_JOIN_HOST_APPEND,
    WL_COLUMNAR_JOIN_HOST_WRITER_END,
    WL_COLUMNAR_JOIN_HOST_SOURCE_END,
    WL_COLUMNAR_JOIN_HOST_ACK,
    WL_COLUMNAR_JOIN_HOST_EXCHANGE,
    WL_COLUMNAR_JOIN_HOST_CONSUMER_START,
    WL_COLUMNAR_JOIN_HOST_CONSUMER_STEP,
    WL_COLUMNAR_JOIN_HOST_CONSUMER_ACK,
    WL_COLUMNAR_JOIN_HOST_RESTORE,
    WL_COLUMNAR_JOIN_HOST_SINK_OUTPUT,
    WL_COLUMNAR_JOIN_HOST_PROBE_SCAN,
    WL_COLUMNAR_JOIN_HOST_STAGES_ALLOC,
    WL_COLUMNAR_JOIN_HOST_STAGES_INIT,
    WL_COLUMNAR_JOIN_HOST_SHAPE_BEGIN,
    WL_COLUMNAR_JOIN_HOST_SHAPE_STEP,
    WL_COLUMNAR_JOIN_HOST_STAGE_SCAN,
    WL_COLUMNAR_JOIN_HOST_STAGE_RIGHT_INIT,
    WL_COLUMNAR_JOIN_HOST_STAGE_RIGHT_HOLD,
    WL_COLUMNAR_JOIN_HOST_STAGE_LEFT_INIT,
    WL_COLUMNAR_JOIN_HOST_STAGE_LEFT_HOLD,
    WL_COLUMNAR_JOIN_HOST_STAGE_KEYS_ALLOC,
    WL_COLUMNAR_JOIN_HOST_STAGE_KEYS,
    WL_COLUMNAR_JOIN_HOST_DRAIN = 60,
    WL_COLUMNAR_JOIN_HOST_FINAL_UNARY,
    WL_COLUMNAR_JOIN_HOST_OLD_PREPARE,
    WL_COLUMNAR_JOIN_HOST_COMMIT,
    WL_COLUMNAR_JOIN_HOST_RETIRE,
    WL_COLUMNAR_JOIN_HOST_RESET,
    WL_COLUMNAR_JOIN_HOST_TRANSFORM_RETIRE,
    WL_COLUMNAR_JOIN_HOST_SHAPE_END,
    WL_COLUMNAR_JOIN_HOST_SHAPE_RETIRE,
    WL_COLUMNAR_JOIN_HOST_SHAPE_FINAL,
    WL_COLUMNAR_JOIN_HOST_ABORT = 100
};

typedef struct {
    const wl_plan_op_t *op;
    col_rel_t *right;
    col_relation_snapshot_t snapshot;
    wl_columnar_relation_scalar_source_t source;
    bool initialized, active;
    uint32_t *keys, input_width, key_cursor;
    uint64_t key_bytes;
    wl_columnar_memory_reservation_t credit;
    bool admitted, committed;
} wl_columnar_join_host_stage_t;

typedef struct {
    uint32_t phase, cursor, column, position, depth;
} wl_columnar_join_host_setup_t;
typedef char wl_columnar_join_host_setup_size_t[
        sizeof(wl_columnar_join_host_setup_t) <= 32 ? 1 : -1];
enum {
    WL_COLUMNAR_JOIN_HOST_SETUP_HEADER = 0,
    WL_COLUMNAR_JOIN_HOST_SETUP_TAIL,
    WL_COLUMNAR_JOIN_HOST_SETUP_FILTER,
    WL_COLUMNAR_JOIN_HOST_SETUP_MAP_COLUMN,
    WL_COLUMNAR_JOIN_HOST_SETUP_MAP_EXPR,
    WL_COLUMNAR_JOIN_HOST_SETUP_BIND,
    WL_COLUMNAR_JOIN_HOST_SETUP_PUBLISH
};

typedef struct {
    const void *identity;
    wl_columnar_eval_plan_numeric_attempt_t *parent;
    const wl_plan_op_t *op;
    const wl_columnar_eval_private_bindings_t *bindings;
    const col_rel_t *left;
    col_rel_t *full, *delta, *right;
    bool left_delta, used_delta, forced_empty, result_delta, committed,
        row_fits;
    wl_columnar_relation_scalar_source_t left_source, right_source, source;
    bool left_initialized, right_initialized, source_initialized;
    uint32_t *keys;
    uint64_t key_bytes;
    wl_columnar_memory_reservation_t key_credit;
    bool keys_admitted, keys_committed;
    uint32_t key_cursor;
    wl_columnar_join_batch_prepare_t prepare;
    col_rel_t **output_slot;
    col_rel_t *output;
    bool unary_started;
    const col_rel_t *pending;
    uint64_t generation, producer_work;
    uint32_t base, extent, row, column;
    wl_columnar_relation_append_batch_t writer;
    wl_columnar_relation_private_batch_drain_t writer_drain;
    bool writer_initialized, unary_finished;
    bool streaming, schema_exchange, exchange_active, producer_done;
    bool consumer_live, consumer_pending, output_in_frame;
    uint32_t tail_end, consumer_cursor;
    int consumer_protocol;
    wl_columnar_filter_status_t consumer_status;
    eval_entry_t transformed;
    int64_t *row_buffer;
    uint64_t row_bytes;
    wl_columnar_memory_reservation_t row_credit;
    bool row_admitted, row_committed;
    wl_columnar_relation_scalar_retirement_t retirement;
    wl_columnar_relation_scalar_retirement_initializer_t retirement_initializer;
    eval_entry_t *retirement_entry;
    bool busy;
    bool retirement_started, retirement_committed, retirement_drained;
    bool retiring_output;
    eval_entry_t retiring_entry;
    wl_columnar_join_host_stage_t *stages;
    uint64_t stages_bytes, stages_initialized;
    wl_columnar_memory_reservation_t stages_credit;
    bool stages_admitted, stages_committed;
    uint32_t stage_cursor, stage_count, stage_cleanup;
    wl_columnar_join_schema_attempt_t shape_attempt;
    wl_columnar_relation_scalar_source_t shape_source;
    bool shape_initialized;
    col_rel_t *shape_current, *shape_next;
    eval_entry_t shape_old;
    bool shape_retiring;
    unsigned phase;
    int result;
    wl_columnar_join_host_setup_t setup;
} wl_columnar_join_host_t;

size_t
wl_columnar_join_pipeline_numeric_host_size(void)
{
    return sizeof(wl_columnar_join_host_t);
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_join_pipeline_test_numeric_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t, uint32_t, uint64_t);
#define WL_COLUMNAR_JOIN_HOST_HOOK(host, event) do { \
            if (wl_columnar_join_pipeline_test_numeric_checkpoint) \
            wl_columnar_join_pipeline_test_numeric_checkpoint( \
                (host)->parent->control, (event), (host)->generation, \
                (host)->extent, (host)->producer_work); \
} while (0)
#else
#define WL_COLUMNAR_JOIN_HOST_HOOK(host, event) ((void)0)
#endif

static int
wl_columnar_join_host_original(wl_columnar_join_host_t *h, uint64_t work)
{
    return wl_evaluation_control_charge_attempt(h->parent->control,
               h->parent->attempt_id, work);
}

static int
wl_columnar_join_host_fund(wl_evaluation_control_t *funding, uint64_t id)
{
    return wl_evaluation_control_charge_attempt(funding, id, 1);
}

static void
wl_columnar_join_host_fail(wl_columnar_join_host_t *h, int rc)
{
    if (!h->result) h->result = rc;
    if (rc == ENOSPC) h->parent->session->memory_budget_denied = true;
    h->phase = WL_COLUMNAR_JOIN_HOST_ABORT;
}

static int
wl_columnar_join_host_reserve(wl_columnar_eval_plan_numeric_attempt_t *r,
    uint64_t bytes, wl_columnar_memory_reservation_t *credit, bool *admitted)
{
    if (!r->join_host_governor) return 0;
    wl_columnar_memory_admission_status_t status;
    int rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(r->join_host_governor), bytes,
        credit, &status, r->control);
    if (!rc && status > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
        rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status ==
            WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : ENOMEM;
        if (rc == ENOSPC) r->session->memory_budget_denied = true;
    }
    if (!rc) *admitted = true;
    return rc;
}

static void
wl_columnar_join_host_terminal(wl_columnar_eval_plan_numeric_attempt_t *r,
    int result, bool committed)
{
    r->child_status.execution_result = result;
    r->child_status.semantic_result = result;
    r->child_status.cleanup_result = 0;
    r->child_status.committed = committed;
    r->join_host_terminal = true;
}

static int
wl_columnar_join_host_bootstrap(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    int rc = wl_evaluation_control_charge_attempt(r->control, r->attempt_id,
            16);
    if (rc) return rc;
    eval_stack_t *stack = NULL;
    rc = wl_columnar_eval_stack_operator_stack_owned(r->frame, r->session,
            &stack, r);
    if (rc) return rc;
    if (!stack->top || stack->items[stack->top - 1].kind
        != WL_COLUMNAR_EVAL_ENTRY_RELATION || !stack->items[stack->top - 1].rel)
        return EINVAL;
    r->join_host_governor = r->session->memory_governor
        ? r->session->memory_governor : stack->items[stack->top -
            1].rel->memory_governor;
    if (!r->join_host_governor) {
        /* Resolve the existing right-side fallback governor before allocation,
         * using the same charged lookup/selector policy as the later owner. */
        const wl_plan_op_t *op = &r->plan->ops[r->cursor];
        bool left_delta = stack->items[stack->top - 1].is_delta;
        const wl_columnar_eval_private_bindings_t *bindings = NULL;
        rc = wl_columnar_eval_plan_numeric_private_bindings_get(r, r->frame,
                r->session, r->control, r->attempt_id, &bindings);
        if (rc) return rc;
        col_rel_t *right = NULL, *delta = NULL;
        rc = wl_columnar_join_attempt_lookup(r->session, op->right_relation,
                false, &right, r->control, r->attempt_id, bindings);
        if (rc) return rc;
        if (right && op->delta_mode != WL_DELTA_FORCE_FULL
            && (op->delta_mode == WL_DELTA_FORCE_DELTA || !left_delta)) {
            rc = wl_columnar_join_attempt_lookup(r->session, op->right_relation,
                    true, &delta, r->control, r->attempt_id, bindings);
            if (rc) return rc;
        }
        if (right && delta && delta->nrows
            && (op->delta_mode == WL_DELTA_FORCE_DELTA
            || (op->delta_mode != WL_DELTA_FORCE_FULL && !left_delta
            && delta->nrows < right->nrows))) right = delta;
        col_rel_t *append = NULL;
        rc = wl_columnar_session_delta_source(r->session, bindings, op,
                &append);
        if (rc) return rc;
        if (append) right = append;
        if (right) r->join_host_governor = right->memory_governor;
    }
    wl_columnar_memory_governor_ref_retain(r->join_host_governor);
    wl_columnar_memory_reservation_init(&r->join_host_credit);
    rc = wl_columnar_join_host_reserve(r, sizeof(wl_columnar_join_host_t),
            &r->join_host_credit, &r->join_host_admitted);
    if (rc) return rc;
    r->join_host =
        wl_columnar_join_attempt_allocate(sizeof(wl_columnar_join_host_t));
    if (!r->join_host) return ENOMEM;
    if (r->join_host_admitted) {
        if (!wl_columnar_memory_commit(&r->join_host_credit,
            r->join_host)) return EINVAL;
        r->join_host_committed = true;
    }
    r->join_host_initialized_bytes = 0;
    return 0;
}

static int
wl_columnar_join_host_initialize(wl_columnar_eval_plan_numeric_attempt_t *r)
{
    uint64_t total = sizeof(wl_columnar_join_host_t);
    if (r->join_host_initialized_bytes < total) {
        uint64_t bytes = total - r->join_host_initialized_bytes;
        if (bytes > 256) bytes = 256;
        int rc = wl_evaluation_control_charge_attempt(r->control, r->attempt_id,
                bytes);
        if (rc) return rc;
        memset((unsigned char *)r->join_host + r->join_host_initialized_bytes,
            0,
            (size_t)bytes);
        r->join_host_initialized_bytes += bytes;
        return 0;
    }
    int rc = wl_evaluation_control_charge_attempt(r->control, r->attempt_id,
            16);
    if (rc) return rc;
    eval_stack_t *stack = NULL;
    rc = wl_columnar_eval_stack_operator_stack_owned(r->frame, r->session,
            &stack, r);
    if (rc) return rc;
    if (!stack->top || !stack->items[stack->top - 1].rel) return EINVAL;
    wl_columnar_join_host_t *h = r->join_host;
    h->identity = h; h->parent = r; h->op = &r->plan->ops[r->cursor];
    h->left = stack->items[stack->top - 1].rel;
    h->left_delta = stack->items[stack->top - 1].is_delta;
    h->phase = WL_COLUMNAR_JOIN_HOST_LEFT_INIT;
    wl_columnar_memory_reservation_init(&h->key_credit);
    wl_columnar_memory_reservation_init(&h->row_credit);
    wl_columnar_memory_reservation_init(&h->stages_credit);
    ++r->join_host_initialized_bytes;
    int classify_error = 0;
    h->streaming = wl_columnar_join_pipeline_preflight_internal(r->plan,
            r->cursor, r->session, &h->tail_end, r->control, r->attempt_id,
            &classify_error) == WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE;
    if (classify_error) return classify_error;
    return wl_columnar_eval_plan_numeric_private_bindings_get(r, r->frame,
               r->session, r->control, r->attempt_id, &h->bindings);
}

static const wl_columnar_join_host_t *
wl_columnar_join_host_valid(const void *owner,
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    const wl_columnar_eval_plan_numeric_attempt_t *r = owner;
    if (!wl_columnar_eval_plan_numeric_owner_valid(owner, frame)
        || !r->plan || !r->plan->ops || r->cursor >= r->end
        || r->end > r->plan->op_count
        || !r->join_host || r->join_host_initialized_bytes
        != sizeof(wl_columnar_join_host_t) + 1u) return NULL;
    const wl_columnar_join_host_t *h = r->join_host;
    return h->identity == h && h->parent == r
           && h->op == &r->plan->ops[r->cursor] ? h : NULL;
}

bool
wl_columnar_join_pipeline_stream_authorized(const void *owner,
    const wl_columnar_eval_stack_cleanup_frame_t *frame, const void *host,
    unsigned action, const wl_plan_op_t *op, const col_rel_t *input,
    uint64_t generation, bool schema)
{
    const wl_columnar_join_host_t *h = wl_columnar_join_host_valid(owner,
            frame);
    if (!h || h != host || !h->streaming || !input || input != h->pending
        || generation != h->generation || schema != h->schema_exchange
        || !h->prepare.ready || h->prepare.aborting || h->prepare.drained
        || h->parent->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        || h->parent->pending_action !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
        || (schema ? !h->producer_done || generation != 0
        : !generation)) return false;
    if (action == WL_COLUMNAR_JOIN_STREAM_ENTER)
        return !op && !h->exchange_active
               && h->phase == WL_COLUMNAR_JOIN_HOST_EXCHANGE;
    if (action == WL_COLUMNAR_JOIN_STREAM_RESTORE)
        return !op && h->exchange_active && !h->consumer_live
               && (h->phase == WL_COLUMNAR_JOIN_HOST_RESTORE
               || h->phase == WL_COLUMNAR_JOIN_HOST_ABORT);
    if (!h->exchange_active || h->consumer_cursor <= h->parent->cursor
        || h->consumer_cursor > h->tail_end
        || op != &h->parent->plan->ops[h->consumer_cursor]) return false;
    if (action == WL_COLUMNAR_JOIN_STREAM_START)
        return h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_START;
    if (action == WL_COLUMNAR_JOIN_STREAM_STEP)
        return h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_STEP
               || h->phase == WL_COLUMNAR_JOIN_HOST_ABORT;
    if (action == WL_COLUMNAR_JOIN_STREAM_ACK)
        return h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_ACK
               || (h->phase == WL_COLUMNAR_JOIN_HOST_ABORT
               && h->consumer_status.progress ==
               WL_COLUMNAR_FILTER_PROGRESS_DONE);
    return false;
}

int
wl_columnar_join_pipeline_probe_source_owned(const void *owner,
    const wl_columnar_eval_stack_cleanup_frame_t *frame, const wl_plan_op_t *op,
    const col_rel_t *left, const col_rel_t *right)
{
    const wl_columnar_join_host_t *h = wl_columnar_join_host_valid(owner,
            frame);
    if (!h || !h->exchange_active || !h->streaming || !h->stages || !left
        || h->stages_initialized != h->stages_bytes
        || h->phase != WL_COLUMNAR_JOIN_HOST_CONSUMER_START
        || h->consumer_cursor <= h->parent->cursor
        || h->consumer_cursor > h->tail_end) return EINVAL;
    const wl_columnar_join_host_stage_t *stage =
        &h->stages[h->consumer_cursor - h->parent->cursor - 1u];
    if (!stage->active || stage->op != op || !stage->initialized
        || stage->source.phase != WL_COLUMNAR_RELATION_SCALAR_SOURCE_HELD
        || !right || stage->right != right || left->ncols != stage->input_width)
        return EINVAL;
    int rc = wl_columnar_join_host_original((wl_columnar_join_host_t *)h, 1);
    if (rc) return rc;
    return wl_columnar_relation_snapshot_equal(stage->snapshot,
               wl_columnar_relation_snapshot(right)) ? 0 : EAGAIN;
}

bool
wl_columnar_join_pipeline_sink_authorized(const void *owner,
    const wl_columnar_join_batch_prepare_t *prepare, const col_rel_t *sink,
    uint32_t base, uint64_t generation,
    const wl_columnar_relation_append_batch_t *writer,
    const wl_columnar_relation_scalar_source_t *source, uint32_t *rows)
{
    const wl_columnar_eval_plan_numeric_attempt_t *r = owner;
    const wl_columnar_join_host_t *h = r
        ? wl_columnar_join_host_valid(r, r->frame) : NULL;
    if (!h || !rows || h->phase != WL_COLUMNAR_JOIN_HOST_ACK
        || prepare != &h->prepare || sink != h->output || base != h->base
        || generation != h->generation || !generation || h->schema_exchange
        || writer != &h->writer || source != &h->source || h->exchange_active
        || h->consumer_live || !h->pending) return false;
    const col_rel_t *copied = h->streaming ? h->transformed.rel : h->pending;
    if (!copied || copied->ncols != sink->ncols) return false;
    *rows = copied->nrows;
    return true;
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_columnar_join_pipeline_test_stream_view(const void *owner,
    const col_rel_t **input, uint64_t *generation, bool *schema,
    const wl_plan_op_t **op)
{
    const wl_columnar_eval_plan_numeric_attempt_t *r = owner;
    const wl_columnar_join_host_t *h = r
        ? wl_columnar_join_host_valid(r, r->frame) : NULL;
    if (!h || !h->exchange_active || !input || !generation || !schema || !op
        || h->phase != WL_COLUMNAR_JOIN_HOST_CONSUMER_START) return EINVAL;
    *input = h->pending; *generation = h->generation;
    *schema = h->schema_exchange;
    *op = &r->plan->ops[h->consumer_cursor];
    return 0;
}
#endif

int
wl_columnar_join_pipeline_numeric_funding_work(
    const wl_columnar_eval_plan_numeric_attempt_t *r, uint64_t *work)
{
    if (!work || !r || !wl_columnar_eval_plan_numeric_owner_valid(r, r->frame))
        return EINVAL;
    if (r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
        && (r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        || r->pending_action !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH))
        return EINVAL;
    *work = 1;
    if (!r->join_host || r->join_host_initialized_bytes
        <= sizeof(wl_columnar_join_host_t)) return 0;
    const wl_columnar_join_host_t *h = wl_columnar_join_host_valid(r, r->frame);
    if (!h) return EINVAL;
    if (h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_START
        || h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_STEP
        || h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_ACK
        || (h->exchange_active && h->consumer_live
        && (h->phase == WL_COLUMNAR_JOIN_HOST_ABORT || r->semantic_result)))
        *work = 384;
    return 0;
}

static int
wl_columnar_join_host_consumer(wl_columnar_join_host_t *h, unsigned action)
{
    wl_columnar_eval_plan_numeric_attempt_t *r = h->parent;
    const wl_plan_op_t *op = &r->plan->ops[h->consumer_cursor];
    int rc = wl_columnar_eval_stack_stream_dispatch_owned(r->frame, r, h, op,
            action, true);
    if (rc) return rc;
    WL_COLUMNAR_JOIN_HOST_HOOK(h, WL_COLUMNAR_JOIN_PIPELINE_TEST_CONSUMER);
    if (action == WL_COLUMNAR_JOIN_STREAM_START) {
        switch (op->op) {
        case WL_PLAN_OP_FILTER:
            rc = wl_columnar_filter_op_attempt_start_owned(op, r->frame,
                    r->session, r->control, r->attempt_id, &h->consumer_status,
                    r);
            break;
        case WL_PLAN_OP_MAP:
            rc = wl_columnar_filter_map_attempt_start_owned(op, r->frame,
                    r->session, r->control, r->attempt_id, &h->consumer_status,
                    r);
            break;
        case WL_PLAN_OP_SEMIJOIN:
        case WL_PLAN_OP_ANTIJOIN:
            rc = wl_columnar_join_probe_op_attempt_start_owned(op, r->frame,
                    r->session, r->control, r->attempt_id, &h->consumer_status,
                    r);
            break;
        default: rc = EINVAL; break;
        }
    } else if (action == WL_COLUMNAR_JOIN_STREAM_STEP) {
        rc = wl_columnar_filter_op_attempt_step_owned(r->frame,
                &h->consumer_status, r);
    } else rc = wl_columnar_filter_op_attempt_ack_owned(r->frame, r);
    bool done = false;
    int state = wl_columnar_eval_stack_stream_consumer_state_owned(r->frame,
            r, h, &h->consumer_live, &done);
    int closed = wl_columnar_eval_stack_stream_dispatch_owned(r->frame, r, h,
            op, action, false);
    if (state || closed) return state ? state : closed;
    h->consumer_protocol = rc; h->consumer_pending = true;
    if (rc) return rc;
    if (action == WL_COLUMNAR_JOIN_STREAM_ACK) {
        if (h->consumer_live) return EBUSY;
        if (h->result || h->phase == WL_COLUMNAR_JOIN_HOST_ABORT)
            h->phase = WL_COLUMNAR_JOIN_HOST_ABORT;
        else if (++h->consumer_cursor > h->tail_end)
            h->phase = WL_COLUMNAR_JOIN_HOST_RESTORE;
        else h->phase = WL_COLUMNAR_JOIN_HOST_CONSUMER_START;
    } else {
        if (h->consumer_status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE) {
            if (!h->result) h->result = h->consumer_status.semantic_result;
            if (h->result == ENOSPC) r->session->memory_budget_denied = true;
            if (h->phase != WL_COLUMNAR_JOIN_HOST_ABORT)
                h->phase = WL_COLUMNAR_JOIN_HOST_CONSUMER_ACK;
        } else if (h->phase != WL_COLUMNAR_JOIN_HOST_ABORT)
            h->phase = WL_COLUMNAR_JOIN_HOST_CONSUMER_STEP;
        /* DONE may be a pre-context stop. The real inert ACK is still
         * delivered before restoring the parked entry. */
        if (!h->consumer_live && !done
            && h->consumer_status.progress != WL_COLUMNAR_FILTER_PROGRESS_DONE)
            return EINVAL;
    }
    return 0;
}

static int
wl_columnar_join_host_forward(wl_columnar_join_host_t *h)
{
    wl_columnar_eval_plan_numeric_attempt_t *r = h->parent;
    const wl_plan_op_t *op = h->op;
    int rc;
    switch (h->phase) {
    case WL_COLUMNAR_JOIN_HOST_LEFT_INIT:
        /* Routing hint only: ordinary JOIN must authenticate the descriptor.
         * Normal scalar HOLD cannot acquire a never-inserted EDB. */
        if (!h->left->ncols) {
            r->join_host_declined = true;
            h->phase = WL_COLUMNAR_JOIN_HOST_ABORT;
            return 0;
        }
        rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
            &h->left_source, &h->left_initialized, r->control, r->attempt_id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_LEFT_HOLD;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_LEFT_HOLD:
        rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &h->left_source, h->left, false);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_FULL;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_FULL:
        rc = wl_columnar_join_attempt_lookup(r->session, op->right_relation,
                false, &h->full, r->control, r->attempt_id, h->bindings);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_DELTA;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_DELTA:
        if (h->full && op->delta_mode != WL_DELTA_FORCE_FULL
            && (op->delta_mode == WL_DELTA_FORCE_DELTA || !h->left_delta)) {
            rc = wl_columnar_join_attempt_lookup(r->session, op->right_relation,
                    true, &h->delta, r->control, r->attempt_id, h->bindings);
            if (rc) return rc;
        }
        h->phase = WL_COLUMNAR_JOIN_HOST_SELECT;
        return 0;
    case WL_COLUMNAR_JOIN_HOST_SELECT: {
        h->right = h->full;
        if (h->right && op->delta_mode == WL_DELTA_FORCE_DELTA) {
            if (h->delta && h->delta->nrows) {
                h->right = h->delta; h->used_delta = true;
            } else h->forced_empty = r->session->delta_seeded;
        } else if (h->right && op->delta_mode != WL_DELTA_FORCE_FULL
            && !h->left_delta && h->delta && h->delta->nrows
            && h->delta->nrows < h->right->nrows) {
            h->right = h->delta; h->used_delta = true;
        }
        col_rel_t *append = NULL;
        rc = wl_columnar_session_delta_source(r->session, h->bindings, op,
                &append);
        if (rc) return rc;
        if (append) {
            h->right = append; h->used_delta = true;
            h->forced_empty = append->nrows == 0;
        }
        /* These are named intrinsic ordinary destinations. No unary claim
         * has started, so the existing bounded child can run after drain. */
        if (!h->right || !h->right->ncols || h->forced_empty
            || h->used_delta || !op->key_count
            || op->materialized || op->right_filter_expr.size
            || (op->project_count && !op->project_indices)) {
            r->join_host_declined = true;
            h->phase = WL_COLUMNAR_JOIN_HOST_ABORT;
        } else h->phase = WL_COLUMNAR_JOIN_HOST_RIGHT_INIT;
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_RIGHT_INIT:
        rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
            &h->right_source, &h->right_initialized, r->control, r->attempt_id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_RIGHT_HOLD;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_RIGHT_HOLD: {
        rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &h->right_source, h->right, false);
        if (rc) return rc;
        if (!h->left_source.width || !h->right_source.width) return EINVAL;
        uint64_t width = op->project_count ? op->project_count
            : (uint64_t)h->left_source.width + h->right_source.width;
        if (width > UINT32_MAX) return EOVERFLOW;
        uint64_t row_bytes = width * sizeof(int64_t)
            + (h->left->timestamps ? sizeof(col_delta_timestamp_t) : 0);
        h->row_fits = width && r->session->join_batch_bytes >= row_bytes;
        h->phase = WL_COLUMNAR_JOIN_HOST_KEYS_ALLOC;
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_KEYS_ALLOC:
        WL_COLUMNAR_JOIN_HOST_HOOK(h, WL_COLUMNAR_JOIN_PIPELINE_TEST_KEYS);
        if (!wl_columnar_memory_size_mul(op->key_count, 2u * sizeof(uint32_t),
            &h->key_bytes) || h->key_bytes > SIZE_MAX) return EOVERFLOW;
        rc = wl_columnar_join_host_reserve(r, h->key_bytes, &h->key_credit,
                &h->keys_admitted);
        if (rc) return rc;
        h->keys = wl_columnar_join_attempt_allocate((size_t)h->key_bytes);
        if (!h->keys) return ENOMEM;
        if (h->keys_admitted) {
            if (!wl_columnar_memory_commit(&h->key_credit, h)) return EINVAL;
            h->keys_committed = true;
        }
        h->phase = WL_COLUMNAR_JOIN_HOST_KEYS;
        return 0;
    case WL_COLUMNAR_JOIN_HOST_KEYS:
        if (h->key_cursor < op->key_count) {
            uint32_t k = h->key_cursor;
            rc = wl_columnar_join_attempt_key(&h->left_source,
                    op->left_keys ? op->left_keys[k] : NULL, &h->keys[k],
                    r->control, r->attempt_id);
            if (rc) return rc;
            rc = wl_columnar_join_attempt_key(&h->right_source,
                    op->right_keys ? op->right_keys[k] : NULL,
                    &h->keys[op->key_count + k], r->control, r->attempt_id);
            if (rc) return rc;
            wirelog_column_type_t lt = h->left_source.types
                ? h->left_source.types[h->keys[k]] : WIRELOG_TYPE_INT64;
            wirelog_column_type_t rt = h->right_source.types
                ? h->right_source.types[h->keys[op->key_count +
                    k]] : WIRELOG_TYPE_INT64;
            if ((lt == WIRELOG_TYPE_FLOAT) !=
                (rt == WIRELOG_TYPE_FLOAT)) return EINVAL;
            ++h->key_cursor;
        } else {
            h->stage_cursor = r->cursor + 1u;
            h->phase = h->streaming ? WL_COLUMNAR_JOIN_HOST_PROBE_SCAN
                : WL_COLUMNAR_JOIN_HOST_UNARY;
        }
        return 0;
    case WL_COLUMNAR_JOIN_HOST_PROBE_SCAN:
        if (h->stage_cursor > h->tail_end) {
            h->phase = WL_COLUMNAR_JOIN_HOST_UNARY;
        } else {
            wl_plan_op_type_t kind = r->plan->ops[h->stage_cursor].op;
            if (kind == WL_PLAN_OP_SEMIJOIN || kind == WL_PLAN_OP_ANTIJOIN)
                h->phase = WL_COLUMNAR_JOIN_HOST_STAGES_ALLOC;
            else ++h->stage_cursor;
        }
        return 0;
    case WL_COLUMNAR_JOIN_HOST_STAGES_ALLOC:
        h->stage_count = h->tail_end - r->cursor;
        if (!wl_columnar_memory_size_mul(h->stage_count, sizeof(*h->stages),
            &h->stages_bytes) || h->stages_bytes > SIZE_MAX) return EOVERFLOW;
        rc = wl_columnar_join_host_reserve(r, h->stages_bytes,
                &h->stages_credit, &h->stages_admitted);
        if (rc) return rc;
        h->stages = wl_columnar_join_attempt_allocate((size_t)h->stages_bytes);
        if (!h->stages) return ENOMEM;
        if (h->stages_admitted) {
            if (!wl_columnar_memory_commit(&h->stages_credit, h)) return EINVAL;
            h->stages_committed = true;
        }
        h->phase = WL_COLUMNAR_JOIN_HOST_STAGES_INIT;
        return 0;
    case WL_COLUMNAR_JOIN_HOST_STAGES_INIT: {
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_STAGE_INIT);
        uint64_t bytes = h->stages_bytes - h->stages_initialized;
        if (bytes > 256) bytes = 256;
        rc = wl_columnar_join_host_original(h, bytes);
        if (rc) return rc;
        memset((unsigned char *)h->stages + h->stages_initialized, 0,
            (size_t)bytes);
        h->stages_initialized += bytes;
        if (h->stages_initialized == h->stages_bytes) {
            h->stage_cursor = r->cursor;
            h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_BEGIN;
        }
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_SHAPE_BEGIN: {
        bool first = h->stage_cursor == r->cursor;
        const wl_plan_op_t *shape_op = &r->plan->ops[h->stage_cursor];
        rc = wl_columnar_join_schema_attempt_begin(&h->shape_attempt,
                shape_op, first ? &h->left_source : &h->shape_source,
                first ? &h->right_source : NULL, "$join_pipeline_shape",
                r->join_host_governor, &h->shape_next, r->control,
                r->attempt_id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_STEP;
        return rc;
    }
    case WL_COLUMNAR_JOIN_HOST_SHAPE_STEP: {
        bool done = false;
        rc = wl_columnar_join_schema_attempt_step(&h->shape_attempt, 128,
                &done);
        if (!rc && done) h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_END;
        return rc;
    }
    case WL_COLUMNAR_JOIN_HOST_STAGE_SCAN: {
        if (h->stage_cursor > h->tail_end) {
            h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_FINAL;
            return 0;
        }
        const wl_plan_op_t *stage_op = &r->plan->ops[h->stage_cursor];
        if (stage_op->op != WL_PLAN_OP_SEMIJOIN &&
            stage_op->op != WL_PLAN_OP_ANTIJOIN) {
            ++h->stage_cursor; return 0;
        }
        wl_columnar_join_host_stage_t *stage =
            &h->stages[h->stage_cursor - r->cursor - 1u];
        stage->op = stage_op;
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_PROBE_FULL);
        rc = wl_columnar_join_attempt_lookup(r->session,
                stage_op->right_relation, false, &stage->right, r->control,
                r->attempt_id, h->bindings);
        if (rc) return rc;
        /* Routing hint only. A zero physical width cannot enter this
         * streamed probe; its later ordinary owner authenticates metadata. */
        if (!stage->right || !stage->right->ncols) {
            h->tail_end = h->stage_cursor - 1u;
            h->streaming = h->tail_end > r->cursor;
            h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_FINAL;
        } else h->phase = WL_COLUMNAR_JOIN_HOST_STAGE_RIGHT_INIT;
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_STAGE_RIGHT_INIT: {
        wl_columnar_join_host_stage_t *stage =
            &h->stages[h->stage_cursor - r->cursor - 1u];
        rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
            &stage->source, &stage->initialized, r->control, r->attempt_id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_STAGE_RIGHT_HOLD;
        return rc;
    }
    case WL_COLUMNAR_JOIN_HOST_STAGE_RIGHT_HOLD: {
        wl_columnar_join_host_stage_t *stage =
            &h->stages[h->stage_cursor - r->cursor - 1u];
        rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &stage->source, stage->right, false);
        if (!rc) {
            stage->snapshot = wl_columnar_relation_snapshot(stage->right);
            h->phase = WL_COLUMNAR_JOIN_HOST_STAGE_LEFT_INIT;
        }
        return rc;
    }
    case WL_COLUMNAR_JOIN_HOST_STAGE_LEFT_INIT:
        rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
            &h->shape_source, &h->shape_initialized, r->control, r->attempt_id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_STAGE_LEFT_HOLD;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_STAGE_LEFT_HOLD:
        rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &h->shape_source, h->shape_current, false);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_STAGE_KEYS_ALLOC;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_STAGE_KEYS_ALLOC: {
        wl_columnar_join_host_stage_t *stage =
            &h->stages[h->stage_cursor - r->cursor - 1u];
        stage->input_width = h->shape_source.width;
        if (!wl_columnar_memory_size_mul(stage->op->key_count,
            2u * sizeof(uint32_t), &stage->key_bytes) ||
            stage->key_bytes > SIZE_MAX) return EOVERFLOW;
        wl_columnar_memory_reservation_init(&stage->credit);
        rc = wl_columnar_join_host_reserve(r, stage->key_bytes, &stage->credit,
                &stage->admitted);
        if (rc) return rc;
        stage->keys =
            wl_columnar_join_attempt_allocate((size_t)stage->key_bytes);
        if (!stage->keys) return ENOMEM;
        if (stage->admitted) {
            if (!wl_columnar_memory_commit(&stage->credit,
                stage)) return EINVAL;
            stage->committed = true;
        }
        h->phase = WL_COLUMNAR_JOIN_HOST_STAGE_KEYS;
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_STAGE_KEYS: {
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_PROBE_KEYS);
        wl_columnar_join_host_stage_t *stage =
            &h->stages[h->stage_cursor - r->cursor - 1u];
        const wl_plan_op_t *stage_op = stage->op;
        uint32_t k = stage->key_cursor;
        if (k == stage_op->key_count) {
            h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_BEGIN; return 0;
        }
        const char *lk = stage_op->left_keys ? stage_op->left_keys[k] : NULL;
        const char *rk = stage_op->right_keys ? stage_op->right_keys[k] : NULL;
        rc = wl_columnar_join_pipeline_key_attempt(
            &h->shape_source, lk, &stage->keys[k], r->control, r->attempt_id);
        if (!rc) rc = wl_columnar_join_pipeline_key_attempt(&stage->source, rk,
                    &stage->keys[stage_op->key_count + k], r->control,
                    r->attempt_id);
        if (rc && rc != EINVAL) return rc;
        bool usable = !rc && stage->keys[k] < h->shape_source.width
            && stage->keys[stage_op->key_count + k] < stage->source.width;
        if (usable) {
            wirelog_column_type_t lt = h->shape_source.types
                ? h->shape_source.types[stage->keys[k]] : WIRELOG_TYPE_INT64;
            wirelog_column_type_t rt = stage->source.types
                ? stage->source.types[stage->keys[stage_op->key_count + k]]
                : WIRELOG_TYPE_INT64;
            usable = (lt == WIRELOG_TYPE_FLOAT) == (rt == WIRELOG_TYPE_FLOAT);
        }
        if (!usable) {
            h->tail_end = h->stage_cursor - 1u;
            h->streaming = h->tail_end > r->cursor;
            h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_FINAL;
        } else ++stage->key_cursor;
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_UNARY:
        /* Runtime probe qualification precedes producer row-fit refusal, as
         * in the raw pipeline. A declined producer owns no unary claim. */
        if (!h->row_fits) {
            r->join_host_declined = true;
            h->phase = WL_COLUMNAR_JOIN_HOST_ABORT;
            return 0;
        }
        if (h->streaming) {
            h->result_delta = false;
            h->phase = WL_COLUMNAR_JOIN_HOST_PREPARE_BEGIN;
            return 0;
        }
        rc = wl_columnar_eval_stack_unary_begin_attempt_owned(r->frame,
                r->control, r->attempt_id, r);
        if (rc) return rc;
        h->unary_started = true;
        h->result_delta = !op->project_count && h->left_delta;
        const col_rel_t *actual = NULL;
        rc = wl_columnar_eval_stack_unary_input_owned(r->frame, &actual, r);
        if (rc) return rc;
        if (actual != h->left) return EAGAIN;
        rc = wl_columnar_eval_stack_unary_output_owned(r->frame,
                &h->output_slot, r);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_PREPARE_BEGIN;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_PREPARE_BEGIN:
        rc = wl_columnar_join_batch_prepare_begin(&h->prepare, r->session,
                op, h->left, h->right, h->left_delta, h->keys,
                h->keys + op->key_count, op->key_count,
                r->session->join_batch_bytes, r->control, r->attempt_id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_PREPARE;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_PREPARE: {
        bool done = false;
        rc = wl_columnar_join_batch_prepare_step(&h->prepare, 256, &done);
        if (!rc && done) {
            WL_COLUMNAR_JOIN_HOST_HOOK(h,
                WL_COLUMNAR_JOIN_PIPELINE_TEST_PREPARED);
            h->phase = WL_COLUMNAR_JOIN_HOST_SELECTION_END;
        }
        return rc;
    }
    case WL_COLUMNAR_JOIN_HOST_SELECTION_END:
        if (h->left_initialized) {
            (void)wl_columnar_relation_scalar_source_end(&h->left_source, 0);
            if (h->left_source.phase !=
                WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED)
                return EBUSY;
            h->left_initialized = false;
        } else if (h->right_initialized) {
            (void)wl_columnar_relation_scalar_source_end(&h->right_source, 0);
            if (h->right_source.phase !=
                WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED)
                return EBUSY;
            h->right_initialized = false;
        } else h->phase = h->streaming ? WL_COLUMNAR_JOIN_HOST_PRODUCE
            : WL_COLUMNAR_JOIN_HOST_OUTPUT;
        return 0;
    case WL_COLUMNAR_JOIN_HOST_OUTPUT:
        rc =
            wl_columnar_relation_new_typed_codes_stamped_operator_attempt_controlled
            (
            "$join", h->prepare.output_width, h->prepare.capacity,
            h->prepare.types, h->left->timestamps != NULL,
            r->join_host_governor,
            h->output_slot, r->control, r->attempt_id);
        if (rc) return rc;
        rc = wl_columnar_join_host_original(h, 0);
        if (rc) return rc;
        h->output = *h->output_slot;
        /* Both constructors copied codes. This admitted int64 buffer can now
         * hold one gathered row; no native type metadata borrows it. */
        h->phase = WL_COLUMNAR_JOIN_HOST_PRODUCE;
        return 0;
    case WL_COLUMNAR_JOIN_HOST_PRODUCE: {
        WL_COLUMNAR_JOIN_HOST_HOOK(h, WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCE);
        col_join_batch_control_result_t result;
        col_join_batch_control_status_t status = col_join_batch_control_step(
            h->prepare.continuation, 256, &result);
        h->producer_work = result.work_ordinal;
        if (status == COL_JOIN_BATCH_CONTROL_PROGRESS) return 0;
        if (status == COL_JOIN_BATCH_CONTROL_DONE) {
            h->producer_done = true;
            h->pending = NULL; h->phase = WL_COLUMNAR_JOIN_HOST_DRAIN;
            WL_COLUMNAR_JOIN_HOST_HOOK(h,
                WL_COLUMNAR_JOIN_PIPELINE_TEST_PRODUCER_DONE);
            if (h->streaming && !h->output) {
                rc = wl_columnar_join_batch_prepare_terminal_schema(&h->prepare,
                        &h->pending);
                if (rc) return rc;
                h->schema_exchange = true; h->generation = 0;
                h->consumer_cursor = r->cursor + 1;
                h->phase = WL_COLUMNAR_JOIN_HOST_EXCHANGE;
            }
            return 0;
        }
        if (status != COL_JOIN_BATCH_CONTROL_BATCH_READY)
            return status ==
                   COL_JOIN_BATCH_CONTROL_CANCELLED ?
                   WL_EVALUATION_CONTROL_CANCELLED
                : status ==
                   COL_JOIN_BATCH_CONTROL_WORK_LIMIT ?
                   WL_EVALUATION_CONTROL_WORK_LIMIT
                : status == COL_JOIN_BATCH_CONTROL_STALE_INPUT ? EAGAIN
                : result.execution_status ? result.execution_status : EINVAL;
        rc = col_join_batch_control_peek(h->prepare.continuation, &h->pending,
                &h->generation);
        if (rc) return rc;
        if ((h->output ? h->output->nrows : 0) != h->extent) return EINVAL;
        h->base = h->extent; h->row = h->column = 0;
        h->phase = h->streaming ? WL_COLUMNAR_JOIN_HOST_EXCHANGE
            : WL_COLUMNAR_JOIN_HOST_SOURCE_INIT;
        h->consumer_cursor = r->cursor + 1;
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_EXCHANGE:
        rc = wl_columnar_eval_stack_stream_enter_owned(r->frame, r, h,
                h->pending, h->generation, h->schema_exchange, false);
        if (!rc) {
            h->exchange_active = true;
            h->phase = WL_COLUMNAR_JOIN_HOST_CONSUMER_START;
            WL_COLUMNAR_JOIN_HOST_HOOK(h,
                WL_COLUMNAR_JOIN_PIPELINE_TEST_EXCHANGED);
        }
        return rc;
    case WL_COLUMNAR_JOIN_HOST_CONSUMER_START:
        return wl_columnar_join_host_consumer(h, WL_COLUMNAR_JOIN_STREAM_START);
    case WL_COLUMNAR_JOIN_HOST_CONSUMER_STEP:
        return wl_columnar_join_host_consumer(h, WL_COLUMNAR_JOIN_STREAM_STEP);
    case WL_COLUMNAR_JOIN_HOST_CONSUMER_ACK:
        return wl_columnar_join_host_consumer(h, WL_COLUMNAR_JOIN_STREAM_ACK);
    case WL_COLUMNAR_JOIN_HOST_RESTORE:
        rc = wl_columnar_join_host_original(h, 128);
        if (rc) return rc;
        rc = wl_columnar_eval_stack_stream_restore_owned(r->frame, r, h,
                &h->transformed);
        if (!rc) {
            h->exchange_active = false;
            h->phase = WL_COLUMNAR_JOIN_HOST_SOURCE_INIT;
            WL_COLUMNAR_JOIN_HOST_HOOK(h,
                WL_COLUMNAR_JOIN_PIPELINE_TEST_RESTORED);
        }
        return rc;
    case WL_COLUMNAR_JOIN_HOST_SOURCE_INIT:
        rc =
            wl_columnar_relation_scalar_source_init_attempt_controlled(
            &h->source,
            &h->source_initialized, r->control, r->attempt_id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_SOURCE_HOLD;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_SOURCE_HOLD:
        rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &h->source, h->streaming ? h->transformed.rel : h->pending,
            (h->streaming ? h->transformed.rel : h->pending)->timestamps !=
            NULL);
        if (!rc) {
            if (h->output && h->source.width != h->output->ncols) return EINVAL;
            h->phase = h->streaming && !h->output
                ? WL_COLUMNAR_JOIN_HOST_SINK_OUTPUT :
                WL_COLUMNAR_JOIN_HOST_GROW;
        }
        return rc;
    case WL_COLUMNAR_JOIN_HOST_SINK_OUTPUT:
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK_SCHEMA);
        if (!h->row_buffer) {
            if (!wl_columnar_memory_size_mul(h->source.width, sizeof(int64_t),
                &h->row_bytes) || h->row_bytes > SIZE_MAX) return EOVERFLOW;
            rc = wl_columnar_join_host_reserve(r, h->row_bytes, &h->row_credit,
                    &h->row_admitted);
            if (rc) return rc;
            h->row_buffer =
                wl_columnar_join_attempt_allocate((size_t)h->row_bytes);
            if (!h->row_buffer) return ENOMEM;
            if (h->row_admitted) {
                if (!wl_columnar_memory_commit(&h->row_credit,
                    h)) return EINVAL;
                h->row_committed = true;
            }
            return 0;
        }
        rc =
            wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
            (
            &h->source, "$join", r->join_host_governor, &h->output);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_GROW;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_GROW: {
        uint64_t need = (uint64_t)h->base + h->source.nrows;
        if (need > UINT32_MAX) return EOVERFLOW;
        if (need > h->output->capacity) {
            uint64_t grown = (uint64_t)h->output->capacity * 2u;
            if (grown > UINT32_MAX) grown = UINT32_MAX;
            if (grown < need) grown = need;
            bool denied = false;
            rc =
                wl_columnar_relation_reserve_capacity_admitted_controlled(
                h->output,
                (uint32_t)grown, &denied, r->control);
            if (rc == ENOMEM && denied) rc = ENOSPC;
            if (rc == ENOSPC && denied && grown > need) {
                h->output->memory_budget_denial_pending = false;
                denied = false;
                rc = wl_columnar_relation_reserve_capacity_admitted_controlled(
                    h->output, (uint32_t)need, &denied, r->control);
                if (rc == ENOMEM && denied) rc = ENOSPC;
            }
            if (rc) return rc;
        }
        h->phase = WL_COLUMNAR_JOIN_HOST_WRITER;
        return 0;
    }
    case WL_COLUMNAR_JOIN_HOST_WRITER:
        rc = wl_columnar_join_host_original(h, 192);
        if (rc) return rc;
        wl_columnar_filter_attempt_batch_init(&h->writer);
        wl_columnar_relation_private_batch_drain_init(&h->writer_drain);
        h->writer_initialized = true;
        rc = wl_columnar_relation_append_batch_begin_controlled(&h->writer,
                h->output, r->control);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_GATHER;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_GATHER:
        if (h->row == h->source.nrows) {
            h->phase = WL_COLUMNAR_JOIN_HOST_WRITER_END;
        } else if (h->column < h->source.width) {
            (h->streaming ? h->row_buffer : h->prepare.types)[h->column]
                = h->source.columns[h->column][h->row];
            ++h->column;
        } else h->phase = WL_COLUMNAR_JOIN_HOST_APPEND;
        return 0;
    case WL_COLUMNAR_JOIN_HOST_APPEND:
        WL_COLUMNAR_JOIN_HOST_HOOK(h, WL_COLUMNAR_JOIN_PIPELINE_TEST_SINK);
        rc =
            wl_columnar_relation_append_batch_selected_source_row_operator_controlled
            (
            &h->writer, &h->source, h->row,
            h->streaming ? h->row_buffer : h->prepare.types, h->source.width);
        if (!rc) {
            ++h->row; h->column = 0; h->phase = WL_COLUMNAR_JOIN_HOST_GATHER;
        }
        return rc;
    case WL_COLUMNAR_JOIN_HOST_WRITER_END:
        rc = wl_columnar_relation_append_batch_end_controlled(&h->writer, 0);
        if (h->writer.phase == WL_COLUMNAR_RELATION_BATCH_ENDED
            && !h->writer.raw.active) h->writer_initialized = false;
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_SOURCE_END;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_SOURCE_END:
        rc = wl_columnar_relation_scalar_source_end(&h->source, 0);
        if (h->source.phase !=
            WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) return EBUSY;
        h->source_initialized = false;
        if (!rc) h->phase = h->schema_exchange
            ? WL_COLUMNAR_JOIN_HOST_TRANSFORM_RETIRE :
                WL_COLUMNAR_JOIN_HOST_ACK;
        return rc;
    case WL_COLUMNAR_JOIN_HOST_ACK:
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_BEFORE_ACK);
        rc = wl_columnar_join_batch_prepare_sink_commit_ack(&h->prepare,
                h->output,
                h->base, h->generation, &h->writer, &h->source, &h->extent, r);
        if (rc) {
            /* The producer API reports errno stops; session evaluation keeps
             * the initiating control's cancellation/work-limit result. */
            int stop = wl_columnar_join_host_original(h, 0);
            if (stop) return stop;
        }
        if (!rc) {
            h->pending = NULL; h->phase = h->streaming
                ? WL_COLUMNAR_JOIN_HOST_TRANSFORM_RETIRE :
                WL_COLUMNAR_JOIN_HOST_PRODUCE;
            WL_COLUMNAR_JOIN_HOST_HOOK(h,
                WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_ACK);
        }
        return rc;
    case WL_COLUMNAR_JOIN_HOST_FINAL_UNARY:
        r->join_host_tail_count = h->tail_end - r->cursor;
        rc = wl_columnar_eval_stack_unary_begin_attempt_owned(r->frame,
                r->control, r->attempt_id, r);
        if (rc) return rc;
        h->unary_started = true;
        const col_rel_t *original = NULL;
        rc = wl_columnar_eval_stack_unary_input_owned(r->frame, &original, r);
        if (rc) return rc;
        if (original != h->left) return EINVAL;
        rc = wl_columnar_eval_stack_unary_output_owned(r->frame,
                &h->output_slot, r);
        if (rc) return rc;
        *h->output_slot = h->output;
        h->output_in_frame = true;
        h->phase = WL_COLUMNAR_JOIN_HOST_OLD_PREPARE;
        return 0;
    case WL_COLUMNAR_JOIN_HOST_OLD_PREPARE: {
        bool ready = false;
        rc = wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned(
            r->frame, r, &ready);
        if (!rc && ready) h->phase = WL_COLUMNAR_JOIN_HOST_COMMIT;
        return rc;
    }
    case WL_COLUMNAR_JOIN_HOST_COMMIT:
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_BEFORE_COMMIT);
        rc = wl_columnar_eval_stack_unary_commit_numeric_join_owned(r->frame,
                &h->committed, r);
        if (h->committed) {
            h->output = NULL; h->output_slot = NULL;
            h->phase = WL_COLUMNAR_JOIN_HOST_RETIRE;
            WL_COLUMNAR_JOIN_HOST_HOOK(h,
                WL_COLUMNAR_JOIN_PIPELINE_TEST_AFTER_COMMIT);
        }
        return rc;
    default: return EINVAL;
    }
}

static int
wl_columnar_join_host_retire(wl_columnar_join_host_t *h, eval_entry_t *entry,
    wl_evaluation_control_t *funding, uint64_t id, bool output, bool *done)
{
    *done = false;
    int rc;
    wl_columnar_relation_scalar_retirement_initializer_t *i =
        &h->retirement_initializer;
    if (i->busy) return EBUSY;
    if (!h->retirement_entry && !h->retirement_started &&
        (!entry->owned || !entry->rel)){
        rc = wl_evaluation_control_charge_attempt(funding, id, 128);
        if (rc) return rc;
        memset(entry, 0, sizeof(*entry)); *done = true; return 0;
    }
    if (!h->retirement_entry) {
        h->retirement_entry = entry; h->retiring_output = output;
    }
    if (h->retirement_entry != entry ||
        h->retiring_output != output) return EINVAL;
    if (h->retirement_drained) {
        rc = wl_evaluation_control_charge_attempt(funding, id, 256);
        if (rc) return rc;
        memset(&h->retirement, 0, sizeof(h->retirement));
        h->retirement_started = h->retirement_committed =
            h->retirement_drained = false;
        h->retiring_output = false; h->retirement_entry = NULL; *done = true;
        return 0;
    }
    if (!h->retirement_started || i->identity) {
        if (!entry->owned || !entry->rel) return EINVAL;
        if (!i->identity) {
            if (entry->rel->memory_budget_denial_pending) {
                rc = wl_evaluation_control_charge_attempt(funding, id, 1);
                if (rc) return rc;
                h->parent->session->memory_budget_denied = true;
                entry->rel->memory_budget_denial_pending = false; return 0;
            }
            return wl_columnar_relation_scalar_retirement_init_begin(i, h,
                       &h->retirement, entry->rel, funding, id);
        }
        if (i->phase == WL_COLUMNAR_RELATION_SCALAR_INIT_DONE) {
            if (!h->retirement_started) return EINVAL;
            return wl_columnar_relation_scalar_retirement_init_reset(i, h,
                       funding, id);
        }
        bool begun = false;
        rc = wl_columnar_relation_scalar_retirement_init_step(i, h,
                &h->retirement,
                entry->rel, funding, id, &begun);
        if (!rc && begun) h->retirement_started = true;
        return rc;
    }
    if (!h->retirement_committed) {
        rc = wl_columnar_relation_scalar_retirement_prepare(&h->retirement,
                funding, id);
        if (rc == EAGAIN) return 0;
        if (rc) return rc;
        rc = wl_columnar_relation_scalar_retirement_commit(&h->retirement);
        if (rc) return rc;
        memset(entry, 0, sizeof(*entry)); h->retirement_committed = true;
        return 0;
    }
    rc = wl_columnar_relation_scalar_retirement_step(&h->retirement, funding,
            id);
    if (rc == EAGAIN) return 0;
    if (!rc) h->retirement_drained = true;
    return rc;
}

static int
wl_columnar_join_host_shape_retire(wl_columnar_join_host_t *h,
    wl_evaluation_control_t *funding, uint64_t id, bool *done)
{
    int rc = wl_columnar_join_host_retire(h, &h->shape_old, funding, id,
            false, done);
    h->shape_retiring = h->retirement_entry != NULL;
    return rc;
}

static int
wl_columnar_join_host_cleanup(wl_columnar_join_host_t *h,
    wl_evaluation_control_t *funding, uint64_t id)
{
    wl_columnar_eval_plan_numeric_attempt_t *r = h->parent;
    int rc;
    if (h->phase == WL_COLUMNAR_JOIN_HOST_SHAPE_END && !h->result) {
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_SCHEMA_END);
        if (h->shape_initialized) {
            rc = wl_columnar_join_host_fund(funding, id);
            if (rc) return rc;
            (void)wl_columnar_relation_scalar_source_end(&h->shape_source, 0);
            h->shape_initialized = false;
            return 0;
        }
        if (!h->shape_attempt.drained) {
            bool done = false;
            return wl_columnar_join_schema_attempt_end(&h->shape_attempt,
                       funding, id, &done);
        }
        rc = wl_evaluation_control_charge_attempt(funding, id, 128);
        if (rc) return rc;
        h->shape_old.rel = h->shape_current; h->shape_old.owned = true;
        h->shape_current = h->shape_next; h->shape_next = NULL;
        if (h->stage_cursor > r->cursor)
            h->stages[h->stage_cursor - r->cursor - 1u].active = true;
        h->phase = WL_COLUMNAR_JOIN_HOST_SHAPE_RETIRE;
        return 0;
    }
    if (h->phase == WL_COLUMNAR_JOIN_HOST_SHAPE_RETIRE && !h->result) {
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_SCHEMA_RETIRE);
        bool done = false;
        rc = wl_columnar_join_host_shape_retire(h, funding, id, &done);
        if (!rc && done) {
            ++h->stage_cursor;
            h->phase = WL_COLUMNAR_JOIN_HOST_STAGE_SCAN;
        }
        return rc;
    }
    if (h->phase == WL_COLUMNAR_JOIN_HOST_SHAPE_FINAL && !h->result) {
        WL_COLUMNAR_JOIN_HOST_HOOK(h,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_SCHEMA_FINAL);
        if (h->shape_initialized) {
            rc = wl_columnar_join_host_fund(funding, id);
            if (rc) return rc;
            (void)wl_columnar_relation_scalar_source_end(&h->shape_source, 0);
            h->shape_initialized = false; return 0;
        }
        if (h->shape_current) {
            rc = wl_evaluation_control_charge_attempt(funding, id, 128);
            if (rc) return rc;
            h->shape_old.rel = h->shape_current; h->shape_old.owned = true;
            h->shape_current = NULL; return 0;
        }
        if (h->shape_old.rel || h->retirement_started) {
            bool done = false;
            return wl_columnar_join_host_shape_retire(h, funding, id, &done);
        }
        rc = wl_columnar_join_host_fund(funding, id);
        if (!rc) h->phase = WL_COLUMNAR_JOIN_HOST_UNARY;
        return rc;
    }
    if (h->exchange_active) {
        if (h->consumer_live) {
            unsigned action = h->consumer_status.progress
                == WL_COLUMNAR_FILTER_PROGRESS_DONE
                ? WL_COLUMNAR_JOIN_STREAM_ACK : WL_COLUMNAR_JOIN_STREAM_STEP;
            return wl_columnar_join_host_consumer(h, action);
        }
        rc = wl_evaluation_control_charge_attempt(funding, id, 128);
        if (rc) return rc;
        rc = wl_columnar_eval_stack_stream_restore_owned(r->frame, r, h,
                &h->transformed);
        if (!rc) h->exchange_active = false;
        return rc;
    }
    if (h->shape_initialized) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        (void)wl_columnar_relation_scalar_source_end(&h->shape_source,
            h->result);
        h->shape_initialized = false;
        return 0;
    }
    if (h->shape_attempt.identity && !h->shape_attempt.drained) {
        bool done = false;
        return wl_columnar_join_schema_attempt_end(&h->shape_attempt, funding,
                   id, &done);
    }
    if (h->shape_current || h->shape_next) {
        rc = wl_evaluation_control_charge_attempt(funding, id, 128);
        if (rc) return rc;
        if (!h->shape_old.rel && !h->retirement_started) {
            col_rel_t **slot = h->shape_current ? &h->shape_current
                : &h->shape_next;
            h->shape_old.rel = *slot; h->shape_old.owned = true; *slot = NULL;
        }
        bool done = false;
        return wl_columnar_join_host_shape_retire(h, funding, id, &done);
    }
    if (h->shape_old.rel || h->shape_retiring) {
        bool done = false;
        return wl_columnar_join_host_shape_retire(h, funding, id, &done);
    }
    if (h->writer_initialized) {
        rc = wl_evaluation_control_charge_attempt(funding, id, 192);
        if (rc) return rc;
        bool done = false;
        rc = wl_columnar_relation_private_batch_drain_step(&h->writer_drain,
                &h->writer, h->result, &done);
        if (!rc && done) h->writer_initialized = false;
        return rc;
    }
    if (h->source_initialized || h->left_initialized || h->right_initialized) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        wl_columnar_relation_scalar_source_t *source = h->source_initialized
            ? &h->source : h->left_initialized ? &h->left_source :
            &h->right_source;
        (void)wl_columnar_relation_scalar_source_end(source, h->result);
        if (source->phase !=
            WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) return EBUSY;
        if (source == &h->source) h->source_initialized = false;
        else if (source == &h->left_source) h->left_initialized = false;
        else h->right_initialized = false;
        return 0;
    }
    if (h->phase == WL_COLUMNAR_JOIN_HOST_TRANSFORM_RETIRE) {
        bool done = false;
        rc = wl_columnar_join_host_retire(h, &h->transformed, funding, id,
                false, &done);
        if (!rc && done) {
            h->pending = NULL; h->schema_exchange = false;
            h->phase = h->producer_done ? WL_COLUMNAR_JOIN_HOST_DRAIN
                : WL_COLUMNAR_JOIN_HOST_PRODUCE;
        }
        return rc;
    }
    if (h->transformed.rel || (h->retirement_started && !h->retiring_output)) {
        bool done = false;
        return wl_columnar_join_host_retire(h, &h->transformed, funding, id,
                   false, &done);
    }
    if (h->stages && h->stages_initialized == h->stages_bytes
        && h->stage_cleanup < h->stage_count) {
        wl_columnar_join_host_stage_t *stage = &h->stages[h->stage_cleanup];
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        if (stage->initialized) {
            (void)wl_columnar_relation_scalar_source_end(&stage->source,
                h->result);
            stage->initialized = false;
        } else if (stage->keys) {
            free(stage->keys); stage->keys = NULL;
        } else if (stage->admitted) {
            bool done = false;
            rc = wl_columnar_memory_reservation_finish_step(&stage->credit,
                    !stage->committed, &done);
            if (!rc && done) stage->admitted = false;
        } else ++h->stage_cleanup;
        return rc;
    }
    if (h->stages) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        free(h->stages); h->stages = NULL;
        return 0;
    }
    if (h->stages_admitted) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        bool done = false;
        rc = wl_columnar_memory_reservation_finish_step(&h->stages_credit,
                !h->stages_committed, &done);
        if (!rc && done) h->stages_admitted = false;
        return rc;
    }
    if (h->prepare.identity && !h->prepare.drained) {
        h->pending = NULL;
        bool done = false;
        return wl_columnar_join_batch_prepare_abort_step(&h->prepare, funding,
                   id, &done);
    }
    h->pending = NULL;
    if (h->unary_started || h->result) h->left = NULL;
    h->right = h->full = h->delta = NULL;
    if (!h->result && !r->join_host_declined && !h->committed
        && h->phase == WL_COLUMNAR_JOIN_HOST_DRAIN) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (!rc) h->phase = h->streaming ? WL_COLUMNAR_JOIN_HOST_FINAL_UNARY
            : WL_COLUMNAR_JOIN_HOST_OLD_PREPARE;
        return rc;
    }
    if (h->streaming && !h->output_in_frame && h->output) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        h->retiring_entry.rel = h->output; h->retiring_entry.owned = true;
        h->output = NULL;
        return 0;
    }
    if (h->retiring_entry.rel ||
        (h->retirement_started && h->retiring_output)) {
        bool done = false;
        return wl_columnar_join_host_retire(h, &h->retiring_entry, funding, id,
                   true, &done);
    }
    if (h->unary_started && !h->unary_finished) {
        if (h->output && h->output->memory_budget_denial_pending) {
            rc = wl_columnar_join_host_fund(funding, id);
            if (rc) return rc;
            r->session->memory_budget_denied = true;
            h->output->memory_budget_denial_pending = false;
            return 0;
        }
        /* The frame's output slot owns provisional storage. Drop borrowed
        * markers before funded finish can transfer it into retirement. */
        h->output = NULL; h->output_slot = NULL;
        bool done = false; int semantic = h->result;
        rc = wl_columnar_eval_stack_unary_finish_scalar_funded_owned(r->frame,
                h->result, r, funding, id, &semantic, &done);
        if (!rc && done) {
            if (!h->result) h->result = semantic;
            h->unary_finished = true; h->phase = WL_COLUMNAR_JOIN_HOST_RESET;
            h->output_in_frame = false;
        }
        return rc;
    }
    if (h->unary_started) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        rc = wl_columnar_eval_stack_unary_reset_owned(r->frame, r);
        if (!rc) h->unary_started = false;
        return rc;
    }
    if (h->keys) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        free(h->keys); h->keys = NULL;
        return 0;
    }
    if (h->keys_admitted) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        bool done = false;
        rc = wl_columnar_memory_reservation_finish_step(&h->key_credit,
                !h->keys_committed, &done);
        if (!rc && done) h->keys_admitted = false;
        return rc;
    }
    if (h->row_buffer) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        free(h->row_buffer); h->row_buffer = NULL;
        return 0;
    }
    if (h->row_admitted) {
        rc = wl_columnar_join_host_fund(funding, id);
        if (rc) return rc;
        bool done = false;
        rc = wl_columnar_memory_reservation_finish_step(&h->row_credit,
                !h->row_committed, &done);
        if (!rc && done) h->row_admitted = false;
        return rc;
    }
    rc = wl_columnar_join_host_fund(funding, id);
    if (rc) return rc;
    wl_columnar_join_host_terminal(r, h->result, h->committed);
    free(h); r->join_host = NULL;
    return 0;
}

int
wl_columnar_join_pipeline_numeric_delta_owned(const void *owner,
    const wl_columnar_eval_stack_cleanup_frame_t *frame, bool *delta)
{
    const wl_columnar_eval_plan_numeric_attempt_t *r = owner;
    if (!delta || !r || !wl_columnar_eval_plan_numeric_owner_valid(r, frame)
        || r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        || r->pending_action != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
        || !r->controlled_materialized_batch_fallback || !r->join_host
        || r->join_host_initialized_bytes !=
        sizeof(wl_columnar_join_host_t) + 1u)
        return EINVAL;
    const wl_columnar_join_host_t *h = r->join_host;
    if (h->identity != h || h->parent != r ||
        h->phase != WL_COLUMNAR_JOIN_HOST_COMMIT
        || h->op != &r->plan->ops[r->cursor] || h->op->op != WL_PLAN_OP_JOIN
        || h->result || h->used_delta || !h->unary_started ||
        !h->prepare.drained)
        return EINVAL;
    *delta = h->result_delta;
    return 0;
}

static int
wl_columnar_join_pipeline_numeric_step_body(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!r || !wl_columnar_eval_plan_numeric_owner_valid(r, r->frame)
        || !r->controlled_materialized_batch_fallback
        || r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        || r->pending_action != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
        || r->session->cleanup_active != r->frame
        || !funding || funding == r->control
        || wl_evaluation_control_active_attempt_id(funding) !=
        funding_id) return EINVAL;
    r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_YIELD;
    int rc;
    if (r->semantic_result && !r->join_host_terminal
        && (!r->join_host || r->join_host_initialized_bytes
        <= sizeof(wl_columnar_join_host_t)))
        wl_columnar_join_host_terminal(r, r->semantic_result, false);
    if (r->join_host_terminal) {
        r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_RETAINED;
        rc = wl_columnar_join_host_fund(funding, funding_id);
        if (rc) return rc;
        if (r->join_host) {
            free(r->join_host); r->join_host = NULL; return 0;
        }
        if (r->join_host_admitted) {
            bool done = false;
            rc =
                wl_columnar_memory_reservation_finish_step(&r->join_host_credit,
                    !r->join_host_committed, &done);
            if (!rc && done) r->join_host_admitted = false;
            return rc;
        }
        wl_columnar_memory_governor_ref_release(r->join_host_governor);
        r->join_host_governor = NULL;
        r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        return 0;
    }
    if (!r->join_host) {
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_join_pipeline_test_numeric_checkpoint)
            wl_columnar_join_pipeline_test_numeric_checkpoint(r->control,
                WL_COLUMNAR_JOIN_PIPELINE_TEST_BOOTSTRAP, 0, 0, 0);
#endif
        rc = wl_columnar_join_host_bootstrap(r);
        if (rc) wl_columnar_join_host_terminal(r, rc, false);
        return 0;
    }
    if (r->join_host_initialized_bytes <= sizeof(wl_columnar_join_host_t)) {
        rc = wl_columnar_join_host_initialize(r);
        if (rc) {
            if (r->join_host_initialized_bytes >
                sizeof(wl_columnar_join_host_t))
                wl_columnar_join_host_fail(r->join_host, rc);
            else wl_columnar_join_host_terminal(r, rc, false);
        }
        return 0;
    }
    wl_columnar_join_host_t *h = r->join_host;
    if (h->identity != h || h->parent != r) return EINVAL;
    if (h->consumer_pending) {
        if (r->protocol_result != h->consumer_protocol) return EINVAL;
        h->consumer_pending = false;
        if (h->consumer_protocol && h->consumer_live
            && h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_START)
            h->phase = h->consumer_status.progress ==
                WL_COLUMNAR_FILTER_PROGRESS_DONE
                ? WL_COLUMNAR_JOIN_HOST_CONSUMER_ACK :
                WL_COLUMNAR_JOIN_HOST_CONSUMER_STEP;
    }
    if (r->semantic_result && !h->result)
        wl_columnar_join_host_fail(h, r->semantic_result);
    if (h->phase < WL_COLUMNAR_JOIN_HOST_DRAIN
        || h->phase == WL_COLUMNAR_JOIN_HOST_OLD_PREPARE
        || h->phase == WL_COLUMNAR_JOIN_HOST_COMMIT
        || h->phase == WL_COLUMNAR_JOIN_HOST_FINAL_UNARY) {
        bool consumer = h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_START
            || h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_STEP
            || h->phase == WL_COLUMNAR_JOIN_HOST_CONSUMER_ACK;
        rc = wl_columnar_join_host_original(h, 1);
        if (!rc) {
            rc = wl_columnar_join_host_forward(h);
            if (consumer && h->consumer_pending) return rc;
        }
        if (rc) wl_columnar_join_host_fail(h, rc);
        return 0;
    }
    r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_RETAINED;
    return wl_columnar_join_host_cleanup(h, funding, funding_id);
}

/* Lexical parent exclusion spans initializer and inherited governor/retirement
 * hooks. Read-only host authentication remains available to its own driver. */
int
wl_columnar_join_pipeline_numeric_step(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (r && r->join_host_busy) return EBUSY;
    if (!r || !wl_columnar_eval_plan_numeric_owner_valid(r,
        r->frame)) return EINVAL;
    wl_columnar_join_host_t *h = NULL;
    if (r->join_host &&
        r->join_host_initialized_bytes > sizeof(wl_columnar_join_host_t)) {
        h = r->join_host;
        if (h->identity != h || h->parent != r) return EINVAL;
        if (h->busy) return EBUSY;
        h->busy = true;
    }
    int rc = wl_columnar_join_pipeline_numeric_step_body(r, funding,
            funding_id);
    if (h && r->join_host == h) h->busy = false;
    return rc;
}

/* The checked setup entry owns this activation. These values classify the
 * actual failing call; equal numeric funding/original stops are not guessed. */
typedef enum {
    WL_COLUMNAR_JOIN_PIPELINE_SETUP_OK,
    WL_COLUMNAR_JOIN_PIPELINE_SETUP_FUNDING,
    WL_COLUMNAR_JOIN_PIPELINE_SETUP_SEMANTIC,
    WL_COLUMNAR_JOIN_PIPELINE_SETUP_INVALID
} wl_columnar_join_pipeline_setup_outcome_t;
typedef struct {
    int protocol;
    wl_columnar_join_pipeline_setup_outcome_t outcome;
} wl_columnar_join_pipeline_setup_result_t;

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_result(wl_columnar_join_setup_result_t result)
{
    return (wl_columnar_join_pipeline_setup_result_t){result.protocol,
                                                      !result.protocol ?
                                                      WL_COLUMNAR_JOIN_PIPELINE_SETUP_OK
        : result.outcome == WL_COLUMNAR_JOIN_SETUP_FUNDING_REFUSED
        ? WL_COLUMNAR_JOIN_PIPELINE_SETUP_FUNDING
        : WL_COLUMNAR_JOIN_PIPELINE_SETUP_SEMANTIC};
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_head(wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id, uint64_t work)
{
    return wl_columnar_join_pipeline_setup_result(wl_columnar_join_setup_head(
                   r->control, r->attempt_id, funding, id, work));
}

static void
wl_columnar_join_pipeline_setup_fail(wl_columnar_eval_plan_numeric_attempt_t *r,
    int error)
{
    if (!r->join_host_terminal)
        wl_columnar_join_host_terminal(r,
            r->semantic_result ? r->semantic_result : error, false);
}

static int
wl_columnar_join_pipeline_setup_lineage(
    wl_columnar_eval_plan_numeric_attempt_t *r)
{
    int rc = wl_columnar_eval_stack_numeric_owner(r->frame, r);
    if (rc) return rc;
    return r->session->cleanup_active == r->frame && r->composed_busy
           && r->join_host_busy
           && r->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
           && r->pending_action ==
           WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
        ? 0 : EINVAL;
}

/* Canonical resource flags and the first semantic result precede fallible
 * outer polls. A late funding stop does not undo completed ownership work. */
static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_tail(wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id, int semantic)
{
    if (semantic) wl_columnar_join_pipeline_setup_fail(r, semantic);
    wl_columnar_join_pipeline_setup_result_t result =
        wl_columnar_join_pipeline_setup_result(wl_columnar_join_setup_tail(
                r->control, r->attempt_id, funding, id));
    if (result.protocol) return result;
    return (wl_columnar_join_pipeline_setup_result_t){semantic,
                                                      semantic ?
                                                      WL_COLUMNAR_JOIN_PIPELINE_SETUP_SEMANTIC
        : WL_COLUMNAR_JOIN_PIPELINE_SETUP_OK};
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_resource_tail(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id, int semantic)
{
    int rc = wl_columnar_join_pipeline_setup_lineage(r);
    return wl_columnar_join_pipeline_setup_tail(r, funding, id,
               semantic ? semantic : rc);
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_cleanup_head(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id, uint64_t work)
{
    int rc = wl_evaluation_control_charge_attempt(funding, id, work);
    return (wl_columnar_join_pipeline_setup_result_t){rc,
                                                      rc ?
                                                      WL_COLUMNAR_JOIN_PIPELINE_SETUP_FUNDING
        : WL_COLUMNAR_JOIN_PIPELINE_SETUP_OK};
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_cleanup_tail(wl_evaluation_control_t *funding,
    uint64_t id, int error)
{
    int rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return (wl_columnar_join_pipeline_setup_result_t){rc,
                                                              WL_COLUMNAR_JOIN_PIPELINE_SETUP_FUNDING};
    return (wl_columnar_join_pipeline_setup_result_t){error,
                                                      error ?
                                                      WL_COLUMNAR_JOIN_PIPELINE_SETUP_INVALID
        : WL_COLUMNAR_JOIN_PIPELINE_SETUP_OK};
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_cleanup(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id)
{
    r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_RETAINED;
    wl_columnar_join_pipeline_setup_result_t result;
    if (r->join_host) {
        result = wl_columnar_join_pipeline_setup_cleanup_head(r, funding, id,
                WL_COLUMNAR_JOIN_PIPELINE_SETUP_ALLOC_WORK);
        if (result.protocol) return result;
        void *host = r->join_host;
        r->join_host = NULL;
        free(host);
        return wl_columnar_join_pipeline_setup_cleanup_tail(funding, id, 0);
    }
    if (r->join_host_admitted) {
        result = wl_columnar_join_pipeline_setup_cleanup_head(r, funding, id,
                WL_COLUMNAR_JOIN_PIPELINE_SETUP_CREDIT_WORK);
        if (result.protocol) return result;
        bool done = false;
        int rc =
            wl_columnar_memory_reservation_finish_step(&r->join_host_credit,
                !r->join_host_committed, &done);
        if (!rc && done) {
            r->join_host_admitted = false;
            r->join_host_committed = false;
        }
        return wl_columnar_join_pipeline_setup_cleanup_tail(funding, id, rc);
    }
    if (r->join_host_governor) {
        result = wl_columnar_join_pipeline_setup_cleanup_head(r, funding, id,
                WL_COLUMNAR_JOIN_PIPELINE_SETUP_REF_WORK);
        if (result.protocol) return result;
        wl_columnar_memory_governor_ref_t *governor = r->join_host_governor;
        r->join_host_governor = NULL;
        wl_columnar_memory_governor_ref_release(governor);
        r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        return wl_columnar_join_pipeline_setup_cleanup_tail(funding, id, 0);
    }
    r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
    return (wl_columnar_join_pipeline_setup_result_t){0,
                                                      WL_COLUMNAR_JOIN_PIPELINE_SETUP_OK};
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_commit(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id)
{
    wl_columnar_join_pipeline_setup_result_t result =
        wl_columnar_join_pipeline_setup_head(r, funding, id,
            WL_COLUMNAR_JOIN_PIPELINE_SETUP_COMMIT_WORK);
    if (result.protocol) return result;
    bool committed = false;
    int rc = wl_columnar_memory_governor_commit_attempt_controlled(
        &r->join_host_credit, r->join_host, &committed,
        r->control, r->attempt_id);
    if (!rc) {
        r->join_host_committed = committed;
        if (!committed) rc = EINVAL;
    }
    return wl_columnar_join_pipeline_setup_resource_tail(r, funding, id, rc);
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_bootstrap(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_join_pipeline_test_numeric_checkpoint)
        wl_columnar_join_pipeline_test_numeric_checkpoint(r->control,
            WL_COLUMNAR_JOIN_PIPELINE_TEST_BOOTSTRAP, 0, 0, 0);
#endif
    int rc = wl_columnar_join_pipeline_setup_lineage(r);
    if (rc) return (wl_columnar_join_pipeline_setup_result_t){rc,
                                                              WL_COLUMNAR_JOIN_PIPELINE_SETUP_INVALID};
    wl_columnar_join_pipeline_setup_result_t result =
        wl_columnar_join_pipeline_setup_head(r, funding, id,
            WL_COLUMNAR_JOIN_PIPELINE_SETUP_STACK_WORK);
    if (result.protocol) return result;
    rc = wl_evaluation_control_charge_attempt(r->control, r->attempt_id, 16);
    eval_stack_t *stack = NULL;
    if (!rc) rc = wl_columnar_eval_stack_operator_stack_owned(r->frame,
                r->session, &stack, r);
    if (!rc && (!stack->top || stack->items[stack->top - 1].kind
        != WL_COLUMNAR_EVAL_ENTRY_RELATION ||
        !stack->items[stack->top - 1].rel))
        rc = EINVAL;
    result = wl_columnar_join_pipeline_setup_tail(r, funding, id, rc);
    if (result.protocol) return result;
    if (!r->join_host_governor) {
        wl_columnar_memory_governor_ref_t *candidate =
            r->session->memory_governor
            ? r->session->memory_governor
            : stack->items[stack->top - 1].rel->memory_governor;
        col_rel_t *right = NULL, *delta = NULL;
        const wl_plan_op_t *op = &r->plan->ops[r->cursor];
        bool left_delta = stack->items[stack->top - 1].is_delta;
        if (!candidate) {
            const wl_columnar_eval_private_bindings_t *bindings = NULL;
            wl_columnar_join_setup_result_t checked =
                wl_columnar_eval_plan_numeric_private_bindings_get_funded(r,
                    r->frame, r->session, r->control, r->attempt_id,
                    funding, id, &bindings);
            result = wl_columnar_join_pipeline_setup_result(checked);
            if (result.protocol) return result;
            if (bindings) return (wl_columnar_join_pipeline_setup_result_t){
                           EINVAL,
                           WL_COLUMNAR_JOIN_PIPELINE_SETUP_SEMANTIC
                };
            result = wl_columnar_join_pipeline_setup_head(r, funding, id,
                    WL_COLUMNAR_JOIN_PIPELINE_SETUP_LOOKUP_WORK);
            if (result.protocol) return result;
            rc = wl_columnar_join_attempt_lookup(r->session, op->right_relation,
                    false, &right, r->control, r->attempt_id, NULL);
            result = wl_columnar_join_pipeline_setup_resource_tail(r, funding,
                    id, rc);
            if (result.protocol) return result;
            if (right && op->delta_mode != WL_DELTA_FORCE_FULL
                && (op->delta_mode == WL_DELTA_FORCE_DELTA || !left_delta)) {
                result = wl_columnar_join_pipeline_setup_head(r, funding, id,
                        WL_COLUMNAR_JOIN_PIPELINE_SETUP_LOOKUP_WORK);
                if (result.protocol) return result;
                rc = wl_columnar_join_attempt_lookup(r->session,
                        op->right_relation,
                        true, &delta, r->control, r->attempt_id, NULL);
                result = wl_columnar_join_pipeline_setup_resource_tail(r,
                        funding, id, rc);
                if (result.protocol) return result;
            }
        }
        result = wl_columnar_join_pipeline_setup_head(r, funding, id,
                WL_COLUMNAR_JOIN_PIPELINE_SETUP_SELECT_WORK);
        if (result.protocol) return result;
        if (!candidate) {
            if (right && delta && delta->nrows
                && (op->delta_mode == WL_DELTA_FORCE_DELTA
                || (op->delta_mode != WL_DELTA_FORCE_FULL && !left_delta
                && delta->nrows < right->nrows))) right = delta;
            col_rel_t *append = NULL;
            rc = wl_columnar_session_delta_source(r->session, NULL, op,
                    &append);
            if (!rc && append) right = append;
            if (!rc && right) candidate = right->memory_governor;
        }
        if (!rc) {
            wl_columnar_memory_governor_ref_retain(candidate);
            r->join_host_governor = candidate;
        }
        result = wl_columnar_join_pipeline_setup_tail(r, funding, id, rc);
        if (result.protocol) return result;
    }
    if (r->join_host_governor && !r->join_host_admitted) {
        result = wl_columnar_join_pipeline_setup_head(r, funding, id,
                WL_COLUMNAR_JOIN_PIPELINE_SETUP_RESERVE_WORK);
        if (result.protocol) return result;
        wl_columnar_memory_admission_status_t status =
            WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(r->join_host_governor),
            sizeof(wl_columnar_join_host_t), &r->join_host_credit,
            &status, r->control, r->attempt_id);
        if (!rc) {
            if (status <= WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
                r->join_host_admitted = true;
            else rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status ==
                    WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : ENOMEM;
        }
        if (rc == ENOSPC) r->session->memory_budget_denied = true;
        result = wl_columnar_join_pipeline_setup_resource_tail(r, funding, id,
                rc);
        if (result.protocol) return result;
    }
    result = wl_columnar_join_pipeline_setup_head(r, funding, id,
            WL_COLUMNAR_JOIN_PIPELINE_SETUP_ALLOC_WORK);
    if (result.protocol) return result;
    r->join_host =
        wl_columnar_join_attempt_allocate(sizeof(wl_columnar_join_host_t));
    rc = r->join_host ? 0 : ENOMEM;
    if (!rc) r->join_host_initialized_bytes = 0;
    result = wl_columnar_join_pipeline_setup_resource_tail(r, funding, id, rc);
    if (result.protocol) return result;
    if (r->join_host_admitted && !r->join_host_committed)
        return wl_columnar_join_pipeline_setup_commit(r, funding, id);
    return result;
}

static void
wl_columnar_join_pipeline_setup_classified(
    wl_columnar_eval_plan_numeric_attempt_t *r, wl_columnar_join_host_t *h)
{
    h->streaming = h->setup.cursor > r->cursor + 1u;
    h->tail_end = h->streaming ? h->setup.cursor - 1u : UINT32_MAX;
    h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_BIND;
}

/* Exactly one old descriptor, MAP-column or opcode charge is executed here.
 * Header/parse progress is private immutable-plan state, never a source loan. */
static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_classify(
    wl_columnar_eval_plan_numeric_attempt_t *r, wl_columnar_join_host_t *h,
    wl_evaluation_control_t *funding, uint64_t id)
{
    wl_columnar_join_pipeline_setup_result_t result =
        wl_columnar_join_pipeline_setup_head(r, funding, id,
            WL_COLUMNAR_JOIN_PIPELINE_SETUP_CLASSIFY_WORK);
    if (result.protocol) return result;
    int rc = wl_evaluation_control_charge_attempt(r->control, r->attempt_id, 1);
    if (rc) return wl_columnar_join_pipeline_setup_tail(r, funding, id, rc);
    const wl_plan_op_t *op = &r->plan->ops[h->setup.cursor];
    if (h->setup.phase == WL_COLUMNAR_JOIN_HOST_SETUP_TAIL) {
        if (op->op == WL_PLAN_OP_FILTER) {
            if (op->materialized || !op->filter_expr.data ||
                !op->filter_expr.size)
                wl_columnar_join_pipeline_setup_classified(r, h);
            else h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_FILTER;
        } else if (op->op == WL_PLAN_OP_SEMIJOIN ||
            op->op == WL_PLAN_OP_ANTIJOIN) {
            if (r->session->tdd_input_run
                || !wl_columnar_join_pipeline_stage_is_consumable(op)
                || ++h->setup.cursor == r->plan->op_count)
                wl_columnar_join_pipeline_setup_classified(r, h);
        } else if (op->op == WL_PLAN_OP_MAP) {
            bool projectable = op->project_count && !op->materialized;
            if (projectable && op->map_expr_count && op->map_exprs)
                h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_MAP_COLUMN;
            else {
                if (projectable && !op->map_expr_count && !op->map_exprs
                    && op->project_indices) ++h->setup.cursor;
                wl_columnar_join_pipeline_setup_classified(r, h);
            }
        } else wl_columnar_join_pipeline_setup_classified(r, h);
    } else if (h->setup.phase == WL_COLUMNAR_JOIN_HOST_SETUP_MAP_COLUMN) {
        const wl_plan_expr_buffer_t *expr = &op->map_exprs[h->setup.column];
        if (expr->data && expr->size)
            h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_MAP_EXPR;
        else if (++h->setup.column == op->map_expr_count) {
            ++h->setup.cursor;
            wl_columnar_join_pipeline_setup_classified(r, h);
        }
    } else {
        bool map = h->setup.phase == WL_COLUMNAR_JOIN_HOST_SETUP_MAP_EXPR;
        const wl_plan_expr_buffer_t *expr = map
            ? &op->map_exprs[h->setup.column] : &op->filter_expr;
        if (!wl_columnar_join_pipeline_expr_next(expr, map,
            &h->setup.position, &h->setup.depth))
            wl_columnar_join_pipeline_setup_classified(r, h);
        else if (h->setup.position == expr->size) {
            if (h->setup.depth != 1)
                wl_columnar_join_pipeline_setup_classified(r, h);
            else if (!map) {
                ++h->setup.cursor;
                h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_TAIL;
                if (h->setup.cursor == r->plan->op_count)
                    wl_columnar_join_pipeline_setup_classified(r, h);
            } else if (++h->setup.column == op->map_expr_count) {
                ++h->setup.cursor;
                wl_columnar_join_pipeline_setup_classified(r, h);
            } else h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_MAP_COLUMN;
            h->setup.position = h->setup.depth = 0;
        }
    }
    return wl_columnar_join_pipeline_setup_tail(r, funding, id, 0);
}

static wl_columnar_join_pipeline_setup_result_t
wl_columnar_join_pipeline_setup_unready(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t id)
{
    wl_columnar_join_pipeline_setup_result_t result;
    if (r->join_host_admitted && !r->join_host_committed)
        return wl_columnar_join_pipeline_setup_commit(r, funding, id);
    uint64_t total = sizeof(wl_columnar_join_host_t);
    if (r->join_host_initialized_bytes < total) {
        result = wl_columnar_join_pipeline_setup_head(r, funding, id,
                WL_COLUMNAR_JOIN_PIPELINE_SETUP_ZERO_WORK);
        if (result.protocol) return result;
        uint64_t bytes = total - r->join_host_initialized_bytes;
        if (bytes > 256) bytes = 256;
        int rc = wl_evaluation_control_charge_attempt(r->control, r->attempt_id,
                bytes);
        if (!rc) {
            memset((unsigned char *)r->join_host +
                r->join_host_initialized_bytes,
                0, (size_t)bytes);
            r->join_host_initialized_bytes += bytes;
        }
        return wl_columnar_join_pipeline_setup_tail(r, funding, id, rc);
    }
    wl_columnar_join_host_t *h = r->join_host;
    if (h->setup.phase == WL_COLUMNAR_JOIN_HOST_SETUP_HEADER) {
        result = wl_columnar_join_pipeline_setup_head(r, funding, id,
                WL_COLUMNAR_JOIN_PIPELINE_SETUP_HEADER_WORK);
        if (result.protocol) return result;
        int rc = wl_evaluation_control_charge_attempt(r->control, r->attempt_id,
                16);
        if (!rc) {
            h->identity = h; h->parent = r; h->op = &r->plan->ops[r->cursor];
            wl_columnar_memory_reservation_init(&h->key_credit);
            wl_columnar_memory_reservation_init(&h->row_credit);
            wl_columnar_memory_reservation_init(&h->stages_credit);
            h->tail_end = UINT32_MAX;
            h->setup.cursor = r->cursor + 1u;
            h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_TAIL;
            if (wl_columnar_join_pipeline_preflight_header(r->plan, r->cursor,
                r->session) != WL_COLUMNAR_JOIN_PIPELINE_ELIGIBLE
                || h->setup.cursor == r->plan->op_count)
                h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_BIND;
        }
        return wl_columnar_join_pipeline_setup_tail(r, funding, id, rc);
    }
    if (h->setup.phase >= WL_COLUMNAR_JOIN_HOST_SETUP_TAIL
        && h->setup.phase <= WL_COLUMNAR_JOIN_HOST_SETUP_MAP_EXPR)
        return wl_columnar_join_pipeline_setup_classify(r, h, funding, id);
    if (h->setup.phase == WL_COLUMNAR_JOIN_HOST_SETUP_BIND) {
        const wl_columnar_eval_private_bindings_t *bindings = NULL;
        wl_columnar_join_setup_result_t checked =
            wl_columnar_eval_plan_numeric_private_bindings_get_funded(r,
                r->frame, r->session, r->control, r->attempt_id,
                funding, id, &bindings);
        result = wl_columnar_join_pipeline_setup_result(checked);
        if (result.protocol) return result;
        if (bindings) return (wl_columnar_join_pipeline_setup_result_t){EINVAL,
                                                                        WL_COLUMNAR_JOIN_PIPELINE_SETUP_SEMANTIC};
        h->setup.phase = WL_COLUMNAR_JOIN_HOST_SETUP_PUBLISH;
        return result;
    }
    if (h->setup.phase != WL_COLUMNAR_JOIN_HOST_SETUP_PUBLISH)
        return (wl_columnar_join_pipeline_setup_result_t){EINVAL,
                                                          WL_COLUMNAR_JOIN_PIPELINE_SETUP_INVALID};
    result = wl_columnar_join_pipeline_setup_head(r, funding, id,
            WL_COLUMNAR_JOIN_PIPELINE_SETUP_PUBLISH_WORK);
    if (result.protocol) return result;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(r->frame, r->session,
            &stack, r);
    if (!rc && (!stack->top || stack->items[stack->top - 1].kind
        != WL_COLUMNAR_EVAL_ENTRY_RELATION || !stack->items[stack->top - 1].rel
        || h->identity != h || h->parent != r
        || h->op != &r->plan->ops[r->cursor]
        || h->key_credit.identity != &h->key_credit
        || h->row_credit.identity != &h->row_credit
        || h->stages_credit.identity != &h->stages_credit
        || h->key_credit.governor || h->row_credit.governor
        || h->stages_credit.governor || h->keys_admitted
        || h->row_admitted || h->stages_admitted)) rc = EINVAL;
    /* HEADER initialized these tokens. The pure classifier and NONE binding
    * proof cannot mutate them; no child has run before this publication. */
    result = wl_columnar_join_pipeline_setup_tail(r, funding, id, rc);
    if (result.protocol) return result;
    h->left = stack->items[stack->top - 1].rel;
    h->left_delta = stack->items[stack->top - 1].is_delta;
    h->bindings = NULL; h->phase = WL_COLUMNAR_JOIN_HOST_LEFT_INIT;
    r->join_host_initialized_bytes = sizeof(*h) + 1u;
    return result;
}

int
wl_columnar_join_pipeline_numeric_setup_step(
    wl_columnar_eval_plan_numeric_attempt_t *r,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!r || r->identity != r) return EINVAL;
    if (r->join_host_busy) return EBUSY;
    int rc = wl_columnar_eval_stack_numeric_owner(r->frame, r);
    if (rc) return rc;
    if (!r->composed_busy || !r->composed_mode
        || !r->controlled_materialized_batch_fallback || r->private_bindings
        || !r->control || !r->attempt_id
        || r->session->cleanup_active != r->frame
        || r->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
        || r->pending_action != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_JOIN_BATCH
        || !r->plan || !r->plan->ops || r->cursor < r->begin
        || r->cursor >= r->end || r->cursor >= r->plan->op_count
        || r->plan->ops[r->cursor].op != WL_PLAN_OP_JOIN
        || !funding || funding == r->control || !funding_id)
        return EINVAL;
    if (r->join_host &&
        r->join_host_initialized_bytes > sizeof(wl_columnar_join_host_t))
        return EBUSY;
    r->join_host_busy = true;
    r->child_status.progress = WL_COLUMNAR_FILTER_PROGRESS_YIELD;
    if (r->semantic_result && !r->join_host_terminal)
        wl_columnar_join_host_terminal(r, r->semantic_result, false);
    wl_columnar_join_pipeline_setup_result_t result = r->join_host_terminal
        ? wl_columnar_join_pipeline_setup_cleanup(r, funding, funding_id)
        : !r->join_host
        ? wl_columnar_join_pipeline_setup_bootstrap(r, funding, funding_id)
        : wl_columnar_join_pipeline_setup_unready(r, funding, funding_id);
    if (result.outcome == WL_COLUMNAR_JOIN_PIPELINE_SETUP_SEMANTIC) {
        wl_columnar_join_pipeline_setup_fail(r, result.protocol);
        result.protocol = 0;
    }
    r->join_host_busy = false;
    return result.protocol;
}
