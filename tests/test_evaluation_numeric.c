/* #1821: real numeric expression lifecycle, budgets and ownership. */
#include "../wirelog/columnar/internal.h"
#include <float.h>
#include <math.h>
#include <stdio.h>

#define CHECK(x) do { if (!(x)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #x); exit(1); \
                      } } while (0)

typedef struct {
    uint8_t bytes[20000];
    uint32_t size, instructions, digits;
} program_t;
static int owner_key;
static unsigned cancel_phase;
static uint64_t cancel_progress;
static bool cancel_failure;
static wl_columnar_memory_governor_t *watch;
static wl_columnar_memory_reservation_t peer;
static bool add_peer, peer_added, saw_physical;
static uint64_t physical_credit;

static void
checkpoint(wl_evaluation_control_t *c, unsigned phase, uint64_t progress,
    bool failed)
{
    if (phase == 8 && watch) {
        saw_physical = true;
        physical_credit = wl_columnar_memory_reserved(watch);
    }
    if (add_peer && !peer_added && phase == cancel_phase) {
        CHECK(wl_columnar_memory_reserve(watch, 17, &peer));
        peer_added = true;
    }
    if (phase == cancel_phase && progress >= cancel_progress
        && (!cancel_failure || failed))
        wl_evaluation_control_request_cancel(c);
}

static wl_evaluation_control_t *
control(uint64_t limit)
{
    wl_evaluation_control_t *c = NULL;
    CHECK(wl_evaluation_control_create(limit, &c) == 0);
    CHECK(wl_evaluation_control_attach(c, &owner_key) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner_key) == 0);
    return c;
}

static uint64_t
finish(wl_evaluation_control_t *c, int result)
{
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_finish(c, &owner_key,
        result > 0 ? result : 0, 0) == result);
    CHECK(wl_evaluation_control_report(c, &report) == 0);
    CHECK(report.result == result);
    CHECK(wl_evaluation_control_detach(c, &owner_key) == 0);
    wl_evaluation_control_release(c);
    return report.consumed;
}

static void
op(program_t *p, uint8_t tag)
{
    CHECK(p->size < sizeof(p->bytes));
    p->bytes[p->size++] = tag;
    p->instructions++;
}

static void
constant(program_t *p, uint8_t tag, int64_t v)
{
    op(p, tag);
    CHECK(sizeof(p->bytes) - p->size >= sizeof(v));
    memcpy(p->bytes + p->size, &v, sizeof(v));
    p->size += sizeof(v);
}

static int64_t
bits(double d)
{
    int64_t v;
    memcpy(&v, &d, sizeof(v));
    return v;
}

static void
variable(program_t *p, uint8_t tag, const char *name)
{
    uint16_t len = (uint16_t)strlen(name);
    op(p, tag);
    CHECK(sizeof(p->bytes) - p->size >= (size_t)len + 2);
    memcpy(p->bytes + p->size, &len, 2);
    p->size += 2;
    memcpy(p->bytes + p->size, name, len);
    p->size += len;
    if (len >= 3)
        p->digits += len - 3;
}

/* Independent encoded-input oracle: both decode passes, each byte/digit,
 * second-pass three fields, two real allocation attempts, scaffolding/move.
 * Governor's initial EMPTY reserve15 and commit4 apply without contention. */
static uint64_t
compile_cost(const program_t *p, bool governed)
{
    return 130 + (uint64_t)p->instructions * 131
           + (uint64_t)p->size * 2 + (uint64_t)p->digits * 2
           + (governed ? 19 : 0);
}

static wl_columnar_expr_controlled_t *
compile(const program_t *p, wl_columnar_memory_governor_ref_t *g,
    wl_evaluation_control_t *c)
{
    wl_columnar_expr_controlled_t *result = NULL;
    bool supported = false;
    CHECK(wl_columnar_expr_compile_controlled(p->bytes, p->size, g,
        &result, &supported, c) == 0);
    CHECK(supported && result);
    return result;
}

static void
parity(const program_t *p, const int64_t *row, uint32_t ncols,
    bool float_variable, uint32_t executed, uint32_t reads)
{
    uint8_t original[20000];
    memcpy(original, p->bytes, p->size);
    int64_t expected = 123;
    wl_columnar_expr_compiled_t *raw = wl_columnar_expr_compile(p->bytes,
            p->size, NULL);
    int semantic;
    if (float_variable) {
        CHECK(raw == NULL); /* Real existing producer refuses VAR_FLOAT. */
        semantic = wl_columnar_expr_eval_run(p->bytes, p->size, row, ncols,
                &expected, NULL);
    } else {
        CHECK(raw != NULL);
        semantic = wl_columnar_expr_eval_compiled(raw, row, ncols, &expected);
        wl_columnar_expr_compiled_free(raw);
    }
    if (semantic)
        expected = 0;
    uint64_t cost = compile_cost(p, false);
    wl_evaluation_control_t *c = control(cost);
    wl_columnar_expr_controlled_t *numeric = compile(p, NULL, c);
    CHECK(finish(c, 0) == cost);
    uint64_t eval_cost = (uint64_t)executed * 65 + reads + 8;
    for (unsigned repeat = 0; repeat < 3; repeat++) {
        c = control(repeat == 0 ? eval_cost - 1 : eval_cost);
        int64_t value = 12345;
        bool failed = !semantic;
        int rc = wl_columnar_expr_eval_controlled(numeric, row, ncols,
                &value, &failed, c);
        if (repeat == 0) {
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
            CHECK(value == 12345 && failed == !semantic);
            CHECK(finish(c, rc) == eval_cost - 8);
        } else {
            CHECK(rc == 0 && value == expected && failed == (semantic != 0));
            CHECK(finish(c, 0) == eval_cost);
        }
    }
    int64_t value = 0;
    bool failed = false;
    CHECK(wl_columnar_expr_eval_controlled(numeric, row, ncols,
        &value, &failed, NULL) == 0);
    CHECK(value == expected && failed == (semantic != 0));
    wl_columnar_expr_controlled_free(numeric);
    CHECK(memcmp(original, p->bytes, p->size) == 0);
}

static void
numeric_semantics(void)
{
    static const uint8_t binary[] = {
        WL_PLAN_EXPR_ARITH_ADD, WL_PLAN_EXPR_ARITH_SUB,
        WL_PLAN_EXPR_ARITH_MUL, WL_PLAN_EXPR_ARITH_DIV,
        WL_PLAN_EXPR_ARITH_MOD, WL_PLAN_EXPR_ARITH_SHL,
        WL_PLAN_EXPR_ARITH_SHR, WL_PLAN_EXPR_ARITH_BAND,
        WL_PLAN_EXPR_ARITH_BOR, WL_PLAN_EXPR_ARITH_BXOR,
        WL_PLAN_EXPR_CMP_EQ, WL_PLAN_EXPR_CMP_NEQ,
        WL_PLAN_EXPR_CMP_LT, WL_PLAN_EXPR_CMP_LTE,
        WL_PLAN_EXPR_CMP_GT, WL_PLAN_EXPR_CMP_GTE,
    };
    static const int64_t pairs[][2] = {
        {7, 2}, {-7, 2}, {0, 0}, {INT64_MIN, -1}, {INT64_MAX, 1},
        {INT64_MIN, 1}, {INT64_MAX, 2}, {1, 64}, {1, -1}, {-1, 63}
    };
    for (size_t i = 0; i < sizeof(binary); i++) {
        for (size_t j = 0; j < sizeof(pairs) / sizeof(pairs[0]); j++) {
            program_t p = {0};
            constant(&p, WL_PLAN_EXPR_CONST_INT, pairs[j][0]);
            constant(&p, WL_PLAN_EXPR_CONST_INT, pairs[j][1]);
            op(&p, binary[i]);
            parity(&p, NULL, 0, false, 3, 0);
        }
    }
    static const uint8_t floats[] = {
        WL_PLAN_EXPR_ARITH_FLOAT_ADD, WL_PLAN_EXPR_ARITH_FLOAT_SUB,
        WL_PLAN_EXPR_ARITH_FLOAT_MUL, WL_PLAN_EXPR_ARITH_FLOAT_DIV,
        WL_PLAN_EXPR_CMP_FLOAT_EQ, WL_PLAN_EXPR_CMP_FLOAT_NEQ,
        WL_PLAN_EXPR_CMP_FLOAT_LT, WL_PLAN_EXPR_CMP_FLOAT_LTE,
        WL_PLAN_EXPR_CMP_FLOAT_GT, WL_PLAN_EXPR_CMP_FLOAT_GTE,
    };
    static const double fpairs[][2] = {
        {1.25, 2.5}, {-0.0, 0.0}, {DBL_MAX, 2.0}, {INFINITY, 1.0},
        {1.0, NAN}, {1.0, 0.0}, {-1.0, 2.0}
    };
    for (size_t i = 0; i < sizeof(floats); i++) {
        for (size_t j = 0; j < sizeof(fpairs) / sizeof(fpairs[0]); j++) {
            program_t p = {0};
            constant(&p, WL_PLAN_EXPR_CONST_FLOAT, bits(fpairs[j][0]));
            constant(&p, WL_PLAN_EXPR_CONST_FLOAT, bits(fpairs[j][1]));
            op(&p, floats[i]);
            uint32_t executed = !isfinite(fpairs[j][0]) ? 1
                : !isfinite(fpairs[j][1]) ? 2 : 3;
            parity(&p, NULL, 0, false, executed, 0);
        }
    }
    static const uint8_t unary[] = { WL_PLAN_EXPR_ARITH_BNOT,
                                     WL_PLAN_EXPR_AGG_COUNT,
                                     WL_PLAN_EXPR_AGG_SUM,
                                     WL_PLAN_EXPR_AGG_MIN,
                                     WL_PLAN_EXPR_AGG_MAX };
    for (size_t i = 0; i < sizeof(unary); i++) {
        program_t p = {0};
        op(&p, unary[i]);
        parity(&p, NULL, 0, false, 1, 0); /* empty stack semantics */
    }
    program_t p = {0};
    for (unsigned i = 0; i < COL_FILTER_STACK + 17; i++)
        constant(&p, WL_PLAN_EXPR_CONST_INT, i);
    parity(&p, NULL, 0, false, p.instructions, 0); /* full-stack push ignored */
    p = (program_t){0};
    op(&p, WL_PLAN_EXPR_BOOL);
    p.bytes[p.size++] = 200;
    parity(&p, NULL, 0, false, 1, 0);
    p = (program_t){0};
    variable(&p, WL_PLAN_EXPR_VAR, "col000000000");
    int64_t row[] = {73};
    parity(&p, row, 1, false, 1, 1);
    variable(&p, WL_PLAN_EXPR_VAR, "col999999999");
    parity(&p, row, 1, false, 2, 1);
    p = (program_t){0};
    variable(&p, WL_PLAN_EXPR_VAR_FLOAT, "col0");
    const double values[] = {1.5, -0.0, NAN, INFINITY};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        row[0] = bits(values[i]);
        parity(&p, row, 1, true, 1, 1);
    }
    parity(&p, NULL, 0, true, 1, 0);
}

static void
refuse(const program_t *p)
{
    wl_columnar_expr_controlled_t *out = NULL;
    bool supported = true;
    wl_evaluation_control_t *c = control(0);
    CHECK(wl_columnar_expr_compile_controlled(p->bytes, p->size, NULL,
        &out, &supported, c) == 0);
    CHECK(!supported && !out);
    (void)finish(c, 0);
}

static void
malformed(void)
{
    program_t p = {0};
    refuse(&p);
    constant(&p, WL_PLAN_EXPR_CONST_INT, INT64_C(0x7071727374757677));
    parity(&p, NULL, 0, false, 1, 0); /* payload bytes are not opcodes */
    for (uint32_t size = 1; size < 9; size++) {
        p.size = size;
        refuse(&p);
    }
    const char *names[] = {"col", "col-1", "col+1", "col1x", "foo0",
                           "col1000000000", "col0000000000", "col 0"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        p = (program_t){0};
        variable(&p, WL_PLAN_EXPR_VAR, names[i]);
        refuse(&p);
    }
    p = (program_t){0};
    variable(&p, WL_PLAN_EXPR_VAR, "col1");
    for (uint32_t size = 1; size < 7; size++) {
        p.size = size;
        refuse(&p);
    }
    const uint8_t unsupported[] = {255, WL_PLAN_EXPR_CONST_STR,
                                   WL_PLAN_EXPR_CMP_STR_EQ,
                                   WL_PLAN_EXPR_ARITH_HASH};
    for (size_t i = 0; i < sizeof(unsupported); i++) {
        p = (program_t){0};
        constant(&p, WL_PLAN_EXPR_CONST_FLOAT, bits(1.0));
        op(&p, unsupported[i]);
        refuse(&p);
    }
    wl_columnar_expr_controlled_t *out = NULL;
    bool supported = true, failed = true;
    int64_t value = 41;
    CHECK(wl_columnar_expr_compile_controlled(NULL, 1, NULL, &out,
        &supported, NULL) == EINVAL);
    CHECK(!out && !supported);
    CHECK(wl_columnar_expr_compile_controlled(NULL, 0, NULL, NULL,
        &supported, NULL) == EINVAL);
    CHECK(wl_columnar_expr_eval_controlled(NULL, NULL, 0, &value,
        &failed, NULL) == EINVAL && value == 41 && failed);
    wl_columnar_expr_controlled_free(NULL);
}

static wl_columnar_memory_governor_ref_t *
governor(uint64_t usable, bool advisory)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = usable,
        .mode = advisory ? WL_COLUMNAR_MEMORY_MODE_ADVISORY
            : WL_COLUMNAR_MEMORY_MODE_ENFORCING,
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    return g;
}

static void
lifetime(void)
{
    program_t p = {0};
    constant(&p, WL_PLAN_EXPR_CONST_INT, 123);
    for (unsigned advisory = 0; advisory < 2; advisory++) {
        wl_columnar_memory_governor_ref_t *g = governor(UINT64_MAX,
                advisory != 0);
        watch = wl_columnar_memory_governor_ref_get(g);
        for (unsigned metered = 0; metered < 2; metered++) {
            wl_evaluation_control_t *c = metered ? control(compile_cost(&p,
                    true)) : NULL;
            wl_columnar_expr_controlled_t *x = compile(&p, g, c);
            uint64_t held = wl_columnar_memory_reserved(watch);
            CHECK(held > 0);
            saw_physical = false;
            wl_columnar_expr_controlled_free(x);
            CHECK(saw_physical && physical_credit == held);
            CHECK(wl_columnar_memory_reserved(watch) == 0);
            if (c)
                CHECK(finish(c, 0) == compile_cost(&p, true));
        }
        for (int fault = 0; fault < 2; fault++) {
            wl_evaluation_control_t *c = control(0);
            wl_columnar_expr_controlled_t *x = NULL;
            bool supported = true;
            wl_columnar_expr_test_fail_allocation = fault;
            CHECK(wl_columnar_expr_compile_controlled(p.bytes, p.size, g,
                &x, &supported, c) == ENOMEM);
            CHECK(!x && !supported && wl_columnar_memory_reserved(watch) == 0);
            (void)finish(c, ENOMEM);
        }
        for (unsigned phase = 1; phase <= 6; phase++) {
            if (phase == 3)
                continue;
            wl_columnar_memory_reservation_init(&peer);
            peer_added = false;
            add_peer = phase >= 4;
            cancel_phase = phase;
            cancel_progress = 1;
            wl_evaluation_control_t *c = control(0);
            wl_columnar_expr_controlled_t *x = NULL;
            bool supported = true;
            CHECK(wl_columnar_expr_compile_controlled(p.bytes, p.size, g,
                &x, &supported, c) == WL_EVALUATION_CONTROL_CANCELLED);
            CHECK(!x && !supported);
            CHECK(wl_columnar_memory_reserved(watch) == (add_peer ? 17 : 0));
            CHECK(peer_added == add_peer);
            if (peer_added)
                CHECK(wl_columnar_memory_rollback(&peer));
            CHECK(wl_columnar_memory_reserved(watch) == 0);
            uint64_t prefix = 64 + 64 + 1; /* first tag */
            if (phase == 2) prefix = 64 + 64 + p.size + 15 + 2 + 64 + 1;
            if (phase == 4) prefix = 64 + 64 + p.size + 15;
            if (phase == 5) prefix = compile_cost(&p, true) - 4;
            if (phase == 6) prefix = compile_cost(&p, true);
            CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == prefix);
            cancel_phase = 0;
            add_peer = false;
            x = compile(&p, g, NULL); /* fresh retry */
            wl_columnar_expr_controlled_free(x);
            CHECK(wl_columnar_memory_reserved(watch) == 0);
        }
        watch = NULL;
        wl_columnar_memory_governor_ref_release(g);
    }
    for (unsigned overflow = 0; overflow < 2; overflow++) {
        wl_columnar_memory_governor_ref_t *g = governor(UINT64_MAX, false);
        wl_columnar_memory_governor_t *raw =
            wl_columnar_memory_governor_ref_get(g);
        wl_columnar_memory_reservation_init(&peer);
        /* Real authority, no fake aggregate counter assignment. */
        CHECK(wl_columnar_memory_reserve(raw, UINT64_MAX - 32, &peer));
        if (!overflow) {
            CHECK(wl_columnar_memory_rollback(&peer));
            wl_columnar_memory_governor_ref_release(g);
            g = governor(1, false);
            raw = wl_columnar_memory_governor_ref_get(g);
        }
        wl_evaluation_control_t *c = control(0);
        wl_columnar_expr_controlled_t *x = NULL;
        bool supported = true;
        int expected = overflow ? EOVERFLOW : ENOSPC;
        CHECK(wl_columnar_expr_compile_controlled(p.bytes, p.size, g,
            &x, &supported, c) == expected);
        CHECK(!x && !supported);
        CHECK(wl_columnar_memory_reserved(raw) ==
            (overflow ? UINT64_MAX - 32 : 0));
        (void)finish(c, expected);
        if (overflow)
            CHECK(wl_columnar_memory_rollback(&peer));
        wl_columnar_memory_governor_ref_release(g);
    }
}

static void
budgets_and_stops(void)
{
    program_t p = {0};
    constant(&p, WL_PLAN_EXPR_CONST_INT, 1);
    for (unsigned i = 0; i < 200; i++)
        op(&p, WL_PLAN_EXPR_ARITH_BNOT);
    uint64_t cost = compile_cost(&p, false);
    for (unsigned iteration = 0; iteration < 3; iteration++) {
        wl_evaluation_control_t *c = control(iteration ? cost : cost - 1);
        wl_columnar_expr_controlled_t *x = NULL;
        bool supported = false;
        int rc = wl_columnar_expr_compile_controlled(p.bytes, p.size, NULL,
                &x, &supported, c);
        if (!iteration) {
            CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !x && !supported);
            CHECK(finish(c, rc) == cost - 64);
        } else {
            CHECK(rc == 0 && x && supported);
            CHECK(finish(c, 0) == cost);
            wl_columnar_expr_controlled_free(x);
        }
    }
    wl_columnar_expr_controlled_t *x = compile(&p, NULL, NULL);
    for (unsigned phase = 3; phase <= 7; phase += 4) {
        cancel_phase = phase;
        cancel_progress = phase == 3 ? 100 : p.instructions;
        wl_evaluation_control_t *c = control(0);
        int64_t value = 9123;
        bool failed = true;
        CHECK(wl_columnar_expr_eval_controlled(x, NULL, 0, &value, &failed,
            c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(value == 9123 && failed);
        uint64_t expected = phase == 3 ? 100 * 65 : p.instructions * 65 + 8;
        CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == expected);
        cancel_phase = 0;
    }
    wl_evaluation_control_t *c = control(1);
    int64_t value = 12;
    bool failed = true;
    CHECK(wl_columnar_expr_eval_controlled(x, NULL, 0, &value, &failed,
        c) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(value == 12 && failed);
    CHECK(finish(c, WL_EVALUATION_CONTROL_WORK_LIMIT) == 1);
    wl_columnar_expr_controlled_free(x);
    p = (program_t){0};
    constant(&p, WL_PLAN_EXPR_CONST_INT, 1);
    constant(&p, WL_PLAN_EXPR_CONST_INT, 0);
    op(&p, WL_PLAN_EXPR_ARITH_DIV);
    x = compile(&p, NULL, NULL);
    cancel_phase = 3;
    cancel_progress = 3;
    cancel_failure = true;
    c = control(0);
    CHECK(wl_columnar_expr_eval_controlled(x, NULL, 0, &value, &failed,
        c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(value == 12 && failed);
    CHECK(finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 3 * 65);
    cancel_phase = 0;
    cancel_failure = false;
    wl_columnar_expr_controlled_free(x);
}

static unsigned governor_cancel_phase;
static void
governor_checkpoint(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    (void)token;
    if (phase == governor_cancel_phase)
        wl_evaluation_control_request_cancel(c);
}

static void
contend(wl_evaluation_control_t *c, wl_columnar_memory_governor_t *g,
    uint64_t old, bool overflow)
{
    (void)c;
    (void)old;
    CHECK(!overflow);
    if (!peer_added) {
        CHECK(wl_columnar_memory_reserve(g, 17, &peer));
        peer_added = true;
    }
}

static void
admission_boundaries(void)
{
    program_t p = {0};
    variable(&p, WL_PLAN_EXPR_VAR, "col12");
    wl_columnar_memory_governor_ref_t *g = governor(UINT64_MAX, false);
    watch = wl_columnar_memory_governor_ref_get(g);
    /* Exhaust every integer work limit through the entire lifecycle. This
     * covers partial byte/digit reads, stores, allocations and token phases. */
    for (unsigned governed = 0; governed < 2; governed++) {
        uint64_t cost = compile_cost(&p, governed != 0);
        for (uint64_t budget = 1; budget <= cost; budget++) {
            wl_evaluation_control_t *c = control(budget);
            wl_columnar_expr_controlled_t *x = NULL;
            bool supported = true;
            int rc = wl_columnar_expr_compile_controlled(p.bytes, p.size,
                    governed ? g : NULL, &x, &supported, c);
            if (budget < cost) {
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT);
                CHECK(!x && !supported);
            } else {
                CHECK(rc == 0 && x && supported);
                wl_columnar_expr_controlled_free(x);
            }
            CHECK(finish(c, rc) <= budget);
            CHECK(wl_columnar_memory_reserved(watch) == 0);
        }
    }
    wl_evaluation_control_t *c = control(0);
    wl_evaluation_control_request_cancel(c);
    wl_columnar_expr_controlled_t *x = NULL;
    bool supported = true;
    CHECK(wl_columnar_expr_compile_controlled(p.bytes, p.size, g, &x,
        &supported, c) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(!x && !supported && finish(c, WL_EVALUATION_CONTROL_CANCELLED) == 0);
    wl_columnar_memory_governor_test_checkpoint = governor_checkpoint;
    const unsigned phases[] = { WL_COLUMNAR_MEMORY_GOVERNOR_RESERVED,
                                WL_COLUMNAR_MEMORY_GOVERNOR_COMMITTING,
                                WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF };
    for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
        governor_cancel_phase = phases[i];
        c = control(0);
        CHECK(wl_columnar_expr_compile_controlled(p.bytes, p.size, g, &x,
            &supported, c) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!x && !supported && wl_columnar_memory_reserved(watch) == 0);
        (void)finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    }
    wl_columnar_memory_governor_test_checkpoint = NULL;
    governor_cancel_phase = 0;
    wl_columnar_memory_reservation_init(&peer);
    peer_added = false;
    wl_columnar_memory_governor_test_before_total_cas = contend;
    c = control(compile_cost(&p, true) + 1);
    x = compile(&p, g, c);
    CHECK(peer_added);
    CHECK(finish(c, 0) == compile_cost(&p, true) + 1);
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_expr_controlled_free(x);
    CHECK(wl_columnar_memory_reserved(watch) == 17);
    CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(wl_columnar_memory_reserved(watch) == 0);
    watch = NULL;
    wl_columnar_memory_governor_ref_release(g);
}

/* New owner tests keep legacy budgets and callbacks unchanged above. */
static unsigned attempt_event, attempt_stop, attempt_mode, attempt_later;
static bool attempt_restarted, attempt_cleanup_restart;
static wl_evaluation_control_t *attempt_control;
static wl_evaluation_control_report_t attempt_old;
static unsigned attempt_expr_phases[20], attempt_gov_phases[10];
enum { ATTEMPT_EXPR, ATTEMPT_PHASE, ATTEMPT_WORK, ATTEMPT_BEFORE,
       ATTEMPT_AFTER, ATTEMPT_KINDS };
static unsigned attempt_kinds[ATTEMPT_KINDS], attempt_stop_kind;
static unsigned attempt_target_kind, attempt_target_phase;
static bool attempt_select_target, attempt_verdict_work, attempt_contention;
static unsigned attempt_failed_cas;

static void
attempt_restart(wl_evaluation_control_t *c)
{
    CHECK(!attempt_restarted);
    CHECK(wl_evaluation_control_finish(c, &owner_key, 0, 0) == 0);
    CHECK(wl_evaluation_control_report(c, &attempt_old) == 0);
    CHECK(wl_evaluation_control_reset(c) == 0);
    CHECK(wl_evaluation_control_begin(c, &owner_key) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(c) != attempt_old.attempt_id);
    attempt_restarted = true;
}

static void
attempt_forward(wl_evaluation_control_t *c, unsigned kind, unsigned phase)
{
    CHECK(c == attempt_control);
    if (attempt_restarted) {
        attempt_later++;
        return;
    }
    CHECK(kind < ATTEMPT_KINDS);
    attempt_kinds[kind]++;
    attempt_event++;
    if (attempt_select_target && !attempt_stop
        && kind == attempt_target_kind
        && (kind == ATTEMPT_WORK ? attempt_verdict_work
            : phase == attempt_target_phase))
        attempt_stop = attempt_event;
    if (!attempt_stop || attempt_event != attempt_stop)
        return;
    attempt_stop_kind = kind;
    if (attempt_mode == 0)
        wl_evaluation_control_request_cancel(c);
    else if (attempt_mode == 1)
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) == EOVERFLOW);
    else
        attempt_restart(c);
}

static void
attempt_checkpoint(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed)
{
    (void)progress;
    CHECK(!failed && phase < 20);
    if (phase == 8) {
        CHECK(!c);
        if (watch) {
            saw_physical = true;
            physical_credit = wl_columnar_memory_reserved(watch);
        }
        if (attempt_cleanup_restart)
            attempt_restart(attempt_control);
        return; /* Mandatory cleanup is not a forward callback. */
    }
    attempt_expr_phases[phase]++;
    attempt_forward(c, ATTEMPT_EXPR, phase);
}

static void
attempt_governor_checkpoint(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token, unsigned phase)
{
    if (!c)
        return;
    CHECK(token && token->identity == token && phase < 10);
    attempt_gov_phases[phase]++;
    if (phase == WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT)
        attempt_verdict_work = true;
    attempt_forward(c, ATTEMPT_PHASE, phase);
}

static void
attempt_governor_work(wl_evaluation_control_t *c, uint64_t work)
{
    if (!c) return;
    CHECK(work > 0 && work <= WL_EVALUATION_CONTROL_QUANTUM);
    attempt_forward(c, ATTEMPT_WORK, 0);
    attempt_verdict_work = false;
}

static void
attempt_governor_before(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, uint64_t old_total, bool overflow)
{
    (void)old_total; (void)overflow;
    if (!c) return;
    if (attempt_contention && !peer_added) {
        peer_added = true;
        CHECK(wl_columnar_memory_reserve(g, 17, &peer));
    }
    attempt_forward(c, ATTEMPT_BEFORE, 0);
}

static void
attempt_governor_after(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, bool success, bool overflow)
{
    (void)g; (void)overflow;
    if (!c) return;
    if (!success) attempt_failed_cas++;
    attempt_forward(c, ATTEMPT_AFTER, 0);
}

static wl_evaluation_control_t *
attempt_begin(uint64_t limit)
{
    attempt_event = attempt_later = 0;
    attempt_restarted = false;
    attempt_verdict_work = false;
    attempt_failed_cas = 0;
    saw_physical = false;
    physical_credit = 0;
    attempt_control = control(limit);
    return attempt_control;
}

static uint64_t
attempt_finish(wl_evaluation_control_t *c, int result)
{
    CHECK(attempt_later == 0);
    uint64_t consumed;
    if (attempt_restarted) {
        uint64_t successor_id = wl_evaluation_control_active_attempt_id(c);
        CHECK(successor_id && successor_id != attempt_old.attempt_id);
        CHECK(wl_evaluation_control_finish(c, &owner_key, 0, 0) == 0);
        wl_evaluation_control_report_t report;
        CHECK(wl_evaluation_control_report(c, &report) == 0);
        CHECK(report.attempt_id == successor_id && report.consumed == 0
            && report.result == 0 && report.execution_status == 0
            && report.cleanup_status == 0
            && report.stop_reason == WL_EVALUATION_CONTROL_NONE);
        CHECK(wl_evaluation_control_active_attempt_id(c) == 0);
        CHECK(wl_evaluation_control_detach(c, &owner_key) == 0);
        wl_evaluation_control_release(c);
        consumed = attempt_old.consumed;
    } else {
        consumed = finish(c, result);
    }
    attempt_control = NULL;
    return consumed;
}

static uint64_t
attempt_compile_cost(const program_t *p, bool governed)
{
    /* Independent encoded-input schedule: entry128, setup224, two alloc224,
     * move224, external160. Each instruction: pass192+decode224 twice,
     * second store192; each encoded byte224 and col digit192 in both passes.
     * Original governor reserve74/54/84/29 and commit74/54/29, each behind
     * its caller224. No contention and no unsupported input in this oracle. */
    return p->size ? UINT64_C(1184) + UINT64_C(1024) * p->instructions
           + UINT64_C(448) * p->size + UINT64_C(384) * p->digits
           + (governed ? UINT64_C(846) : 0) : UINT64_C(128);
}

static wl_columnar_expr_attempt_numeric_t *
attempt_compile(const program_t *p, wl_columnar_memory_governor_ref_t *g,
    wl_evaluation_control_t *c)
{
    wl_columnar_expr_attempt_numeric_t *x = NULL;
    bool supported = false;
    CHECK(wl_columnar_expr_compile_attempt_controlled(p->bytes, p->size, g,
        &x, &supported, c, wl_evaluation_control_active_attempt_id(c)) == 0);
    CHECK(x && supported);
    return x;
}

static void
attempt_program_support(void)
{
    const uint8_t tags[] = {
        WL_PLAN_EXPR_ARITH_ADD, WL_PLAN_EXPR_ARITH_SUB,
        WL_PLAN_EXPR_ARITH_MUL, WL_PLAN_EXPR_ARITH_DIV,
        WL_PLAN_EXPR_ARITH_MOD, WL_PLAN_EXPR_ARITH_SHL,
        WL_PLAN_EXPR_ARITH_SHR, WL_PLAN_EXPR_ARITH_BAND,
        WL_PLAN_EXPR_ARITH_BOR, WL_PLAN_EXPR_ARITH_BXOR,
        WL_PLAN_EXPR_ARITH_BNOT, WL_PLAN_EXPR_CMP_EQ, WL_PLAN_EXPR_CMP_NEQ,
        WL_PLAN_EXPR_CMP_LT, WL_PLAN_EXPR_CMP_LTE,
        WL_PLAN_EXPR_CMP_GT, WL_PLAN_EXPR_CMP_GTE,
        WL_PLAN_EXPR_ARITH_FLOAT_ADD, WL_PLAN_EXPR_ARITH_FLOAT_SUB,
        WL_PLAN_EXPR_ARITH_FLOAT_MUL, WL_PLAN_EXPR_ARITH_FLOAT_DIV,
        WL_PLAN_EXPR_CMP_FLOAT_EQ, WL_PLAN_EXPR_CMP_FLOAT_NEQ,
        WL_PLAN_EXPR_CMP_FLOAT_LT, WL_PLAN_EXPR_CMP_FLOAT_LTE,
        WL_PLAN_EXPR_CMP_FLOAT_GT, WL_PLAN_EXPR_CMP_FLOAT_GTE,
        WL_PLAN_EXPR_AGG_COUNT, WL_PLAN_EXPR_AGG_SUM,
        WL_PLAN_EXPR_AGG_MIN, WL_PLAN_EXPR_AGG_MAX
    };
    for (unsigned governed = 0; governed < 2; governed++) {
        wl_columnar_memory_governor_ref_t *g =
            governor(WL_COLUMNAR_MEMORY_MIN_BUDGET, false);
        watch = wl_columnar_memory_governor_ref_get(g);
        for (size_t i = 0; i < sizeof(tags); i++) {
            program_t p = {0};
            constant(&p, WL_PLAN_EXPR_CONST_INT, INT64_MIN);
            constant(&p, WL_PLAN_EXPR_CONST_FLOAT, bits(-0.0));
            op(&p, tags[i]);
            uint8_t original[20000]; memcpy(original, p.bytes, p.size);
            wl_columnar_expr_compiled_t *raw =
                wl_columnar_expr_compile(p.bytes, p.size, NULL);
            CHECK(raw); wl_columnar_expr_compiled_free(raw);
            wl_evaluation_control_t *c = attempt_begin(0);
            wl_columnar_expr_attempt_numeric_t *x =
                attempt_compile(&p, governed ? g : NULL, c);
            CHECK(attempt_finish(c, 0) == attempt_compile_cost(&p, governed));
            wl_columnar_expr_attempt_numeric_free(x);
            CHECK(wl_columnar_memory_reserved(watch) == 0
                && wl_columnar_memory_governor_ref_is_sole(g));
            CHECK(memcmp(original, p.bytes, p.size) == 0);
        }
        for (unsigned floating = 0; floating < 2; floating++) {
            program_t p = {0};
            variable(&p, floating ? WL_PLAN_EXPR_VAR_FLOAT : WL_PLAN_EXPR_VAR,
                "col123456789");
            wl_columnar_expr_compiled_t *raw =
                wl_columnar_expr_compile(p.bytes, p.size, NULL);
            CHECK((raw == NULL) == (floating != 0));
            wl_columnar_expr_compiled_free(raw);
            wl_evaluation_control_t *c = attempt_begin(0);
            wl_columnar_expr_attempt_numeric_t *x =
                attempt_compile(&p, governed ? g : NULL, c);
            CHECK(attempt_finish(c, 0) == attempt_compile_cost(&p, governed));
            wl_columnar_expr_attempt_numeric_free(x);
            CHECK(wl_columnar_memory_reserved(watch) == 0);
        }
        watch = NULL;
        wl_columnar_memory_governor_ref_release(g);
    }
}

static void
attempt_literal_boundaries(void)
{
    program_t p = {0};
    op(&p, WL_PLAN_EXPR_BOOL); p.bytes[p.size++] = 7;
    variable(&p, WL_PLAN_EXPR_VAR, "col9");
    constant(&p, WL_PLAN_EXPR_CONST_INT, 42);
    op(&p, WL_PLAN_EXPR_ARITH_BNOT);
    const unsigned lengths[] = {2, 7, 9, 1};
    const unsigned digits[] = {0, 1, 0, 0};
    for (unsigned governed = 0; governed < 2; governed++) {
        uint64_t prefixes[256], total = 0;
        size_t count = 0;
#define ATTEMPT_LITERAL(work) do { \
            CHECK(count < sizeof(prefixes) / sizeof(prefixes[0])); \
            total += (work); prefixes[count++] = total; \
} \
        while (0)
        ATTEMPT_LITERAL(128);
        for (unsigned i = 0; i < 4; i++) {
            ATTEMPT_LITERAL(192); ATTEMPT_LITERAL(224);
            for (unsigned j = 0; j < lengths[i]; j++) ATTEMPT_LITERAL(224);
            for (unsigned j = 0; j < digits[i]; j++) ATTEMPT_LITERAL(192);
        }
        ATTEMPT_LITERAL(224);
        if (governed) {
            ATTEMPT_LITERAL(224); ATTEMPT_LITERAL(74);
            ATTEMPT_LITERAL(54); ATTEMPT_LITERAL(84); ATTEMPT_LITERAL(29);
        }
        ATTEMPT_LITERAL(224); ATTEMPT_LITERAL(224);
        for (unsigned i = 0; i < 4; i++) {
            ATTEMPT_LITERAL(192); ATTEMPT_LITERAL(224);
            for (unsigned j = 0; j < lengths[i]; j++) ATTEMPT_LITERAL(224);
            for (unsigned j = 0; j < digits[i]; j++) ATTEMPT_LITERAL(192);
            ATTEMPT_LITERAL(192);
        }
        ATTEMPT_LITERAL(224);
        if (governed) {
            ATTEMPT_LITERAL(224); ATTEMPT_LITERAL(74);
            ATTEMPT_LITERAL(54); ATTEMPT_LITERAL(29);
        }
        ATTEMPT_LITERAL(160);
#undef ATTEMPT_LITERAL
        CHECK(total == attempt_compile_cost(&p, governed != 0));
        wl_columnar_memory_governor_ref_t *g =
            governor(WL_COLUMNAR_MEMORY_MIN_BUDGET, false);
        watch = wl_columnar_memory_governor_ref_get(g);
        for (size_t i = 0; i < count; i++) {
            for (unsigned below = 0; below < 2; below++) {
                wl_evaluation_control_t *c = attempt_begin(prefixes[i] - below);
                wl_columnar_expr_attempt_numeric_t *x = NULL;
                bool supported = true;
                int rc = wl_columnar_expr_compile_attempt_controlled(p.bytes,
                        p.size, governed ? g : NULL, &x, &supported, c,
                        wl_evaluation_control_active_attempt_id(c));
                if (!below && i + 1 == count) {
                    CHECK(rc == 0 && supported && x);
                    CHECK(attempt_finish(c, 0) == total);
                    wl_columnar_expr_attempt_numeric_free(x);
                } else {
                    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && !x
                        && !supported);
                    CHECK(attempt_finish(c, rc) == (below
                        ? (i ? prefixes[i - 1] : 0) : prefixes[i]));
                }
                CHECK(wl_columnar_memory_reserved(watch) == 0
                    && wl_columnar_memory_governor_ref_is_sole(g));
            }
        }
        watch = NULL; wl_columnar_memory_governor_ref_release(g);
    }
}

static void
attempt_fault_sweeps(void)
{
    program_t p = {0};
    variable(&p, WL_PLAN_EXPR_VAR, "col123456789");
    constant(&p, WL_PLAN_EXPR_CONST_FLOAT, bits(3.5));
    for (unsigned i = 0; i < 10; i++) op(&p, WL_PLAN_EXPR_ARITH_BNOT);
    for (unsigned variant = 0; variant < 3; variant++) {
        bool governed = variant != 0;
        attempt_contention = variant == 2;
        wl_columnar_memory_governor_ref_t *g =
            governor(WL_COLUMNAR_MEMORY_MIN_BUDGET, false);
        watch = wl_columnar_memory_governor_ref_get(g);
        wl_columnar_memory_reservation_init(&peer); peer_added = false;
        wl_evaluation_control_t *c = attempt_begin(0);
        wl_columnar_expr_attempt_numeric_t *x =
            attempt_compile(&p, governed ? g : NULL, c);
        unsigned events = attempt_event;
        CHECK(!attempt_contention || (peer_added && attempt_failed_cas == 1));
        CHECK(events > 100 && attempt_finish(c, 0)
            == attempt_compile_cost(&p,
            governed) + (attempt_contention ? 84 : 0));
        wl_columnar_expr_attempt_numeric_free(x);
        if (peer_added) CHECK(wl_columnar_memory_rollback(&peer));
        /* Fault telemetry enumerates sites, never constructs a cost oracle. */
        for (unsigned event = 1; event <= events; event++) {
            for (unsigned mode = 0; mode < 3; mode++) {
                wl_columnar_memory_reservation_init(&peer); peer_added = false;
                attempt_stop = event; attempt_mode = mode;
                c = attempt_begin(0); x = NULL;
                bool supported = true;
                int expected = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED
                    : mode == 1 ? EOVERFLOW : EINVAL;
                CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes,
                    p.size, governed ? g : NULL, &x, &supported, c,
                    wl_evaluation_control_active_attempt_id(c)) == expected);
                /* Same-ID CAS guards check identity only: before-CAS can
                * reach after-CAS and the work tail; after-CAS reaches tail.
                * Stale identity forbids every later forward callback. */
                unsigned suffix = mode == 2 ? 0
                    : attempt_stop_kind == ATTEMPT_BEFORE ? 2
                    : attempt_stop_kind == ATTEMPT_AFTER ? 1 : 0;
                CHECK(!x && !supported && attempt_event == event + suffix);
                CHECK(attempt_finish(c, expected) > 0);
                CHECK(wl_columnar_memory_reserved(watch) ==
                    (peer_added ? 17 : 0)
                    && wl_columnar_memory_governor_ref_is_sole(g));
                if (peer_added) CHECK(wl_columnar_memory_rollback(&peer));
                CHECK(wl_columnar_memory_reserved(watch) == 0);
            }
        }
        attempt_stop = 0; attempt_contention = false;
        c = attempt_begin(0); x = attempt_compile(&p, governed ? g : NULL, c);
        CHECK(attempt_finish(c, 0) == attempt_compile_cost(&p, governed));
        wl_columnar_expr_attempt_numeric_free(x);
        CHECK(wl_columnar_memory_reserved(watch) == 0);
        watch = NULL; wl_columnar_memory_governor_ref_release(g);
    }
    for (unsigned kind = 0; kind < ATTEMPT_KINDS; kind++)
        CHECK(attempt_kinds[kind] > 0);
    for (unsigned phase = 1; phase < 20; phase++) {
        if (phase == 3 || phase == 7 || phase == 8 || phase == 9) continue;
        CHECK(attempt_expr_phases[phase] > 0);
    }
    for (unsigned phase = 1; phase <= 8; phase++)
        CHECK(attempt_gov_phases[phase] > 0);
}

static void
attempt_arguments_and_unsupported(void)
{
    program_t p = {0}; constant(&p, WL_PLAN_EXPR_CONST_INT, 42);
    for (unsigned state = 0; state < 3; state++) {
        wl_evaluation_control_t *c = attempt_begin(0);
        uint64_t id = wl_evaluation_control_active_attempt_id(c);
        if (state == 0) id = 0;
        if (state == 1) id++;
        if (state == 2) {
            CHECK(wl_evaluation_control_finish(c, &owner_key, 0, 0) == 0);
        }
        wl_columnar_expr_attempt_numeric_t *x = NULL;
        bool supported = true;
        CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes, p.size,
            NULL, &x, &supported, c, id) == EINVAL);
        CHECK(!x && !supported && attempt_event == 0);
        if (state == 2) {
            wl_evaluation_control_report_t report;
            CHECK(wl_evaluation_control_report(c, &report) == 0
                && report.consumed == 0);
            CHECK(wl_evaluation_control_detach(c, &owner_key) == 0);
            wl_evaluation_control_release(c); attempt_control = NULL;
        } else CHECK(attempt_finish(c, EINVAL) == 0);
    }
    wl_columnar_expr_attempt_numeric_t *x = NULL;
    bool supported = true;
    CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes, p.size,
        NULL, &x, &supported, NULL, 1) == EINVAL && !x && !supported);
    for (uint32_t cut = 0; cut < p.size; cut++) {
        wl_evaluation_control_t *c = attempt_begin(0);
        supported = true;
        CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes, cut, NULL,
            &x, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) == 0);
        CHECK(!x && !supported); CHECK(attempt_finish(c, 0) > 0);
    }
    const char *names[] = {"x", "bad0", "colx", "col1234567890", "col"};
    for (unsigned i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        program_t bad = {0}; variable(&bad, WL_PLAN_EXPR_VAR, names[i]);
        wl_evaluation_control_t *c = attempt_begin(0); supported = true;
        CHECK(wl_columnar_expr_compile_attempt_controlled(bad.bytes, bad.size,
            NULL, &x, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) == 0);
        CHECK(!x && !supported); CHECK(attempt_finish(c, 0) > 0);
    }
    for (unsigned exact = 0; exact < 2; exact++) {
        wl_evaluation_control_t *empty = attempt_begin(exact ? 128 : 127);
        supported = true;
        int expected = exact ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
        CHECK(wl_columnar_expr_compile_attempt_controlled(NULL, 0, NULL, &x,
            &supported, empty,
            wl_evaluation_control_active_attempt_id(empty)) == expected);
        CHECK(!x && !supported
            && attempt_finish(empty, expected) == (exact ? 128 : 0));
    }
    uint8_t unknown = UINT8_MAX;
    wl_evaluation_control_t *c = attempt_begin(0); supported = true;
    CHECK(wl_columnar_expr_compile_attempt_controlled(&unknown, 1, NULL,
        &x, &supported, c, wl_evaluation_control_active_attempt_id(c)) == 0);
    CHECK(!x && !supported); CHECK(attempt_finish(c, 0) > 0);
    c = attempt_begin(0); supported = true;
    CHECK(wl_columnar_expr_compile_attempt_controlled(NULL, 1, NULL, &x,
        &supported, c, wl_evaluation_control_active_attempt_id(c)) == EINVAL);
    CHECK(!x && !supported && attempt_finish(c, EINVAL) == 128);
}

static void
attempt_peer_contention(wl_evaluation_control_t *c,
    wl_columnar_memory_governor_t *g, uint64_t amount, bool growth)
{
    (void)amount; (void)growth;
    if (!c || peer_added) return;
    peer_added = true;
    CHECK(wl_columnar_memory_reserve(g, 17, &peer));
}

static void
attempt_resources_and_lifetime(void)
{
    program_t p = {0}; constant(&p, WL_PLAN_EXPR_CONST_INT, 7);
    wl_columnar_memory_governor_ref_t *g =
        governor(WL_COLUMNAR_MEMORY_MIN_BUDGET, false);
    watch = wl_columnar_memory_governor_ref_get(g);
    for (unsigned governed = 0; governed < 2; governed++) {
        for (int ordinal = 0; ordinal < 2; ordinal++) {
            wl_evaluation_control_t *c = attempt_begin(0);
            wl_columnar_expr_test_fail_allocation = ordinal;
            wl_columnar_expr_attempt_numeric_t *x = NULL;
            bool supported = true;
            CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes, p.size,
                governed ? g : NULL, &x, &supported, c,
                wl_evaluation_control_active_attempt_id(c)) == ENOMEM);
            wl_columnar_expr_test_fail_allocation = -1;
            CHECK(!x && !supported && attempt_finish(c, ENOMEM) > 0);
            CHECK(saw_physical == (ordinal != 0));
            CHECK(!governed || !ordinal || physical_credit > 0);
            CHECK(wl_columnar_memory_reserved(watch) == 0
                && wl_columnar_memory_governor_ref_is_sole(g));
        }
    }
    /* A positive allocation verdict survives restart in mandatory disposal;
     * successor is neither charged nor assigned the original failure. */
    wl_evaluation_control_t *c = attempt_begin(0);
    wl_columnar_expr_test_fail_allocation = 1;
    attempt_cleanup_restart = true;
    wl_columnar_expr_attempt_numeric_t *x = NULL;
    bool supported = true;
    CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes, p.size, g,
        &x, &supported, c,
        wl_evaluation_control_active_attempt_id(c)) == ENOMEM);
    attempt_cleanup_restart = false;
    wl_columnar_expr_test_fail_allocation = -1;
    CHECK(!x && !supported && attempt_restarted && saw_physical
        && attempt_finish(c, ENOMEM) > 0);
    CHECK(wl_columnar_memory_reserved(watch) == 0
        && wl_columnar_memory_governor_ref_is_sole(g));

    wl_columnar_memory_reservation_init(&peer); peer_added = false;
    wl_columnar_memory_governor_test_before_total_cas = attempt_peer_contention;
    c = attempt_begin(0); x = attempt_compile(&p, g, c);
    CHECK(peer_added && attempt_finish(c, 0)
        == attempt_compile_cost(&p, true) + 84); /* One genuine failed CAS. */
    wl_columnar_memory_governor_test_before_total_cas = attempt_governor_before;
    wl_columnar_expr_attempt_numeric_free(x);
    CHECK(wl_columnar_memory_reserved(watch) == 17);
    CHECK(wl_columnar_memory_rollback(&peer)
        && wl_columnar_memory_reserved(watch) == 0);
    watch = NULL; wl_columnar_memory_governor_ref_release(g);

    /* Advisory labels allowed admission; finite usable capacity still binds. */
    g = governor(1, true);
    watch = wl_columnar_memory_governor_ref_get(g);
    wl_columnar_memory_reservation_t advisory_probe;
    wl_columnar_memory_reservation_init(&advisory_probe);
    CHECK(wl_columnar_memory_reserve_checked(watch, 2, &advisory_probe)
        == WL_COLUMNAR_MEMORY_ADMISSION_DENIED);
    CHECK(wl_columnar_memory_reserved(watch) == 0);
    c = attempt_begin(0); x = NULL; supported = true;
    CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes, p.size, g,
        &x, &supported, c,
        wl_evaluation_control_active_attempt_id(c)) == ENOSPC);
    CHECK(!x && !supported && attempt_finish(c, ENOSPC) > 0);
    CHECK(wl_columnar_memory_reserved(watch) == 0
        && wl_columnar_memory_governor_ref_is_sole(g));
    watch = NULL; wl_columnar_memory_governor_ref_release(g);

    g = governor(WL_COLUMNAR_MEMORY_MIN_BUDGET, true);
    watch = wl_columnar_memory_governor_ref_get(g);
    wl_columnar_memory_reservation_init(&advisory_probe);
    CHECK(wl_columnar_memory_reserve_checked(watch, 17, &advisory_probe)
        == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY);
    CHECK(wl_columnar_memory_reserved(watch) == 17
        && wl_columnar_memory_rollback(&advisory_probe)
        && wl_columnar_memory_reserved(watch) == 0);
    c = attempt_begin(0); x = attempt_compile(&p, g, c);
    CHECK(attempt_finish(c, 0) == attempt_compile_cost(&p, true)
        && wl_columnar_memory_reserved(watch) > 1
        && !wl_columnar_memory_governor_ref_is_sole(g));
    wl_columnar_expr_attempt_numeric_free(x);
    CHECK(wl_columnar_memory_reserved(watch) == 0
        && wl_columnar_memory_governor_ref_is_sole(g));
    watch = NULL; wl_columnar_memory_governor_ref_release(g);

    for (unsigned overflow = 0; overflow < 2; overflow++) {
        g = governor(overflow ? UINT64_MAX : 1, false);
        watch = wl_columnar_memory_governor_ref_get(g);
        wl_columnar_memory_reservation_init(&peer);
        if (overflow) CHECK(wl_columnar_memory_reserve(watch,
                UINT64_MAX - 32, &peer));
        c = attempt_begin(0); x = NULL; supported = true;
        int expected = overflow ? EOVERFLOW : ENOSPC;
        CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes, p.size, g,
            &x, &supported, c,
            wl_evaluation_control_active_attempt_id(c)) == expected);
        CHECK(!x && !supported && attempt_finish(c, expected) > 0);
        CHECK(wl_columnar_memory_reserved(watch)
            == (overflow ? UINT64_MAX - 32 : 0)
            && wl_columnar_memory_governor_ref_is_sole(g));
        if (overflow) CHECK(wl_columnar_memory_rollback(&peer));
        watch = NULL; wl_columnar_memory_governor_ref_release(g);
    }
    /* Caller releases its reference; the opaque owner alone pins real credit. */
    g = governor(WL_COLUMNAR_MEMORY_MIN_BUDGET, false);
    watch = wl_columnar_memory_governor_ref_get(g);
    c = attempt_begin(0); x = attempt_compile(&p, g, c);
    CHECK(attempt_finish(c, 0) == attempt_compile_cost(&p, true));
    wl_columnar_memory_governor_ref_release(g);
    CHECK(wl_columnar_memory_governor_ref_is_sole(g)
        && wl_columnar_memory_reserved(watch) > 0);
    saw_physical = false;
    wl_columnar_expr_attempt_numeric_free(x);
    CHECK(saw_physical && physical_credit > 0);
    watch = NULL; /* Last reference was consumed by owner; do not dereference. */
    wl_columnar_expr_attempt_numeric_free(NULL);
}

static void
attempt_typed_overlaps(void)
{
    program_t p = {0}; constant(&p, WL_PLAN_EXPR_CONST_INT, 7);
    /* Real denial, real aggregate overflow, and both allocator failures.
     * Only completed typed verdicts overlap the selected hook below. */
    for (unsigned outcome = 0; outcome < 4; outcome++) {
        for (unsigned seam = 0; seam < (outcome < 2 ? 2u : 1u); seam++) {
            for (unsigned mode = 0; mode < 3; mode++) {
                wl_columnar_memory_governor_ref_t *g = governor(
                    outcome == 0 ? 1 : outcome == 1 ? UINT64_MAX
                        : WL_COLUMNAR_MEMORY_MIN_BUDGET, false);
                watch = wl_columnar_memory_governor_ref_get(g);
                wl_columnar_memory_reservation_init(&peer);
                if (outcome == 1) CHECK(wl_columnar_memory_reserve(watch,
                        UINT64_MAX - 32, &peer));
                wl_columnar_expr_test_fail_allocation = outcome < 2 ? -1
                    : (int)outcome - 2;
                attempt_select_target = true;
                attempt_target_kind = outcome < 2
                    ? seam == 0 ? ATTEMPT_PHASE : ATTEMPT_WORK : ATTEMPT_EXPR;
                attempt_target_phase = outcome < 2
                    ? WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT : outcome ==
                    2 ? 14 : 15;
                attempt_mode = mode; attempt_stop = 0;
                wl_evaluation_control_t *c = attempt_begin(0);
                wl_columnar_expr_attempt_numeric_t *x = NULL;
                bool supported = true;
                int typed = outcome == 0 ? ENOSPC
                    : outcome == 1 ? EOVERFLOW : ENOMEM;
                int expected = mode == 0 ? typed : mode ==
                    1 ? EOVERFLOW : EINVAL;
                CHECK(wl_columnar_expr_compile_attempt_controlled(p.bytes,
                    p.size, g, &x, &supported, c,
                    wl_evaluation_control_active_attempt_id(c)) == expected);
                CHECK(attempt_stop && !x && !supported);
                unsigned suffix = mode != 0 || outcome >= 2 ? 0
                    : seam == 0 ? 2 : 1; /* verdict tail then compiler phase4 */
                CHECK(attempt_event == attempt_stop + suffix);
                CHECK(attempt_finish(c, expected) > 0);
                attempt_select_target = false; attempt_stop = 0;
                wl_columnar_expr_test_fail_allocation = -1;
                CHECK(wl_columnar_memory_reserved(watch)
                    == (outcome == 1 ? UINT64_MAX - 32 : 0)
                    && wl_columnar_memory_governor_ref_is_sole(g));
                if (outcome == 1) CHECK(wl_columnar_memory_rollback(&peer));
                watch = NULL; wl_columnar_memory_governor_ref_release(g);
            }
        }
    }
    CHECK(attempt_gov_phases[WL_COLUMNAR_MEMORY_GOVERNOR_VERDICT] > 0);
}

static void
attempt_compile_lifecycle(void)
{
    memset(attempt_expr_phases, 0, sizeof(attempt_expr_phases));
    memset(attempt_gov_phases, 0, sizeof(attempt_gov_phases));
    wl_columnar_expr_test_checkpoint = attempt_checkpoint;
    memset(attempt_kinds, 0, sizeof(attempt_kinds));
    wl_columnar_memory_governor_test_checkpoint = attempt_governor_checkpoint;
    wl_columnar_memory_governor_test_work_checkpoint = attempt_governor_work;
    wl_columnar_memory_governor_test_before_total_cas = attempt_governor_before;
    wl_columnar_memory_governor_test_after_total_cas = attempt_governor_after;
    attempt_program_support();
    attempt_literal_boundaries();
    attempt_fault_sweeps();
    attempt_arguments_and_unsupported();
    attempt_resources_and_lifetime();
    attempt_typed_overlaps();
    CHECK(!attempt_control && !watch && !attempt_stop
        && !attempt_cleanup_restart);
    wl_columnar_expr_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
}

int
main(void)
{
    wl_columnar_expr_test_checkpoint = checkpoint;
    numeric_semantics();
    malformed();
    lifetime();
    budgets_and_stops();
    admission_boundaries();
    wl_columnar_expr_test_checkpoint = NULL;
    attempt_compile_lifecycle();
    puts("numeric lifecycle: PASS");
    return 0;
}
