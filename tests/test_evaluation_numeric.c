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
static wl_columnar_memory_governor_t *watch;
static wl_columnar_memory_reservation_t peer;
static bool add_peer, peer_added, saw_physical;
static uint64_t physical_credit;

static void
checkpoint(wl_evaluation_control_t *c, unsigned phase, uint64_t progress,
    bool failed)
{
    (void)failed;
    if (phase == 8 && watch) {
        saw_physical = true;
        physical_credit = wl_columnar_memory_reserved(watch);
    }
    if (add_peer && !peer_added && phase == cancel_phase) {
        CHECK(wl_columnar_memory_reserve(watch, 17, &peer));
        peer_added = true;
    }
    if (phase == cancel_phase && progress >= cancel_progress
        )
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
    wl_columnar_expr_controlled_free(compile(&p, NULL, NULL)); /* payload is not opcodes */
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
    bool supported = true;
    CHECK(wl_columnar_expr_compile_controlled(NULL, 1, NULL, &out,
        &supported, NULL) == EINVAL);
    CHECK(!out && !supported);
    CHECK(wl_columnar_expr_compile_controlled(NULL, 0, NULL, NULL,
        &supported, NULL) == EINVAL);
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
compile_budgets(void)
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

static void
compiler_acceptance(void)
{
    const uint8_t tags[] = {
        WL_PLAN_EXPR_ARITH_ADD, WL_PLAN_EXPR_ARITH_SUB,
        WL_PLAN_EXPR_ARITH_MUL, WL_PLAN_EXPR_ARITH_DIV,
        WL_PLAN_EXPR_ARITH_MOD, WL_PLAN_EXPR_ARITH_SHL,
        WL_PLAN_EXPR_ARITH_SHR, WL_PLAN_EXPR_ARITH_BAND,
        WL_PLAN_EXPR_ARITH_BOR, WL_PLAN_EXPR_ARITH_BXOR,
        WL_PLAN_EXPR_ARITH_BNOT, WL_PLAN_EXPR_CMP_EQ,
        WL_PLAN_EXPR_CMP_NEQ, WL_PLAN_EXPR_CMP_LT,
        WL_PLAN_EXPR_CMP_LTE, WL_PLAN_EXPR_CMP_GT, WL_PLAN_EXPR_CMP_GTE,
        WL_PLAN_EXPR_ARITH_FLOAT_ADD, WL_PLAN_EXPR_ARITH_FLOAT_SUB,
        WL_PLAN_EXPR_ARITH_FLOAT_MUL, WL_PLAN_EXPR_ARITH_FLOAT_DIV,
        WL_PLAN_EXPR_CMP_FLOAT_EQ, WL_PLAN_EXPR_CMP_FLOAT_NEQ,
        WL_PLAN_EXPR_CMP_FLOAT_LT, WL_PLAN_EXPR_CMP_FLOAT_LTE,
        WL_PLAN_EXPR_CMP_FLOAT_GT, WL_PLAN_EXPR_CMP_FLOAT_GTE,
        WL_PLAN_EXPR_AGG_COUNT, WL_PLAN_EXPR_AGG_SUM,
        WL_PLAN_EXPR_AGG_MIN, WL_PLAN_EXPR_AGG_MAX,
    };
    program_t p = {0};
    constant(&p, WL_PLAN_EXPR_CONST_INT, INT64_MAX);
    constant(&p, WL_PLAN_EXPR_CONST_FLOAT, bits(1.25));
    op(&p, WL_PLAN_EXPR_BOOL);
    p.bytes[p.size++] = 200;
    for (size_t i = 0; i < sizeof(tags); i++)
        op(&p, tags[i]);
    variable(&p, WL_PLAN_EXPR_VAR, "col000000000");
    variable(&p, WL_PLAN_EXPR_VAR_FLOAT, "col999999999");
    wl_evaluation_control_t *c = control(compile_cost(&p, false));
    wl_columnar_expr_controlled_t *x = compile(&p, NULL, c);
    CHECK(finish(c, 0) == compile_cost(&p, false));
    wl_columnar_expr_controlled_free(x);
}

int
main(void)
{
    wl_columnar_expr_test_checkpoint = checkpoint;
    compiler_acceptance();
    malformed();
    lifetime();
    compile_budgets();
    admission_boundaries();
    wl_columnar_expr_test_checkpoint = NULL;
    puts("numeric preparation/lifetime: PASS");
    return 0;
}
