/* #2299 MAP metadata preserves raw compile-first precedence, independently
 * of the inexpensive producer-only query. No expression is evaluated here. */
#include "../wirelog/columnar/internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__, #x); exit(1); } } while (0)
static int owner;
static wl_evaluation_control_t *begin(uint64_t limit) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    return c;
}
static void finish(wl_evaluation_control_t *c, int rc) {
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0 ? rc : 0, 0) == rc);
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
}
static wl_columnar_expr_attempt_scalar_t *compile(const uint8_t *b,
    uint32_t n) {
    wl_columnar_expr_attempt_scalar_t *p = NULL; bool supported = false;
    wl_evaluation_control_t *c = begin(0);
    CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(b, n, NULL,
        &p, &supported, c,
        wl_evaluation_control_active_attempt_id(c)) && p && supported);
    finish(c, 0);
    return p;
}
static uint32_t var(uint8_t *b, uint8_t tag, unsigned column) {
    b[0] = tag; uint16_t length = 4; memcpy(b+1, &length, 2);
    memcpy(b+3, "col0", 4); b[6] = (uint8_t)('0'+column); return 7;
}
static void parity(const uint8_t *b, uint32_t n,
    const wirelog_column_type_t *types, uint32_t width) {
    wl_columnar_expr_compiled_t *raw = wl_columnar_expr_compile(b, n, NULL);
    wirelog_column_type_t expected = (wirelog_column_type_t)99;
    bool known = raw && wl_columnar_expr_compiled_result_type(raw, types,
            width, &expected);
    if (!known) known = wl_columnar_expr_interpreted_result_type(b, n,
                types, width, &expected);
    wl_columnar_expr_attempt_scalar_t *p = compile(b, n);
    wirelog_column_type_t actual = (wirelog_column_type_t)99;
    bool supported = !known;
    wl_evaluation_control_t *c = begin(0);
    CHECK(!wl_columnar_expr_scalar_map_schema_attempt_controlled(p, types,
        width, &actual, &supported, c,
        wl_evaluation_control_active_attempt_id(c)));
    finish(c, 0);
    CHECK(supported == known && actual == expected);
    wl_columnar_expr_attempt_scalar_free(p);
    wl_columnar_expr_compiled_free(raw);
}
/* Literal payments below are independent of production constants. Each VAR
 * child has nine224 payments and one192 digit; scalar-only tags one224.
 * We test every positive payment exact and one short, including internal child
 * cuts. Prefix rejection must preserve BOTH caller output sentinels. */
#define V 224, 224, 224, 224, 224, 224, 224, 224, 224, 192
static const uint16_t bare[] = {224, 224, V, 224, 224, 224}; /*3328*/
static const uint16_t late0[] = {224, 224, V, 224, 224, 224, 224}; /*3552*/
static const uint16_t late1[] = {224, 224, V, 224, 224, 224, 224, 224, 224}; /*4000*/
static const uint16_t cmp0[] = {224, 224, V, 224, 224, 224, V, 224, 224, 224,
                                224, 224};                                    /*6656*/
static const uint16_t cmp1[] = {224, 224, V, 224, 224, 224, V, 224, 224, 224,
                                224, 224, 224, 224};                                  /*7104*/
static const uint16_t underflow[] = {224, 224, 224, 224, 224, 224}; /*1344*/
static const uint16_t string[] = {224, 224, 224, 224, 224, 224, 224, 224}; /*1792*/
static const uint16_t opaque[] = {224, 224, 224}; /*672*/
#undef V
static void cuts(const uint8_t *b, uint32_t n, uint32_t width,
    const uint16_t *fees, size_t count, uint64_t total, bool known,
    wirelog_column_type_t expected) {
    wl_columnar_expr_attempt_scalar_t *p = compile(b, n);
    uint64_t sum = 0;
    for (size_t cut = 0; cut < count; ++cut) {
        sum += fees[cut];
        for (unsigned below = 0; below < 2; ++below) {
            wl_evaluation_control_t *c = begin(sum-below);
            wirelog_column_type_t actual = (wirelog_column_type_t)99;
            bool supported = true;
            int rc = wl_columnar_expr_scalar_map_schema_attempt_controlled(p,
                    NULL, width, &actual, &supported, c,
                    wl_evaluation_control_active_attempt_id(c));
            uint64_t consumed = 0;
            CHECK(!wl_evaluation_control_test_active_consumed(c,
                wl_evaluation_control_active_attempt_id(c), &consumed));
            if (cut+1 == count && !below) {
                CHECK(!rc && supported == known &&
                    actual == (known ? expected : (wirelog_column_type_t)99));
                CHECK(consumed == total);
            } else {
                CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT &&
                    actual == (wirelog_column_type_t)99 && supported);
                CHECK(consumed == (below ? sum-fees[cut] : sum));
            }
            finish(c, rc);
        }
    }
    CHECK(sum == total);
    wl_columnar_expr_attempt_scalar_free(p);
}
#define CUT(b, n, w, fees, total, k, t) cuts(b, n, w, fees, \
            sizeof(fees)/sizeof(*fees), total, k, t)
static void literal_oracles(void) {
    uint8_t b[32]; uint32_t n = var(b, WL_PLAN_EXPR_VAR, 0);
    CUT(b, n, 0, bare, 3328, true, WIRELOG_TYPE_INT64);
    b[n++] = WL_PLAN_EXPR_STR_FN_TO_STRING;
    CUT(b, n, 0, late0, 3552, false, WIRELOG_TYPE_INT64);
    CUT(b, n, 1, late1, 4000, true, WIRELOG_TYPE_STRING);
    n = var(b, WL_PLAN_EXPR_VAR, 0); n += var(b+n, WL_PLAN_EXPR_VAR, 0);
    b[n++] = WL_PLAN_EXPR_CMP_STR_EQ;
    CUT(b, n, 0, cmp0, 6656, true, WIRELOG_TYPE_BOOL);
    CUT(b, n, 1, cmp1, 7104, true, WIRELOG_TYPE_BOOL);
    b[0] = WL_PLAN_EXPR_CMP_STR_EQ;
    CUT(b, 1, 0, underflow, 1344, true, WIRELOG_TYPE_BOOL);
    b[0] = WL_PLAN_EXPR_CONST_STR; uint16_t length = 1;
    memcpy(b+1, &length, 2); b[3] = 'x';
    CUT(b, 4, 0, string, 1792, true, WIRELOG_TYPE_STRING);
    const uint8_t extension[] = {WL_PLAN_EXPR_EXTENSION_CALL, 1, 0, 'x', 0, 0,
                                 0, 0};
    CUT(extension, sizeof(extension), 0, opaque, 672, false,
        WIRELOG_TYPE_INT64);
}
static unsigned stop_phase, action; static bool stopped;
static void stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    (void)progress; (void)failed;
    if (!c || stopped || phase != stop_phase) return;
    stopped = true;
    if (!action) wl_evaluation_control_request_cancel(c);
    else if (action == 1 || action == 4) {
        CHECK(wl_evaluation_control_charge(c, UINT64_MAX) ==
            (action == 1 ? WL_EVALUATION_CONTROL_WORK_LIMIT : EOVERFLOW));
    } else {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
        if (action == 3) {
            CHECK(!wl_evaluation_control_reset(c));
            CHECK(!wl_evaluation_control_begin(c, &owner));
        }
    }
}
static void interruption(void) {
    const unsigned phases[] = {610, 611, 70, 1, 19, 614, 612, 613};
    uint8_t b[8]; uint32_t n = var(b, WL_PLAN_EXPR_VAR, 0);
    wl_columnar_expr_attempt_scalar_t *p = compile(b, n);
    for (size_t k = 0; k < sizeof(phases)/sizeof(*phases); ++k)
        for (action = 0; action < 5; ++action) {
            wl_evaluation_control_t *c = begin(action == 1?UINT64_MAX:0);
            uint64_t original = wl_evaluation_control_active_attempt_id(c);
            wirelog_column_type_t actual = (wirelog_column_type_t)99;
            bool supported = true; stopped = false; stop_phase = phases[k];
            wl_columnar_expr_test_checkpoint = stop;
            int rc = wl_columnar_expr_scalar_map_schema_attempt_controlled(p,
                    NULL, 0, &actual, &supported, c, original);
            wl_columnar_expr_test_checkpoint = NULL;
            CHECK(stopped && actual == (wirelog_column_type_t)99 && supported);
            CHECK(rc == (action == 0?WL_EVALUATION_CONTROL_CANCELLED:
                action == 1?WL_EVALUATION_CONTROL_WORK_LIMIT:
                action == 4?EOVERFLOW:EINVAL));
            if (action == 3) {
                uint64_t consumed = 99;
                CHECK(!wl_evaluation_control_test_active_consumed(c,
                    wl_evaluation_control_active_attempt_id(c),
                    &consumed) && !consumed);
            }
            if (action == 2) {
                CHECK(!wl_evaluation_control_detach(c, &owner));
                wl_evaluation_control_release(c);
            } else finish(c, action == 3?0:rc);
        }
    wl_columnar_expr_attempt_scalar_free(p);
}
static void grammar_parity(void) {
    /* Exercise every existing byte tag accepted by the scalar owner. Both
     * complete raw compilation and raw interpreter inference are authoritative. */
    const wirelog_column_type_t types[] = {WIRELOG_TYPE_STRING,
                                           WIRELOG_TYPE_FLOAT};
    const wirelog_column_type_t bad[] = {(wirelog_column_type_t)99,
                                         WIRELOG_TYPE_BOOL};
    for (unsigned tag = 0; tag < 256; ++tag) {
        uint8_t b[1024]; uint32_t n = 0;
        for (unsigned prefix = 0; prefix < 3; ++prefix) {
            n = 0;
            for (unsigned i = 0; i < prefix;
                ++i) n += var(b+n, WL_PLAN_EXPR_VAR, i);
            if (tag == WL_PLAN_EXPR_VAR || tag == WL_PLAN_EXPR_VAR_FLOAT ||
                tag == WL_PLAN_EXPR_VAR_STRING)
                n += var(b+n, (uint8_t)tag, 0);
            else {
                b[n++] = (uint8_t)tag;
                if (tag == WL_PLAN_EXPR_CONST_INT ||
                    tag == WL_PLAN_EXPR_CONST_FLOAT) {
                    int64_t value = 0; memcpy(b+n, &value, 8); n += 8;
                } else if (tag == WL_PLAN_EXPR_BOOL)b[n++] = 1;
                else if (tag == WL_PLAN_EXPR_CONST_STR) {
                    uint16_t length = 1; memcpy(b+n, &length, 2); n += 2;
                    b[n++] = 'x';
                }
            }
            wl_columnar_expr_attempt_scalar_t *p = NULL; bool supported = false;
            wl_evaluation_control_t *c = begin(0);
            CHECK(!wl_columnar_expr_compile_scalar_attempt_controlled(b, n,
                NULL, &p, &supported, c,
                wl_evaluation_control_active_attempt_id(c)));
            finish(c, 0);
            if (!supported) {
                CHECK(!p); continue;
            }
            wl_columnar_expr_attempt_scalar_free(p);
            for (unsigned width = 0; width < 3; ++width) {
                parity(b, n, NULL, width); parity(b, n, types, width);
                parity(b, n, bad, width);
            }
        }
    }
    uint8_t b[1024]; uint32_t n = 0;
    for (unsigned i = 0; i < COL_FILTER_STACK;
        ++i) n += var(b+n, WL_PLAN_EXPR_VAR, 0);
    n += var(b+n, WL_PLAN_EXPR_VAR_FLOAT, 1); /*full stack ignores this producer*/
    parity(b, n, types, 2); parity(b, n, bad, 2);
    b[n++] = WL_PLAN_EXPR_CMP_STR_EQ; /*replaces invalid earlier type*/
    parity(b, n, bad, 2);
    n = var(b, WL_PLAN_EXPR_VAR, 0); b[n++] = WL_PLAN_EXPR_BOOL; b[n++] = 1;
    parity(b, n, bad, 2);
    b[n++] = WL_PLAN_EXPR_STR_FN_TO_STRING; /*rawdisabled after interpdead*/
    parity(b, n, bad, 2);
}
int main(void) {
    literal_oracles(); grammar_parity(); interruption();
    puts("whole scalar MAP schema: all checks passed"); return 0;
}
