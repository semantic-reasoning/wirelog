/* #2299: independent byte-work bounds, stops and borrowed string lifetime. */
#include "../wirelog/columnar/internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x)                                                               \
        do {                                                                         \
            if (!(x)) {                                                                \
                fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x);                  \
                exit(1);                                                                 \
            }                                                                          \
        } while (0)
static int owner;
static unsigned stop_phase, mode, calls;
static uint64_t stop_progress;
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);

static wl_evaluation_control_t *begin(uint64_t limit) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    return c;
}
static uint64_t finish(wl_evaluation_control_t *c, int rc) {
    wl_evaluation_control_report_t report;
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0 ? rc : 0, 0) == rc);
    CHECK(!wl_evaluation_control_report(c, &report));
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
    return report.consumed;
}
static void checkpoint(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    CHECK(!failed && phase >= 40 && phase <= 48);
    calls++;
    if (phase != stop_phase || progress != stop_progress)
        return;
    if (mode == 0)
        wl_evaluation_control_request_cancel(c);
    else {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &owner));
        /* If a stale continuation calls the hook again it must fail visibly. */
        stop_phase = 99;
    }
}
static uint64_t id(wl_evaluation_control_t *c) {
    return wl_evaluation_control_active_attempt_id(c);
}
static void exact_boundaries(void) {
    /* Literal debit oracles, independent of observed consumed work.
     * T=224; entry+final=448; each inspected byte/pair=224;
     * COPY/HASH additionally charge their distinct STORE/update. */
    for (unsigned below = 0; below < 2; below++) {
        size_t n = 999;
        int cmp = 9;
        uint32_t hash = 99;
        wl_evaluation_control_t *c =
            begin(1120 - below); /* "ab" + NUL + entry/final */
        int rc =
            wl_columnar_expr_cstr_length_attempt_controlled("ab", &n, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(n == (below ? 999 : 2));
        CHECK(finish(c, rc) == 1120 - below * 224);
        c = begin(896 - below); /* differing second pair */
        rc = wl_columnar_expr_bytes_compare_attempt_controlled(
            (const uint8_t *)"ax", (const uint8_t *)"ay", 2, &cmp, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(cmp == (below ? 9 : -1));
        finish(c, rc);
        uint8_t dst[3] = {9, 9, 9};
        size_t copied = 999;
        c = begin(1792 - below); /*3 LOADs +3 STOREs +entry/final */
        rc = wl_columnar_expr_bytes_copy_attempt_controlled(
            dst, (const uint8_t *)"abc", 3, &copied, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(copied == 3 && !memcmp(dst, "abc", 3));
        finish(c, rc);
        c = begin(1344 - below); /*2 LOADs +2 updates +entry/final */
        rc = wl_columnar_expr_bytes_hash_attempt_controlled(
            (const uint8_t *)"ab", 2, 2166136261u, &hash, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(hash == (below ? 99 : UINT32_C(0x4d2505ca)));
        finish(c, rc);
        size_t pos = 999;
        bool found = false;
        c = begin(2240 - below); /* aaab/aab:2 candidates,6 inspected pairs */
        /* Actual pairs: candidate0 a/a,a/a,a/b; candidate1 a/a,a/a,b/b.
         * 2+6+2 =10 regions, hence2240, not a successful-run measurement. */
        rc = wl_columnar_expr_bytes_find_attempt_controlled(
            (const uint8_t *)"aaab", 4, (const uint8_t *)"aab", 3, &pos, &found,
            c,
            id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(pos == (below ? 999 : 1) && found == !below);
        finish(c, rc);
    }
}
static void empty_and_values(void) {
    wl_evaluation_control_t *c = begin(0);
    uint64_t attempt = id(c);
    size_t n = 999, pos = 999;
    bool found = false;
    int cmp = 9;
    uint32_t hash = 99;
    CHECK(!wl_columnar_expr_cstr_length_attempt_controlled("", &n, c,
        attempt) &&
        n == 0);
    CHECK(!wl_columnar_expr_bytes_compare_attempt_controlled(NULL, NULL, 0,
        &cmp,
        c, attempt) &&
        cmp == 0);
    CHECK(!wl_columnar_expr_bytes_copy_attempt_controlled(NULL, NULL, 0, &n, c,
        attempt) &&
        n == 0);
    CHECK(!wl_columnar_expr_bytes_hash_attempt_controlled(NULL, 0, 17, &hash, c,
        attempt) &&
        hash == 17);
    CHECK(!wl_columnar_expr_bytes_find_attempt_controlled(NULL, 0, NULL, 0,
        &pos,
        &found, c, attempt) &&
        found && pos == 0);
    CHECK(!wl_columnar_expr_bytes_find_attempt_controlled(
            NULL, 0, (const uint8_t *)"a", 1, &pos, &found, c, attempt) &&
        !found && pos == 0);
    const uint8_t high[] = {0xff, 0}, low[] = {0x7f, 0};
    CHECK(!wl_columnar_expr_bytes_compare_attempt_controlled(high, low, 2, &cmp,
        c, attempt) &&
        cmp == 1);
    CHECK(!wl_columnar_expr_bytes_hash_attempt_controlled(
            (const uint8_t *)"foobar", 6, 2166136261u, &hash, c, attempt) &&
        hash == UINT32_C(0xbf9cf968));
    CHECK(!wl_columnar_expr_bytes_hash_attempt_controlled((const uint8_t *)"a",
        1,
        0, &hash, c, attempt) &&
        hash == UINT32_C(1627429043));
    const uint8_t utf8[] = {0xed, 0x95, 0x9c, 0, 0xff};
    uint8_t dst[5];
    CHECK(!wl_columnar_expr_bytes_copy_attempt_controlled(dst, utf8, 5, &n, c,
        attempt) &&
        n == 5 && !memcmp(dst, utf8, 5));
    finish(c, 0);
    for (unsigned kind = 0; kind < 5; kind++) {
        c = begin(448);
        n = 999;
        cmp = 9;
        hash = 99;
        found = false;
        pos = 999;
        int rc = kind ==
            0   ? wl_columnar_expr_bytes_compare_attempt_controlled(
            NULL, NULL, 0, &cmp, c, id(c))
             : kind == 1 ? wl_columnar_expr_bytes_copy_attempt_controlled(
            NULL, NULL, 0, &n, c, id(c))
             : kind == 2 ? wl_columnar_expr_bytes_hash_attempt_controlled(
            NULL, 0, 17, &hash, c, id(c))
             : kind == 3 ? wl_columnar_expr_bytes_find_attempt_controlled(
            NULL, 0, NULL, 0, &pos, &found, c, id(c))
                         : wl_columnar_expr_bytes_find_attempt_controlled(
            NULL, 0, (const uint8_t *)"a", 1, &pos, &found,
            c, id(c));
        CHECK(!rc && finish(c, 0) == 448);
    }
}
static void stops(void) {
    static const unsigned phases[] = {40, 41, 42, 43, 44, 45, 46, 47, 48};
    for (mode = 0; mode < 2; mode++)
        for (unsigned i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
            wl_evaluation_control_t *c = begin(0);
            uint64_t attempt = id(c);
            size_t n = 999, pos = 999;
            int cmp = 9;
            uint32_t hash = 99;
            bool found = false;
            uint8_t dst[2] = {9, 9};
            stop_phase = phases[i];
            stop_progress = phases[i] == 47 ? 2 : 0;
            calls = 0;
            wl_columnar_expr_test_checkpoint = checkpoint;
            int rc;
            switch (stop_phase) {
            case 41:
            case 48:
                rc =
                    wl_columnar_expr_cstr_length_attempt_controlled("", &n, c,
                        attempt);
                break;
            case 42:
                rc = wl_columnar_expr_bytes_compare_attempt_controlled(
                    (const uint8_t *)"a", (const uint8_t *)"a", 1, &cmp, c,
                    attempt);
                break;
            case 45:
                rc = wl_columnar_expr_bytes_hash_attempt_controlled(
                    (const uint8_t *)"a", 1, 0, &hash, c, attempt);
                break;
            case 46:
            case 47:
                rc = wl_columnar_expr_bytes_find_attempt_controlled(
                    (const uint8_t *)"aaab", 4, (const uint8_t *)"aab", 3, &pos,
                    &found,
                    c, attempt);
                break;
            default:
                rc = wl_columnar_expr_bytes_copy_attempt_controlled(
                    dst, (const uint8_t *)"ab", 2, &n, c, attempt);
                break;
            }
            wl_columnar_expr_test_checkpoint = NULL;
            CHECK(rc == (mode ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED));
            CHECK(hash == 99 && cmp == 9 && pos == 999 && !found);
            if (phases[i] == 41 || phases[i] == 48)
                CHECK(n == 999);
            if (phases[i] == 40)
                CHECK(n == 999 && dst[0] == 9);
            if (phases[i] == 43)
                CHECK(n == 0 && dst[0] == 9);
            if (phases[i] == 44)
                CHECK(n == 1 && dst[0] == 'a' && dst[1] == 9);
            if (mode) {
                uint64_t consumed = 999;
                CHECK(
                    !wl_evaluation_control_test_active_consumed(c, id(c),
                    &consumed) &&
                    consumed == 0);
                unsigned before = calls;
                wl_columnar_expr_test_checkpoint = checkpoint;
                CHECK(wl_columnar_expr_cstr_length_attempt_controlled(
                        "a", &n, c, attempt) == EINVAL &&
                    before == calls);
                wl_columnar_expr_test_checkpoint = NULL;
                finish(c, 0);
            } else
                finish(c, rc);
        }
    /* Work denial after LOAD admits no STORE. */
    wl_evaluation_control_t *c = begin(448);
    uint8_t dst = 9;
    size_t copied = 999;
    int rc = wl_columnar_expr_bytes_copy_attempt_controlled(
        &dst, (const uint8_t *)"a", 1, &copied, c, id(c));
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && dst == 9 && copied == 0);
    finish(c, rc);
    c = begin(448);
    uint32_t hash = 99;
    rc = wl_columnar_expr_bytes_hash_attempt_controlled((const uint8_t *)"a", 1,
            0, &hash, c, id(c));
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && hash == 99);
    finish(c, rc);
    char large[2049];
    memset(large, 'a', 2048);
    large[2048] = 0;
    c = begin(0);
    size_t n = 999;
    stop_phase = 41;
    stop_progress = 1024;
    mode = 0;
    wl_columnar_expr_test_checkpoint = checkpoint;
    rc = wl_columnar_expr_cstr_length_attempt_controlled(large, &n, c, id(c));
    wl_columnar_expr_test_checkpoint = NULL;
    CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && n == 999);
    finish(c, rc);
    c = begin(459424); /*2048 bytes+NUL+entry/final =2051*224. */
    CHECK(!wl_columnar_expr_cstr_length_attempt_controlled(large, &n, c,
        id(c)) &&
        n == 2048);
    CHECK(finish(c, 0) == 459424);
}
static void invalid_and_pin(void) {
    wl_evaluation_control_t *c = begin(0);
    uint64_t attempt = id(c);
    size_t n = 999;
    CHECK(wl_columnar_expr_cstr_length_attempt_controlled(NULL, &n, c,
        attempt) ==
        EINVAL &&
        n == 999);
    CHECK(wl_columnar_expr_bytes_copy_attempt_controlled(
            NULL, (const uint8_t *)"a", 1, &n, c, attempt) == EINVAL &&
        n == 999);
    CHECK(wl_columnar_expr_cstr_length_attempt_controlled("a", &n, NULL,
        attempt) == EINVAL &&
        n == 999);
    CHECK(wl_columnar_expr_cstr_length_attempt_controlled("a", &n, c, 0) ==
        EINVAL &&
        n == 999);
    CHECK(finish(c, 0) == 0);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = 1000000,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(g);
    wl_intern_t *table = wl_intern_create();
    CHECK(table);
    int64_t value = wl_intern_put(table, "pinned");
    CHECK(value >= 0);
    CHECK(!wl_intern_attach_memory_governor(table, g));
    CHECK(!wl_intern_pin_acquire(table, g));
    const char *borrowed = wl_intern_reverse(table, value);
    CHECK(borrowed);
    wl_intern_free(table); /* The pin retains the real retired owner. */
    c = begin(2016);     /*6 bytes+NUL+entry/final =9*224. */
    CHECK(!wl_columnar_expr_cstr_length_attempt_controlled(borrowed, &n, c,
        id(c)) &&
        n == 6);
    CHECK(finish(c, 0) == 2016);
    c = begin(1792);
    int cmp = 9;
    CHECK(!wl_columnar_expr_bytes_compare_attempt_controlled(
            (const uint8_t *)borrowed, (const uint8_t *)"pinned", 6, &cmp, c,
            id(c)) &&
        cmp == 0);
    CHECK(finish(c, 0) == 1792);
    wl_intern_pin_release(table);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
int main(void) {
    exact_boundaries();
    empty_and_values();
    stops();
    invalid_and_pin();
    puts("expression byte primitives: all checks passed");
    return 0;
}
