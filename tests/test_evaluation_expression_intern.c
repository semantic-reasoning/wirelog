/* #2299 transactional interning: independent bounds and publication oracles. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
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
static unsigned stop_phase, stop_mode;
static uint64_t stop_progress;
static wl_intern_t *duplicate_table;
static int64_t duplicate_id;
static wl_evaluation_control_t *begin(uint64_t limit) {
    wl_evaluation_control_t *c = NULL;
    CHECK(!wl_evaluation_control_create(limit, &c));
    CHECK(!wl_evaluation_control_attach(c, &owner));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    return c;
}
static uint64_t id(wl_evaluation_control_t *c) {
    return wl_evaluation_control_active_attempt_id(c);
}
static uint64_t finish(wl_evaluation_control_t *c, int rc) {
    wl_evaluation_control_report_t r;
    CHECK(wl_evaluation_control_finish(c, &owner, rc > 0 ? rc : 0, 0) == rc);
    CHECK(!wl_evaluation_control_report(c, &r));
    CHECK(!wl_evaluation_control_detach(c, &owner));
    wl_evaluation_control_release(c);
    return r.consumed;
}
static void *duplicate_writer(void *arg) {
    duplicate_id = wl_intern_put(arg, "raced");
    return NULL;
}
static void hook(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress) {
    if (phase != stop_phase || progress != stop_progress)
        return;
    if (stop_mode == 0)
        wl_evaluation_control_request_cancel(c);
    else if (stop_mode == 1) {
        CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
        CHECK(!wl_evaluation_control_reset(c));
        CHECK(!wl_evaluation_control_begin(c, &owner));
    } else if (stop_mode == 2) {
        /* The raw writer races only the unlocked preparation boundary. */
        wl_thread_t writer;
        CHECK(!wl_thread_create(&writer, duplicate_writer, duplicate_table));
        CHECK(!wl_thread_join(&writer));
        CHECK(duplicate_id >= 0);
    }
}
static wl_columnar_memory_governor_ref_t *governor(wl_intern_t *table) {
    wl_columnar_memory_resolution_t r = {
        .budget_bytes = WL_COLUMNAR_MEMORY_MIN_BUDGET,
        .usable_bytes = 10000000,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *g =
        wl_columnar_memory_governor_ref_create(&r);
    CHECK(g);
    CHECK(!wl_intern_attach_memory_governor(table, g));
    return g;
}
static void release(wl_intern_t *table, wl_columnar_memory_governor_ref_t *g) {
    wl_intern_free(table);
    if (g) {
        CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)));
        wl_columnar_memory_governor_ref_release(g);
    }
}
static void fill(wl_intern_t *table, unsigned count) {
    for (unsigned i = 0; i < count; i++) {
        char s[16];
        snprintf(s, sizeof(s), "key%02u", i);
        CHECK(wl_intern_put(table, s) == (int64_t)i);
    }
}
static void unchanged(wl_intern_t *table, unsigned count, const char *pointer) {
    CHECK(wl_intern_count(table) == count);
    if (count)
        CHECK(wl_intern_reverse(table, 0) == pointer && !strcmp(pointer,
            "key00"));
}
static void independent_work(void) {
    for (unsigned below = 0; below < 2; below++) {
        wl_intern_t *table = wl_intern_create();
        CHECK(table);
        int64_t out = 999;
        /* First unique x:5*n+9 byte-helper regions, 81 fixed/init regions.
         * n=1 =>95*224. The first reverse segment has64 entries. */
        wl_evaluation_control_t *c = begin(21280 - below);
        int rc = wl_intern_control_put_attempt(table, "x", &out, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(out == (below ? 999 : 0) && wl_intern_count(table) == !below);
        finish(c, rc);
        if (below)
            CHECK(wl_intern_put(table, "x") == 0);
        out = 999;
        c = begin(6720 - below); /*Duplicate x:30 regions including1probe. */
        rc = wl_intern_control_put_attempt(table, "x", &out, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0) &&
            out == (below ? 999 : 0));
        CHECK(wl_intern_count(table) == 1);
        finish(c, rc);
        release(table, NULL);
        table = wl_intern_create();
        CHECK(table);
        fill(table, 48);
        const char *pointer = wl_intern_reverse(table, 0);
        out = 999;
        c = begin(307552 - below);
        /* key00..key47 ->next:10 old probes,128 initializations,
         * 48 migrations of length5 (22 fixed/byte regions each),
         * 61 destination probes and1 new probe, plus9 old collision
         * length scans of5bytes+NUL (8regions each):1373*224. */
        rc = wl_intern_control_put_attempt(table, "next", &out, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(out == (below ? 999 : 48));
        if (below) {
            unchanged(table, 48, pointer);
            CHECK(wl_intern_control_test_slot_capacity(table) == 64);
        } else
            CHECK(wl_intern_count(table) == 49 &&
                wl_intern_control_test_slot_capacity(table) == 128);
        finish(c, rc);
        release(table, NULL);
    }
}
static void collision_and_midwork(void) {
    for (unsigned below = 0; below < 2; below++) {
        wl_intern_t *table = wl_intern_create();
        CHECK(table);
        CHECK(wl_intern_put(table, "c19") == 0);
        int64_t out = 999;
        /* c19/c42 hash to bucket32 in64 slots. One failed comparison
         * inspects2pairs; candidate length3+NUL costs6regions, comparison
         * costs4. Unique base40 +10 =50regions. */
        wl_evaluation_control_t *c = begin(11200 - below);
        int rc = wl_intern_control_put_attempt(table, "c42", &out, c, id(c));
        CHECK(rc == (below ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(out == (below ? 999 : 1));
        finish(c, rc);
        release(table, NULL);
    }
    wl_intern_t *table = wl_intern_create();
    CHECK(table);
    int64_t out = 999;
    /* Before segment initialization:14 fixed+29 byte regions for next.
     * With33 admitted entry initializations, budget76*224 stops at entry33. */
    wl_evaluation_control_t *c = begin(17024);
    int rc = wl_intern_control_put_attempt(table, "next", &out, c, id(c));
    CHECK(rc == WL_EVALUATION_CONTROL_WORK_LIMIT && out == 999 &&
        !wl_intern_count(table));
    CHECK(finish(c, rc) == 17024);
    c = begin(0);
    CHECK(!wl_intern_control_put_attempt(table, "next", &out, c, id(c)) &&
        out == 0);
    finish(c, 0);
    release(table, NULL);
}
static unsigned expression_phase;
extern void (*wl_columnar_expr_test_checkpoint)(wl_evaluation_control_t *,
    unsigned, uint64_t, bool);
static void byte_stop(wl_evaluation_control_t *c, unsigned phase,
    uint64_t progress, bool failed) {
    CHECK(!failed);
    if (phase == expression_phase && progress == (phase == 42 ? 1u : 2u))
        wl_evaluation_control_request_cancel(c);
}
static void commit_stop(wl_evaluation_control_t *c,
    wl_columnar_memory_reservation_t *token,
    unsigned phase) {
    (void)token;
    if (phase == WL_COLUMNAR_MEMORY_GOVERNOR_COMMIT_CUTOFF)
        wl_evaluation_control_request_cancel(c);
}
static void nested_stops(void) {
    for (unsigned which = 0; which < 3; which++) {
        wl_intern_t *table = wl_intern_create();
        CHECK(table);
        if (which == 1)
            CHECK(wl_intern_put(table, "c19") == 0);
        wl_columnar_memory_governor_ref_t *g = governor(table);
        uint64_t old =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
        const char *ptr = which == 1 ? wl_intern_reverse(table, 0) : NULL;
        wl_evaluation_control_t *c = begin(0);
        int64_t out = 999;
        expression_phase = which == 0 ? 44 : 42;
        if (which < 2)
            wl_columnar_expr_test_checkpoint = byte_stop;
        else
            wl_columnar_memory_governor_test_checkpoint = commit_stop;
        int rc = wl_intern_control_put_attempt(
            table, which == 1 ? "c42" : "next", &out, c, id(c));
        wl_columnar_expr_test_checkpoint = NULL;
        wl_columnar_memory_governor_test_checkpoint = NULL;
        CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && out == 999);
        CHECK(wl_intern_count(table) == (which == 1 ? 1u : 0u));
        if (ptr)
            CHECK(wl_intern_reverse(table, 0) == ptr);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)) ==
            old);
        finish(c, rc);
        release(table, g);
    }
}
static void values_and_memory(void) {
    for (unsigned governed = 0; governed < 2; governed++) {
        wl_intern_t *table = wl_intern_create();
        CHECK(table);
        wl_columnar_memory_governor_ref_t *g =
            governed ? governor(table) : NULL;
        const char *values[] = {"",    "a",  "\xc3\xa9", "\xff",
                                "zero\0ignored",
                                "c19", "c42"};
        wl_evaluation_control_t *c = begin(0);
        for (unsigned i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
            int64_t out = 999;
            CHECK(
                !wl_intern_control_put_attempt(table, values[i], &out, c,
                id(c)));
            CHECK(!strcmp(wl_intern_reverse(table, out), values[i]));
            CHECK(wl_intern_get(table, values[i]) == out);
            CHECK(wl_intern_put(table, values[i]) == out);
        }
        CHECK(wl_intern_get(table, "zero") == 4);
        finish(c, 0);
        if (g) {
            wl_columnar_memory_governor_t *budget =
                wl_columnar_memory_governor_ref_get(g);
            uint64_t old = wl_columnar_memory_reserved(budget);
            atomic_store_explicit(&budget->usable_bytes, old,
                memory_order_relaxed);
            c = begin(0);
            int64_t out = 999;
            CHECK(!wl_intern_control_put_attempt(table, "a", &out, c,
                id(c)) &&
                out == 1);
            finish(c, 0);
            CHECK(wl_columnar_memory_reserved(budget) == old);
        }
        release(table, g);
    }
    for (unsigned count = 0; count <= 64; count += 16)
        if (count == 0 || count == 48 || count == 64)
            for (unsigned below = 0; below < 2; below++) {
                wl_intern_t *table = wl_intern_create();
                CHECK(table);
                fill(table, count);
                const char *pointer = count ? wl_intern_reverse(table,
                        0) : NULL;
                wl_columnar_memory_governor_ref_t *g = governor(table);
                wl_columnar_memory_governor_t *budget =
                    wl_columnar_memory_governor_ref_get(g);
                uint64_t old = wl_columnar_memory_reserved(budget);
                /* next is5bytes; resize retires64*4 and allocates128*4;
                 * segment0 has64 pointers, segment1 has128 pointers. */
                uint64_t replacement = old + 5 +
                    (count == 0    ? 64 * sizeof(char *)
                                : count == 64 ? 128 * sizeof(char *)
                                              : 256);
                atomic_store_explicit(&budget->usable_bytes,
                    old + replacement - below,
                    memory_order_relaxed);
                wl_evaluation_control_t *c = begin(0);
                int64_t out = 999;
                int rc =
                    wl_intern_control_put_attempt(table, "next", &out, c,
                        id(c));
                CHECK(rc == (below ? WL_INTERN_ERR_MEMORY_BUDGET : 0));
                if (below) {
                    CHECK(out == 999);
                    unchanged(table, count, pointer);
                    CHECK(wl_columnar_memory_reserved(budget) == old);
                } else
                    CHECK(out == (int64_t)count &&
                        wl_columnar_memory_reserved(budget) == replacement);
                finish(c, rc);
                if (below) {
                    atomic_store_explicit(&budget->usable_bytes, 10000000,
                        memory_order_relaxed);
                    c = begin(0);
                    CHECK(!wl_intern_control_put_attempt(table, "next", &out,
                        c,
                        id(c)) &&
                        out == (int64_t)count);
                    finish(c, 0);
                }
                release(table, g);
            }
}
static void cancellations(void) {
    static const struct {
        unsigned count, phase;
        uint64_t progress;
    } cases[] = {{0, 2, 4},   {0, 3, 0},   {0, 5, 0},    {0, 6, 32},
                 {0, 10, 0},
                 {48, 7, 64}, {48, 8, 24}, {48, 16, 24}, {48, 10, 48}};
    for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
        for (stop_mode = 0; stop_mode < 2; stop_mode++) {
            wl_intern_t *table = wl_intern_create();
            CHECK(table);
            fill(table, cases[i].count);
            const char *pointer = cases[i].count ? wl_intern_reverse(table,
                    0) : NULL;
            wl_columnar_memory_governor_ref_t *g = governor(table);
            uint64_t reserved =
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                        g));
            wl_evaluation_control_t *c = begin(0);
            uint64_t attempt = id(c);
            int64_t out = 999;
            stop_phase = cases[i].phase;
            stop_progress = cases[i].progress;
            wl_intern_control_test_checkpoint = hook;
            int rc =
                wl_intern_control_put_attempt(table, "next", &out, c,
                    attempt);
            wl_intern_control_test_checkpoint = NULL;
            CHECK(rc ==
                (stop_mode ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED) &&
                out == 999);
            unchanged(table, cases[i].count, pointer);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)) == reserved);
            if (stop_mode) {
                uint64_t consumed = 999;
                CHECK(
                    !wl_evaluation_control_test_active_consumed(c, id(c),
                    &consumed) &&
                    consumed == 0);
                finish(c, 0);
            } else
                finish(c, rc);
            c = begin(0);
            CHECK(!wl_intern_control_put_attempt(table, "next", &out, c,
                id(c)) &&
                out == (int64_t)cases[i].count);
            finish(c, 0);
            release(table, g);
        }
}
static void allocation_and_duplicate(void) {
    for (unsigned count = 0; count <= 48; count += 48)
        for (unsigned fail = 0; fail < 2; fail++) {
            wl_intern_t *table = wl_intern_create();
            CHECK(table);
            fill(table, count);
            const char *pointer = count ? wl_intern_reverse(table, 0) : NULL;
            wl_columnar_memory_governor_ref_t *g = governor(table);
            uint64_t reserved =
                wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                        g));
            wl_evaluation_control_t *c = begin(0);
            int64_t out = 999;
            wl_intern_control_test_fail_allocation = (int)fail;
            int rc = wl_intern_control_put_attempt(table, "next", &out, c,
                    id(c));
            wl_intern_control_test_fail_allocation = -1;
            CHECK(rc == ENOMEM && out == 999);
            unchanged(table, count, pointer);
            CHECK(wl_columnar_memory_reserved(
                    wl_columnar_memory_governor_ref_get(g)) == reserved);
            finish(c, rc);
            release(table, g);
        }
    wl_intern_t *table = wl_intern_create();
    CHECK(table);
    duplicate_table = table;
    duplicate_id = -1;
    stop_phase = 2;
    stop_progress = 5;
    stop_mode = 2;
    wl_evaluation_control_t *c = begin(0);
    int64_t out = 999;
    wl_intern_control_test_checkpoint = hook;
    CHECK(!wl_intern_control_put_attempt(table, "raced", &out, c, id(c)) &&
        out == duplicate_id && wl_intern_count(table) == 1);
    wl_intern_control_test_checkpoint = NULL;
    finish(c, 0);
    release(table, NULL);
    table = wl_intern_create();
    CHECK(table);
    wl_columnar_memory_governor_ref_t *g = governor(table);
    CHECK(!wl_intern_pin_acquire(table, g));
    wl_intern_free(table);
    c = begin(0);
    out = 999;
    CHECK(!wl_intern_control_put_attempt(table, "pinned", &out, c, id(c)) &&
        !strcmp(wl_intern_reverse(table, out), "pinned"));
    finish(c, 0);
    CHECK(wl_intern_pin_acquire(table, g) == EPIPE);
    wl_intern_pin_release(table);
    CHECK(!wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g)));
    wl_columnar_memory_governor_ref_release(g);
}
static void duplicate_and_late_stop(void) {
    for (stop_mode = 0; stop_mode < 2; stop_mode++) {
        wl_intern_t *table = wl_intern_create();
        CHECK(table);
        fill(table, 1);
        wl_columnar_memory_governor_ref_t *g = governor(table);
        uint64_t old =
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(g));
        const char *ptr = wl_intern_reverse(table, 0);
        wl_evaluation_control_t *c = begin(0);
        int64_t out = 999;
        stop_phase = 10;
        stop_progress = 0;
        wl_intern_control_test_checkpoint = hook;
        int rc = wl_intern_control_put_attempt(table, "key00", &out, c,
                id(c));
        wl_intern_control_test_checkpoint = NULL;
        CHECK(rc == (stop_mode ? EINVAL : WL_EVALUATION_CONTROL_CANCELLED) &&
            out == 999);
        unchanged(table, 1, ptr);
        CHECK(wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                g)) ==
            old);
        finish(c, stop_mode ? 0 : rc);
        release(table, g);
    }
    wl_intern_t *table = wl_intern_create();
    CHECK(table);
    wl_evaluation_control_t *c = begin(0);
    int64_t out = 999;
    CHECK(wl_intern_control_put_attempt(NULL, "x", &out, c,
        id(c)) == EINVAL &&
        out == 999);
    CHECK(wl_intern_control_put_attempt(table, NULL, &out, c, id(c)) ==
        EINVAL &&
        out == 999);
    CHECK(wl_intern_control_put_attempt(table, "x", &out, c, 0) == EINVAL &&
        out == 999);
    CHECK(wl_intern_control_put_attempt(table, "x", &out, NULL,
        1) == EINVAL &&
        out == 999);
    uint64_t consumed = 999;
    CHECK(!wl_evaluation_control_test_active_consumed(c, id(c), &consumed) &&
        consumed == 0);
    CHECK(!wl_intern_control_put_attempt(table, "late", &out, c, id(c)) &&
        out == 0);
    /* First observable boundary after the indivisible publication suffix. */
    wl_evaluation_control_request_cancel(c);
    CHECK(!wl_evaluation_control_finish(c, &owner, 0, 0));
    CHECK(!wl_evaluation_control_begin(c, &owner));
    out = 999;
    CHECK(wl_intern_control_put_attempt(table, "next", &out, c, id(c)) ==
        WL_EVALUATION_CONTROL_CANCELLED &&
        out == 999);
    CHECK(wl_intern_count(table) == 1 && wl_intern_get(table, "late") == 0);
    finish(c, WL_EVALUATION_CONTROL_CANCELLED);
    release(table, NULL);
    table = wl_intern_create();
    CHECK(table);
    wl_columnar_memory_governor_ref_t *g = governor(table);
    wl_columnar_memory_governor_t *budget =
        wl_columnar_memory_governor_ref_get(g);
    uint64_t old = wl_columnar_memory_reserved(budget);
    budget->mode = WL_COLUMNAR_MEMORY_MODE_ADVISORY;
    atomic_store_explicit(&budget->usable_bytes, old + 1000000,
        memory_order_relaxed);
    c = begin(0);
    out = 999;
    CHECK(!wl_intern_control_put_attempt(table, "advisory", &out, c,
        id(c)) &&
        out == 0);
    CHECK(wl_columnar_memory_reserved(budget) > old);
    finish(c, 0);
    release(table, g);
}
int main(void) {
    independent_work();
    collision_and_midwork();
    nested_stops();
    values_and_memory();
    cancellations();
    allocation_and_duplicate();
    duplicate_and_late_stop();
    puts("controlled intern: all checks passed");
    return 0;
}
