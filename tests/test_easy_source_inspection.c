/*
 * test_easy_source_inspection.c - easy-facade host source inspection
 * (Issue #2321).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Counts come from real wirelog_easy_* inserts, removes and evaluation.
 * The init-failure stages are injected through a test hook; the large
 * history check measures the query's own allocations and governed
 * reservations, not RSS.
 */

#include "wirelog/wirelog-easy.h"
#include "wirelog/wirelog-advanced.h"
#include "wirelog/columnar/internal.h"
#include "wirelog/columnar/memory_governor.h"
#include "wirelog/intern.h"
#include "wirelog/session.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__GLIBC__)
#include <malloc.h>
#if defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 33)
#define EASY_TEST_HAVE_MALLINFO2 1
#endif
#endif
#endif

/*
 * Allocation-call counter (glibc, unsanitized builds).  The executable's
 * definitions interpose libc's for the whole process and forward to glibc's
 * own entry points, so a query window that makes no call proves the query
 * kept no heap state of any size, not merely none left over.
 */
#if defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) \
    || __has_feature(memory_sanitizer)
#define EASY_TEST_SANITIZED 1
#endif
#endif
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define EASY_TEST_SANITIZED 1
#endif
#if defined(__GLIBC__) && !defined(EASY_TEST_SANITIZED)
#define EASY_TEST_COUNT_ALLOCS 1
extern void *__libc_malloc(size_t size);
extern void *__libc_calloc(size_t n, size_t size);
extern void *__libc_realloc(void *ptr, size_t size);
extern void *__libc_memalign(size_t alignment, size_t size);
extern void __libc_free(void *ptr);
static _Thread_local unsigned long easy_alloc_calls;

void *malloc(size_t size);
void *calloc(size_t n, size_t size);
void *realloc(void *ptr, size_t size);
void *aligned_alloc(size_t alignment, size_t size);
int posix_memalign(void **out, size_t alignment, size_t size);
void free(void *ptr);

void *
malloc(size_t size)
{
    easy_alloc_calls++;
    return __libc_malloc(size);
}

void *
calloc(size_t n, size_t size)
{
    easy_alloc_calls++;
    return __libc_calloc(n, size);
}

void *
realloc(void *ptr, size_t size)
{
    easy_alloc_calls++;
    return __libc_realloc(ptr, size);
}

void *
aligned_alloc(size_t alignment, size_t size)
{
    easy_alloc_calls++;
    return __libc_memalign(alignment, size);
}

int
posix_memalign(void **out, size_t alignment, size_t size)
{
    easy_alloc_calls++;
    /* Test-grade: valid callers only; no EINVAL alignment check. */
    void *p = __libc_memalign(alignment, size);
    if (!p)
        return ENOMEM;
    *out = p;
    return 0;
}

void
free(void *ptr)
{
    __libc_free(ptr);
}
#endif

void wirelog_easy_testhook_fail_next_init(int stage);
wl_session_t *wirelog_easy_testhook_inner(wirelog_easy_session_t *s);

static int failures;

#define CHECK(cond, ...)                                                   \
        do {                                                               \
            if (!(cond)) {                                                 \
                fprintf(stderr, "%s:%d: CHECK(%s) failed: ", __FILE__,     \
                    __LINE__, #cond);                                      \
                fprintf(stderr, __VA_ARGS__);                              \
                fputc('\n', stderr);                                       \
                failures++;                                                \
            }                                                              \
        } while (0)

#define ALL UINT64_MAX
#define FAIL(st) (UINT64_MAX - (uint64_t)(st))

static wirelog_easy_session_t *
open_easy(const char *src)
{
    wirelog_easy_session_t *s = NULL;
    wirelog_error_t err = wirelog_easy_open(src, &s);
    if (err != WIRELOG_OK || !s) {
        fprintf(stderr, "open failed (%d):\n%s\n", err, src);
        exit(2);
    }
    return s;
}

static uint64_t
count1(wirelog_easy_session_t *s, const char *rel, int64_t v)
{
    uint64_t n = 99;
    wirelog_source_query_status_t st = wirelog_easy_source_count(s, rel, &v,
            1, ALL, &n);
    if (st != WIRELOG_SOURCE_QUERY_OK)
        return n == 0 ? FAIL(st) : FAIL(99);
    return n;
}

static void
ins1(wirelog_easy_session_t *s, const char *rel, int64_t v)
{
    wirelog_error_t err = wirelog_easy_insert(s, rel, &v, 1);
    CHECK(err == WIRELOG_OK, "insert %s(%lld) err=%d", rel, (long long)v,
        err);
}

static void
rem1(wirelog_easy_session_t *s, const char *rel, int64_t v)
{
    wirelog_error_t err = wirelog_easy_remove(s, rel, &v, 1);
    CHECK(err == WIRELOG_OK, "remove %s(%lld) err=%d", rel, (long long)v,
        err);
}

static void
step(wirelog_easy_session_t *s)
{
    wirelog_error_t err = wirelog_easy_step(s);
    CHECK(err == WIRELOG_OK, "step err=%d", err);
}

static const char *SRC =
    ".decl e(x: int64)\n"
    ".decl f(x: int64)\n"
    ".decl h(x: int64)\n"
    "f(1).\n"
    "h(X) :- e(X).\n";

/* ======================================================================== */
/* Lazy session                                                             */
/* ======================================================================== */

static void
test_lazy_inspection_does_not_build(void)
{
    wirelog_easy_session_t *s = open_easy(SRC);
    CHECK(wirelog_easy_testhook_inner(s) == NULL, "starts unbuilt");

    CHECK(count1(s, "e", 1) == 0, "eligible relation before build");
    CHECK(count1(s, "h", 1) == 0, "head before build");
    bool ex = true;
    CHECK(wirelog_easy_source_key_exists(s, "e", 1, 0, &ex)
        == WIRELOG_SOURCE_QUERY_OK && !ex, "key before build, limit 0");
    CHECK(wirelog_easy_source_validate(s, "e", 1) == WIRELOG_SOURCE_QUERY_OK,
        "validate before build");
    /* The inline-fact relation is refused from program metadata alone. */
    CHECK(count1(s, "f", 1) == FAIL(WIRELOG_SOURCE_QUERY_UNSUPPORTED),
        "inline-fact relation before build");
    CHECK(wirelog_easy_source_validate(s, "f", 1)
        == WIRELOG_SOURCE_QUERY_UNSUPPORTED, "validate fact relation");
    CHECK(count1(s, "nope", 1) == FAIL(WIRELOG_SOURCE_QUERY_UNKNOWN_RELATION),
        "unknown before build");
    int64_t two[2] = { 1, 2 };
    uint64_t n = 9;
    CHECK(wirelog_easy_source_count(s, "e", two, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT && n == 0, "arity");

    int64_t id = 5;
    bool found = true;
    CHECK(wirelog_easy_lookup_symbol(s, "never-seen", &id, &found)
        == WIRELOG_SOURCE_QUERY_OK && !found && id == -1,
        "absent symbol before build");
    int64_t alpha = wirelog_easy_intern(s, "alpha");
    CHECK(wirelog_easy_lookup_symbol(s, "alpha", &id, &found)
        == WIRELOG_SOURCE_QUERY_OK && found && id == alpha,
        "interned symbol before build");

    CHECK(wirelog_easy_testhook_inner(s) == NULL,
        "no inspection call built the session or loaded facts");

    /* The first building call loads facts; inspection then reads the
     * resident source. */
    ins1(s, "e", 1);
    CHECK(wirelog_easy_testhook_inner(s) != NULL, "insert built it");
    CHECK(count1(s, "e", 1) == 1, "pending insert after build");
    CHECK(count1(s, "f", 1) == FAIL(WIRELOG_SOURCE_QUERY_UNSUPPORTED),
        "fact relation after load");
    /* Identical host insert and removal of every copy: still refused. */
    ins1(s, "f", 1);
    rem1(s, "f", 1);
    rem1(s, "f", 1);
    step(s);
    CHECK(count1(s, "f", 1) == FAIL(WIRELOG_SOURCE_QUERY_UNSUPPORTED),
        "fact relation after host insert/remove");
    wirelog_easy_close(s);
}

/* ======================================================================== */
/* Transitions, head input, symbols                                         */
/* ======================================================================== */

static void
test_easy_transitions(void)
{
    wirelog_easy_session_t *s = open_easy(SRC);
    step(s);
    const char *rels[2] = { "e", "h" };
    for (int r = 0; r < 2; r++) {
        const char *rel = rels[r];
        CHECK(count1(s, rel, 7) == 0, "%s 0", rel);
        ins1(s, rel, 7);
        step(s);
        CHECK(count1(s, rel, 7) == 1, "%s 1", rel);
        ins1(s, rel, 7);
        step(s);
        CHECK(count1(s, rel, 7) == 2, "%s 2: %llu", rel,
            (unsigned long long)count1(s, rel, 7));
        rem1(s, rel, 7);
        step(s);
        CHECK(count1(s, rel, 7) == 1, "%s back to 1", rel);
        rem1(s, rel, 7);
        step(s);
        CHECK(count1(s, rel, 7) == 0, "%s back to 0", rel);
    }
    /* Inserted plus derived counts inserted occurrences only. */
    ins1(s, "h", 9);
    ins1(s, "e", 9);
    ins1(s, "e", 10);
    step(s);
    CHECK(count1(s, "h", 9) == 1, "inserted+derived");
    CHECK(count1(s, "h", 10) == 0, "derived only");
    wirelog_easy_close(s);
}

static void
test_easy_symbols(void)
{
    wirelog_easy_session_t *s = open_easy(
        ".decl audit(who: symbol, seq: int64)\n");
    int64_t id = 0;
    bool found = true;
    CHECK(wirelog_easy_source_validate(s, "audit", 2)
        == WIRELOG_SOURCE_QUERY_OK, "validate empty");
    CHECK(wirelog_easy_lookup_symbol(s, "bob", &id, &found)
        == WIRELOG_SOURCE_QUERY_OK && !found, "bob absent");
    int64_t alice = wirelog_easy_intern(s, "alice");
    int64_t row[2] = { alice, 3 };
    CHECK(wirelog_easy_insert(s, "audit", row, 2) == WIRELOG_OK, "insert");
    CHECK(wirelog_easy_insert(s, "audit", row, 2) == WIRELOG_OK, "insert");
    step(s);
    uint64_t n = 0;
    CHECK(wirelog_easy_source_count(s, "audit", row, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_OK && n == 2, "mixed count");
    bool ex = false;
    CHECK(wirelog_easy_source_key_exists(s, "audit", alice, ALL, &ex)
        == WIRELOG_SOURCE_QUERY_OK && ex, "symbol key");
    int64_t bad[2] = { 999999, 3 };
    CHECK(wirelog_easy_source_count(s, "audit", bad, 2, ALL, &n)
        == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT && n == 0, "bad id");
    CHECK(wirelog_easy_lookup_symbol(s, "bob", &id, &found)
        == WIRELOG_SOURCE_QUERY_OK && !found && id == -1, "bob still absent");
    wirelog_easy_close(s);
}

/* ======================================================================== */
/* Callbacks                                                                */
/* ======================================================================== */

struct reentry {
    wirelog_easy_session_t *s;
    wirelog_source_query_status_t count, key, validate, lookup;
    uint32_t calls;
};

static void
reenter_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    struct reentry *r = (struct reentry *)user_data;
    uint64_t n = 5;
    bool ex = true;
    int64_t id = 0;
    bool found = true;
    (void)relation;
    (void)ncols;
    (void)diff;
    r->calls++;
    r->count = wirelog_easy_source_count(r->s, "e", row, 1, ALL, &n);
    if (n != 0)
        r->count = (wirelog_source_query_status_t)99;
    r->key = wirelog_easy_source_key_exists(r->s, "e", row[0], ALL, &ex);
    if (ex)
        r->key = (wirelog_source_query_status_t)99;
    r->validate = wirelog_easy_source_validate(r->s, "e", 1);
    r->lookup = wirelog_easy_lookup_symbol(r->s, "x", &id, &found);
    if (found || id != -1)
        r->lookup = (wirelog_source_query_status_t)99;
}

static void
reenter_tuple(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    reenter_delta(relation, row, ncols, 1, user_data);
}

static void
test_easy_callback_reentry(void)
{
    wirelog_easy_session_t *s = open_easy(SRC);
    struct reentry r = { 0 };
    r.s = s;
    CHECK(wirelog_easy_set_delta_cb(s, reenter_delta, &r) == WIRELOG_OK,
        "cb");
    ins1(s, "e", 4);
    step(s);
    CHECK(r.calls > 0, "delta callback ran");
    CHECK(r.count == WIRELOG_SOURCE_QUERY_BUSY
        && r.key == WIRELOG_SOURCE_QUERY_BUSY
        && r.validate == WIRELOG_SOURCE_QUERY_BUSY
        && r.lookup == WIRELOG_SOURCE_QUERY_BUSY,
        "delta callback: %d %d %d %d", r.count, r.key, r.validate, r.lookup);
    CHECK(count1(s, "e", 4) == 1, "after delta callback");

    struct reentry t = { 0 };
    t.s = s;
    CHECK(wirelog_easy_snapshot(s, "h", reenter_tuple, &t) == WIRELOG_OK,
        "snapshot");
    CHECK(t.calls > 0 && t.count == WIRELOG_SOURCE_QUERY_BUSY
        && t.lookup == WIRELOG_SOURCE_QUERY_BUSY,
        "snapshot callback: %d %d", t.count, t.lookup);
    CHECK(wirelog_easy_set_delta_cb(s, NULL, NULL) == WIRELOG_OK, "clear");
    CHECK(count1(s, "e", 4) == 1, "after callback removal");
    wirelog_easy_close(s);
}

/* ======================================================================== */
/* Initialization failure                                                   */
/* ======================================================================== */

static void
test_init_failure_marker(void)
{
    for (int stage = 1; stage <= 3; stage++) {
        wirelog_easy_session_t *s = open_easy(SRC);
        /* A rejected argument does not reach initialization. */
        CHECK(wirelog_easy_insert(s, NULL, NULL, 1) != WIRELOG_OK, "args");
        CHECK(count1(s, "e", 1) == 0, "stage %d: pristine zero", stage);

        wirelog_easy_testhook_fail_next_init(stage);
        int64_t v = 1;
        CHECK(wirelog_easy_insert(s, "e", &v, 1) != WIRELOG_OK,
            "stage %d: injected failure", stage);
        CHECK(wirelog_easy_testhook_inner(s) == NULL,
            "stage %d: nothing published", stage);
        for (int i = 0; i < 3; i++) {
            uint64_t n = 9;
            CHECK(wirelog_easy_source_count(s, "e", &v, 1, ALL, &n)
                == WIRELOG_SOURCE_QUERY_INITIALIZATION_FAILED && n == 0,
                "stage %d: count refuses", stage);
            bool ex = true;
            CHECK(wirelog_easy_source_key_exists(s, "e", 1, ALL, &ex)
                == WIRELOG_SOURCE_QUERY_INITIALIZATION_FAILED && !ex,
                "stage %d: key refuses", stage);
            CHECK(wirelog_easy_source_validate(s, "e", 1)
                == WIRELOG_SOURCE_QUERY_INITIALIZATION_FAILED,
                "stage %d: validate refuses", stage);
            int64_t id = 3;
            bool found = true;
            CHECK(wirelog_easy_lookup_symbol(s, "alpha", &id, &found)
                == WIRELOG_SOURCE_QUERY_INITIALIZATION_FAILED && id == -1
                && !found, "stage %d: lookup refuses", stage);
        }
        /* Invalid arguments are still reported as such. */
        uint64_t n = 9;
        CHECK(wirelog_easy_source_count(s, "e", NULL, 1, ALL, &n)
            == WIRELOG_SOURCE_QUERY_INVALID_ARGUMENT, "args before marker");
        CHECK(wirelog_easy_testhook_inner(s) == NULL,
            "stage %d: inspection did not retry", stage);

        /* A normal retry builds, clears the marker, and the failed
         * insert left no phantom receipt. */
        ins1(s, "e", 1);
        CHECK(count1(s, "e", 1) == 1, "stage %d: exact after retry: %llu",
            stage, (unsigned long long)count1(s, "e", 1));
        step(s);
        CHECK(count1(s, "e", 1) == 1 && count1(s, "h", 1) == 0,
            "stage %d: after step", stage);
        wirelog_easy_close(s);
    }
}

static void
test_eager_open_failure(void)
{
    for (int stage = 1; stage <= 3; stage++) {
        wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
        opts.eager_build = true;
        wirelog_easy_session_t *s = (wirelog_easy_session_t *)&opts;
        wirelog_easy_testhook_fail_next_init(stage);
        CHECK(wirelog_easy_open_opts(SRC, &opts, &s) != WIRELOG_OK
            && s == NULL, "stage %d: eager open fails cleanly", stage);
    }
    wirelog_easy_open_opts_t opts = WIRELOG_EASY_OPEN_OPTS_INIT;
    opts.eager_build = true;
    wirelog_easy_session_t *s = NULL;
    CHECK(wirelog_easy_open_opts(SRC, &opts, &s) == WIRELOG_OK && s,
        "eager open");
    if (s) {
        CHECK(wirelog_easy_testhook_inner(s) != NULL, "eagerly built");
        CHECK(count1(s, "e", 1) == 0, "eager: host-only relation");
        CHECK(count1(s, "f", 1) == FAIL(WIRELOG_SOURCE_QUERY_UNSUPPORTED),
            "eager: fact relation");
        wirelog_easy_close(s);
    }
}

/* ======================================================================== */
/* Easy / advanced parity                                                   */
/* ======================================================================== */

static void
test_parity_with_advanced(void)
{
    wirelog_easy_session_t *es = open_easy(SRC);
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(SRC, &err);
    CHECK(prog != NULL, "parse");
    if (!prog) {
        wirelog_easy_close(es);
        return;
    }
    CHECK(wirelog_optimize(prog, &err), "optimize");
    wirelog_session_t *as = NULL;
    CHECK(wirelog_session_create(prog, WIRELOG_BACKEND_COLUMNAR, 1, &as)
        == WIRELOG_OK, "advanced create");
    if (!as) {
        wirelog_program_free(prog);
        wirelog_easy_close(es);
        return;
    }
    wirelog_easy_step(es);
    wirelog_session_step(as);
    const int64_t script[][3] = {
        /* op (1 insert, -1 remove, 0 step), relation (0 e, 1 h), value */
        { 1, 0, 3 }, { 1, 0, 3 }, { 1, 1, 3 }, { 0, 0, 0 }, { -1, 0, 3 },
        { 1, 1, 4 }, { -1, 1, 5 }, { 0, 0, 0 }, { -1, 1, 3 }, { 0, 0, 0 },
    };
    const char *names[2] = { "e", "h" };
    for (size_t i = 0; i < sizeof(script) / sizeof(script[0]); i++) {
        int64_t v = script[i][2];
        const char *rel = names[script[i][1]];
        if (script[i][0] == 1) {
            wirelog_easy_insert(es, rel, &v, 1);
            wirelog_session_insert(as, rel, &v, 1, 1);
        } else if (script[i][0] == -1) {
            wirelog_easy_remove(es, rel, &v, 1);
            wirelog_session_remove(as, rel, &v, 1, 1);
        } else {
            wirelog_easy_step(es);
            wirelog_session_step(as);
        }
        for (int r = 0; r < 2; r++)
            for (int64_t q = 2; q <= 5; q++) {
                uint64_t en = 7, an = 8;
                wirelog_source_query_status_t est = wirelog_easy_source_count(
                    es, names[r], &q, 1, ALL, &en);
                wirelog_source_query_status_t ast
                    = wirelog_session_source_count(as, names[r], &q, 1, ALL,
                        &an);
                CHECK(est == ast && en == an,
                    "step %zu %s(%lld): easy %d/%llu advanced %d/%llu", i,
                    names[r], (long long)q, est, (unsigned long long)en, ast,
                    (unsigned long long)an);
                bool ek = false, ak = true;
                est = wirelog_easy_source_key_exists(es, names[r], q, ALL,
                        &ek);
                ast = wirelog_session_source_key_exists(as, names[r], q, ALL,
                        &ak);
                CHECK(est == ast && ek == ak, "key parity step %zu", i);
            }
        uint64_t en = 7, an = 8;
        int64_t one = 1;
        CHECK(wirelog_easy_source_count(es, "f", &one, 1, ALL, &en)
            == wirelog_session_source_count(as, "f", &one, 1, ALL, &an),
            "fact relation parity");
    }
    wirelog_session_destroy(as);
    wirelog_program_free(prog);
    wirelog_easy_close(es);
}

/* ======================================================================== */
/* Large history: the query's auxiliary state does not grow                 */
/* ======================================================================== */

static void
measure_history(uint32_t rows, int64_t *heap_delta, uint64_t *reserve_delta,
    uint64_t *count_out, unsigned long *alloc_calls)
{
    wirelog_easy_session_t *s = open_easy(
        ".decl audit(k: int64, v: int64)\n");
    int64_t *batch = (int64_t *)malloc((size_t)rows * 2 * sizeof(int64_t));
    if (!batch) {
        fprintf(stderr, "batch alloc failed\n");
        exit(2);
    }
    /* Insert, step, and remove half of it again, so the history includes
     * evaluated rows, removals and duplicates. */
    for (uint32_t i = 0; i < rows; i++) {
        int64_t row[2] = { (int64_t)(i % 97), (int64_t)(i % 3) };
        CHECK(wirelog_easy_insert(s, "audit", row, 2) == WIRELOG_OK, "ins");
        batch[(size_t)2 * i] = row[0];
        batch[(size_t)2 * i + 1] = row[1];
    }
    step(s);
    for (uint32_t i = 0; i < rows / 2; i++)
        CHECK(wirelog_easy_remove(s, "audit", &batch[(size_t)2 * i], 2)
            == WIRELOG_OK, "rem");
    step(s);
    free(batch);

    wl_col_session_t *cs = COL_SESSION(wirelog_easy_testhook_inner(s));
    wl_columnar_memory_governor_t *g
        = wl_columnar_memory_governor_ref_get(cs->memory_governor);
    uint64_t reserved = wl_columnar_memory_reserved(g);
    int64_t q[2] = { 5, 2 };
    uint64_t n = 0;
#ifdef EASY_TEST_HAVE_MALLINFO2
    struct mallinfo2 before = mallinfo2();
#endif
#ifdef EASY_TEST_COUNT_ALLOCS
    unsigned long calls_before = easy_alloc_calls;
#endif
    wirelog_source_query_status_t st = wirelog_easy_source_count(s, "audit",
            q, 2, ALL, &n);
    bool ex = false;
    wirelog_source_query_status_t kst = wirelog_easy_source_key_exists(s,
            "audit", 1000, ALL, &ex);
#ifdef EASY_TEST_COUNT_ALLOCS
    *alloc_calls = easy_alloc_calls - calls_before;
#else
    *alloc_calls = 0;
#endif
#ifdef EASY_TEST_HAVE_MALLINFO2
    struct mallinfo2 after = mallinfo2();
    *heap_delta = (int64_t)after.uordblks - (int64_t)before.uordblks;
#else
    *heap_delta = 0;
#endif
    CHECK(st == WIRELOG_SOURCE_QUERY_OK && kst == WIRELOG_SOURCE_QUERY_OK
        && !ex, "queries over %u rows", rows);
    *reserve_delta = wl_columnar_memory_reserved(g) - reserved;
    *count_out = n;
    wirelog_easy_close(s);
}

static uint64_t
expected_count(uint32_t rows)
{
    /* Rows i with i % 97 == 5 and i % 3 == 2, inserted for every i and
     * removed again for i < rows / 2. */
    uint64_t n = 0;
    for (uint32_t i = rows / 2; i < rows; i++)
        if (i % 97 == 5 && i % 3 == 2)
            n++;
    return n;
}

static void
test_large_history_constant_query_state(void)
{
    const uint32_t scales[2] = { 2000, 40000 };
#ifdef EASY_TEST_HAVE_MALLINFO2
    /* glibc counts chunks parked in its tcache as in use, so with tcache
     * enabled a small allocation served from it would not show; meson sets
     * GLIBC_TUNABLES=glibc.malloc.tcache_count=0 for this test. */
    const char *tunables = getenv("GLIBC_TUNABLES");
    if (!tunables || !strstr(tunables, "glibc.malloc.tcache_count=0"))
        printf("note: tcache enabled; the heap delta below can miss a "
            "small allocation\n");
#else
    printf("note: no mallinfo2; heap bytes are not measured\n");
#endif
#ifndef EASY_TEST_COUNT_ALLOCS
    printf("note: allocation calls are not counted on this build\n");
#endif
    for (int i = 0; i < 2; i++) {
        int64_t heap = 1;
        uint64_t reserve = 1;
        uint64_t n = 0;
        unsigned long calls = 1;
        measure_history(scales[i], &heap, &reserve, &n, &calls);
        printf("history %u rows: query allocation calls %lu, heap delta "
            "%lld bytes, governed reservation delta %llu bytes, count %llu\n",
            scales[i], calls, (long long)heap, (unsigned long long)reserve,
            (unsigned long long)n);
        CHECK(calls == 0, "query allocation calls at %u rows: %lu",
            scales[i], calls);
        CHECK(n == expected_count(scales[i]), "exact count at %u: %llu vs %llu",
            scales[i], (unsigned long long)n,
            (unsigned long long)expected_count(scales[i]));
        CHECK(heap == 0, "query heap delta at %u rows: %lld", scales[i],
            (long long)heap);
        CHECK(reserve == 0, "governed reservation delta at %u rows: %llu",
            scales[i], (unsigned long long)reserve);
    }
}

int
main(void)
{
    test_lazy_inspection_does_not_build();
    test_easy_transitions();
    test_easy_symbols();
    test_easy_callback_reentry();
    test_init_failure_marker();
    test_eager_open_failure();
    test_parity_with_advanced();
    test_large_history_constant_query_state();
    if (failures) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("test_easy_source_inspection: all checks passed\n");
    return 0;
}
