/*
 * test_wide_relation.c - relations wider than COL_STACK_MAX (Issue #1000)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * The columnar operators copy a row into a fixed
 * `int64_t buf[COL_STACK_MAX]` (32) and then fill it at the *relation's*
 * width via col_rel_row_copy_out().  Any relation wider than 32 columns
 * therefore writes past the end of the buffer: `stack-buffer-overflow`
 * under ASan and `*** stack smashing detected ***` (SIGABRT, rc 134) in a
 * plain release build, so a sanitizer-only test would miss a regression in
 * the shipping configuration.
 *
 * These tests drive the real pipeline (parse -> optimize -> plan ->
 * evaluate) through wl_run_pipeline, one rule shape per operator kind, and
 * assert the *correct tuples* rather than merely a clean exit.  Every shape
 * runs at three widths with the same expected answer:
 *
 *   32  - the boundary that is correct today (positive control)
 *   33  - one past the boundary (the reported reproducer)
 *   200 - well past it, to catch an off-by-one in the new bound
 *
 * Note that the effective limit is the width of the *intermediate*
 * relation, not the declared width: an unprojected join materialises
 * lhs->ncols + rhs->ncols columns, so a join chain overflows below the
 * declared 32-column boundary.
 */

#include "../wirelog/cli/driver.h"

#include "test_tmpdir.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;
static int tests_skipped = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

#define SKIP(msg)                         \
        do {                                  \
            tests_skipped++;                  \
            printf(" ... SKIP: %s\n", (msg)); \
        } while (0)

/* --- growable string buffer for generating wide Datalog sources ---------- */

typedef struct {
    char *data;
    size_t len;
    size_t cap;
    int oom;
} sbuf_t;

static void
sb_init(sbuf_t *s)
{
    s->data = NULL;
    s->len = 0;
    s->cap = 0;
    s->oom = 0;
}

static void
sb_free(sbuf_t *s)
{
    free(s->data);
    sb_init(s);
}

static void
sb_reserve(sbuf_t *s, size_t extra)
{
    if (s->oom)
        return;
    if (s->len + extra + 1 <= s->cap)
        return;
    size_t cap = s->cap ? s->cap : 1024;
    while (cap < s->len + extra + 1)
        cap *= 2;
    char *p = (char *)realloc(s->data, cap);
    if (!p) {
        s->oom = 1;
        return;
    }
    s->data = p;
    s->cap = cap;
}

static void
sb_puts(sbuf_t *s, const char *text)
{
    size_t n = strlen(text);
    sb_reserve(s, n);
    if (s->oom)
        return;
    memcpy(s->data + s->len, text, n);
    s->len += n;
    s->data[s->len] = '\0';
}

static void
sb_printf(sbuf_t *s, const char *fmt, ...)
{
    char tmp[512];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        s->oom = 1;
        return;
    }
    sb_puts(s, tmp);
}

/* ".decl name(c0: int64, ..., c{n-1}: int64)\n" */
static void
sb_decl(sbuf_t *s, const char *name, unsigned n)
{
    sb_printf(s, ".decl %s(", name);
    for (unsigned i = 0; i < n; i++)
        sb_printf(s, "%sc%u: int64", i ? ", " : "", i);
    sb_puts(s, ")\n");
}

/* "name(base, base+1, ..., base+n-1).\n" */
static void
sb_fact_seq(sbuf_t *s, const char *name, long base, unsigned n)
{
    sb_printf(s, "%s(", name);
    for (unsigned i = 0; i < n; i++)
        sb_printf(s, "%s%ld", i ? ", " : "", base + (long)i);
    sb_puts(s, ").\n");
}

/* "name(v0, v1, pad, pad, ..., pad).\n" */
static void
sb_fact_edge(sbuf_t *s, const char *name, long v0, long v1, unsigned n)
{
    sb_printf(s, "%s(%ld, %ld", name, v0, v1);
    for (unsigned i = 2; i < n; i++)
        sb_puts(s, ", 0");
    sb_puts(s, ").\n");
}

/*
 * Emit an argument list of n terms: "<prefix>0, <prefix>1, ...", with the
 * entries listed in @over ("index=term" pairs, terminated by index == n)
 * substituted.  Used to build wide rule bodies and heads.
 */
typedef struct {
    unsigned index;
    const char *term;
} sb_override_t;

static void
sb_args(sbuf_t *s, const char *prefix, unsigned n, const sb_override_t *over,
    unsigned over_count)
{
    for (unsigned i = 0; i < n; i++) {
        const char *sub = NULL;
        for (unsigned k = 0; k < over_count; k++) {
            if (over[k].index == i) {
                sub = over[k].term;
                break;
            }
        }
        if (i)
            sb_puts(s, ", ");
        if (sub)
            sb_puts(s, sub);
        else
            sb_printf(s, "%s%u", prefix, i);
    }
}

/*
 * Run the full pipeline on @src, capturing printed tuples into a temp file.
 * On success (*out_text != NULL) the caller must free(*out_text).
 *
 * The pre-fix failure is a SIGABRT from the stack protector, which would
 * take the whole test binary down and hide every later case.  Where fork()
 * is available the pipeline therefore runs in a child process, so a crash
 * is reported as a failure of that one case (rc 128 + signal) and the
 * remaining shapes still run.
 *
 * Returns 0 on success, 128 + signal if the child died on a signal, the
 * pipeline's non-zero rc otherwise, or -999 on capture-file failure.
 */
static int
run_pipeline_capture(const char *src, char **out_text)
{
    /* pid-suffixed: a fixed name collides when two copies of the suite run
     * concurrently, which reproducibly failed most of the 18 pipeline cases
     * when eight were run at once. */
    char out_name[64];
    snprintf(out_name, sizeof(out_name), "wl_wide_rel_out_%ld.txt",
        (long)getpid());
    char out_path[512];
    test_tmppath(out_path, sizeof(out_path), out_name);

    *out_text = NULL;

#ifndef _WIN32
    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0)
        return -999;
    if (pid == 0) {
        FILE *f = fopen(out_path, "w");
        if (!f)
            _exit(99);
        int crc = wl_run_pipeline(src, 1, false, false, 0, f);
        fclose(f);
        /* exit(), not _exit(): _exit bypasses atexit, so LeakSanitizer
         * never ran for any of the pipeline cases -- the clean
         * detect_leaks=1 result came entirely from the other tests. */
        exit(crc == 0 ? 0 : 1);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
        return -999;
    int rc;
    if (WIFSIGNALED(status))
        rc = 128 + WTERMSIG(status);
    else
        rc = WIFEXITED(status) ? WEXITSTATUS(status) : -999;
#else
    FILE *f = fopen(out_path, "w");
    if (!f)
        return -999;
    int rc = wl_run_pipeline(src, 1, false, false, 0, f);
    fclose(f);
#endif

    *out_text = wl_read_file(out_path);
    remove(out_path);
    return rc;
}

static int
count_substr(const char *hay, const char *needle)
{
    int count = 0;
    const char *p = hay;
    while (p && (p = strstr(p, needle)) != NULL) {
        count++;
        p++;
    }
    return count;
}

/*
 * Shared checker: run @src and require that @prefix appears exactly
 * @expect_count times and that every string in @expected appears exactly
 * once.  Returns 0 on success; on failure fills @msg and returns -1.
 */
static int
check_program(const char *src, const char *prefix, int expect_count,
    const char *const *expected, size_t expected_count, char *msg,
    size_t msg_size)
{
    char *output = NULL;
    int rc = run_pipeline_capture(src, &output);

    if (rc != 0) {
        snprintf(msg, msg_size,
            "wl_run_pipeline returned %d (134/SIGABRT = stack smashing)", rc);
        free(output);
        return -1;
    }
    if (!output) {
        snprintf(msg, msg_size, "no output captured");
        return -1;
    }

    int got = count_substr(output, prefix);
    if (got != expect_count) {
        snprintf(msg, msg_size, "expected %d '%s' tuples, got %d:\n%.800s",
            expect_count, prefix, got, output);
        free(output);
        return -1;
    }
    for (size_t i = 0; i < expected_count; i++) {
        if (count_substr(output, expected[i]) != 1) {
            snprintf(msg, msg_size, "missing/duplicated tuple '%s' in:\n%.800s",
                expected[i], output);
            free(output);
            return -1;
        }
    }
    free(output);
    return 0;
}

/* Widths every shape is exercised at: boundary, boundary+1, far past. */
static const unsigned k_widths[] = { 32u, 33u, 200u };
#define K_WIDTH_COUNT (sizeof(k_widths) / sizeof(k_widths[0]))

/* ======================================================================== */
/* Shape 1: MAP  (col_op_map)                                               */
/* ======================================================================== */

/*
 *   .decl w(c0 .. c{n-1})
 *   w(0, 1, .., n-1).  w(100, 101, .., 100+n-1).
 *   o(v0) :- w(v0, .., v{n-1}).
 *
 * Reaches the per-row buffer in col_op_map -- the reported reproducer.
 */
static void
build_map_src(sbuf_t *s, unsigned n)
{
    sb_decl(s, "w", n);
    sb_puts(s, ".decl o(x: int64)\n.output o\n");
    sb_fact_seq(s, "w", 0, n);
    sb_fact_seq(s, "w", 100, n);
    sb_puts(s, "o(v0) :- w(");
    sb_args(s, "v", n, NULL, 0);
    sb_puts(s, ").\n");
}

static void
test_map_wide(void)
{
    static const char *const expected[] = { "o(0)", "o(100)" };

    for (size_t i = 0; i < K_WIDTH_COUNT; i++) {
        unsigned n = k_widths[i];
        char name[128];
        snprintf(name, sizeof(name), "MAP over %u-column relation", n);
        TEST(name);

        sbuf_t s;
        sb_init(&s);
        build_map_src(&s, n);
        if (s.oom) {
            sb_free(&s);
            FAIL("source generation OOM");
            continue;
        }

        char msg[1024];
        if (check_program(s.data, "o(", 2, expected, 2, msg, sizeof(msg)) != 0)
            FAIL(msg);
        else
            PASS();
        sb_free(&s);
    }
}

/* ======================================================================== */
/* Shape 2: FILTER slow path  (col_op_filter, compiled-expression branch)   */
/* ======================================================================== */

/*
 *   o(v0) :- w(v0, .., v{n-1}), v1 + v2 > 100.
 *
 * `v1 + v2 > 100` is not a simple column-vs-constant compare, so the
 * fast path is skipped and the per-row buffer in the slow path is used.
 */
static void
build_filter_src(sbuf_t *s, unsigned n)
{
    sb_decl(s, "w", n);
    sb_puts(s, ".decl o(x: int64)\n.output o\n");
    sb_fact_seq(s, "w", 0, n);
    sb_fact_seq(s, "w", 100, n);
    sb_puts(s, "o(v0) :- w(");
    sb_args(s, "v", n, NULL, 0);
    sb_puts(s, "), v1 + v2 > 100.\n");
}

static void
test_filter_wide(void)
{
    static const char *const expected[] = { "o(100)" };

    for (size_t i = 0; i < K_WIDTH_COUNT; i++) {
        unsigned n = k_widths[i];
        char name[128];
        snprintf(name, sizeof(name), "FILTER (slow path) over %u columns", n);
        TEST(name);

        sbuf_t s;
        sb_init(&s);
        build_filter_src(&s, n);
        if (s.oom) {
            sb_free(&s);
            FAIL("source generation OOM");
            continue;
        }

        char msg[1024];
        if (check_program(s.data, "o(", 1, expected, 1, msg, sizeof(msg)) != 0)
            FAIL(msg);
        else
            PASS();
        sb_free(&s);
    }
}

/* ======================================================================== */
/* Shapes 3 & 4: JOIN right-child filter  (fill_filtered_rel)              */
/* ======================================================================== */

/*
 *   o(a, b) :- s(a), w(a, 1, b, v3, .., v{n-1}).           [const_count 1]
 *   o(a, b) :- s(a), w(a, 1, b, 3, v4, .., v{n-1}).        [const_count 2]
 *
 * Constants inside the join's right atom are collected into
 * op->right_filter_expr, which fill_filtered_rel applies over the *wide*
 * right relation.  One constant is a simple compare (fast path); two
 * constants form a conjunction and take the compiled-expression path.
 */
static void
build_join_const_src(sbuf_t *s, unsigned n, unsigned const_count)
{
    sb_override_t over[4];
    unsigned over_count = 0;
    over[over_count].index = 0;
    over[over_count++].term = "a";
    over[over_count].index = 1;
    over[over_count++].term = "1";
    over[over_count].index = 2;
    over[over_count++].term = "b";
    if (const_count > 1) {
        over[over_count].index = 3;
        over[over_count++].term = "3";
    }

    sb_puts(s, ".decl s(x: int64)\n");
    sb_decl(s, "w", n);
    sb_puts(s, ".decl o(x: int64, y: int64)\n.output o\n");
    sb_puts(s, "s(0). s(100).\n");
    sb_fact_seq(s, "w", 0, n);
    sb_fact_seq(s, "w", 100, n);
    sb_puts(s, "o(a, b) :- s(a), w(");
    sb_args(s, "v", n, over, over_count);
    sb_puts(s, ").\n");
}

static void
test_join_right_filter_wide(void)
{
    static const char *const expected[] = { "o(0, 2)" };

    for (unsigned consts = 1; consts <= 2; consts++) {
        for (size_t i = 0; i < K_WIDTH_COUNT; i++) {
            unsigned n = k_widths[i];
            char name[160];
            snprintf(name, sizeof(name),
                "JOIN right-child filter (%s path) over %u columns",
                consts == 1 ? "fast" : "slow", n);
            TEST(name);

            sbuf_t s;
            sb_init(&s);
            build_join_const_src(&s, n, consts);
            if (s.oom) {
                sb_free(&s);
                FAIL("source generation OOM");
                continue;
            }

            char msg[1024];
            if (check_program(s.data, "o(", 1, expected, 1, msg,
                sizeof(msg)) != 0)
                FAIL(msg);
            else
                PASS();
            sb_free(&s);
        }
    }
}

/* ======================================================================== */
/* Shape 5: REDUCE  (col_op_reduce)                                         */
/* ======================================================================== */

/*
 *   w(0, 1, 2, .., n-1).  w(0, 5, 2, .., n-1).  w(7, 9, 2, .., n-1).
 *   o(g, sum(v1)) :- w(g, v1, .., v{n-1}).
 *
 * Group-by on column 0 with a per-row buffer over the full input width.
 */
static void
build_reduce_src(sbuf_t *s, unsigned n)
{
    sb_override_t over[1] = { { 0u, "g" } };

    sb_decl(s, "w", n);
    sb_puts(s, ".decl o(g: int64, t: int64)\n.output o\n");
    for (unsigned r = 0; r < 3; r++) {
        static const long head[3][2] = { { 0, 1 }, { 0, 5 }, { 7, 9 } };
        sb_printf(s, "w(%ld, %ld", head[r][0], head[r][1]);
        for (unsigned i = 2; i < n; i++)
            sb_printf(s, ", %u", i);
        sb_puts(s, ").\n");
    }
    sb_puts(s, "o(g, sum(v1)) :- w(");
    sb_args(s, "v", n, over, 1);
    sb_puts(s, ").\n");
}

static void
test_reduce_wide(void)
{
    static const char *const expected[] = { "o(0, 6)", "o(7, 9)" };

    for (size_t i = 0; i < K_WIDTH_COUNT; i++) {
        unsigned n = k_widths[i];
        char name[128];
        snprintf(name, sizeof(name), "REDUCE (sum group-by) over %u columns",
            n);
        TEST(name);

        sbuf_t s;
        sb_init(&s);
        build_reduce_src(&s, n);
        if (s.oom) {
            sb_free(&s);
            FAIL("source generation OOM");
            continue;
        }

        char msg[1024];
        if (check_program(s.data, "o(", 2, expected, 2, msg, sizeof(msg)) != 0)
            FAIL(msg);
        else
            PASS();
        sb_free(&s);
    }
}

/* ======================================================================== */
/* Shapes 6 & 7: recursive self-join -> K-fusion merge  (col_rel_merge_k)   */
/* ======================================================================== */

/*
 * Two recursive atoms  -> K-fusion with K = 2 -> col_rel_merge_k(k == 2).
 * Three recursive atoms -> K-fusion with K = 3 -> col_rel_merge_k(k >= 3).
 *
 * Both paths carry a `last_row_buf[COL_STACK_MAX]` dedup buffer that is
 * filled at the relation's width.  Edges 1->2->3->4 are padded with zeros
 * in columns 2..n-1, so the answer does not depend on the width.
 */
static void
build_tc_src(sbuf_t *s, unsigned n, unsigned atoms)
{
    sb_override_t head_over[1] = { { 1u, "z" } };
    sb_override_t mid_over[2];
    sb_override_t tail_over[2];

    sb_decl(s, "e", n);
    sb_decl(s, "t", n);
    sb_puts(s, ".output t\n");
    sb_fact_edge(s, "e", 1, 2, n);
    sb_fact_edge(s, "e", 2, 3, n);
    sb_fact_edge(s, "e", 3, 4, n);

    sb_puts(s, "t(");
    sb_args(s, "v", n, NULL, 0);
    sb_puts(s, ") :- e(");
    sb_args(s, "v", n, NULL, 0);
    sb_puts(s, ").\n");

    sb_puts(s, "t(");
    sb_args(s, "v", n, head_over, 1);
    sb_puts(s, ") :- t(");
    sb_args(s, "v", n, NULL, 0);
    sb_puts(s, ")");

    if (atoms == 2) {
        /* t(v0, z, ..) :- t(v0, v1, ..), t(v1, z, ..). */
        mid_over[0].index = 0;
        mid_over[0].term = "v1";
        mid_over[1].index = 1;
        mid_over[1].term = "z";
        sb_puts(s, ", t(");
        sb_args(s, "w", n, mid_over, 2);
        sb_puts(s, ")");
    } else {
        /* t(v0, z, ..) :- t(v0, v1, ..), t(v1, y, ..), t(y, z, ..). */
        mid_over[0].index = 0;
        mid_over[0].term = "v1";
        mid_over[1].index = 1;
        mid_over[1].term = "y";
        sb_puts(s, ", t(");
        sb_args(s, "w", n, mid_over, 2);
        sb_puts(s, ")");

        tail_over[0].index = 0;
        tail_over[0].term = "y";
        tail_over[1].index = 1;
        tail_over[1].term = "z";
        sb_puts(s, ", t(");
        sb_args(s, "x", n, tail_over, 2);
        sb_puts(s, ")");
    }
    sb_puts(s, ".\n");
}

/* "t(a, b, 0, 0, ..., 0)" as printed by the CLI, for exact-tuple matching. */
static void
build_expected_tuple(char *dst, size_t dst_size, long a, long b, unsigned n)
{
    size_t off = (size_t)snprintf(dst, dst_size, "t(%ld, %ld", a, b);
    for (unsigned i = 2; i < n && off + 4 < dst_size; i++)
        off += (size_t)snprintf(dst + off, dst_size - off, ", 0");
    snprintf(dst + off, dst_size - off, ")");
}

static void
test_recursive_selfjoin_wide(void)
{
    /* 2 atoms: full transitive closure of 1->2->3->4 (6 pairs).
     * 3 atoms: base edges plus the single 3-hop composition 1->4. */
    static const long pairs2[6][2]
        = { { 1, 2 }, { 1, 3 }, { 1, 4 }, { 2, 3 }, { 2, 4 }, { 3, 4 } };
    static const long pairs3[4][2]
        = { { 1, 2 }, { 1, 4 }, { 2, 3 }, { 3, 4 } };

    for (unsigned atoms = 2; atoms <= 3; atoms++) {
        for (size_t i = 0; i < K_WIDTH_COUNT; i++) {
            unsigned n = k_widths[i];
            char name[160];
            snprintf(name, sizeof(name),
                "recursive %u-atom self-join (K-fusion merge) over %u columns",
                atoms, n);
            TEST(name);

            sbuf_t s;
            sb_init(&s);
            build_tc_src(&s, n, atoms);
            if (s.oom) {
                sb_free(&s);
                FAIL("source generation OOM");
                continue;
            }

            unsigned count = (atoms == 2) ? 6u : 4u;
            char *tuples = (char *)malloc((size_t)count * 1024u);
            const char *ptrs[6];
            if (!tuples) {
                sb_free(&s);
                FAIL("tuple buffer OOM");
                continue;
            }
            for (unsigned k = 0; k < count; k++) {
                long a = (atoms == 2) ? pairs2[k][0] : pairs3[k][0];
                long b = (atoms == 2) ? pairs2[k][1] : pairs3[k][1];
                build_expected_tuple(tuples + (size_t)k * 1024u, 1024u, a, b,
                    n);
                ptrs[k] = tuples + (size_t)k * 1024u;
            }

            char msg[1024];
            if (check_program(s.data, "t(", (int)count, ptrs, count, msg,
                sizeof(msg)) != 0)
                FAIL(msg);
            else
                PASS();
            free(tuples);
            sb_free(&s);
        }
    }
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_wide_relation (Issue #1000) ===\n");

    test_map_wide();
    test_filter_wide();
    test_join_right_filter_wide();
    test_reduce_wide();
    test_recursive_selfjoin_wide();

    printf("\n=== Results: %d run, %d passed, %d failed, %d skipped ===\n",
        tests_run, tests_passed, tests_failed, tests_skipped);
    return tests_failed > 0 ? 1 : 0;
}
