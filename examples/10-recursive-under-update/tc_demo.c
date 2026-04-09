/*
 * tc_demo.c - Example 10: Recursive Under Update (Transitive Closure)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 *
 * Demonstrates incremental maintenance of a RECURSIVE Datalog rule under
 * insert/remove/re-insert (Issue #443).  A 4-node chain (a->b->c->d) is
 * built, then edge(b,c) is removed (retracting derived reach tuples) and
 * re-inserted (re-deriving them).
 *
 * Build: meson compile -C build tc_demo
 * Run:   ./build/examples/10-recursive-under-update/tc_demo
 */

#include "wirelog/wl_easy.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *TC_SRC =
    ".decl edge(x: symbol, y: symbol)\n"
    ".decl reach(x: symbol, y: symbol)\n"
    "reach(X, Y) :- edge(X, Y).\n"
    "reach(X, Z) :- reach(X, Y), edge(Y, Z).\n";

/* Per-step buffered delta for deterministic output ordering.
 * Deltas within a single step are not contractually ordered by the
 * columnar backend; buffer-and-sort makes the golden file stable. */
typedef struct {
    char relation[64];
    int64_t row[2];
    int32_t diff;
} buffered_delta_t;

#define MAX_BUFFER 32

typedef struct {
    wl_easy_session_t *sess;
    buffered_delta_t buf[MAX_BUFFER];
    int n;
    int plus_reach;
    int minus_reach;
} demo_ctx_t;

static int
delta_compare(const void *a, const void *b)
{
    const buffered_delta_t *da = a;
    const buffered_delta_t *db = b;
    /* Sort by sign (positive before negative), then relation, then row IDs. */
    if (da->diff != db->diff)
        return (da->diff < db->diff) - (da->diff > db->diff);
    int c = strcmp(da->relation, db->relation);
    if (c != 0)
        return c;
    if (da->row[0] != db->row[0])
        return (da->row[0] > db->row[0]) - (da->row[0] < db->row[0]);
    return (da->row[1] > db->row[1]) - (da->row[1] < db->row[1]);
}

static void
on_delta_wrapper(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    demo_ctx_t *ctx = (demo_ctx_t *)user_data;

    /* Count reach deltas for in-driver assertions. */
    if (strcmp(relation, "reach") == 0) {
        if (diff > 0)
            ctx->plus_reach++;
        else if (diff < 0)
            ctx->minus_reach++;
    }

    if (ctx->n >= MAX_BUFFER) {
        fprintf(stderr, "delta buffer overflow\n");
        abort();
    }
    buffered_delta_t *b = &ctx->buf[ctx->n++];
    strncpy(b->relation, relation, sizeof(b->relation) - 1);
    b->relation[sizeof(b->relation) - 1] = '\0';
    b->row[0] = (ncols > 0) ? row[0] : 0;
    b->row[1] = (ncols > 1) ? row[1] : 0;
    b->diff = diff;
}

static void
flush_sorted(demo_ctx_t *ctx)
{
    if (ctx->n == 0)
        return;
    qsort(ctx->buf, (size_t)ctx->n, sizeof(ctx->buf[0]), delta_compare);
    for (int i = 0; i < ctx->n; i++) {
        buffered_delta_t *b = &ctx->buf[i];
        wl_easy_print_delta(b->relation, b->row, 2, b->diff, ctx->sess);
    }
    ctx->n = 0;
}

#define CHECK(expr, msg) do { \
            if ((expr) != WIRELOG_OK) { \
                fprintf(stderr, "%s\n", msg); \
                wl_easy_close(s); \
                return 1; \
            } \
} while (0)

#define ASSERT_DELTAS(label, actual, expected) do { \
            if ((actual) != (expected)) { \
                fprintf(stderr, \
                    "ASSERTION FAILED: %s: expected %d, got %d\n", \
                    label, expected, actual); \
                wl_easy_close(s); \
                return 2; \
            } \
} while (0)

int
main(void)
{
    printf("Example 10: Recursive Under Update (Transitive Closure)\n");
    printf("=======================================================\n");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(TC_SRC, &s) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_open failed\n");
        return 1;
    }

    demo_ctx_t ctx = { .sess = s, .n = 0, .plus_reach = 0, .minus_reach = 0 };
    CHECK(wl_easy_set_delta_cb(s, on_delta_wrapper, &ctx),
        "wl_easy_set_delta_cb failed");

    /* Pre-intern symbols for deterministic intern IDs. */
    (void)wl_easy_intern(s, "a");
    (void)wl_easy_intern(s, "b");
    (void)wl_easy_intern(s, "c");
    (void)wl_easy_intern(s, "d");

    /* ---- Step 1: Insert edges a->b->c->d ---- */
    wl_easy_banner("step 1: insert edges a->b->c->d");
    CHECK(wl_easy_insert_sym(s, "edge", "a", "b", NULL),
        "insert edge(a,b) failed");
    CHECK(wl_easy_insert_sym(s, "edge", "b", "c", NULL),
        "insert edge(b,c) failed");
    CHECK(wl_easy_insert_sym(s, "edge", "c", "d", NULL),
        "insert edge(c,d) failed");
    CHECK(wl_easy_step(s), "step 1 failed");
    flush_sorted(&ctx);
    ASSERT_DELTAS("step 1 +reach", ctx.plus_reach, 6);

    /* ---- Step 2: Remove edge(b,c) ---- */
    ctx.plus_reach = 0;
    ctx.minus_reach = 0;
    wl_easy_banner("step 2: remove edge(b,c)");
    CHECK(wl_easy_remove_sym(s, "edge", "b", "c", NULL),
        "remove edge(b,c) failed");
    CHECK(wl_easy_step(s), "step 2 failed");
    flush_sorted(&ctx);
    ASSERT_DELTAS("step 2 -reach", ctx.minus_reach, 4);

    /* ---- Step 3: Re-insert edge(b,c) ---- */
    ctx.plus_reach = 0;
    ctx.minus_reach = 0;
    wl_easy_banner("step 3: re-insert edge(b,c)");
    CHECK(wl_easy_insert_sym(s, "edge", "b", "c", NULL),
        "re-insert edge(b,c) failed");
    CHECK(wl_easy_step(s), "step 3 failed");
    flush_sorted(&ctx);
    ASSERT_DELTAS("step 3 +reach", ctx.plus_reach, 4);

    printf("\nDone.\n");

    wl_easy_close(s);
    return 0;
}
