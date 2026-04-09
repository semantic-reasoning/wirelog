/*
 * retract_demo.c - Example 09: Retraction Basics (-1 deltas)
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
 * Demonstrates incremental retraction (Issue #442).  The driver inserts a
 * mutual friendship pair (observing two '+' mutual deltas), inserts a
 * one-way follow (no deltas), then retracts one side of the mutual pair
 * (observing a '-' mutual delta).
 *
 * Build: meson compile -C build retract_demo
 * Run:   ./build/examples/09-retraction-basics/retract_demo
 */

#include "wirelog/wl_easy.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *FRIENDSHIP_SRC =
    ".decl friend(a: symbol, b: symbol)\n"
    ".decl mutual(a: symbol, b: symbol)\n"
    "mutual(A, B) :- friend(A, B), friend(B, A).\n";

/* Per-step buffered delta for deterministic output ordering.
 * Deltas within a single step are not contractually ordered by the
 * columnar backend; buffer-and-sort makes the golden file stable. */
typedef struct {
    char relation[64];
    int64_t row[2];
    int32_t diff;
} buffered_delta_t;

#define MAX_BUFFER 16

typedef struct {
    wl_easy_session_t *sess;
    buffered_delta_t buf[MAX_BUFFER];
    int n;
    int minus_mutual_count;
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

    /* Count first so ordering/buffering can't mask the assertion. */
    if (diff < 0 && strcmp(relation, "mutual") == 0)
        ctx->minus_mutual_count++;

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

int
main(void)
{
    printf("Example 09: Retraction Basics (-1 deltas)\n");
    printf("=========================================\n");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(FRIENDSHIP_SRC, &s) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_open failed\n");
        return 1;
    }

    demo_ctx_t ctx = { .sess = s, .n = 0, .minus_mutual_count = 0 };
    CHECK(wl_easy_set_delta_cb(s, on_delta_wrapper, &ctx),
        "wl_easy_set_delta_cb failed");

    (void)wl_easy_intern(s, "alice");
    (void)wl_easy_intern(s, "bob");
    (void)wl_easy_intern(s, "carol");

    /* ---- Step 1 ---- */
    wl_easy_banner("step 1: alice <-> bob");
    CHECK(wl_easy_insert_sym(s, "friend", "alice", "bob", NULL),
        "insert friend(alice,bob) failed");
    CHECK(wl_easy_insert_sym(s, "friend", "bob", "alice", NULL),
        "insert friend(bob,alice) failed");
    CHECK(wl_easy_step(s), "step 1 failed");
    flush_sorted(&ctx);

    /* ---- Step 2 ---- */
    wl_easy_banner("step 2: one-way alice -> carol");
    CHECK(wl_easy_insert_sym(s, "friend", "alice", "carol", NULL),
        "insert friend(alice,carol) failed");
    CHECK(wl_easy_step(s), "step 2 failed");
    flush_sorted(&ctx);

    /* ---- Step 3 ---- */
    wl_easy_banner("step 3: bob unfriends alice");
    int before = ctx.minus_mutual_count;
    CHECK(wl_easy_remove_sym(s, "friend", "bob", "alice", NULL),
        "remove friend(bob,alice) failed");
    CHECK(wl_easy_step(s), "step 3 failed");
    flush_sorted(&ctx);
    int step3_minus = ctx.minus_mutual_count - before;

    printf("\nDone.\n");

    if (step3_minus != 2) {
        fprintf(stderr,
            "ASSERTION FAILED: expected 2 '-1' deltas on mutual in step 3, got %d\n",
            step3_minus);
        wl_easy_close(s);
        return 2;
    }

    wl_easy_close(s);
    return 0;
}
