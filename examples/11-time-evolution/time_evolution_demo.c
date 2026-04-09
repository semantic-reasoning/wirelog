/*
 * time_evolution_demo.c - Example 11: Time Evolution (Per-Epoch Delta Isolation)
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
 * Demonstrates per-epoch delta isolation (Issue #444).  Each call to
 * wl_easy_step() is a discrete time epoch; the delta callback fires only
 * for tuples derived during that epoch, not since the beginning.
 *
 * Build: meson compile -C build time_evolution_demo
 * Run:   ./build/examples/11-time-evolution/time_evolution_demo
 */

#include "wirelog/wl_easy.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *EVENTS_SRC =
    ".decl event(id: symbol, kind: symbol)\n"
    ".decl alert(id: symbol)\n"
    "alert(ID) :- event(ID, \"error\").\n";

/* Per-step buffered delta for deterministic output ordering.
 * Deltas within a single step are not contractually ordered by the
 * columnar backend; buffer-and-sort makes the golden file stable. */
typedef struct {
    char relation[64];
    int64_t row[2];
    uint32_t ncols;
    int32_t diff;
} buffered_delta_t;

#define MAX_BUFFER 16

typedef struct {
    wl_easy_session_t *sess;
    buffered_delta_t buf[MAX_BUFFER];
    int n;
    int plus_alert_count;
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

    /* Count +alert deltas for per-epoch assertion. */
    if (diff > 0 && strcmp(relation, "alert") == 0)
        ctx->plus_alert_count++;

    if (ctx->n >= MAX_BUFFER) {
        fprintf(stderr, "delta buffer overflow\n");
        abort();
    }
    buffered_delta_t *b = &ctx->buf[ctx->n++];
    strncpy(b->relation, relation, sizeof(b->relation) - 1);
    b->relation[sizeof(b->relation) - 1] = '\0';
    b->ncols = ncols;
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
        wl_easy_print_delta(b->relation, b->row, b->ncols, b->diff, ctx->sess);
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
    printf("Example 11: Time Evolution (Per-Epoch Delta Isolation)\n");
    printf("=====================================================\n");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(EVENTS_SRC, &s) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_open failed\n");
        return 1;
    }

    demo_ctx_t ctx = { .sess = s, .n = 0, .plus_alert_count = 0 };
    CHECK(wl_easy_set_delta_cb(s, on_delta_wrapper, &ctx),
        "wl_easy_set_delta_cb failed");

    /* Pre-intern all symbols used in the demo. */
    (void)wl_easy_intern(s, "e1");
    (void)wl_easy_intern(s, "e2");
    (void)wl_easy_intern(s, "e3");
    (void)wl_easy_intern(s, "e4");
    (void)wl_easy_intern(s, "e5");
    (void)wl_easy_intern(s, "error");
    (void)wl_easy_intern(s, "info");

    /* ---- Epoch 1 ---- */
    wl_easy_banner("epoch 1: insert e1(error), e2(info), e3(error)");
    CHECK(wl_easy_insert_sym(s, "event", "e1", "error", NULL),
        "insert event(e1,error) failed");
    CHECK(wl_easy_insert_sym(s, "event", "e2", "info", NULL),
        "insert event(e2,info) failed");
    CHECK(wl_easy_insert_sym(s, "event", "e3", "error", NULL),
        "insert event(e3,error) failed");
    ctx.plus_alert_count = 0;
    CHECK(wl_easy_step(s), "step 1 failed");
    flush_sorted(&ctx);

    if (ctx.plus_alert_count != 2) {
        fprintf(stderr,
            "ASSERTION FAILED: expected 2 '+' alert deltas in epoch 1, "
            "got %d\n", ctx.plus_alert_count);
        wl_easy_close(s);
        return 2;
    }

    /* ---- Epoch 2 ---- */
    wl_easy_banner("epoch 2: insert e4(info)");
    CHECK(wl_easy_insert_sym(s, "event", "e4", "info", NULL),
        "insert event(e4,info) failed");
    ctx.plus_alert_count = 0;
    CHECK(wl_easy_step(s), "step 2 failed");
    flush_sorted(&ctx);

    if (ctx.plus_alert_count != 0) {
        fprintf(stderr,
            "ASSERTION FAILED: expected 0 '+' alert deltas in epoch 2, "
            "got %d\n", ctx.plus_alert_count);
        wl_easy_close(s);
        return 2;
    }

    /* ---- Epoch 3 ---- */
    wl_easy_banner("epoch 3: insert e5(error)");
    CHECK(wl_easy_insert_sym(s, "event", "e5", "error", NULL),
        "insert event(e5,error) failed");
    ctx.plus_alert_count = 0;
    CHECK(wl_easy_step(s), "step 3 failed");
    flush_sorted(&ctx);

    if (ctx.plus_alert_count != 1) {
        fprintf(stderr,
            "ASSERTION FAILED: expected 1 '+' alert delta in epoch 3, "
            "got %d\n", ctx.plus_alert_count);
        wl_easy_close(s);
        return 2;
    }

    printf("\nDone.\n");

    wl_easy_close(s);
    return 0;
}
