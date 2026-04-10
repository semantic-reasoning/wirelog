/*
 * snapshot_demo.c - Example 12: Snapshot vs Delta
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
 * Runs the same access-control Datalog program through both
 * wl_easy_step (delta callback, streaming) and wl_easy_snapshot
 * (one-shot batch) and verifies that they produce identical results.
 *
 * IMPORTANT: wl_easy_snapshot() is an evaluating call -- calling
 * wl_easy_step() followed by wl_easy_snapshot() on the same insert
 * batch would double-count derived tuples.  This driver therefore
 * uses two independent sessions: one for the delta path and one for
 * the snapshot path.
 *
 * Build: meson compile -C build snapshot_demo
 * Run:   ./build/examples/12-snapshot-vs-delta/snapshot_demo
 */

#include "wirelog/wl_easy.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *ACCESS_CONTROL_SRC =
    ".decl can(user: symbol, perm: symbol)\n"
    ".decl granted(user: symbol, perm: symbol)\n"
    "granted(U, P) :- can(U, P).\n";

/* ---- Symbol lookup table ---- */

typedef struct {
    int64_t id;
    const char *str;
} sym_entry_t;

#define MAX_SYMS 16

typedef struct {
    sym_entry_t entries[MAX_SYMS];
    int n;
} sym_table_t;

static void
sym_table_add(sym_table_t *t, int64_t id, const char *str)
{
    if (t->n >= MAX_SYMS) {
        fprintf(stderr, "sym_table overflow\n");
        abort();
    }
    t->entries[t->n].id = id;
    t->entries[t->n].str = str;
    t->n++;
}

static const char *
sym_table_lookup(const sym_table_t *t, int64_t id)
{
    for (int i = 0; i < t->n; i++) {
        if (t->entries[i].id == id)
            return t->entries[i].str;
    }
    return NULL;
}

/* ---- Result collector ---- */

#define MAX_RESULTS 16
#define RESULT_LEN 128

typedef struct {
    char rows[MAX_RESULTS][RESULT_LEN];
    int n;
    sym_table_t *syms;
} result_set_t;

static void
format_tuple(result_set_t *rs, const char *relation, const int64_t *row,
    uint32_t ncols)
{
    if (rs->n >= MAX_RESULTS) {
        fprintf(stderr, "result_set overflow\n");
        abort();
    }
    char *buf = rs->rows[rs->n];
    int off = snprintf(buf, RESULT_LEN, "%s(", relation);
    for (uint32_t i = 0; i < ncols; i++) {
        if (i > 0)
            off += snprintf(buf + off, RESULT_LEN - off, ", ");
        const char *str = sym_table_lookup(rs->syms, row[i]);
        if (!str) {
            fprintf(stderr,
                "reverse-intern miss for relation '%s' column %u "
                "id %" PRId64 "\n",
                relation, i, row[i]);
            abort();
        }
        off += snprintf(buf + off, RESULT_LEN - off, "\"%s\"", str);
    }
    snprintf(buf + off, RESULT_LEN - off, ")");
    rs->n++;
}

/* ---- Callbacks ---- */

static void
on_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    if (diff <= 0)
        return; /* only collect insertions */
    result_set_t *rs = (result_set_t *)user_data;
    format_tuple(rs, relation, row, ncols);
}

static void
on_snapshot(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    result_set_t *rs = (result_set_t *)user_data;
    format_tuple(rs, relation, row, ncols);
}

/* ---- Sorting ---- */

static int
row_compare(const void *a, const void *b)
{
    return strcmp((const char *)a, (const char *)b);
}

/* ---- Helpers ---- */

#define CHECK(expr, msg, sess) do { \
            if ((expr) != WIRELOG_OK) { \
                fprintf(stderr, "%s\n", msg); \
                wl_easy_close(sess); \
                return 1; \
            } \
} while (0)

static void
insert_facts(wl_easy_session_t *s, sym_table_t *syms)
{
    int64_t alice = wl_easy_intern(s, "alice");
    int64_t bob = wl_easy_intern(s, "bob");
    int64_t carol = wl_easy_intern(s, "carol");
    int64_t rd = wl_easy_intern(s, "read");
    int64_t wr = wl_easy_intern(s, "write");
    int64_t adm = wl_easy_intern(s, "admin");

    sym_table_add(syms, alice, "alice");
    sym_table_add(syms, bob,   "bob");
    sym_table_add(syms, carol, "carol");
    sym_table_add(syms, rd,    "read");
    sym_table_add(syms, wr,    "write");
    sym_table_add(syms, adm,   "admin");

    int64_t r1[] = { alice, rd  };
    int64_t r2[] = { alice, wr  };
    int64_t r3[] = { bob,   rd  };
    int64_t r4[] = { bob,   adm };
    int64_t r5[] = { carol, rd  };
    wl_easy_insert(s, "can", r1, 2);
    wl_easy_insert(s, "can", r2, 2);
    wl_easy_insert(s, "can", r3, 2);
    wl_easy_insert(s, "can", r4, 2);
    wl_easy_insert(s, "can", r5, 2);
}

int
main(void)
{
    printf("Example 12: Snapshot vs Delta\n");
    printf("=============================\n\n");

    /* ---- Path A: delta mode ---- */
    wl_easy_session_t *sd = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &sd) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_open (delta) failed\n");
        return 1;
    }

    sym_table_t delta_syms = { .n = 0 };
    result_set_t delta_rs = { .n = 0, .syms = &delta_syms };

    CHECK(wl_easy_set_delta_cb(sd, on_delta, &delta_rs),
        "wl_easy_set_delta_cb failed", sd);

    insert_facts(sd, &delta_syms);

    printf("=== delta mode: wl_easy_step ===\n");
    CHECK(wl_easy_step(sd), "wl_easy_step failed", sd);
    wl_easy_close(sd);

    /* ---- Path B: snapshot mode ---- */
    wl_easy_session_t *ss = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &ss) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_open (snapshot) failed\n");
        return 1;
    }

    sym_table_t snap_syms = { .n = 0 };
    result_set_t snap_rs = { .n = 0, .syms = &snap_syms };

    insert_facts(ss, &snap_syms);

    printf("=== snapshot mode: wl_easy_snapshot ===\n");
    CHECK(wl_easy_snapshot(ss, "granted", on_snapshot, &snap_rs),
        "wl_easy_snapshot failed", ss);
    wl_easy_close(ss);

    /* ---- Sort both result sets ---- */
    qsort(delta_rs.rows, (size_t)delta_rs.n, RESULT_LEN, row_compare);
    qsort(snap_rs.rows, (size_t)snap_rs.n, RESULT_LEN, row_compare);

    /* ---- Side-by-side comparison ---- */
    printf("\n");
    int max = delta_rs.n > snap_rs.n ? delta_rs.n : snap_rs.n;
    int pass = 1;
    for (int i = 0; i < max; i++) {
        const char *d = (i < delta_rs.n) ? delta_rs.rows[i] : "(none)";
        const char *s = (i < snap_rs.n) ? snap_rs.rows[i] : "(none)";
        int match = (strcmp(d, s) == 0);
        if (!match)
            pass = 0;
        printf("delta: %-30s  snapshot: %-30s  %s\n",
            d, s, match ? "MATCH" : "MISMATCH");
    }

    printf("\n%s\n", pass ? "PASS" : "FAIL");

    return pass ? 0 : 1;
}
