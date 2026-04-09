/*
 * delta_demo.c - Example 08: Delta Queries with Callback API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Demonstrates the wl_easy facade over wirelog's delta-callback pipeline.
 * As can(user, permission) tuples are inserted, delta callbacks fire for
 * each newly derived granted() fact via wl_easy_print_delta().
 *
 * Build: meson compile -C build delta_demo
 * Run:   ./build/examples/08-delta-queries/delta_demo
 */

#include "wirelog/wl_easy.h"

#include <stdio.h>
#include <stdlib.h>

static const char *ACCESS_CONTROL_SRC
    = ".decl can(user: symbol, perm: symbol)\n"
    ".decl granted(user: symbol, perm: symbol)\n"
    "granted(U, P) :- can(U, P).\n";

int
main(void)
{
    printf("Example 08: Delta Queries with Callback API\n");
    printf("============================================\n\n");

    wl_easy_session_t *s = NULL;
    if (wl_easy_open(ACCESS_CONTROL_SRC, &s) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_open failed\n");
        return 1;
    }

    int64_t id_alice = wl_easy_intern(s, "alice");
    int64_t id_bob = wl_easy_intern(s, "bob");
    int64_t id_carol = wl_easy_intern(s, "carol");
    int64_t id_read = wl_easy_intern(s, "read");
    int64_t id_write = wl_easy_intern(s, "write");
    int64_t id_admin = wl_easy_intern(s, "admin");
    (void)id_alice; (void)id_bob; (void)id_carol;
    (void)id_read;  (void)id_write; (void)id_admin;

    if (wl_easy_set_delta_cb(s, wl_easy_print_delta, s) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_set_delta_cb failed\n");
        wl_easy_close(s);
        return 1;
    }

    printf("Inserting access control facts...\n\n");

    int64_t r1[] = { id_alice, id_read  };
    int64_t r2[] = { id_alice, id_write };
    int64_t r3[] = { id_bob,   id_read  };
    int64_t r4[] = { id_bob,   id_admin };
    int64_t r5[] = { id_carol, id_read  };
    wl_easy_insert(s, "can", r1, 2);
    wl_easy_insert(s, "can", r2, 2);
    wl_easy_insert(s, "can", r3, 2);
    wl_easy_insert(s, "can", r4, 2);
    wl_easy_insert(s, "can", r5, 2);

    printf("Delta output from wl_session_step():\n");
    if (wl_easy_step(s) != WIRELOG_OK) {
        fprintf(stderr, "wl_easy_step failed\n");
        wl_easy_close(s);
        return 1;
    }

    printf("\nDone.\n");
    wl_easy_close(s);
    return 0;
}
