/* Exact long-name delta identity in real raw and materialized consumers. */
#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#endif
#include "wirelog/columnar/internal.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/wirelog.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", \
                                      __FILE__, __LINE__, #x); abort(); \
                      } } while (0)
static void name_fill(char *name, size_t length) {
    memset(name, 'a', length); name[length] = '\0';
}
static col_rel_t *relation(const char *name, uint32_t width,
    const int64_t *rows, uint32_t count) {
    col_rel_t *r = col_rel_new_auto(name, width); CHECK(r);
    for (uint32_t i = 0; i < count; ++i)
        CHECK(!col_rel_append_row(r, rows + (size_t)i * width));
    return r;
}

static void hash_lookup(void) {
    char name[301], full[304], sibling[304], decoy[256], retract[304];
    name_fill(name, 300); snprintf(full, sizeof(full), "$d$%s", name);
    memcpy(sibling, full, sizeof(full)); sibling[302] = 'b';
    memcpy(decoy, full, 255); decoy[255] = '\0';
    snprintf(retract, sizeof(retract), "$r$%s", name);
    const int64_t row = 37;
    col_rel_t *registry[] = {NULL, relation("$", 1, &row, 1),
                             relation(decoy, 1, &row, 1),
                             relation(sibling, 1, &row, 1),
                             relation(full, 1, &row, 1),
                             relation(retract, 1, &row, 1),
                             relation("$d$a\xc3\xa9", 1, &row, 1)};
    wl_col_session_t s = {0}; s.rels = registry;
    s.nrels = s.rel_cap = sizeof(registry) / sizeof(registry[0]);
    CHECK(!s.rel_hash_nbuckets);
    CHECK(wl_columnar_session_find_prefixed(&s, "$d$", name) == registry[4]);
    CHECK(s.rel_hash_nbuckets && session_find_rel(&s, full) == registry[4]);
    CHECK(wl_columnar_session_hash_lookup_prefixed(&s, "$r$",
        name) == registry[5]);
    CHECK(wl_columnar_session_hash_lookup_prefixed(&s, "$d$",
        "a\xc3\xa9") == registry[6]);
    CHECK(!wl_columnar_session_find_prefixed(&s, "$d$", "absent"));
    /* A direct registry builder fills a previously NULL slot after the
     * index was built. This is a built-hash miss with a real live registry
     * entry, not a modified bucket or a natural session-registration claim. */
    col_rel_t *saved = registry[4]; registry[4] = NULL;
    CHECK(!session_rel_build_hash(&s)); registry[4] = saved;
    CHECK(!session_rel_hash_lookup(&s, full));
    CHECK(wl_columnar_session_find_prefixed(&s, "$d$", name) == saved);
    CHECK(!session_rel_build_hash(&s));
    CHECK(wl_columnar_session_hash_lookup_prefixed(&s, "$d$",
        name) == registry[4]);
    session_rel_free_hash(&s);
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = UINT64_C(64) << 20, .usable_bytes = UINT64_C(64) << 20,
            .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING
    };
    wl_columnar_memory_governor_ref_t *ref =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(ref); s.memory_governor = ref;
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve_checked(g, resolution.usable_bytes - 1,
        &peer)
        == WL_COLUMNAR_MEMORY_ADMISSION_OK);
    uint64_t denials = wl_columnar_memory_denials(g);
    CHECK(wl_columnar_session_find_prefixed(&s, "$d$", name) == registry[4]
        && !s.rel_hash_nbuckets && wl_columnar_memory_denials(g) == denials + 1
        && !s.memory_budget_denied);
    CHECK(wl_columnar_memory_rollback(&peer) &&
        !wl_columnar_memory_reserved(g));
    s.memory_governor = NULL; wl_columnar_memory_governor_ref_release(ref);
    for (unsigned i = 1; i < sizeof(registry)/sizeof(registry[0]); ++i)
        col_rel_destroy(registry[i]);
    puts(
        "exact prefixed lookup: lazy/built/hash-miss/holes/UTF8/siblings passed");
}

int
main(int argc, char **argv)
{
    CHECK(argc == 2 && !strcmp(argv[1], "hash"));
    hash_lookup();
    return 0;
}
