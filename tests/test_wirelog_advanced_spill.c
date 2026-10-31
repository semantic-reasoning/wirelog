/* Public-only #1372 host integration against the production shared library. */
#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#endif
#include "wirelog/wirelog-advanced.h"
#include "test_tmpdir.h"
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#else
#include <dirent.h>
#include <unistd.h>
#endif

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, \
                                      __LINE__,#x); abort(); } } while (0)
static char directory[1024];
static const char *query =
    ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
    ".decl result(k: int64, x: int64, y: int64)\n"
    "result(k,x,y + 0) :- a(k,x), b(k,y).\n";
static wirelog_program_t *parse(const char *text)
{
    wirelog_error_t error;
    wirelog_program_t *program = wirelog_parse_string(text, &error);
    if (!program) fprintf(stderr, "parse error %d: %s\n", (int)error, text);
    CHECK(program && error == WIRELOG_OK);
    return program;
}
static wirelog_session_options_v1_t configured(void)
{
    wirelog_session_options_v1_t o = {0};
    o.struct_size = sizeof(o); o.abi_version = 1;
    o.spill_directory = directory; o.spill_quota_bytes = 1024*1024;
    o.spill_max_row_bytes = 64; o.spill_join_batch_bytes = 96;
    o.spill_join_index_bytes = 1024;
    return o;
}
static void environment(const char *name, const char *value)
{
#ifdef _WIN32
    CHECK(_putenv_s(name, value ? value : "") == 0);
#else
    CHECK(value ? setenv(name, value, 1) == 0 : unsetenv(name) == 0);
#endif
}
static char *copy_environment(const char *name)
{
    const char *value = getenv(name);
    if (!value) return NULL;
    char *copy = malloc(strlen(value)+1); CHECK(copy);
    memcpy(copy, value, strlen(value)+1); return copy;
}
/* POSIX files are immediately unlinked. Linux can additionally prove that
 * anonymous descriptors scoped to this test's unique directory are closed. */
static unsigned live_files(void)
{
    unsigned count = 0;
#ifdef _WIN32
    char pattern[1100]; snprintf(pattern, sizeof(pattern), "%s/*", directory);
    WIN32_FIND_DATAA entry;
    HANDLE find = FindFirstFileA(pattern, &entry);
    if (find == INVALID_HANDLE_VALUE) {
        CHECK(GetLastError() == ERROR_FILE_NOT_FOUND); return 0;
    }
    do {
        if (strcmp(entry.cFileName, ".") != 0 && strcmp(entry.cFileName,
            "..") != 0) count++;
    }while (FindNextFileA(find, &entry));
    CHECK(GetLastError() == ERROR_NO_MORE_FILES); FindClose(find);
#elif defined(__linux__)
    DIR *dir = opendir("/proc/self/fd"); CHECK(dir);
    struct dirent *entry;
    while ((entry = readdir(dir))) {
        char path[512], target[1200];
        int length = snprintf(path, sizeof(path), "/proc/self/fd/%s",
                entry->d_name);
        CHECK(length > 0 && (size_t)length < sizeof(path));
        ssize_t n = readlink(path, target, sizeof(target)-1);
        if (n < 0) continue;
        target[n] = '\0';
        size_t prefix = strlen(directory);
        if (strncmp(target, directory,
            prefix) == 0 && target[prefix] == '/') count++;
    }
    closedir(dir);
#endif
    return count;
}
static void populate(wirelog_session_t *s, bool empty)
{
    for (int64_t i = 0; i < 4; i++) {
        int64_t row[] = {i, 10+i};
        CHECK(wirelog_session_insert(s, "a", row, 1, 2) == WIRELOG_OK);
    }
    for (int64_t i = 0; i < (empty ? 1 : 128); i++) {
        int64_t row[] = {i, 100+i};
        CHECK(wirelog_session_insert(s, "b", row, 1, 2) == WIRELOG_OK);
        if (empty) CHECK(wirelog_session_remove(s, "b", row, 1,
                2) == WIRELOG_OK);
    }
}
typedef struct { unsigned count, seen, second; } events_t;
static void observe(const char *name, const int64_t *row, uint32_t cols,
    void *ctx)
{
    events_t *e = ctx;
    bool second = strcmp(name, "second") == 0;
    CHECK(second || strcmp(name, "result") == 0);
    CHECK(cols == 3 && row[0] >= 0 && row[0] < 4 && row[1] == row[0]+10 &&
        row[2] == row[0]+100+second);
    unsigned bit = 1u << (unsigned)row[0];
    unsigned *seen = second ? &e->second : &e->seen;
    CHECK(!(*seen & bit)); *seen |= bit; e->count++;
}
static void ignore_delta(const char *name, const int64_t *row, uint32_t cols,
    int32_t diff, void *ctx)
{
    (void)name; (void)row; (void)cols; (void)diff; (void)ctx;
}
static void count_event(const char *name, const int64_t *row, uint32_t cols,
    void *ctx)
{
    (void)name; (void)row; (void)cols; (*(unsigned *)ctx)++;
}
static void check_mutation(wirelog_session_t *s)
{
    int64_t row[] = {0, 10};
    CHECK(wirelog_session_insert(s, "a", row, 1, 2) == WIRELOG_ERR_EXEC);
    CHECK(wirelog_session_remove(s, "a", row, 1, 2) == WIRELOG_ERR_EXEC);
    uint32_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_INT64},
        offsets[] = {0, 1};
    uint64_t lanes[] = {0, 10};
    wirelog_typed_row_v1_t typed = {sizeof(typed), 1, 0, 2, 2, 2, types,
                                    offsets, types, lanes};
    CHECK(wirelog_session_insert_typed(s, "a", &typed, 1,
        NULL) == WIRELOG_ERR_EXEC);
    CHECK(wirelog_session_remove_typed(s, "a", &typed, 1,
        NULL) == WIRELOG_ERR_EXEC);
    wirelog_compound_arg_t arg = {WIRELOG_TYPE_INT64, 1}; uint64_t handle;
    CHECK(wirelog_session_make_compound(s, "pair", 1, &arg,
        &handle) == WIRELOG_ERR_EXEC);
}
static void lifecycle(void)
{
    const char *multiple =
        ".decl a(k: int64, v: int64)\n.decl b(k: int64, v: int64)\n"
        ".decl result(k: int64, x: int64, y: int64)\n.decl second(k: int64, x: int64, y: int64)\n"
        "result(k,x,y + 0) :- a(k,x),b(k,y).\nsecond(k,x,y + 1) :- a(k,x),b(k,y).\n";
    for (unsigned mode = 0; mode < 6; mode++) {
        bool empty = mode == 4, two = mode == 5, resident = mode == 0;
        wirelog_program_t *p = parse(two ? multiple : query);
        wirelog_session_t *s = NULL;
        wirelog_session_options_v1_t o = configured();
        char path[sizeof(directory)]; strcpy(path, directory);
        o.spill_directory = path;
        CHECK((resident ? wirelog_session_create(p, WIRELOG_BACKEND_DEFAULT, 1,
            &s) :
            wirelog_session_create_with_options(p,
            mode%2 ? WIRELOG_BACKEND_COLUMNAR : WIRELOG_BACKEND_DEFAULT,
            mode == 2 ? 0 : 1, &o, &s)) == WIRELOG_OK);
        memset(path, 'x', strlen(path)); /* constructor must own its directory */
        populate(s, empty);
        if (!resident) {
            CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
#if defined(_WIN32) || defined(__linux__)
            CHECK(live_files() > 0);
#endif
            CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_ERR_EXEC);
            check_mutation(s);
        }
        if (mode == 3) {
            CHECK(wirelog_session_step(s) == WIRELOG_OK);
            CHECK(wirelog_session_step(s) == WIRELOG_OK);
        } else {
            for (unsigned repeat = 0; repeat < 2; repeat++) {
                events_t e = {0};
                CHECK(wirelog_session_snapshot(s, observe, &e) == WIRELOG_OK);
                CHECK(e.count == (empty ? 0 : two ? 8 : 4));
                CHECK(e.seen == (empty ? 0 : 15));
                CHECK(e.second == (two ? 15 : 0));
            }
        }
        wirelog_session_destroy(s); wirelog_program_free(p);
        CHECK(live_files() == 0);
    }
}
static void descriptors(void)
{
    wirelog_program_t *p = parse(query);
    wirelog_session_options_v1_t valid = configured();
    for (unsigned kind = 0; kind < 9; kind++) {
        wirelog_session_options_v1_t o = valid;
        if (kind == 0) o.struct_size = 4;
        if (kind == 1) o.abi_version = 99;
        if (kind == 2) o.spill_directory = "";
        if (kind == 3) o.spill_directory = NULL;
        if (kind == 4) o.spill_quota_bytes = 0;
        if (kind == 5) o.spill_max_row_bytes = 31;
        if (kind == 6) o.spill_join_index_bytes = 0;
        if (kind == 7) o.spill_join_batch_bytes = 0;
        if (kind ==
            8) o.spill_directory = "/wirelog-nonexistent-directory-1372";
        wirelog_session_t *s = (wirelog_session_t *)(uintptr_t)1;
        CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1,
            &o, &s) == WIRELOG_ERR_EXEC && !s);
        CHECK(live_files() == 0);
    }
    uint32_t *short_descriptor = malloc(sizeof(*short_descriptor));
    CHECK(short_descriptor);
    *short_descriptor = sizeof(*short_descriptor);
    wirelog_session_t *s = NULL;
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1,
        (const wirelog_session_options_v1_t *)short_descriptor,
        &s) == WIRELOG_ERR_EXEC && !s);
    free(short_descriptor);
    struct { wirelog_session_options_v1_t prefix; unsigned char ignored[16];
    } oversized;
    memset(&oversized, 0xa5, sizeof(oversized)); oversized.prefix = valid;
    oversized.prefix.struct_size = sizeof(oversized);
    wirelog_error_t oversized_rc = wirelog_session_create_with_options(p,
            WIRELOG_BACKEND_DEFAULT, 1, &oversized.prefix, &s);
    CHECK(oversized_rc == WIRELOG_OK);
    wirelog_session_destroy(s);
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1,
        NULL, &s) == WIRELOG_OK);
    populate(s, false); events_t e = {0};
    CHECK(wirelog_session_snapshot(s, observe,
        &e) == WIRELOG_OK && e.count == 4);
    wirelog_session_destroy(s);
    CHECK(wirelog_session_create_with_options(NULL, WIRELOG_BACKEND_DEFAULT, 1,
        &valid, &s) == WIRELOG_ERR_EXEC && !s);
    CHECK(wirelog_session_create_with_options(p, (wirelog_backend_kind_t)99, 1,
        &valid, &s) == WIRELOG_ERR_EXEC && !s);
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1,
        &valid, NULL) == WIRELOG_ERR_EXEC);
    CHECK(wirelog_session_seal_join_input(NULL, "b") == WIRELOG_ERR_EXEC);
    wirelog_program_free(p); CHECK(live_files() == 0);
}
static void refusals(void)
{
    const char *scan =
        ".decl b(k:int64,v:int64)\n.decl result(k:int64,v:int64)\nresult(k,v + 1) :- b(k,v).\n";
    for (unsigned kind = 0; kind < 8; kind++) {
        wirelog_program_t *p = parse(kind == 5 ? scan : query);
        wirelog_session_options_v1_t o = configured();
        if (kind == 0) {
            memset(&o, 0, sizeof(o)); o.struct_size = sizeof(o);
            o.abi_version = 1;
        }
        if (kind == 1) o.spill_join_batch_bytes = o.spill_join_index_bytes = 0;
        if (kind == 7) o.spill_quota_bytes = 512;
        wirelog_session_t *s = NULL;
        CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT,
            kind == 2 ? 2 : 1, &o, &s) == WIRELOG_OK);
        if (kind == 5) {
            int64_t row[] = {1, 2};
            CHECK(wirelog_session_insert(s, "b", row, 1, 2) == WIRELOG_OK);
        }else populate(s, false);
        if (kind == 3) wirelog_session_set_delta_cb(s, ignore_delta, NULL);
        if (kind == 6) CHECK(wirelog_session_step(s) == WIRELOG_OK);
        CHECK(wirelog_session_seal_join_input(s,
            kind == 4 ? "missing" : "b") ==
            (kind == 7 ? WIRELOG_ERR_IO : WIRELOG_ERR_EXEC));
        CHECK(live_files() == 0);
        if (kind == 3) wirelog_session_set_delta_cb(s, NULL, NULL);
        if (kind == 3 || kind == 4) {
            CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
            events_t e = {0};
            CHECK(wirelog_session_snapshot(s, observe,
                &e) == WIRELOG_OK && e.count == 4);
        } else if (kind == 0 || kind == 1 || kind == 2 || kind == 5) {
            unsigned count = 0;
            CHECK(wirelog_session_snapshot(s, count_event,
                &count) == WIRELOG_OK && count == (kind == 5 ? 1u : 4u));
        } else if (kind == 7) {
            int64_t row[] = {200, 300};
            CHECK(wirelog_session_insert(s, "b", row, 1, 2) == WIRELOG_OK);
            CHECK(wirelog_session_remove(s, "b", row, 1, 2) == WIRELOG_OK);
            for (int64_t i = 1; i < 128; i++) {
                int64_t old[] = {i, 100+i};
                CHECK(wirelog_session_remove(s, "b", old, 1, 2) == WIRELOG_OK);
            }
            CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
            events_t e = {0};
            CHECK(wirelog_session_snapshot(s, observe,
                &e) == WIRELOG_OK && e.count == 1 && e.seen == 1);
        }
        wirelog_session_destroy(s); wirelog_program_free(p);
        CHECK(live_files() == 0);
    }
}
typedef struct { uint32_t type; uint64_t bits[3]; unsigned seen;
} typed_events_t;
static void typed_observe(const char *name, const wirelog_typed_row_v1_t *row,
    int32_t diff, void *ctx)
{
    typed_events_t *e = ctx;
    CHECK(strcmp(name,
        "result") == 0 && diff == 1 && row && row->logical_ncols == 3 &&
        row->physical_nlanes == 3);
    CHECK(row->types[0] == WIRELOG_TYPE_INT64 &&
        row->types[1] == WIRELOG_TYPE_INT64 && row->types[2] == e->type);
    CHECK(row->physical_types[2] == e->type && row->lane_offsets[2] == 2);
    uint64_t k = row->lanes[0];
    CHECK(k < 3 && row->lanes[1] == k+10 && row->lanes[2] == e->bits[k]);
    CHECK(!(e->seen & (1u << k))); e->seen |= 1u << k;
}
static void typed_projection(void)
{
    const char *text = ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:float)\n"
        ".decl result(k:int64,x:int64,y:float)\nresult(k,x,y) :- a(k,x),b(k,y).\n";
    wirelog_program_t *p = parse(text);
    wirelog_session_options_v1_t o = configured(); wirelog_session_t *s = NULL;
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1, &o,
        &s) == WIRELOG_OK);
    typed_events_t expected = {WIRELOG_TYPE_FLOAT, {0}, 0};
    expected.bits[0] = UINT64_C(0x8000000000000000);         /* negative zero */
    expected.bits[1] = UINT64_C(0xbff8000000000000);         /* -1.5 */
    expected.bits[2] = UINT64_C(0x3ff4000000000000);         /* 1.25 */
    uint32_t types[] = {WIRELOG_TYPE_INT64, expected.type}, offsets[] = {0, 1};
    uint64_t invalid_lanes[] = {99, UINT64_C(0x7ff8000000000042)};
    wirelog_typed_row_v1_t invalid = {sizeof(invalid), 1, 0, 2, 2, 2, types,
                                      offsets, types, invalid_lanes};
    CHECK(wirelog_session_insert_typed(s, "b", &invalid, 1,
        NULL) == WIRELOG_ERR_EXEC);
    for (uint64_t i = 0; i < 3; i++){
        int64_t left[] = {(int64_t)i, (int64_t)i+10};
        uint64_t lanes[] = {i, expected.bits[i]};
        wirelog_typed_row_v1_t row = {sizeof(row), 1, 0, 2, 2, 2, types,
                                      offsets, types, lanes};
        CHECK(wirelog_session_insert(s, "a", left, 1, 2) == WIRELOG_OK);
        CHECK(wirelog_session_insert_typed(s, "b", &row, 1,
            NULL) == WIRELOG_OK);
    }
    CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
    /* Existing typed output canonicalizes signed zero. */
    expected.bits[0] = 0;
    for (unsigned repeat = 0; repeat < 2; repeat++){
        expected.seen = 0;
        CHECK(wirelog_session_snapshot_typed(s, typed_observe,
            &expected) == WIRELOG_OK && expected.seen == 7);
    }
    wirelog_session_destroy(s); wirelog_program_free(p);
    CHECK(live_files() == 0);
}
static void inline_facts(void)
{
    const char *text = ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n"
        ".decl result(k:int64,x:int64,y:int64)\na(1,11).\nb(1,101).\n"
        "result(k,x,y + 0) :- a(k,x),b(k,y).\n";
    wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
    wirelog_session_options_v1_t o = configured();
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1, &o,
        &s) == WIRELOG_OK);
    CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
    events_t e = {0};
    CHECK(wirelog_session_snapshot(s, observe,
        &e) == WIRELOG_OK && e.count == 1 && e.seen == 2);
    wirelog_session_destroy(s); wirelog_program_free(p);
    CHECK(live_files() == 0);
}
static void output_floor(void)
{
    /* The public policy accepts decimal bytes >=256 MiB. An invalid lower
    * setting is EXEC, not evidence of real governed admission refusal. */
    environment("WIRELOG_MEMORY_BUDGET", "1");
    wirelog_program_t *p = parse(query); wirelog_session_t *s = NULL;
    wirelog_session_options_v1_t o = configured();
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1, &o,
        &s) == WIRELOG_ERR_EXEC && !s);
    wirelog_program_free(p);
    environment("WIRELOG_MEMORY_BUDGET", "268435456");
    p = parse(query);
    o.spill_quota_bytes = 16*1024*1024;
    o.spill_join_batch_bytes = 64*1024; o.spill_join_index_bytes = 64*1024;
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1, &o,
        &s) == WIRELOG_OK);
    for (int64_t i = 0; i < 4096; i++) {
        int64_t row[] = {0, i};
        CHECK(wirelog_session_insert(s, "a", row, 1, 2) == WIRELOG_OK);
        CHECK(wirelog_session_insert(s, "b", row, 1, 2) == WIRELOG_OK);
    }
    CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
    unsigned events = 0;
    CHECK(wirelog_session_snapshot(s, count_event,
        &events) == WIRELOG_ERR_MEMORY_BUDGET && events == 0);
#if defined(_WIN32) || defined(__linux__)
    CHECK(live_files() > 0);
#endif
    /* A subsequent unsupported seal must not inherit the prior budget error. */
    CHECK(wirelog_session_seal_join_input(s, "missing") == WIRELOG_ERR_EXEC);
    wirelog_session_destroy(s); wirelog_program_free(p);
    CHECK(live_files() == 0);
}
static wirelog_session_t *knob_session(wirelog_program_t *p,
    const char *knob)
{
    wirelog_session_t *s = NULL;
    wirelog_session_options_v1_t o = configured();
    environment("WIRELOG_JOIN_BATCH_BYTES", knob);
    CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1,
        &o, &s) == WIRELOG_OK);
    populate(s, false);
    return s;
}
static void knob_snapshot(wirelog_session_t *s)
{
    events_t e = {0};
    CHECK(wirelog_session_snapshot(s, observe, &e) == WIRELOG_OK);
    CHECK(e.count == 4 && e.seen == 15 && e.second == 0);
}
/* Issue #2287: WIRELOG_JOIN_BATCH_BYTES is read when the session is created.
 * A value that turns it on refuses the seal before capture and leaves the
 * source mutable; the other values below leave sealing available. On
 * Windows, environment() deletes the variable for "", so that case repeats
 * the unset one there. */
static void join_batch_knob(void)
{
    static const char *const off[] = {
        "0", "", "abc", "5x", "0x10", "18446744073709551616"
    };
    static const char *const on[] = {"4096", " 5", "-1"};
    wirelog_program_t *p = parse(query);
    for (unsigned i = 0; i < sizeof(off)/sizeof(off[0]); i++) {
        wirelog_session_t *s = knob_session(p, off[i]);
        CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
#if defined(_WIN32) || defined(__linux__)
        CHECK(live_files() > 0);
#endif
        knob_snapshot(s);
        wirelog_session_destroy(s);
        CHECK(live_files() == 0);
    }
    for (unsigned i = 0; i < sizeof(on)/sizeof(on[0]); i++) {
        wirelog_session_t *s = knob_session(p, on[i]);
        CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_ERR_EXEC);
        CHECK(live_files() == 0);
        /* Clearing the environment does not change a created session. */
        environment("WIRELOG_JOIN_BATCH_BYTES", NULL);
        CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_ERR_EXEC);
        int64_t row[] = {200, 300};
        CHECK(wirelog_session_insert(s, "b", row, 1, 2) == WIRELOG_OK);
        CHECK(wirelog_session_remove(s, "b", row, 1, 2) == WIRELOG_OK);
        knob_snapshot(s);
        wirelog_session_destroy(s);
        CHECK(live_files() == 0);
    }
    /* Setting the environment after creation does not refuse the seal. */
    wirelog_session_t *s = knob_session(p, NULL);
    environment("WIRELOG_JOIN_BATCH_BYTES", "4096");
    CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_OK);
    environment("WIRELOG_JOIN_BATCH_BYTES", NULL);
    knob_snapshot(s);
    wirelog_session_destroy(s); wirelog_program_free(p);
    CHECK(live_files() == 0);
}

typedef struct { unsigned keys, joined, seen; uint64_t key_bits[2];
} mixed_events_t;
static void mixed_observe(const char *name, const int64_t *row, uint32_t cols,
    void *ctx)
{
    mixed_events_t *e = ctx;
    if (!strcmp(name, "keys")) {
        CHECK(cols == 1 && row[0] >= 0 && row[0] < 128);
        unsigned key = (unsigned)row[0];
        uint64_t mask = UINT64_C(1) << (key % 64);
        CHECK(!(e->key_bits[key / 64] & mask)); e->key_bits[key / 64] |= mask;
        e->keys++;
    } else {
        events_t one = {0}; observe(name, row, cols, &one);
        CHECK(!(e->seen & one.seen)); e->seen |= one.seen; e->joined++;
    }
}
static void mixed_projection(void)
{
    const char *predicates[] = {"", ", v >= 164", ", v >= -1", ", v > 999"};
    for (unsigned mode = 0; mode < 4; mode++)
        for (unsigned empty = 0; empty < 2; empty++)
            for (unsigned sealed = 0; sealed < 3; sealed++) {
                char text[512];
                snprintf(text, sizeof(text),
                    ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n"
                    ".decl result(k:int64,x:int64,y:int64)\n.decl keys(k:int64)\n"
                    "result(k,x,y + 0) :- a(k,x),b(k,y).\nkeys(k) :- b(k,v)%s.\n",
                    predicates[mode]);
                wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
                wirelog_session_options_v1_t o = configured();
                CHECK(wirelog_session_create_with_options(p,
                    WIRELOG_BACKEND_DEFAULT, 1,
                    &o, &s) == WIRELOG_OK);
                populate(s, empty != 0);
                if (sealed) {
                    CHECK(wirelog_session_seal_join_input(s,
                        "b") == WIRELOG_OK);
                    check_mutation(s);
                }
                if (sealed == 2) CHECK(wirelog_session_step(s) == WIRELOG_OK);
                for (unsigned repeat = 0; repeat < 2; repeat++) {
                    mixed_events_t e = {0};
                    CHECK(wirelog_session_snapshot(s, mixed_observe,
                        &e) == WIRELOG_OK);
                    unsigned count = empty ||
                        mode == 3 ? 0u : mode == 1 ? 64u : 128u;
                    CHECK(e.keys == count && e.joined == (empty ? 0u : 4u) &&
                        e.seen == (empty ? 0u : 15u));
                    CHECK(e.key_bits[0] == (count == 128 ? UINT64_MAX : 0));
                    CHECK(e.key_bits[1] == (count ? UINT64_MAX : 0));
                }
                wirelog_session_destroy(s); wirelog_program_free(p);
                CHECK(live_files() == 0);
            }
}
static void unsupported_filters(void)
{
    const char *rules[] = {
        "keys(k) :- b(k,v), v + 1 > 164.",
        "keys(k) :- b(k,v), v > k.",
        "keys(k) :- b(k,v), v > 100, v + 1 < 200.",
        /* An unrelated resident filter must not inherit the cold exception. */
        "keys(k) :- a(k,v), v + 1 > 11.",
        /* A predicate on the right JOIN input remains unsupported. */
        "keys(k) :- a(k,v), b(k,w), w > 164."
    };
    for (unsigned i = 0; i < sizeof(rules)/sizeof(rules[0]); i++) {
        char text[512];
        snprintf(text, sizeof(text),
            ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n"
            ".decl result(k:int64,x:int64,y:int64)\n.decl keys(k:int64)\n"
            "result(k,x,y + 0) :- a(k,x),b(k,y).\n%s\n", rules[i]);
        wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
        wirelog_session_options_v1_t o = configured();
        CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1,
            &o, &s) == WIRELOG_OK);
        populate(s, false);
        CHECK(wirelog_session_seal_join_input(s, "b") == WIRELOG_ERR_EXEC);
        /* Failed preflight leaves a usable resident session and input. */
        mixed_events_t e = {0};
        CHECK(wirelog_session_snapshot(s, mixed_observe, &e) == WIRELOG_OK);
        CHECK(e.joined == 4 && e.seen == 15);
        wirelog_session_destroy(s); wirelog_program_free(p);
        CHECK(live_files() == 0);
    }
}

static void standalone_reads(void)
{
    CHECK(wirelog_session_seal_input(NULL, "b") == WIRELOG_ERR_EXEC);
    for (unsigned filtered = 0; filtered < 2; filtered++)
        for (unsigned empty = 0; empty < 2; empty++)
            for (unsigned mode = 0; mode < 3; mode++) {
                char text[256];
                snprintf(text, sizeof(text),
                    ".decl b(k:int64,v:int64)\n.decl keys(k:int64)\n"
                    "keys(k) :- b(k,v)%s.\n", filtered ? ", v >= 164" : "");
                wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
                wirelog_session_options_v1_t o = configured();
                CHECK(wirelog_session_create_with_options(p,
                    WIRELOG_BACKEND_DEFAULT,
                    1, &o, &s) == WIRELOG_OK);
                for (unsigned i = 0; i < (empty ? 1u : 128u); i++) {
                    int64_t row[] = {(int64_t)i, 100+(int64_t)i};
                    CHECK(wirelog_session_insert(s, "b", row, 1,
                        2) == WIRELOG_OK);
                    if (empty) CHECK(wirelog_session_remove(s, "b", row, 1,
                            2) == WIRELOG_OK);
                }
                if (mode) {
                    CHECK((mode == 1 ? wirelog_session_seal_input(s, "b") :
                        wirelog_session_seal_join_input(s, "b")) == WIRELOG_OK);
                    check_mutation(s);
                    CHECK(wirelog_session_seal_input(s,
                        "b") == WIRELOG_ERR_EXEC);
                }
                if (mode == 2) CHECK(wirelog_session_step(s) == WIRELOG_OK);
                for (unsigned repeat = 0; repeat < 2; repeat++) {
                    mixed_events_t e = {0};
                    CHECK(wirelog_session_snapshot(s, mixed_observe,
                        &e) == WIRELOG_OK);
                    unsigned count = empty ? 0u : filtered ? 64u : 128u;
                    CHECK(e.keys == count && !e.joined);
                    CHECK(e.key_bits[0] == (count == 128 ? UINT64_MAX : 0));
                    CHECK(e.key_bits[1] == (count ? UINT64_MAX : 0));
                }
                /* Every evaluation attempt closes the sealing window. */
                CHECK(wirelog_session_seal_input(s, "b") == WIRELOG_ERR_EXEC);
                wirelog_session_destroy(s); wirelog_program_free(p);
                CHECK(live_files() == 0);
            }
}

typedef struct { uint64_t keys[3]; } sealed_set_events_t;
static void sealed_set_observe(const char *name, const int64_t *row,
    uint32_t width,
    void *context)
{
    sealed_set_events_t *e = context;
    unsigned which = !strcmp(name, "pa") ? 0u : !strcmp(name, "pb") ? 1u : 2u;
    CHECK(which != 2 || !strcmp(name, "pc"));
    CHECK(width == 1 && row[0] >= 0 && row[0] < 64);
    uint64_t bit = UINT64_C(1) << (unsigned)row[0];
    CHECK(!(e->keys[which] & bit)); e->keys[which] |= bit;
}
static void sealed_sets(void)
{
    const char *text =
        ".decl a(k:int64,v:int64)\n.decl b(k:int64,x:int64,v:int64)\n"
        ".decl c(k:int64,v:int64)\n"
        ".decl pa(k:int64)\n.decl pb(k:int64)\n.decl pc(k:int64)\n"
        "pa(k) :- a(k,v), v >= 132.\npb(k) :- b(k,x,v), v >= 216.\n"
        "pc(k) :- c(k,v).\n";
    const char *orders[][3] = {{"a", "b", "c"}, {"b", "a", "c"},
                               {"c", "b", "a"}};
    for (unsigned selectivity = 0; selectivity < 3; selectivity++)
        for (unsigned empty = 0; empty < 2; empty++)
            for (unsigned order = 0; order < 3; order++)
                for (unsigned count = 0; count < 4; count++) {
                    wirelog_program_t *p = parse(text);
                    wirelog_session_t *s = NULL;
                    wirelog_session_options_v1_t o = configured();
                    CHECK(wirelog_session_create_with_options(p,
                        WIRELOG_BACKEND_DEFAULT, 1, &o, &s) == WIRELOG_OK);
                    for (unsigned i = 0; i < (empty ? 1u : 64u); i++) {
                        int64_t a[] = {(int64_t)i,
                                       (selectivity == 0 ? 100 : selectivity ==
                                       1 ? 0 : 1000)+(int64_t)i};
                        int64_t b[] = {(int64_t)i, -(int64_t)i,
                                       (selectivity == 0 ? 200 : selectivity ==
                                       1 ? 0 : 1000)+(int64_t)i};
                        int64_t c[] = {(int64_t)i, 300+(int64_t)i};
                        CHECK(wirelog_session_insert(s, "a", a, 1,
                            2) == WIRELOG_OK);
                        CHECK(wirelog_session_insert(s, "b", b, 1,
                            3) == WIRELOG_OK);
                        CHECK(wirelog_session_insert(s, "c", c, 1,
                            2) == WIRELOG_OK);
                        if (empty) {
                            CHECK(wirelog_session_remove(s, "a", a, 1,
                                2) == WIRELOG_OK);
                            CHECK(wirelog_session_remove(s, "b", b, 1,
                                3) == WIRELOG_OK);
                            CHECK(wirelog_session_remove(s, "c", c, 1,
                                2) == WIRELOG_OK);
                        }
                    }
                    for (unsigned i = 0; i < count; i++) {
                        CHECK((i % 2 ? wirelog_session_seal_join_input(s,
                            orders[order][i]) :
                            wirelog_session_seal_input(s,
                            orders[order][i])) == WIRELOG_OK);
                        check_mutation(s);
                    }
                    if (count) CHECK(wirelog_session_seal_input(s,
                            orders[order][0]) == WIRELOG_ERR_EXEC);
                    if (order ==
                        1) CHECK(wirelog_session_step(s) == WIRELOG_OK);
                    for (unsigned repeat = 0; repeat < 2; repeat++) {
                        sealed_set_events_t e = {0};
                        CHECK(wirelog_session_snapshot(s, sealed_set_observe,
                            &e) == WIRELOG_OK);
                        CHECK(e.keys[0] == (empty ||
                            selectivity == 1 ? 0 : selectivity ==
                            2 ? UINT64_MAX : UINT64_MAX << 32));
                        CHECK(e.keys[1] == (empty ||
                            selectivity == 1 ? 0 : selectivity ==
                            2 ? UINT64_MAX : UINT64_MAX << 16));
                        CHECK(e.keys[2] == (empty ? 0 : UINT64_MAX));
                    }
                    CHECK(wirelog_session_seal_input(s,
                        "c") == WIRELOG_ERR_EXEC);
                    wirelog_session_destroy(s); wirelog_program_free(p);
                    CHECK(live_files() == 0);
                }
}

static void join_directions(void)
{
    for (unsigned empty = 0; empty < 2; empty++)
        for (unsigned direction = 0; direction < 5; direction++) {
            wirelog_program_t *p = parse(query);
            wirelog_session_t *s = NULL;
            wirelog_session_options_v1_t o = configured();
            CHECK(wirelog_session_create_with_options(p,
                WIRELOG_BACKEND_DEFAULT, 1, &o, &s) == WIRELOG_OK);
            populate(s, empty != 0);
            if (direction == 4) CHECK(wirelog_session_seal_input(s,
                    "b") == WIRELOG_OK);
            if (direction == 1 ||
                direction >= 3) CHECK(wirelog_session_seal_input(s,
                    "a") == WIRELOG_OK);
            if (direction == 2 ||
                direction == 3) CHECK(wirelog_session_seal_join_input(s,
                    "b") == WIRELOG_OK);
            if (direction) check_mutation(s);
            if (direction == 4) CHECK(wirelog_session_step(s) == WIRELOG_OK);
            for (unsigned repeat = 0; repeat < 2; repeat++) {
                events_t e = {0};
                CHECK(wirelog_session_snapshot(s, observe, &e) == WIRELOG_OK);
                CHECK(e.count == (empty ? 0u : 4u) &&
                    e.seen == (empty ? 0u : 15u));
            }
            wirelog_session_destroy(s); wirelog_program_free(p);
            CHECK(live_files() == 0);
        }
}
typedef struct { unsigned seen[3]; } join_chain_events_t;
static void join_chain_observe(const char *name, const int64_t *row,
    uint32_t width, void *context)
{
    join_chain_events_t *e = context;
    unsigned idx = !strcmp(name, "result") ? 0u : !strcmp(name,
            "scan") ? 1u : 2u;
    CHECK(idx != 2 || !strcmp(name, "second"));
    CHECK(width == (idx == 0 ? 4u : idx == 1 ? 1u : 3u) && row[0] >= 0 &&
        row[0] < 8);
    if (idx != 1) CHECK(row[1] == row[0]+10 && row[2] == row[0]+100);
    if (idx == 0) CHECK(row[3] == row[0]+1000);
    unsigned bit = 1u << (unsigned)row[0]; CHECK(!(e->seen[idx]&bit));
    e->seen[idx] |= bit;
}
static void join_chains(void)
{
    const char *text =
        ".decl a(k:int64,v:int64)\n.decl b(k:int64,v:int64)\n.decl c(k:int64,v:int64)\n"
        ".decl result(k:int64,x:int64,y:int64,z:int64)\nresult(k,x,y,z+0) :- a(k,x),b(k,y),c(y,z).\n"
        ".decl scan(k:int64)\nscan(k) :- a(k,x).\n"
        ".decl second(k:int64,x:int64,y:int64)\nsecond(k,x,y+0) :- a(k,x),b(k,y).\n";
    for (unsigned order = 0; order < 3; order++) {
        wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
        wirelog_session_options_v1_t o = configured();
        CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT, 1,
            &o, &s) == WIRELOG_OK);
        for (int64_t i = 0; i < 8; i++) {
            int64_t a[] = {i, 10+i}, b[] = {i, 100+i}, c[] = {100+i, 1000+i};
            CHECK(wirelog_session_insert(s, "a", a, 1, 2) == WIRELOG_OK);
            CHECK(wirelog_session_insert(s, "b", b, 1, 2) == WIRELOG_OK);
            CHECK(wirelog_session_insert(s, "c", c, 1, 2) == WIRELOG_OK);
        }
        if (order) {
            const char *names[] = {"a", "b", "c"};
            for (unsigned i = 0; i < 3;
                i++) CHECK(wirelog_session_seal_input(s,
                    names[order == 1 ? i : 2-i]) == WIRELOG_OK);
        }
        if (order == 2) CHECK(wirelog_session_step(s) == WIRELOG_OK);
        for (unsigned repeat = 0; repeat < 2; repeat++) {
            join_chain_events_t e = {0};
            CHECK(wirelog_session_snapshot(s, join_chain_observe,
                &e) == WIRELOG_OK);
            CHECK(e.seen[0] == 255 && e.seen[1] == 255 && e.seen[2] == 255);
        }
        wirelog_session_destroy(s); wirelog_program_free(p);
        CHECK(live_files() == 0);
    }
}

typedef struct { int64_t ids[3]; unsigned bits[2]; } reference_public_events_t;
static void reference_public_observe(const char *name,
    const wirelog_typed_row_v1_t *row, int32_t diff, void *context)
{
    reference_public_events_t *e = context;
    unsigned idx = !strcmp(name, "result") ? 0u : 1u;
    CHECK(idx == 0 || !strcmp(name, "other"));
    unsigned width = idx ? 2 : 3;
    CHECK(diff == 1 && row && row->logical_ncols == width &&
        row->physical_nlanes == width);
    CHECK(row->types[0] == WIRELOG_TYPE_STRING &&
        row->physical_types[0] == WIRELOG_TYPE_STRING &&
        row->types[1] == WIRELOG_TYPE_INT64);
    unsigned key = (unsigned)row->lanes[1];
    CHECK(key < 3 && row->lanes[0] == (uint64_t)e->ids[key]);
    if (!idx) CHECK(row->types[2] == WIRELOG_TYPE_STRING &&
            row->physical_types[2] == WIRELOG_TYPE_STRING &&
            row->lanes[2] == row->lanes[0]);
    CHECK(!(e->bits[idx]&(1u<<key))); e->bits[idx] |= 1u<<key;
}
static const char *const projection_predicates[] = {
    "", ", k >= 1", ", k >= 0, k < 2, k != 1",
    ", k >= 0, k < 3, k != -1", ", k >= 2, k < 2"
};
static const unsigned projection_predicate_masks[] = {7, 6, 1, 7, 0};
static void reference_public_projections(void)
{
    for (unsigned filtered = 0; filtered < 5; filtered++)
        for (unsigned empty = 0; empty < 2; empty++)
            for (unsigned order = 0; order < 3; order++) {
                char text[1024];
                snprintf(text, sizeof(text),
                    ".decl a(k:int64,s:symbol,u:symbol)\n.decl b(s:symbol,k:int64)\n"
                    ".decl result(s:symbol,k:int64,t:symbol)\n.decl other(s:symbol,k:int64)\n"
                    "a(0,\"\",\"unused\").\na(1,\"alpha\",\"unused\").\na(2,\"한글\",\"unused\").\n"
                    "b(\"\",0).\nb(\"alpha\",1).\nb(\"한글\",2).\n"
                    "result(s,k,s) :- a(k,s,u)%s.\nother(s,k) :- b(s,k).\n",
                    projection_predicates[filtered]);
                wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
                wirelog_session_options_v1_t o = configured();
                CHECK(wirelog_session_create_with_options(p,
                    WIRELOG_BACKEND_DEFAULT, 1, &o, &s) == WIRELOG_OK);
                int64_t *facts = NULL; uint32_t rows = 0, cols = 0;
                CHECK(wirelog_program_get_facts(p, "a", &facts, &rows,
                    &cols) == 0 && rows == 3 && cols == 3);
                reference_public_events_t expected = {0};
                for (unsigned i = 0; i < rows; i++) {
                    CHECK(facts[i*cols] >= 0 && facts[i*cols] < 3);
                    expected.ids[facts[i*cols]] = facts[i*cols+1];
                    if (empty) CHECK(wirelog_session_remove(s, "a",
                            facts+i*cols, 1, cols) == WIRELOG_OK);
                }
                if (!empty) CHECK(wirelog_session_insert(s, "a", facts, 1,
                        cols) == WIRELOG_OK);                                                /* Duplicate row. */
                free(facts); facts = NULL;
                CHECK(wirelog_program_get_facts(p, "b", &facts, &rows,
                    &cols) == 0 && rows == 3 && cols == 2);
                if (empty) CHECK(wirelog_session_remove(s, "b", facts, rows,
                        cols) == WIRELOG_OK);
                free(facts);
                if (order) {
                    CHECK(wirelog_session_seal_input(s,
                        order == 1 ? "a" : "b") == WIRELOG_OK);
                    CHECK(wirelog_session_seal_join_input(s,
                        order == 1 ? "b" : "a") == WIRELOG_OK);
                }
                if (order == 2) CHECK(wirelog_session_step(s) == WIRELOG_OK);
                for (unsigned repeat = 0; repeat < 2; repeat++) {
                    reference_public_events_t e = expected;
                    CHECK(wirelog_session_snapshot_typed(s,
                        reference_public_observe, &e) == WIRELOG_OK);
                    CHECK(e.bits[0] ==
                        (empty ? 0u : projection_predicate_masks[filtered]) &&
                        e.bits[1] == (empty ? 0u : 7u));
                }
                wirelog_session_destroy(s); wirelog_program_free(p);
                CHECK(live_files() == 0);
            }
}

typedef struct { uint64_t ids[3]; unsigned seen[4]; } reference_chain_events_t;
static void reference_chain_observe(const char *name,
    const wirelog_typed_row_v1_t *row, int32_t diff, void *context)
{
    reference_chain_events_t *e = context;
    unsigned which = !strcmp(name, "result") ? 0 : !strcmp(name, "self") ? 1 :
        !strcmp(name, "scan") ? 2 : 3;
    CHECK(which != 3 || !strcmp(name, "second"));
    unsigned width = which == 2 ? 2 : 4;
    CHECK(diff == 1 && row->logical_ncols == width &&
        row->physical_nlanes == width);
    unsigned i = (unsigned)row->lanes[1]; CHECK(i < 3);
    CHECK(row->types[0] == WIRELOG_TYPE_STRING &&
        row->types[1] == WIRELOG_TYPE_INT64);
    CHECK(row->physical_types[0] == WIRELOG_TYPE_STRING &&
        row->lanes[0] == e->ids[i]);
    if (which != 2) {
        CHECK(row->types[2] == WIRELOG_TYPE_STRING &&
            row->types[3] == WIRELOG_TYPE_INT64);
        CHECK(row->physical_types[2] == WIRELOG_TYPE_STRING);
        CHECK(row->lanes[2] == e->ids[which == 1 ? i : (i+1)%3]);
        CHECK(row->lanes[3] == i + (which == 0 ? 100u : which == 1 ? 0u : 10u));
    }
    CHECK(!(e->seen[which] & (1u<<i))); e->seen[which] |= 1u<<i;
}
static void reference_public_joins(void)
{
    const char *text =
        ".decl a(k:symbol,x:int64)\n.decl b(k:symbol,next:symbol,y:int64)\n"
        ".decl c(next:symbol,z:int64)\n"
        ".decl result(k:symbol,x:int64,next:symbol,z:int64)\n"
        ".decl self(k:symbol,x:int64,p:symbol,y:int64)\n"
        ".decl scan(k:symbol,x:int64)\n"
        ".decl second(k:symbol,x:int64,next:symbol,y:int64)\n"
        "a(\"\",0).\na(\"alpha\",1).\na(\"한글\",2).\n"
        "b(\"\",\"alpha\",10).\nb(\"alpha\",\"한글\",11).\nb(\"한글\",\"\",12).\n"
        "c(\"alpha\",100).\nc(\"한글\",101).\nc(\"\",102).\n"
        "result(k,x,n,z+0) :- a(k,x),b(k,n,y),c(n,z).\n"
        "self(k,x,k,y+0) :- a(k,x),a(k,y).\n"
        "scan(k,x) :- a(k,x).\n"
        "second(k,x,n,y+0) :- a(k,x),b(k,n,y).\n";
    for (unsigned direction = 0; direction < 5; direction++) {
        wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
        wirelog_session_options_v1_t o = configured();
        CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT,
            1, &o, &s) == WIRELOG_OK);
        int64_t *facts = NULL; uint32_t rows = 0, cols = 0;
        CHECK(wirelog_program_get_facts(p, "a", &facts, &rows,
            &cols) == 0 && rows == 3 && cols == 2);
        reference_chain_events_t expected = {0};
        for (unsigned i = 0; i < rows;
            i++) expected.ids[facts[i*2+1]] = (uint64_t)facts[i*2];
        free(facts);
        const char *names[] = {"a", "b", "c"};
        if (direction == 1) CHECK(wirelog_session_seal_input(s,
                "a") == WIRELOG_OK);
        if (direction == 2) CHECK(wirelog_session_seal_input(s,
                "c") == WIRELOG_OK);
        if (direction >= 3) for (unsigned i = 0; i < 3; i++)
                CHECK(wirelog_session_seal_input(s,
                    names[direction == 3 ? i : 2-i]) == WIRELOG_OK);
        for (unsigned repeat = 0; repeat < 2; repeat++) {
            reference_chain_events_t e = expected;
            CHECK(wirelog_session_snapshot_typed(s, reference_chain_observe,
                &e) == WIRELOG_OK);
            for (unsigned i = 0; i < 4; i++) CHECK(e.seen[i] == 7);
            CHECK(wirelog_session_step(s) == WIRELOG_OK);
        }
        wirelog_session_destroy(s); wirelog_program_free(p);
        CHECK(live_files() == 0);
    }
}

static void inline_public_projections(void)
{
    for (unsigned filtered = 0; filtered < 5; filtered++)
        for (unsigned empty = 0; empty < 2; empty++)
            for (unsigned order = 0; order < 3; order++) {
                char text[1024];
                snprintf(text, sizeof(text),
                    ".decl a(p:pair(int64,symbol) inline,q:box(symbol) inline)\n.decl b(p:pair(symbol,int64) inline)\n"
                    ".decl result(s:symbol,k:int64,t:symbol)\n.decl other(s:symbol,k:int64)\n"
                    "a(0,\"\",\"unused\").\na(1,\"alpha\",\"unused\").\na(2,\"한글\",\"unused\").\n"
                    "b(\"\",0).\nb(\"alpha\",1).\nb(\"한글\",2).\n"
                    "result(s,k,s) :- a(pair(k,s),box(u))%s.\nother(s,k) :- b(pair(s,k)).\n",
                    projection_predicates[filtered]);
                wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
                wirelog_session_options_v1_t o = configured();
                CHECK(wirelog_session_create_with_options(p,
                    WIRELOG_BACKEND_DEFAULT, 1, &o, &s) == WIRELOG_OK);
                int64_t *facts = NULL; uint32_t rows = 0, cols = 0;
                CHECK(wirelog_program_get_facts(p, "a", &facts, &rows,
                    &cols) == 0 && rows == 3 && cols == 3);
                reference_public_events_t expected = {0};
                for (unsigned i = 0; i < rows; i++) {
                    CHECK(facts[i*cols] >= 0 && facts[i*cols] < 3);
                    expected.ids[facts[i*cols]] = facts[i*cols+1];
                    if (empty) CHECK(wirelog_session_remove(s, "a",
                            facts+i*cols, 1, cols) == WIRELOG_OK);
                }
                if (!empty) CHECK(wirelog_session_insert(s, "a", facts, 1,
                        cols) == WIRELOG_OK);                                                /* Duplicate row. */
                free(facts); facts = NULL;
                CHECK(wirelog_program_get_facts(p, "b", &facts, &rows,
                    &cols) == 0 && rows == 3 && cols == 2);
                if (empty) CHECK(wirelog_session_remove(s, "b", facts, rows,
                        cols) == WIRELOG_OK);
                free(facts);
                if (order) {
                    CHECK(wirelog_session_seal_input(s,
                        order == 1 ? "a" : "b") == WIRELOG_OK);
                    CHECK(wirelog_session_seal_join_input(s,
                        order == 1 ? "b" : "a") == WIRELOG_OK);
                }
                if (order == 2) CHECK(wirelog_session_step(s) == WIRELOG_OK);
                for (unsigned repeat = 0; repeat < 2; repeat++) {
                    reference_public_events_t e = expected;
                    CHECK(wirelog_session_snapshot_typed(s,
                        reference_public_observe, &e) == WIRELOG_OK);
                    CHECK(e.bits[0] ==
                        (empty ? 0u : projection_predicate_masks[filtered]) &&
                        e.bits[1] == (empty ? 0u : 7u));
                }
                wirelog_session_destroy(s); wirelog_program_free(p);
                CHECK(live_files() == 0);
            }
}

static void inline_public_joins(void)
{
    const char *text =
        ".decl a(p:pair(symbol,int64) inline)\n.decl b(p:triple(symbol,symbol,int64) inline)\n"
        ".decl c(p:pair(symbol,int64) inline)\n"
        ".decl result(k:symbol,x:int64,next:symbol,z:int64)\n"
        ".decl self(k:symbol,x:int64,p:symbol,y:int64)\n"
        ".decl scan(k:symbol,x:int64)\n"
        ".decl second(k:symbol,x:int64,next:symbol,y:int64)\n"
        "a(\"\",0).\na(\"alpha\",1).\na(\"한글\",2).\n"
        "b(\"\",\"alpha\",10).\nb(\"alpha\",\"한글\",11).\nb(\"한글\",\"\",12).\n"
        "c(\"alpha\",100).\nc(\"한글\",101).\nc(\"\",102).\n"
        "result(k,x,n,z+0) :- a(pair(k,x)),b(triple(k,n,y)),c(pair(n,z)).\n"
        "self(k,x,k,y+0) :- a(pair(k,x)),a(pair(k,y)).\n"
        "scan(k,x) :- a(pair(k,x)).\n"
        "second(k,x,n,y+0) :- a(pair(k,x)),b(triple(k,n,y)).\n";
    for (unsigned direction = 0; direction < 5; direction++) {
        wirelog_program_t *p = parse(text); wirelog_session_t *s = NULL;
        wirelog_session_options_v1_t o = configured();
        CHECK(wirelog_session_create_with_options(p, WIRELOG_BACKEND_DEFAULT,
            1, &o, &s) == WIRELOG_OK);
        int64_t *facts = NULL; uint32_t rows = 0, cols = 0;
        CHECK(wirelog_program_get_facts(p, "a", &facts, &rows,
            &cols) == 0 && rows == 3 && cols == 2);
        reference_chain_events_t expected = {0};
        for (unsigned i = 0; i < rows;
            i++) expected.ids[facts[i*2+1]] = (uint64_t)facts[i*2];
        free(facts);
        const char *names[] = {"a", "b", "c"};
        if (direction == 1) CHECK(wirelog_session_seal_input(s,
                "a") == WIRELOG_OK);
        if (direction == 2) CHECK(wirelog_session_seal_input(s,
                "c") == WIRELOG_OK);
        if (direction >= 3) for (unsigned i = 0; i < 3; i++)
                CHECK(wirelog_session_seal_input(s,
                    names[direction == 3 ? i : 2-i]) == WIRELOG_OK);
        for (unsigned repeat = 0; repeat < 2; repeat++) {
            reference_chain_events_t e = expected;
            CHECK(wirelog_session_snapshot_typed(s, reference_chain_observe,
                &e) == WIRELOG_OK);
            for (unsigned i = 0; i < 4; i++) CHECK(e.seen[i] == 7);
            CHECK(wirelog_session_step(s) == WIRELOG_OK);
        }
        wirelog_session_destroy(s); wirelog_program_free(p);
        CHECK(live_files() == 0);
    }
}

int main(void)
{
    /* Issue #2287: a session created with a nonzero WIRELOG_JOIN_BATCH_BYTES
     * refuses wirelog_session_seal_join_input, so clear the inherited knob
     * and its _STRICT companion before the first session exists. */
    environment("WIRELOG_JOIN_BATCH_BYTES", NULL);
    environment("WIRELOG_JOIN_BATCH_STRICT", NULL);
    char *budget = copy_environment("WIRELOG_MEMORY_BUDGET");
    environment("WIRELOG_MEMORY_BUDGET", "268435456");
#ifdef _WIN32
    snprintf(directory, sizeof(directory), "%s/wirelog-public-spill-%lu",
        test_tmpdir(), (unsigned long)GetCurrentProcessId());
    CHECK(_mkdir(directory) == 0);
#else
    snprintf(directory, sizeof(directory), "%s/wirelog-public-spill-XXXXXX",
        test_tmpdir());
    CHECK(mkdtemp(directory));
#endif
    inline_public_projections(); inline_public_joins();
    reference_public_joins();
    reference_public_projections();
    join_directions(); join_chains();
    descriptors(); lifecycle(); refusals(); typed_projection(); inline_facts();
    sealed_sets(); standalone_reads(); output_floor(); mixed_projection();
    unsupported_filters();
    join_batch_knob();
#ifdef _WIN32
    CHECK(_rmdir(directory) == 0);
#else
    CHECK(rmdir(directory) == 0);
#endif
    environment("WIRELOG_MEMORY_BUDGET", budget); free(budget);
    return 0;
}
