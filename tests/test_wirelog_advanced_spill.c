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
        ".decl b(k:int64,v:int64)\n.decl result(k:int64,v:int64)\nresult(k,v) :- b(k,v).\n";
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

int main(void)
{
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
    descriptors(); lifecycle(); refusals(); typed_projection(); inline_facts();
    output_floor();
#ifdef _WIN32
    CHECK(_rmdir(directory) == 0);
#else
    CHECK(rmdir(directory) == 0);
#endif
    environment("WIRELOG_MEMORY_BUDGET", budget); free(budget);
    return 0;
}
