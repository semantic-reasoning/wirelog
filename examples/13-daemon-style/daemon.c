/*
 * daemon.c - Example 13: Daemon-style session rotation
 *
 * Demonstrates a long-running wirelog_easy caller that preserves persistent EDB
 * facts across session rotations.  The example uses a public side compound
 * declaration for event metadata and rotates when the compound arena reports
 * saturation through the installed public API.
 */

#if !defined(_WIN32)
#  define _POSIX_C_SOURCE 200809L
#endif

#include "wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(__linux__) || defined(__APPLE__)
#  include <unistd.h>
#elif defined(_WIN32)
#  include <windows.h>
#endif

static const char *DAEMON_SRC
    = ".decl event(id: int64, tenant: symbol, risk: int64, "
    "payload: metadata/4 side)\n"
    ".decl pulse(id: int64)\n"
    ".decl hot_event(id: int64, tenant: symbol)\n"
    "pulse(ID) :- event(ID, _, _, _), ID < 0.\n"
    "pulse(ID) :- pulse(ID).\n"
    "hot_event(ID, Tenant) :- event(ID, Tenant, Risk, _), Risk > 80.\n";

typedef struct {
    int64_t tenant_a;
    int64_t tenant_b;
    int64_t info;
    int64_t warn;
    int64_t host_a;
    int64_t host_b;
} symbol_ids_t;

typedef struct {
    uint64_t id;
    bool tenant_a;
    bool hot;
    bool host_a;
} event_record_t;

typedef struct {
    event_record_t *records;
    size_t count;
    size_t cap;
} edb_store_t;

typedef struct {
    uint64_t hot_deltas;
    uint64_t max_hot_id_seen;
    bool suppress_deltas;
} delta_stats_t;

typedef struct {
    uint64_t max_events;
    uint64_t max_seconds;
    uint64_t log_every;
    uint64_t sleep_ms;
} daemon_opts_t;

typedef enum {
    INSERT_OK = 0,
    INSERT_SATURATED = 1,
    INSERT_FAILED = 2,
} insert_status_t;

typedef struct {
#if defined(_WIN32)
    LARGE_INTEGER counter;
#else
    struct timespec ts;
#endif
} monotonic_time_t;

static void
on_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    delta_stats_t *stats = (delta_stats_t *)user_data;
    if (stats && stats->suppress_deltas)
        return;
    if (stats && diff > 0 && strcmp(relation, "hot_event") == 0 && ncols > 0
        && row[0] > 0 && (uint64_t)row[0] > stats->max_hot_id_seen) {
        stats->max_hot_id_seen = (uint64_t)row[0];
        stats->hot_deltas++;
    }
}

static int
parse_u64(const char *s, uint64_t *out)
{
    char *endp = NULL;
    unsigned long long v;
    if (!s || !out)
        return -1;
    v = strtoull(s, &endp, 10);
    if (endp == s || *endp != '\0' || v == 0)
        return -1;
    *out = (uint64_t)v;
    return 0;
}

static int
parse_args(int argc, char **argv, daemon_opts_t *opts)
{
    opts->max_events = 512;
    opts->max_seconds = 0;
    opts->log_every = 64;
    opts->sleep_ms = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--events") == 0 && i + 1 < argc) {
            if (parse_u64(argv[++i], &opts->max_events) != 0)
                return -1;
        } else if (strcmp(argv[i], "--seconds") == 0 && i + 1 < argc) {
            if (parse_u64(argv[++i], &opts->max_seconds) != 0)
                return -1;
        } else if (strcmp(argv[i], "--log-every") == 0 && i + 1 < argc) {
            if (parse_u64(argv[++i], &opts->log_every) != 0)
                return -1;
        } else if (strcmp(argv[i], "--sleep-ms") == 0 && i + 1 < argc) {
            if (parse_u64(argv[++i], &opts->sleep_ms) != 0)
                return -1;
        } else {
            return -1;
        }
    }
    return 0;
}

static void
usage(const char *argv0)
{
    fprintf(stderr,
        "usage: %s [--events N] [--seconds N] [--log-every N] "
        "[--sleep-ms N]\n",
        argv0);
}

static uint64_t
rss_kb(void)
{
#if defined(__linux__)
    FILE *f = fopen("/proc/self/statm", "r");
    unsigned long pages = 0;
    long page_kb = 0;
    if (!f)
        return 0;
    if (fscanf(f, "%*s %lu", &pages) != 1) {
        fclose(f);
        return 0;
    }
    fclose(f);
    page_kb = sysconf(_SC_PAGESIZE) / 1024;
    if (page_kb <= 0)
        return 0;
    return (uint64_t)pages * (uint64_t)page_kb;
#else
    return 0;
#endif
}

static double
monotonic_elapsed_seconds(const monotonic_time_t *start)
{
#if defined(_WIN32)
    LARGE_INTEGER now;
    LARGE_INTEGER freq;
    QueryPerformanceCounter(&now);
    QueryPerformanceFrequency(&freq);
    return (double)(now.QuadPart - start->counter.QuadPart)
           / (double)freq.QuadPart;
#else
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
        return 0.0;
    return (double)(now.tv_sec - start->ts.tv_sec)
           + (double)(now.tv_nsec - start->ts.tv_nsec) / 1000000000.0;
#endif
}

static int
monotonic_now(monotonic_time_t *out)
{
#if defined(_WIN32)
    return QueryPerformanceCounter(&out->counter) ? 0 : -1;
#else
    return clock_gettime(CLOCK_MONOTONIC, &out->ts) == 0 ? 0 : -1;
#endif
}

static void
sleep_millis(uint64_t ms)
{
#if defined(__linux__) || defined(__APPLE__)
    if (ms > 0) {
        struct timespec req;
        req.tv_sec = (time_t)(ms / 1000u);
        req.tv_nsec = (long)(ms % 1000u) * 1000000L;
        (void)nanosleep(&req, NULL);
    }
#elif defined(_WIN32)
    if (ms > 0)
        Sleep((DWORD)ms);
#else
    (void)ms;
#endif
}

static int
edb_push(edb_store_t *edb, const event_record_t *record)
{
    event_record_t *next;
    size_t next_cap;
    if (edb->count == edb->cap) {
        next_cap = edb->cap ? edb->cap * 2u : 128u;
        next
            = (event_record_t *)realloc(edb->records, next_cap * sizeof(*next));
        if (!next)
            return -1;
        edb->records = next;
        edb->cap = next_cap;
    }
    edb->records[edb->count++] = *record;
    return 0;
}

static void
edb_free(edb_store_t *edb)
{
    free(edb->records);
    edb->records = NULL;
    edb->count = 0;
    edb->cap = 0;
}

static int
intern_symbols(wirelog_easy_session_t *s, symbol_ids_t *ids)
{
    ids->tenant_a = wirelog_easy_intern(s, "tenant-a");
    ids->tenant_b = wirelog_easy_intern(s, "tenant-b");
    ids->info = wirelog_easy_intern(s, "info");
    ids->warn = wirelog_easy_intern(s, "warn");
    ids->host_a = wirelog_easy_intern(s, "edge-01");
    ids->host_b = wirelog_easy_intern(s, "edge-02");
    return (ids->tenant_a < 0 || ids->tenant_b < 0 || ids->info < 0
           || ids->warn < 0 || ids->host_a < 0 || ids->host_b < 0)
        ? -1
        : 0;
}

static int
enable_delta_cb(wirelog_easy_session_t *s, delta_stats_t *stats)
{
    return wirelog_easy_set_delta_cb(s, on_delta, stats) == WIRELOG_OK ? 0 : -1;
}

static int
open_session(wirelog_easy_session_t **out, symbol_ids_t *ids,
    delta_stats_t *stats,
    bool emit_live_deltas)
{
    wirelog_easy_session_t *s = NULL;
    if (wirelog_easy_open(DAEMON_SRC, &s) != WIRELOG_OK)
        return -1;
    if (intern_symbols(s, ids) != 0) {
        wirelog_easy_close(s);
        return -1;
    }
    if (emit_live_deltas && enable_delta_cb(s, stats) != 0) {
        wirelog_easy_close(s);
        return -1;
    }
    *out = s;
    return 0;
}

static insert_status_t
insert_event(wirelog_easy_session_t *s, const symbol_ids_t *ids,
    const event_record_t *record)
{
    const int64_t level = record->hot ? ids->warn : ids->info;
    const int64_t ts = (int64_t)(1700000000u + record->id);
    const int64_t host = record->host_a ? ids->host_a : ids->host_b;
    const int64_t risk = record->hot ? 95 : 10;
    wirelog_compound_arg_t payload_args[4] = {
        { WIRELOG_TYPE_STRING, level },
        { WIRELOG_TYPE_INT64, ts },
        { WIRELOG_TYPE_STRING, host },
        { WIRELOG_TYPE_INT64, risk },
    };
    uint64_t payload = WIRELOG_COMPOUND_HANDLE_NULL;
    wirelog_error_t rc = wirelog_easy_make_compound(s, "metadata", 4,
            payload_args, &payload);
    if (rc == WIRELOG_ERR_COMPOUND_SATURATED)
        return INSERT_SATURATED;
    if (rc != WIRELOG_OK || payload == WIRELOG_COMPOUND_HANDLE_NULL)
        return INSERT_FAILED;

    int64_t row[4];
    row[0] = (int64_t)record->id;
    row[1] = record->tenant_a ? ids->tenant_a : ids->tenant_b;
    row[2] = risk;
    row[3] = (int64_t)payload;
    return wirelog_easy_insert(s, "event", row, 4) == WIRELOG_OK ? INSERT_OK
                                                            : INSERT_FAILED;
}

static int
replay_edb(wirelog_easy_session_t *s, const symbol_ids_t *ids,
    const edb_store_t *edb)
{
    for (size_t i = 0; i < edb->count; i++) {
        if (insert_event(s, ids, &edb->records[i]) != INSERT_OK)
            return -1;
    }
    return 0;
}

static int
rotate_session(wirelog_easy_session_t **session_io, symbol_ids_t *ids,
    const edb_store_t *edb, delta_stats_t *stats, uint64_t *rotations)
{
    wirelog_easy_session_t *fresh = NULL;
    if (open_session(&fresh, ids, stats, false) != 0)
        return -1;
    if (replay_edb(fresh, ids, edb) != 0) {
        wirelog_easy_close(fresh);
        return -1;
    }
    if (enable_delta_cb(fresh, stats) != 0) {
        wirelog_easy_close(fresh);
        return -1;
    }
    wirelog_easy_close(*session_io);
    *session_io = fresh;
    (*rotations)++;
    return 0;
}

static event_record_t
make_event_record(uint64_t event_id)
{
    event_record_t record;
    record.id = event_id;
    record.tenant_a = (event_id % 2u) != 0u;
    record.hot = (event_id % 5u) == 0u;
    record.host_a = (event_id % 2u) != 0u;
    return record;
}

int
main(int argc, char **argv)
{
    daemon_opts_t opts;
    wirelog_easy_session_t *session = NULL;
    symbol_ids_t ids;
    delta_stats_t stats = { 0 };
    edb_store_t edb = { 0 };
    uint64_t rotations = 0;
    uint64_t saturations = 0;
    uint64_t event_id = 0;
    monotonic_time_t start;

    if (parse_args(argc, argv, &opts) != 0) {
        usage(argv[0]);
        return 2;
    }
    if (monotonic_now(&start) != 0) {
        fprintf(stderr, "[daemon] failed to read monotonic clock\n");
        return 1;
    }
    if (open_session(&session, &ids, &stats, true) != 0) {
        fprintf(stderr, "[daemon] failed to open wirelog session\n");
        return 1;
    }

    while (event_id < opts.max_events) {
        if (opts.max_seconds != 0
            && monotonic_elapsed_seconds(&start) >= (double)opts.max_seconds)
            break;
        event_id++;
        event_record_t record = make_event_record(event_id);

        insert_status_t insert_rc = insert_event(session, &ids, &record);
        if (insert_rc == INSERT_SATURATED) {
            saturations++;
            fprintf(stderr,
                "[daemon] rotation request: compound arena saturated "
                "event=%" PRIu64 "\n",
                event_id);
            if (rotate_session(&session, &ids, &edb, &stats, &rotations)
                != 0) {
                fprintf(stderr, "[daemon] rotation failed\n");
                wirelog_easy_close(session);
                edb_free(&edb);
                return 1;
            }
            insert_rc = insert_event(session, &ids, &record);
        }
        if (insert_rc != INSERT_OK) {
            fprintf(stderr, "[daemon] insert failed at event=%" PRIu64 "\n",
                event_id);
            wirelog_easy_close(session);
            edb_free(&edb);
            return 1;
        }
        if (wirelog_easy_step(session) != WIRELOG_OK) {
            fprintf(stderr, "[daemon] step failed at event=%" PRIu64 "\n",
                event_id);
            wirelog_easy_close(session);
            edb_free(&edb);
            return 1;
        }
        if (edb_push(&edb, &record) != 0) {
            fprintf(stderr, "[daemon] out of memory while recording EDB\n");
            wirelog_easy_close(session);
            edb_free(&edb);
            return 1;
        }

        if (opts.log_every != 0 && (event_id % opts.log_every) == 0) {
            fprintf(stderr,
                "[daemon] t=%.2fs events=%" PRIu64 " edb=%zu hot=%" PRIu64
                " rotations=%" PRIu64 " saturations=%" PRIu64
                " rss_kb=%" PRIu64 "\n",
                monotonic_elapsed_seconds(&start), event_id, edb.count,
                stats.hot_deltas,
                rotations, saturations, rss_kb());
        }

        sleep_millis(opts.sleep_ms);
    }

    fprintf(stderr,
        "[daemon] summary: events=%" PRIu64 " edb=%zu hot=%" PRIu64
        " rotations=%" PRIu64 " saturations=%" PRIu64 " rss_kb=%" PRIu64
        "\n",
        event_id, edb.count, stats.hot_deltas, rotations, saturations,
        rss_kb());

    wirelog_easy_close(session);
    edb_free(&edb);
    return rotations > 0 ? 0 : 1;
}
