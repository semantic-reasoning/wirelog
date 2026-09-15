/* Focused contract tests for the inactive source access gate (#1492). */

#include "../wirelog/columnar/source_access.h"
#include "wirelog/thread.h"

#include <stdio.h>
#include <string.h>

static int failures;

#define CHECK(condition, message) do { \
            if (!(condition)) { printf("FAIL: %s\n", message); failures++; \
                                return; } \
} while (0)

static void
test_lifecycle(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 }, second = { 0 }, copy;
    wl_columnar_source_access_writer_t writer = { 0 };
    wl_columnar_source_access_gate_init(&gate);
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == 0,
        "reader acquire");
    copy = reader;
    CHECK(wl_columnar_source_access_reader_release(&copy) == EINVAL,
        "copied reader release rejected");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 1
        && reader.owner == &gate, "failed copy release changed state");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &second) == 0,
        "second reader acquire");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 2,
        "multiple readers share the gate");
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == EBUSY,
        "writer blocked by reader");
    CHECK(wl_columnar_source_access_reader_release(&second) == 0,
        "second reader release");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 1,
        "one reader remains after second release");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "reader release");
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == 0,
        "writer acquire");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == EBUSY,
        "reader blocked by writer");
    CHECK(wl_columnar_source_access_writer_acquire(&gate, &writer) == EINVAL,
        "writer token reuse rejected");
    CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
        "writer release");
}

struct cross_thread_arg {
    wl_columnar_source_access_reader_t *reader;
    int result;
};

static void *
cross_thread_release(void *opaque)
{
    struct cross_thread_arg *arg = opaque;
    arg->result = wl_columnar_source_access_reader_release(arg->reader);
    return NULL;
}

static void
test_invalid_and_cross_thread(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 }, copy;
    struct cross_thread_arg arg = { &reader, 0 };
    wl_thread_t thread;
    wl_columnar_source_access_gate_init(&gate);
    CHECK(wl_columnar_source_access_reader_acquire(NULL, &reader) == EINVAL,
        "null gate rejected");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, NULL) == EINVAL,
        "null token rejected");
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == 0,
        "reader acquire");
    copy = reader;
    CHECK(wl_columnar_source_access_reader_release(&copy) == EINVAL,
        "copy rejected");
    CHECK(wl_thread_create(&thread, cross_thread_release, &arg) == 0,
        "cross-thread setup");
    CHECK(wl_thread_join(&thread) == 0 && arg.result == EINVAL,
        "cross-thread release rejected");
    CHECK(reader.owner == &gate
        && atomic_load_explicit(&gate.state, memory_order_relaxed) == 1,
        "cross-thread failure preserved state");
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0,
        "owning thread release");
    CHECK(wl_columnar_source_access_reader_release(&reader) == EINVAL,
        "duplicate release rejected");

    memset(&reader, 0, sizeof(reader));
    arg.result = EINVAL;
    CHECK(wl_columnar_source_access_reader_acquire_transferable(&gate,
        &reader) == 0, "transferable reader acquire");
    copy = reader;
    CHECK(wl_columnar_source_access_reader_release(&copy) == EINVAL,
        "copied transferable reader release rejected");
    CHECK(wl_thread_create(&thread, cross_thread_release, &arg) == 0,
        "transferable cross-thread setup");
    CHECK(wl_thread_join(&thread) == 0 && arg.result == 0,
        "transferable cross-thread release");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed) == 0
        && reader.owner == NULL, "transferable release balanced gate");
}

static void
test_reader_overflow(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_source_access_gate_init(&gate);
    atomic_store_explicit(&gate.state,
        WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u, memory_order_relaxed);
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader)
        == EOVERFLOW, "reader overflow rejected");
    CHECK(atomic_load_explicit(&gate.state, memory_order_relaxed)
        == WL_COLUMNAR_SOURCE_ACCESS_WRITER - 1u && reader.owner == NULL,
        "overflow preserved state");
    atomic_store_explicit(&gate.state, WL_COLUMNAR_SOURCE_ACCESS_WRITER,
        memory_order_relaxed);
    CHECK(wl_columnar_source_access_reader_acquire(&gate, &reader) == EBUSY,
        "writer sentinel remains exclusive");
}

struct stress_arg {
    wl_columnar_source_access_gate_t *gate;
    unsigned iterations;
    unsigned successes;
};

static void *
reader_stress(void *opaque)
{
    struct stress_arg *arg = opaque;
    for (unsigned i = 0; i < arg->iterations; i++) {
        wl_columnar_source_access_reader_t token = { 0 };
        int rc = wl_columnar_source_access_reader_acquire(arg->gate, &token);
        if (rc == 0) {
            arg->successes++;
            if (wl_columnar_source_access_reader_release(&token) != 0)
                return (void *)1;
        } else if (rc != EBUSY && rc != EOVERFLOW)
            return (void *)1;
    }
    return NULL;
}

static void
test_concurrent_readers(void)
{
    enum { THREADS = 4 };
    wl_columnar_source_access_gate_t gate = { 0 };
    struct stress_arg args[THREADS];
    wl_thread_t threads[THREADS];
    wl_columnar_source_access_gate_init(&gate);
    unsigned created = 0;
    for (unsigned i = 0; i < THREADS; i++) {
        args[i] = (struct stress_arg){ &gate, 1000, 0 };
        if (wl_thread_create(&threads[i], reader_stress, &args[i]) != 0)
            break;
        created++;
    }
    int joins_ok = 1;
    for (unsigned i = 0; i < created; i++)
        if (wl_thread_join(&threads[i]) != 0)
            joins_ok = 0;
    CHECK(created == THREADS && joins_ok, "reader stress lifecycle");
    unsigned total = 0;
    for (unsigned i = 0; i < THREADS; i++)
        total += args[i].successes;
    CHECK(total > 0 && atomic_load_explicit(&gate.state, memory_order_relaxed)
        == 0, "reader stress balanced");
}

enum publication_phase {
    PUBLICATION_START,
    PUBLICATION_WRITER_HELD,
    PUBLICATION_READER_BLOCKED,
    PUBLICATION_DONE,
    PUBLICATION_FAILED,
};

enum { PUBLICATION_WAIT_LIMIT = 1000000u };

struct publication_payload {
    uint64_t first;
    uint64_t second;
    uint64_t third;
};

struct publication_arg {
    wl_columnar_source_access_gate_t *gate;
    atomic_int phase;
    struct publication_payload payload;
    atomic_int reader_busy;
    atomic_int writer_success;
    atomic_int reader_success;
};

static void
publication_fail(struct publication_arg *arg)
{
    atomic_store_explicit(&arg->phase, PUBLICATION_FAILED,
        memory_order_release);
}

static void *
publication_writer(void *opaque)
{
    struct publication_arg *arg = opaque;
    wl_columnar_source_access_writer_t token = { 0 };
    if (wl_columnar_source_access_writer_acquire(arg->gate, &token) != 0) {
        publication_fail(arg);
        return NULL;
    }

    atomic_store_explicit(&arg->writer_success, 1, memory_order_relaxed);
    atomic_store_explicit(&arg->phase, PUBLICATION_WRITER_HELD,
        memory_order_release);
    unsigned waited = 0;
    while (atomic_load_explicit(&arg->phase, memory_order_acquire)
        != PUBLICATION_READER_BLOCKED && waited++ < PUBLICATION_WAIT_LIMIT) {
        if (atomic_load_explicit(&arg->phase, memory_order_acquire)
            == PUBLICATION_FAILED) {
            (void)wl_columnar_source_access_writer_release(&token);
            return NULL;
        }
    }
    if (atomic_load_explicit(&arg->phase, memory_order_acquire)
        != PUBLICATION_READER_BLOCKED) {
        (void)wl_columnar_source_access_writer_release(&token);
        publication_fail(arg);
        return NULL;
    }

    /* The reader has already observed EBUSY.  Write only after that
     * coordination so the gate's release/acquire edge is the publication
     * being tested below, not a test mutex or control flag. */
    arg->payload = (struct publication_payload){
        UINT64_C(0x1122334455667788),
        UINT64_C(0x99aabbccddeeff00),
        UINT64_C(0x13579bdf2468ace0),
    };
    if (wl_columnar_source_access_writer_release(&token) != 0) {
        publication_fail(arg);
        return NULL;
    }
    return NULL;
}

static void *
publication_reader(void *opaque)
{
    struct publication_arg *arg = opaque;
    wl_columnar_source_access_reader_t token = { 0 };
    unsigned waited = 0;
    while (atomic_load_explicit(&arg->phase, memory_order_acquire)
        != PUBLICATION_WRITER_HELD && waited++ < PUBLICATION_WAIT_LIMIT) {
        if (atomic_load_explicit(&arg->phase, memory_order_acquire)
            == PUBLICATION_FAILED)
            return NULL;
    }
    if (atomic_load_explicit(&arg->phase, memory_order_acquire)
        != PUBLICATION_WRITER_HELD) {
        publication_fail(arg);
        return NULL;
    }

    if (wl_columnar_source_access_reader_acquire(arg->gate, &token)
        != EBUSY) {
        if (token.owner != NULL)
            (void)wl_columnar_source_access_reader_release(&token);
        publication_fail(arg);
        return NULL;
    }
    atomic_store_explicit(&arg->reader_busy, 1, memory_order_relaxed);
    atomic_store_explicit(&arg->phase, PUBLICATION_READER_BLOCKED,
        memory_order_release);

    for (waited = 0; waited < PUBLICATION_WAIT_LIMIT; waited++) {
        int rc = wl_columnar_source_access_reader_acquire(arg->gate, &token);
        if (rc == 0)
            break;
        if (rc != EBUSY) {
            publication_fail(arg);
            return NULL;
        }
        if (atomic_load_explicit(&arg->phase, memory_order_acquire)
            == PUBLICATION_FAILED)
            return NULL;
    }
    if (token.owner == NULL) {
        publication_fail(arg);
        return NULL;
    }
    if (arg->payload.first != UINT64_C(0x1122334455667788)
        || arg->payload.second != UINT64_C(0x99aabbccddeeff00)
        || arg->payload.third != UINT64_C(0x13579bdf2468ace0)) {
        (void)wl_columnar_source_access_reader_release(&token);
        publication_fail(arg);
        return NULL;
    }
    if (wl_columnar_source_access_reader_release(&token) != 0) {
        publication_fail(arg);
        return NULL;
    }
    atomic_store_explicit(&arg->reader_success, 1, memory_order_relaxed);
    atomic_store_explicit(&arg->phase, PUBLICATION_DONE,
        memory_order_release);
    return NULL;
}

static void
test_writer_publication(void)
{
    wl_columnar_source_access_gate_t gate = { 0 };
    struct publication_arg arg = { .gate = &gate };
    wl_thread_t writer, reader;
    wl_columnar_source_access_gate_init(&gate);
    atomic_init(&arg.phase, PUBLICATION_START);
    atomic_init(&arg.reader_busy, 0);
    atomic_init(&arg.writer_success, 0);
    atomic_init(&arg.reader_success, 0);
    int writer_created = wl_thread_create(&writer, publication_writer, &arg);
    int reader_created = writer_created == 0
        ? wl_thread_create(&reader, publication_reader, &arg) : -1;
    if (writer_created != 0 || reader_created != 0) {
        publication_fail(&arg);
        if (writer_created == 0)
            (void)wl_thread_join(&writer);
        if (reader_created == 0)
            (void)wl_thread_join(&reader);
        CHECK(0, "publication thread create");
    }
    int writer_joined = wl_thread_join(&writer) == 0;
    int reader_joined = wl_thread_join(&reader) == 0;
    int publication_ok = writer_joined && reader_joined
        && atomic_load_explicit(&arg.phase, memory_order_acquire)
        == PUBLICATION_DONE
        && atomic_load_explicit(&arg.reader_busy, memory_order_relaxed)
        && atomic_load_explicit(&arg.writer_success, memory_order_relaxed)
        && atomic_load_explicit(&arg.reader_success, memory_order_relaxed)
        && atomic_load_explicit(&gate.state, memory_order_relaxed) == 0;
    CHECK(publication_ok, "writer publication ordering and balance");
}

int
main(void)
{
    printf("Source access gate tests (#1492)\n");
    test_lifecycle();
    test_invalid_and_cross_thread();
    test_reader_overflow();
    test_concurrent_readers();
    test_writer_publication();
    printf("%s\n", failures == 0 ? "PASS" : "FAIL");
    return failures == 0 ? 0 : 1;
}
